package tuya

// Rollover hides the Tuya relay's ~600s session cap. The relay force-closes
// the media DataChannel at ~600 seconds regardless of traffic (verified:
// application-level keepalives over both the DataChannel and MQTT do not defer
// it), and the resulting teardown runs through a DTLS timeout, so a plain
// reconnect leaves tens of seconds of dead air. To shrink that, dial a standby
// session shortly before the cap and splice it in as the active transport.
//
// The camera does NOT fan one encoder out to a late-joining session: when a
// second session starts receiving it switches its single uplink to the newest
// one and the first session's media stops (verified: the primary froze within
// ~2s of the standby going live, and the two sessions never shared enough
// concurrent keyframes to correlate a timestamp offset). So the splice aligns
// by wall-clock time, which is universal: the standby's first keyframe is
// rebased to continue from the primary's last packet advanced by the elapsed
// wall gap. The handover gap is just the time for the standby to emit its first
// IDR after it starts receiving (about one GOP), instead of the DTLS-timeout
// reconnect.
//
// Object model: the primary Client is a stable shell the producer holds.
// Start blocks on svcDone, not on any transport, so retiring a connection
// during a splice does not wake the producer or detach consumers. The shell
// owns the receivers consumers subscribe to; whichever transport is active
// (activeConn) writes into them through a shared handler that applies the
// transport's timestamp/sequence shift (zero for the shell itself). A standby
// is a full Dial that is never Start()ed; its video handler waits for the
// first keyframe, then the shell splices it in and swaps its handlers to
// write-through. Every transport routes death through parent.transportClosed
// under spliceMu: a pending standby dying clears the slot, a retired transport
// dying is ignored, and only the active transport ending the session stops the
// service. Opt-in via ?rollover=1, HEVC/DataChannel streams only.

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/pion/rtp"
)

// DebugLog receives rollover trace messages when set. The pkg/ tree does not
// import the app logger, so internal/tuya wires one in at startup.
var DebugLog func(format string, args ...any)

func debugf(format string, args ...any) {
	if fn := DebugLog; fn != nil {
		fn(format, args...)
		return
	}
	fmt.Fprintf(os.Stderr, "[tuya] "+format+"\n", args...)
}

const (
	// dial a standby once the active session is this old: the relay cap is
	// ~600s, so this leaves margin for the dial and first keyframe, and room
	// for one retry when an attempt hangs and is abandoned
	rolloverDialAge = 480 * time.Second
	// hard bound on one standby dial. The dial pauses the primary's fan-out,
	// so a stuck dial must abort well inside the splice-wait window.
	rolloverDialTimeout = 35 * time.Second
	// after a standby is ready, how long to wait for its first keyframe
	// before giving up on it; also how long the watchdog tolerates the
	// primary pausing while the camera switches uplinks
	rolloverSpliceWait = 25 * time.Second
	// relay session cap measured in the field
	rolloverRelayCap = 600 * time.Second
	// do not start a rollover this close to the cap: not enough time to
	// dial and splice, let the cap fire and the producer redial
	rolloverTooLate = 40 * time.Second
	// one frame at 90 kHz / 30 fps, the minimum forward step at a splice
	rolloverMinStep = uint32(3000)
)

// isH265KeyframeStart reports whether an RTP packet begins an IDR/CRA access
// unit. Frames on these cameras start with VPS (32) or an aggregation packet
// (48) carrying it; 19/20/21 are IDR/CRA slice types, 49 is a fragmentation
// unit whose inner type is in the third payload byte.
func isH265KeyframeStart(payload []byte) bool {
	if len(payload) < 3 {
		return false
	}
	switch t := (payload[0] >> 1) & 0x3F; t {
	case 19, 20, 21, 32, 33:
		return true
	case 48:
		switch (payload[2] >> 1) & 0x3F {
		case 19, 20, 21, 32, 33:
			return true
		}
	case 49:
		if payload[2]&0x80 != 0 { // S bit: first fragment of the NAL
			switch payload[2] & 0x1F {
			case 19, 20, 21:
				return true
			}
		}
	}
	return false
}

// splicer tracks the primary write edge on the shell timeline. The counters
// persist across rollovers: each splice advances them and the next standby
// aligns to the current edge.
type splicer struct {
	mu       sync.Mutex
	lastPts  uint32 // last video packet written to the shell receiver
	lastPseq uint16
	lastAts  uint32    // last audio packet written to the shell receiver
	lastWall time.Time // wall time of the last video packet
}

// observePrimary records a video packet as written to the shell.
func (s *splicer) observePrimary(p *rtp.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPts, s.lastPseq, s.lastWall = p.Timestamp, p.SequenceNumber, time.Now()
}

// observePrimaryAudio records an audio packet as written to the shell.
func (s *splicer) observePrimaryAudio(p *rtp.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAts = p.Timestamp
}

// edge returns the primary write edge.
func (s *splicer) edge() (lastPts uint32, lastPseq uint16, lastAts uint32, lastWall time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPts, s.lastPseq, s.lastAts, s.lastWall
}

// alignShift computes the timestamp shift that places a standby keyframe
// (raw ts kfTS, seen at now) on the primary timeline, continuing from lastPts
// advanced by the wall-clock gap since the primary's last packet. The result
// always moves the timeline forward, so consumers never see a timestamp go
// backward.
func alignShift(lastPts uint32, lastWall time.Time, kfTS uint32, now time.Time) uint32 {
	var advance uint32
	if !lastWall.IsZero() {
		if d := now.Sub(lastWall); d > 0 {
			advance = uint32(int64(90000) * int64(d) / int64(time.Second))
		}
	}
	target := lastPts + advance
	if int32(target-lastPts) <= 0 {
		target = lastPts + rolloverMinStep
	}
	return target - kfTS
}

// standbyURL strips the rollover flag so a standby never spawns its own chain.
func standbyURL(rawURL string) string {
	u, err := url.Parse(strings.ReplaceAll(rawURL, "#", "%23"))
	if err != nil {
		return rawURL
	}
	q := u.Query()
	q.Del("rollover")
	u.RawQuery = q.Encode()
	return u.String()
}

// writeVideo is the active-transport video handler: rebase into the shell
// timeline and write to the shell receiver. The shell uses it with zero shift.
// writeMu serializes shell writes across transports: a straggler from the
// retired transport, dispatched before its retired flag was set, would
// otherwise interleave with the splice and push non-monotonic timestamps into
// the consumers' muxer.
func (t *Client) writeVideo(p *rtp.Packet) {
	c := t.parent
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if t.retired.Load() {
		return
	}
	p.Timestamp += t.tsShift
	p.SequenceNumber += t.seqShift
	c.video.WriteRTP(p)
	if c.watchdog != nil {
		c.watchdog.feed(p.Timestamp)
	}
	c.sp.observePrimary(p)
}

// audioSink writes a reclocked audio packet to the shell receiver. The
// timestamp is already on the shell timeline (reclockPCM for the shell,
// reclockContinuation for a spliced standby), so no shift is applied here.
func (t *Client) audioSink(p *rtp.Packet) {
	c := t.parent
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if t.retired.Load() {
		return
	}
	c.audio.WriteRTP(p)
	c.sp.observePrimaryAudio(p)
}

// reclockContinuation rebuilds PCM timestamps from payload size, seeded so the
// first packet continues from start on the shell timeline. Audio runs on the
// codec clock (not the 90 kHz video clock), so it cannot reuse the video
// shift; seeding from the shell edge keeps audio monotonic across a splice.
// Non-PCM codecs pass through unchanged.
func reclockContinuation(codec *core.Codec, start uint32, handler core.HandlerFunc) core.HandlerFunc {
	bpf := pcm.BytesPerFrame(codec)
	if bpf == 0 {
		return handler
	}
	var ts uint32
	var init bool
	return func(packet *rtp.Packet) {
		if init {
			ts += uint32(len(packet.Payload) / bpf)
		} else {
			ts, init = start, true
		}
		packet.Timestamp = ts
		handler(packet)
	}
}

// standbyVideo is a pending standby's video handler: wait for the first
// keyframe, then splice the standby in on it. Packets before the first IDR are
// undecodable and dropped.
func (t *Client) standbyVideo(p *rtp.Packet) {
	if t.retired.Load() || !isH265KeyframeStart(p.Payload) {
		return
	}
	t.parent.spliceAtKeyframe(t, p)
}

// spliceAtKeyframe swaps the standby in as the active transport, aligning its
// first keyframe to continue the primary stream by wall-clock time. Runs under
// spliceMu so it cannot race a transport death event. The triggering packet is
// written here; later packets flow through the swapped write-through handler on
// the same DataChannel goroutine, so the stream stays ordered.
func (c *Client) spliceAtKeyframe(t *Client, p *rtp.Packet) {
	c.spliceMu.Lock()
	if c.standby != t || c.activeConn.Load() == t {
		c.spliceMu.Unlock()
		return
	}
	old := c.activeConn.Load()
	// stop the old transport writing before the new one starts
	if old != nil {
		old.retired.Store(true)
	}

	lastPts, lastPseq, lastAts, lastWall := c.sp.edge()
	now := time.Now()
	t.tsShift = alignShift(lastPts, lastWall, p.Timestamp, now)
	t.seqShift = uint16(int32(lastPseq+1) - int32(p.SequenceNumber))

	// audio continues from the shell edge advanced by the same wall gap, so
	// it stays in step with the rebased video
	var audioHandler core.HandlerFunc
	if t.audioSSRC != nil && c.audio != nil && c.audio.Codec != nil {
		seed := lastAts
		if d := now.Sub(lastWall); d > 0 {
			seed += uint32(int64(c.audio.Codec.ClockRate) * int64(d) / int64(time.Second))
		}
		audioHandler = reclockContinuation(c.audio.Codec, seed, t.audioSink)
	}

	c.standby = nil
	c.activeConn.Store(t)
	t.activeAt = now

	// swap the standby's handlers to write-through for subsequent packets
	t.setHandler(*t.videoSSRC, t.writeVideo)
	if audioHandler != nil {
		t.setHandler(*t.audioSSRC, audioHandler)
	}
	c.spliceMu.Unlock()

	// write the keyframe that triggered the splice, on the shell timeline
	t.writeVideo(p)

	debugf("rollover spliced gap=%s shift=%d relay=%s",
		now.Sub(lastWall).Truncate(time.Millisecond), t.tsShift, t.conn.RemoteAddr)

	if old != nil {
		go func() { _ = old.retire() }()
	}
}

// retire tears down a transport that has been spliced out. When the retired
// transport is a plain standby, Stop is its own (empty) service. When it is
// the shell itself, only the peer connection and API session close: the
// shell's receivers stay open because consumers are attached to them, and
// svcDone stays pending so the producer is not woken mid-splice.
func (t *Client) retire() error {
	t.retired.Store(true)
	if t.parent != nil && t != t.parent {
		return t.Stop()
	}
	t.clearHandlers()
	if t.conn != nil {
		_ = t.conn.Close()
	}
	if t.api != nil {
		t.api.Close()
	}
	return nil
}

// transportClosed routes a transport death. A retired transport (already
// spliced out) is ignored: the splice owns its teardown, and its relay closing
// the DataChannel at the cap must not end the service. A pending standby dying
// just clears the slot. Only the active transport ending the session stops the
// service.
func (c *Client) transportClosed(t *Client, err error) {
	if t.retired.Load() {
		debugf("rollover retired transport closed: %v", err)
		return
	}
	c.spliceMu.Lock()
	switch {
	case c.standby == t:
		c.standby = nil
		c.spliceMu.Unlock()
		debugf("rollover standby closed: %v", err)
		_ = t.Stop()
	case c.activeConn.Load() == t:
		c.spliceMu.Unlock()
		debugf("rollover active transport closed: %v", err)
		c.endService(err)
	default:
		// a standby that was already cleared, or a transport that lost the
		// splice race: tear it down, leave the service alone
		c.spliceMu.Unlock()
		_ = t.Stop()
	}
}

func (c *Client) endService(err error) {
	c.svcStopped.Store(true)
	c.closeMu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.closeMu.Unlock()
	c.svcDone.Done(err)
}

// startRollover arms the shell for splicing: install write-through handlers
// (the shell is its own first transport) and the standby maintenance loop.
func (c *Client) startRollover() {
	c.spliceMu.Lock()
	if c.sp == nil {
		c.sp = &splicer{}
	}
	if c.rolloverStop == nil {
		c.rolloverStop = make(chan struct{})
	}
	c.spliceMu.Unlock()

	c.setHandler(*c.videoSSRC, c.writeVideo)
	if c.audioSSRC != nil && c.audio != nil {
		c.setHandler(*c.audioSSRC, reclockPCM(c.audio.Codec, c.audioSink))
	}
	debugf("rollover armed, dialing at age=%s", rolloverDialAge)
	go c.rolloverLoop()
}

// rolloverLoop dials a standby ahead of the relay cap and drops one that never
// produced a keyframe in time.
func (c *Client) rolloverLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.rolloverStop:
			return
		case <-ticker.C:
		}
		if c.svcStopped.Load() {
			return
		}

		c.spliceMu.Lock()
		active, standby := c.activeConn.Load(), c.standby
		var activeAge, standbyAge time.Duration
		if active != nil {
			activeAge = time.Since(active.activeAt)
		}
		if standby != nil {
			standbyAge = time.Since(standby.dialAt)
		}
		c.spliceMu.Unlock()
		if active == nil {
			return
		}

		if standby != nil {
			if standbyAge > rolloverSpliceWait {
				// never produced a keyframe to splice on; drop it so the
				// watchdog stops being suppressed and a fresh dial can run
				debugf("rollover standby idle %s, dropping", standbyAge.Truncate(time.Second))
				standby.retired.Store(true)
				c.spliceMu.Lock()
				if c.standby == standby {
					c.standby = nil
				}
				c.spliceMu.Unlock()
				go standby.Stop()
			}
			continue
		}

		if activeAge < rolloverDialAge || activeAge > rolloverRelayCap-rolloverTooLate {
			// too early, or too late to dial and splice before the cap
			continue
		}
		if c.dialing.CompareAndSwap(false, true) {
			go c.dialStandby()
		}
	}
}

// dialStandby creates a second session against the same camera. It is fully
// connected (probe done, SSRCs known) but never Start()ed; its video handler
// waits for the first keyframe to splice on.
func (c *Client) dialStandby() {
	defer c.dialing.Store(false)

	c.spliceMu.Lock()
	stop := c.rolloverStop
	c.spliceMu.Unlock()
	select {
	case <-stop:
		return
	default:
	}

	debugf("rollover dialing standby")
	type dialResult struct {
		p core.Producer
		e error
	}
	ch := make(chan dialResult, 1)
	// Dial blocks on camera signalling; a hung attempt leaks this goroutine
	// and its half-built client (no Start, no watchdog, relay cap reaps the
	// session) but frees the shell's primary to resume.
	go func() {
		nc, err := Dial(standbyURL(c.rawURL))
		ch <- dialResult{nc, err}
	}()
	var nc core.Producer
	select {
	case r := <-ch:
		if r.e != nil {
			debugf("rollover standby dial failed: %v", r.e)
			return
		}
		nc = r.p
	case <-time.After(rolloverDialTimeout):
		debugf("rollover standby dial timeout")
		// stop the client if the dial lands later: a live half-session
		// keeps the camera switched away from the primary
		go func() {
			if r := <-ch; r.e == nil && r.p != nil {
				_ = r.p.Stop()
			}
		}()
		return
	case <-c.rolloverStop:
		return
	}

	t := nc.(*Client)
	if !t.isHEVC || t.videoSSRC == nil {
		debugf("rollover standby not HEVC, dropping")
		_ = t.Stop()
		return
	}

	t.parent = c
	t.retired.Store(false)
	t.dialAt = time.Now()

	// Register as the pending standby before installing the splice trigger, so
	// the first keyframe the standby emits splices immediately (a handler set
	// before registration would find c.standby unset and drop that IDR).
	c.spliceMu.Lock()
	select {
	case <-c.rolloverStop:
		c.spliceMu.Unlock()
		_ = t.Stop()
		return
	default:
	}
	if c.standby != nil {
		c.spliceMu.Unlock()
		_ = t.Stop()
		return
	}
	c.standby = t
	c.spliceMu.Unlock()

	// Route the standby's video into the splice trigger; drop its audio until
	// it becomes active (the splice reseeds audio from the shell edge).
	t.setHandler(*t.videoSSRC, t.standbyVideo)
	if t.audioSSRC != nil {
		t.setHandler(*t.audioSSRC, func(*rtp.Packet) {})
	}

	debugf("rollover standby ready relay=%s", t.conn.RemoteAddr)
}
