package tuya

import (
	"errors"
	"sync"
	"time"
)

// errRelayStall closes the client when media arrives persistently slower
// than real time, which means the selected relay leg is degraded. The
// producer reconnect logic re-dials and ICE picks a new relay.
var errRelayStall = errors.New("tuya: relay stall, media slower than realtime")

// mediaWatchdog compares the progress of RTP media time against wall-clock
// time. A degraded Tuya relay leg can keep the DataChannel open but deliver
// only a fraction of the packets, so the stream silently falls behind real
// time (observed: 25 pkt/s instead of 152, playback at 0.16x with the lag
// growing by ~50s every minute). When media time advances slower than
// stallRatio of wall time for stallWindows consecutive checks, onStall is
// called once. The expected reaction is to close the client so the producer
// re-dials and ICE picks a fresh relay.
type mediaWatchdog struct {
	clock        uint64 // RTP clock rate, ticks per media second
	stallRatio   float64
	stallWindows int
	period       time.Duration
	onStall      func()

	mu       sync.Mutex
	media    int64 // accumulated media time in ns
	refWall  time.Time
	lastTS   uint32
	stop     chan struct{}
	stopOnce sync.Once
}

func newMediaWatchdog(clockRate uint32, onStall func()) *mediaWatchdog {
	return &mediaWatchdog{
		clock:        uint64(clockRate),
		stallRatio:   0.5,
		stallWindows: 3,
		period:       20 * time.Second,
		onStall:      onStall,
		stop:         make(chan struct{}),
	}
}

// feed records the RTP timestamp of a received media packet.
func (w *mediaWatchdog) feed(ts uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if w.refWall.IsZero() {
		w.refWall = now
		w.lastTS = ts
		return
	}
	// forward delta across 32-bit wrap, ignore jumps over 60s
	if d := int64(uint32(ts - w.lastTS)); d >= 0 && d < int64(w.clock)*60 {
		w.media += d * int64(time.Second) / int64(w.clock)
	}
	w.lastTS = ts
}

// snapshot returns accumulated media time and wall time since first packet.
func (w *mediaWatchdog) snapshot() (media int64, wall int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.refWall.IsZero() {
		return 0, 0
	}
	return w.media, time.Since(w.refWall).Nanoseconds()
}

func (w *mediaWatchdog) start() {
	go func() {
		t := time.NewTicker(w.period)
		defer t.Stop()

		prevMedia, prevWall := w.snapshot()
		lag := 0
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
				media, wall := w.snapshot()
				dm, dw := media-prevMedia, wall-prevWall
				prevMedia, prevWall = media, wall
				if dw <= 0 {
					continue
				}
				if float64(dm)/float64(dw) < w.stallRatio {
					if lag++; lag >= w.stallWindows {
						w.onStall()
						return
					}
				} else {
					lag = 0
				}
			}
		}
	}()
}

func (w *mediaWatchdog) stopWatchdog() {
	w.stopOnce.Do(func() { close(w.stop) })
}
