package tuya

import (
	"sync/atomic"
	"testing"
	"time"
)

// feedAt pushes RTP timestamps advancing at rate of realtime for d.
func (w *mediaWatchdog) feedAt(t *testing.T, rate float64, d time.Duration) {
	t.Helper()
	const clock = 90000
	tick := 5 * time.Millisecond
	var ts uint64
	done := time.After(d)
	for {
		select {
		case <-done:
			return
		case <-time.After(tick):
			ts += uint64(float64(clock) * tick.Seconds() * rate)
			w.feed(uint32(ts))
		}
	}
}

func TestMediaWatchdog_Stall(t *testing.T) {
	var fired atomic.Bool
	w := newMediaWatchdog(90000, func() { fired.Store(true) })
	w.period = 20 * time.Millisecond
	w.stallWindows = 2
	w.start()
	// media advances at 0.2x realtime, well below the 0.5 stall ratio
	w.feedAt(t, 0.2, 300*time.Millisecond)
	w.stopWatchdog()
	if !fired.Load() {
		t.Fatal("watchdog did not fire on stalled media")
	}
}

func TestMediaWatchdog_Realtime(t *testing.T) {
	var fired atomic.Bool
	w := newMediaWatchdog(90000, func() { fired.Store(true) })
	w.period = 20 * time.Millisecond
	w.stallWindows = 2
	w.start()
	w.feedAt(t, 1.0, 300*time.Millisecond)
	w.stopWatchdog()
	if fired.Load() {
		t.Fatal("watchdog fired on realtime media")
	}
}

func TestMediaWatchdog_Wraparound(t *testing.T) {
	w := newMediaWatchdog(90000, func() {})
	w.feed(0xFFFFFFFE) // first packet sets reference
	w.feed(2)          // wrapped forward by 4 ticks
	want := int64(4) * int64(time.Second) / 90000
	media, _ := w.snapshot()
	if media != want {
		t.Fatalf("media=%d want=%d", media, want)
	}
	// large backward jump must be ignored
	w.feed(0)
	media2, _ := w.snapshot()
	if media2 != want {
		t.Fatalf("backward jump changed media: %d != %d", media2, want)
	}
}
