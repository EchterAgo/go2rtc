package tuya

import (
	"strings"
	"testing"
	"time"
)

func TestIsH265KeyframeStart(t *testing.T) {
	nal := func(tp byte) []byte { return []byte{tp << 1, 0x01, 0x02, 0x03} }
	tests := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{"IDR_W_RADL", nal(19), true},
		{"IDR_N_LP", nal(20), true},
		{"CRA_NUT", nal(21), true},
		{"VPS", nal(32), true},
		{"SPS", nal(33), true},
		{"non-key slice", nal(1), false},
		{"short", []byte{0x02}, false},
	}
	for _, tt := range tests {
		if got := isH265KeyframeStart(tt.payload); got != tt.want {
			t.Errorf("%s: got %v want %v", tt.name, got, tt.want)
		}
	}

	agg := []byte{48 << 1, 0x01, 19 << 1, 0x02, 0x03}
	if !isH265KeyframeStart(agg) {
		t.Error("aggregation with IDR should be keyframe")
	}
	fu := []byte{49 << 1, 0x01, 0x80 | 19, 0x02}
	if !isH265KeyframeStart(fu) {
		t.Error("FU-A start with IDR should be keyframe")
	}
	fuMid := []byte{49 << 1, 0x01, 19, 0x02}
	if isH265KeyframeStart(fuMid) {
		t.Error("FU-A middle should not be keyframe start")
	}
}

func TestAlignShift_ContinuesForward(t *testing.T) {
	// primary last packet at ts=1000000, 2s ago; standby keyframe at ts=500
	lastWall := time.Now().Add(-2 * time.Second)
	shift := alignShift(1000000, lastWall, 500, time.Now())
	// rebased standby keyframe = 500 + shift, should be ~1000000 + 2s*90000
	got := uint32(int32(500) + int32(shift))
	want := uint32(1000000 + 2*90000)
	// allow 100ms slack for scheduling
	if d := int32(got - want); d < -9000 || d > 9000 {
		t.Errorf("rebased kf = %d, want ~%d (delta %d)", got, want, d)
	}
}

func TestAlignShift_NeverGoesBackward(t *testing.T) {
	// standby keyframe arrives before the primary's last wall time (fan-out
	// case): the rebased keyframe must still be > lastPts
	now := time.Now()
	shift := alignShift(1000000, now.Add(time.Second), 999999, now)
	got := uint32(int32(999999) + int32(shift))
	if int32(got-1000000) <= 0 {
		t.Errorf("rebased kf = %d must be > lastPts 1000000", got)
	}
}

func TestAlignShift_ZeroGap(t *testing.T) {
	// splice immediately: lastWall == now, standby kf continues by one frame
	now := time.Now()
	shift := alignShift(1000000, now, 777, now)
	got := uint32(int32(777) + int32(shift))
	// advance is ~0, so target clamps to lastPts + minStep
	if got != 1000000+rolloverMinStep {
		t.Errorf("rebased kf = %d, want %d", got, 1000000+rolloverMinStep)
	}
}

func TestStandbyURL(t *testing.T) {
	in := "tuya://x?email=a&password=b&device_id=c&resolution=hd&rollover=1"
	out := standbyURL(in)
	if strings.Contains(out, "rollover") {
		t.Errorf("rollover not stripped: %s", out)
	}
	if !strings.Contains(out, "resolution=hd") {
		t.Errorf("params lost: %s", out)
	}
}
