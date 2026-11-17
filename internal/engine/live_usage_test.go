package engine

import (
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// TestLiveUsage_FromTheBeat is the live usage contract: the reading as the
// Orker sent it, its time the beat's less the reading's age, stale past two
// sampling intervals and a tick; every reason an Orker names mapped; a beat
// without usage told apart by whether its container started; and a usage
// this engine cannot read, or whose numbers no sampler sends, UNDECODABLE on
// the usage alone.
func TestLiveUsage_FromTheBeat(t *testing.T) {
	beat := time.Unix(10_000, 0).UTC()
	cores := 1.5
	reason := func(r orkstackv1.LiveUsageUnavailableReason) *orkstackv1.LiveUsage {
		return &orkstackv1.LiveUsage{UnavailableReason: r}
	}
	cases := []struct {
		name     string
		progress task.HeartbeatProgress
		now      time.Time
		want     *orkstackv1.LiveUsage
	}{
		{
			name: "a fresh reading",
			progress: task.HeartbeatProgress{Phase: "run", Started: true,
				Usage: []byte(`{"cpu_cores":1.5,"cpu_s":12.25,"mem":1024,"peak":2048,"peak_exact":true,"limit":4096,"age_ms":2000,"interval_ms":5000,"tick_ms":5000}`)},
			now: beat.Add(time.Second),
			want: &orkstackv1.LiveUsage{Available: true, CpuCores: &cores, CpuSeconds: 12.25, MemoryBytes: 1024,
				MemoryPeakBytes: 2048, MemoryPeakExact: true, MemoryLimitBytes: 4096,
				SampledAt: timestamppb.New(beat.Add(-2 * time.Second))},
		},
		{
			// The window is 2 × 5 s + 5 s = 15 s; the reading is 2 s older than
			// the beat and the beat 14 s old, so it is 16 s old: stale.
			name: "stale past two intervals and a tick",
			progress: task.HeartbeatProgress{Phase: "run", Started: true,
				Usage: []byte(`{"cpu_s":1,"mem":1,"age_ms":2000,"interval_ms":5000,"tick_ms":5000}`)},
			now: beat.Add(14 * time.Second),
			want: &orkstackv1.LiveUsage{Available: true, CpuSeconds: 1, MemoryBytes: 1,
				SampledAt: timestamppb.New(beat.Add(-2 * time.Second)), Stale: true},
		},
		{
			// The bound counts the tick: at the 10 s ceiling it is 20 s.
			name: "within the bound at the tick ceiling",
			progress: task.HeartbeatProgress{Phase: "run", Started: true,
				Usage: []byte(`{"cpu_s":1,"mem":1,"interval_ms":5000,"tick_ms":10000}`)},
			now: beat.Add(19 * time.Second),
			want: &orkstackv1.LiveUsage{Available: true, CpuSeconds: 1, MemoryBytes: 1,
				SampledAt: timestamppb.New(beat)},
		},
		{"not visible", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"CGROUP_NOT_VISIBLE","interval_ms":5000}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_CGROUP_NOT_VISIBLE)},
		{"no cgroup", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"NO_CGROUP"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NO_CGROUP)},
		{"disabled", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"DISABLED"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_DISABLED)},
		{"no sample yet", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"NO_SAMPLE_YET"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NO_SAMPLE_YET)},
		{"ended before a sample", task.HeartbeatProgress{Phase: "exit", Started: true, Usage: []byte(`{"unavailable":"ENDED_BEFORE_FIRST_SAMPLE"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NO_SAMPLE_YET)},
		{"read failed", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"READ_FAILED"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_READ_FAILED)},
		{"start failed", task.HeartbeatProgress{Phase: "run", Usage: []byte(`{"unavailable":"START_FAILED"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_START_FAILED)},
		{"unverified", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"CGROUP_UNVERIFIED"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_CGROUP_UNVERIFIED)},
		{"a reason this engine does not know", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"SOMETHING_NEWER"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"unencodable at the Orker", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"` + task.HeartbeatUsageUnencodable + `"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"an older Orker, started", task.HeartbeatProgress{Phase: "run", Started: true}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_ORKER_TOO_OLD)},
		{"before the start", task.HeartbeatProgress{Phase: "image"}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_PREPARING)},
		{"wrong types", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"cpu_s":"lots"}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"not an object", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`[1,2]`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"negative memory", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"cpu_s":1,"mem":-1}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"negative age", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"cpu_s":1,"age_ms":-5,"interval_ms":5000}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"an absurd age", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"cpu_s":1,"age_ms":9223372036854775807,"interval_ms":5000}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"an empty usage carries no reading", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"only unknown keys", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"cpu":1.2,"tick_ms":5000}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{"a reason with numbers", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"unavailable":"DISABLED","cpu_s":5,"interval_ms":5000}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
		{
			"a genuine zero reading is still a reading",
			task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"interval_ms":5000,"tick_ms":5000}`)},
			beat,
			&orkstackv1.LiveUsage{Available: true, SampledAt: timestamppb.New(beat)},
		},
		{
			// One reading: the rate is not known yet, so cpu_cores is unset.
			"no rate yet",
			task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"cpu_s":0.5,"mem":10,"interval_ms":5000}`)},
			beat,
			&orkstackv1.LiveUsage{Available: true, CpuSeconds: 0.5, MemoryBytes: 10, SampledAt: timestamppb.New(beat)},
		},
		{"an absurd interval", task.HeartbeatProgress{Phase: "run", Started: true, Usage: []byte(`{"cpu_s":1,"interval_ms":9223372036854775807}`)}, beat,
			reason(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := liveUsage(tc.progress, beat, tc.now)
			if !proto.Equal(got, tc.want) {
				t.Fatalf("liveUsage = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUsableUsage_RefusesWhatNoSamplerSends: not-finite and negative figures
// are never shown as a measurement.
func TestUsableUsage_RefusesWhatNoSamplerSends(t *testing.T) {
	nan, inf, neg := math.NaN(), math.Inf(1), -0.5
	for _, u := range []task.HeartbeatUsage{
		{CPUSeconds: nan}, {CPUSeconds: inf}, {CPUSeconds: -1},
		{CPUCores: &nan}, {CPUCores: &inf}, {CPUCores: &neg},
		{Limit: -1}, {MemoryPeak: -1}, {Tick: -1}, {Tick: int64(2 * time.Hour / time.Millisecond)},
	} {
		if usableUsage(u) {
			t.Errorf("usableUsage(%+v) = true", u)
		}
	}
	if !usableUsage(task.HeartbeatUsage{CPUSeconds: 1, Memory: 1, Interval: 5000, Tick: 10000}) {
		t.Fatal("a plain reading is refused")
	}
}
