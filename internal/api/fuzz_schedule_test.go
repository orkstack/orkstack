package api

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// FuzzValidateScheduleSpec: a spec that is not refused holds every rule the
// reference states, and a refusal names the field that broke the first rule
// in the documented order. The oracle restates the rules from the schedule
// reference; cron syntax is the cluster's to judge and is not asserted.
func FuzzValidateScheduleSpec(f *testing.F) {
	for _, seed := range []struct {
		cron1, cron2, zone  string
		interval, jitter    int64
		catchup, start, end int64
		overlap             int32
		present             uint8
	}{
		{"0 2 * * *", "", "Europe/Lisbon", 0, 0, 0, 0, 0, 0, 0},
		{"@daily", "TZ=UTC 0 2 * * *", "", 0, 0, 0, 0, 0, 0, 0},
		{"CRON_TZ=Europe/Lisbon 0 2 * * *", "", "", 0, 0, 0, 0, 0, 0, 0},
		{"cron_tz=UTC @daily", "", "", 0, 0, 0, 0, 0, 0, 0},
		{"", "", "", 3600e9, 0, 0, 0, 0, 0, 1},
		{"", "", "", 0, 0, 0, 0, 0, 0, 1},
		{"", "", "", -1, 0, 0, 0, 0, 0, 1},
		{"@daily", "", "", 0, -1, 0, 0, 0, 0, 2},
		{"@daily", "", "", 0, 0, 9e9, 0, 0, 0, 4},
		{"@daily", "", "", 0, 0, 10e9, 0, 0, 0, 4},
		{"@daily", "", "Local", 0, 0, 0, 0, 0, 0, 0},
		{"@daily", "", "Mars/Olympus", 0, 0, 0, 0, 0, 0, 0},
		{"@daily", "", "", 0, 0, 0, 2000000000, 1900000000, 0, 24},
		{"@daily", "", "", 0, 0, 0, 0, 1, 0, 16},
		{"@daily", "", "", 0, 0, 0, 0, 0, 5, 0},
		{"@daily", "", "", 0, 0, 0, 0, 0, 3, 0},
		{"@daily\n", "", "", 0, 0, 0, 0, 0, 0, 0},
		{" ", "", "", 0, 0, 0, 0, 0, 0, 0},
		{strings.Repeat("*", 201), "", "", 0, 0, 0, 0, 0, 0, 0},
		{strings.Repeat("*", 200), "", "", 0, 0, 0, 0, 0, 0, 0},
		{"é * * * *", "", "UTC", 0, 0, 0, 0, 0, 0, 0},
	} {
		f.Add(seed.cron1, seed.cron2, seed.zone, seed.interval, seed.jitter, seed.catchup, seed.start, seed.end, seed.overlap, seed.present)
	}
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, cron1, cron2, zone string, interval, jitter, catchup, start, end int64, overlap int32, present uint8) {
		if !utf8.ValidString(cron1) || !utf8.ValidString(cron2) || !utf8.ValidString(zone) {
			return // a protobuf string is UTF-8 by the time a spec holds it: no door delivers otherwise
		}
		spec := &orkstackv1.ScheduleSpec{Cron: []string{cron1, cron2}, Timezone: zone, Overlap: orkstackv1.OverlapPolicy(overlap)}
		if cron2 == "" {
			spec.Cron = spec.Cron[:1]
			if cron1 == "" {
				spec.Cron = nil
			}
		}
		if present&1 != 0 {
			spec.Interval = durationpb.New(time.Duration(interval))
		}
		if present&2 != 0 {
			spec.Jitter = durationpb.New(time.Duration(jitter))
		}
		if present&4 != 0 {
			spec.CatchupWindow = durationpb.New(time.Duration(catchup))
		}
		if present&8 != 0 {
			spec.StartAt = timestamppb.New(time.Unix(start%1e11, 0))
		}
		if present&16 != 0 {
			spec.EndAt = timestamppb.New(time.Unix(end%1e11, 0))
		}

		wantField := ""
		switch {
		case len(spec.Cron) == 0 && spec.Interval == nil:
			wantField = "spec"
		}
		for i, expr := range spec.Cron {
			if wantField != "" {
				break
			}
			trimmed := strings.TrimSpace(expr)
			upper := strings.Map(func(r rune) rune {
				if r >= 'a' && r <= 'z' {
					return r - 32
				}
				return r
			}, trimmed)
			controlled := strings.IndexFunc(expr, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0
			if trimmed == "" || len(expr) > 200 || controlled || strings.HasPrefix(upper, "CRON_TZ=") || strings.HasPrefix(upper, "TZ=") {
				wantField = "spec.cron[" + string(rune('0'+i)) + "]"
			}
		}
		switch {
		case wantField != "":
		case spec.Interval != nil && time.Duration(interval) <= 0:
			wantField = "spec.interval"
		case spec.Jitter != nil && time.Duration(jitter) < 0:
			wantField = "spec.jitter"
		case zone == "Local":
			wantField = "spec.timezone"
		case spec.CatchupWindow != nil && time.Duration(catchup) < 10*time.Second:
			wantField = "spec.catchup_window"
		case spec.StartAt != nil && spec.EndAt != nil && spec.StartAt.AsTime().After(spec.EndAt.AsTime()):
			wantField = "spec.start_at"
		case spec.EndAt != nil && !spec.EndAt.AsTime().After(now):
			wantField = "spec.end_at"
		case overlap != 0 && overlap != 1 && overlap != 2 && overlap != 3 && overlap != 4 && overlap != 6:
			wantField = "spec.overlap"
		}

		err := ValidateScheduleSpec(spec, now)
		// Whether a name is a zone of the IANA database is the host's to say,
		// so a zone refusal is allowed wherever no earlier rule is broken.
		zoneRefusal := err != nil && strings.HasPrefix(err.Error(), "spec.timezone:") && zone != "" &&
			(wantField == "" || wantField == "spec.catchup_window" || wantField == "spec.start_at" ||
				wantField == "spec.end_at" || wantField == "spec.overlap")
		switch {
		case err == nil && wantField != "":
			t.Fatalf("ValidateScheduleSpec accepted %v, which breaks the rule of %s", spec, wantField)
		case err != nil && !utf8.ValidString(err.Error()):
			t.Fatalf("ValidateScheduleSpec refused with text that is not UTF-8: %q", err)
		case zoneRefusal:
		case err != nil && wantField == "":
			t.Fatalf("ValidateScheduleSpec refused %v as %q, and no rule of the reference is broken", spec, err)
		case err != nil && !strings.HasPrefix(err.Error(), wantField+":"):
			t.Fatalf("ValidateScheduleSpec refused %v as %q, want the field %s first", spec, err, wantField)
		}
	})
}
