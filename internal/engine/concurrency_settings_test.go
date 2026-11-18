package engine

import (
	"strings"
	"testing"
	"time"
)

// The defaults pass, and each rule refuses naming its setting.
func TestConcurrencySettings_Check(t *testing.T) {
	gate := DefaultGateTunables()
	if err := DefaultConcurrencySettings().Check(gate); err != nil {
		t.Fatalf("the defaults are refused: %v", err)
	}
	for _, tc := range []struct {
		name  string
		edit  func(*ConcurrencySettings)
		names string
	}{
		{"a bound below one", func(s *ConcurrencySettings) { s.MaxLimitsPerOrkflow = 0 }, "max_limits_per_orkflow"},
		{"no slot bytes", func(s *ConcurrencySettings) { s.PerSlotBytes = 0 }, "per_slot_bytes"},
		{"a zero duration", func(s *ConcurrencySettings) { s.RetryAfter = 0 }, "retry_after"},
		{"min above max", func(s *ConcurrencySettings) { s.MinQueueTimeout = 8 * 24 * time.Hour }, "min_queue_timeout"},
		{"default below min", func(s *ConcurrencySettings) { s.DefaultQueueTimeout = time.Second }, "default_queue_timeout"},
		{"default above max", func(s *ConcurrencySettings) { s.DefaultQueueTimeout = 8 * 24 * time.Hour }, "default_queue_timeout"},
		{"back-off max below min", func(s *ConcurrencySettings) { s.AcquireBackoffMax = time.Millisecond }, "acquire_backoff_max"},
		{"back-off as long as the acquire", func(s *ConcurrencySettings) { s.AcquireBackoffMin, s.AcquireBackoffMax = gate.AcquireTimeout, gate.AcquireTimeout }, "acquire_backoff_min"},
		{"requeue max below interval", func(s *ConcurrencySettings) { s.RequeueIntervalMax = time.Second }, "requeue_interval_max"},
		{"one run fills a line", func(s *ConcurrencySettings) { s.MaxRunSlotEntries = gate.MaxEntries + 1 }, "max_run_slot_entries"},
		{"an event shows more than the API", func(s *ConcurrencySettings) { s.EventHoldersShown = s.APIHoldersShown + 1 }, "event_holders_shown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := DefaultConcurrencySettings()
			tc.edit(&s)
			if err := s.Check(gate); err == nil || !strings.Contains(err.Error(), tc.names) {
				t.Fatalf("Check = %v, want a refusal naming %s", err, tc.names)
			}
		})
	}
}
