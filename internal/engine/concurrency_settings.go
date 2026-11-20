package engine

import (
	"fmt"
	"time"
)

// ConcurrencySettings are engine.concurrency.*'s settings that a run's
// submit and plan read (E88 §9.6), beside the gate's own tunables
// (GateTunables) and the census's (CensusSettings): the bounds a namespace
// and an Orkflow are held to, how long a run waits in line, how it asks
// again, and how much of a key's holders its event and its API answer show.
// Read at boot; what a run decides with them is pinned into its plan at
// submit, so a change applies to new runs only.
type ConcurrencySettings struct {
	MaxActiveKeysPerNamespace int
	MaxQueuedRunsPerNamespace int
	MaxLimitsPerOrkflow       int
	MaxRunSlotEntries         int
	PerSlotBytes              int64
	DefaultQueueTimeout       time.Duration
	MinQueueTimeout           time.Duration
	MaxQueueTimeout           time.Duration
	AcquireBackoffMin         time.Duration
	AcquireBackoffMax         time.Duration
	RequeueInterval           time.Duration
	RequeueBackoffFactor      int
	RequeueIntervalMax        time.Duration
	RetryAfter                time.Duration
	EventHoldersShown         int
	APIHoldersShown           int
}

// DefaultConcurrencySettings are §9.6's defaults.
func DefaultConcurrencySettings() ConcurrencySettings {
	return ConcurrencySettings{
		MaxActiveKeysPerNamespace: 1000,
		MaxQueuedRunsPerNamespace: 1000,
		MaxLimitsPerOrkflow:       32,
		MaxRunSlotEntries:         64,
		PerSlotBytes:              2 << 10,
		DefaultQueueTimeout:       2 * time.Hour,
		MinQueueTimeout:           10 * time.Second,
		MaxQueueTimeout:           7 * 24 * time.Hour,
		AcquireBackoffMin:         200 * time.Millisecond,
		AcquireBackoffMax:         10 * time.Second,
		RequeueInterval:           time.Minute,
		RequeueBackoffFactor:      2,
		RequeueIntervalMax:        15 * time.Minute,
		RetryAfter:                time.Minute,
		EventHoldersShown:         5,
		APIHoldersShown:           10,
	}
}

// Check refuses settings no run could be decided with, naming the setting:
// a bound below one, a duration not above zero, a queue timeout outside its
// own bounds, a back-off whose maximum is below its minimum, and a holders
// list longer than a run's answer carries. g is the gate's tunables, which
// a run's entries and its acquire must fit.
func (s ConcurrencySettings) Check(g GateTunables) error {
	for _, c := range []struct {
		name string
		v    int
	}{
		{"max_active_keys_per_namespace", s.MaxActiveKeysPerNamespace},
		{"max_queued_runs_per_namespace", s.MaxQueuedRunsPerNamespace},
		{"max_limits_per_orkflow", s.MaxLimitsPerOrkflow},
		{"max_run_slot_entries", s.MaxRunSlotEntries},
		{"requeue_backoff_factor", s.RequeueBackoffFactor},
		{"event_holders_shown", s.EventHoldersShown},
		{"api_holders_shown", s.APIHoldersShown},
	} {
		if c.v < 1 {
			return fmt.Errorf("engine.concurrency.%s is %d; it must be at least 1", c.name, c.v)
		}
	}
	if s.PerSlotBytes < 1 {
		return fmt.Errorf("engine.concurrency.per_slot_bytes is %d; it must be at least 1", s.PerSlotBytes)
	}
	for _, c := range []struct {
		name string
		v    time.Duration
	}{
		{"default_queue_timeout", s.DefaultQueueTimeout}, {"min_queue_timeout", s.MinQueueTimeout},
		{"max_queue_timeout", s.MaxQueueTimeout}, {"acquire_backoff_min", s.AcquireBackoffMin},
		{"acquire_backoff_max", s.AcquireBackoffMax}, {"requeue_interval", s.RequeueInterval},
		{"requeue_interval_max", s.RequeueIntervalMax}, {"retry_after", s.RetryAfter},
	} {
		if c.v <= 0 {
			return fmt.Errorf("engine.concurrency.%s is %s; it must be above zero", c.name, c.v)
		}
	}
	switch {
	case s.MinQueueTimeout > s.MaxQueueTimeout:
		return fmt.Errorf("engine.concurrency.min_queue_timeout (%s) is above max_queue_timeout (%s)", s.MinQueueTimeout, s.MaxQueueTimeout)
	case s.DefaultQueueTimeout < s.MinQueueTimeout || s.DefaultQueueTimeout > s.MaxQueueTimeout:
		return fmt.Errorf("engine.concurrency.default_queue_timeout (%s) must lie within min_queue_timeout (%s) and max_queue_timeout (%s)",
			s.DefaultQueueTimeout, s.MinQueueTimeout, s.MaxQueueTimeout)
	case s.AcquireBackoffMax < s.AcquireBackoffMin:
		return fmt.Errorf("engine.concurrency.acquire_backoff_max (%s) is below acquire_backoff_min (%s)", s.AcquireBackoffMax, s.AcquireBackoffMin)
	case s.AcquireBackoffMin >= g.AcquireTimeout:
		return fmt.Errorf("engine.concurrency.acquire_backoff_min (%s) must be below acquire_timeout (%s): a run could not ask its gate twice",
			s.AcquireBackoffMin, g.AcquireTimeout)
	case s.RequeueIntervalMax < s.RequeueInterval:
		return fmt.Errorf("engine.concurrency.requeue_interval_max (%s) is below requeue_interval (%s)", s.RequeueIntervalMax, s.RequeueInterval)
	case s.MaxRunSlotEntries > g.MaxEntries:
		return fmt.Errorf("engine.concurrency.max_run_slot_entries (%d) is above max_entries (%d): one run could fill a key's whole line",
			s.MaxRunSlotEntries, g.MaxEntries)
	case s.EventHoldersShown > s.APIHoldersShown:
		return fmt.Errorf("engine.concurrency.event_holders_shown (%d) is above api_holders_shown (%d)", s.EventHoldersShown, s.APIHoldersShown)
	}
	return nil
}
