package main

import (
	"errors"

	"github.com/orkstack/orkstack/internal/engine"
)

// forceCancelSettingsFromEnv reads engine.runs.force_cancel_* (E88). A value
// that does not parse, or with which no force cancel could be decided, is
// refused at boot naming its setting (ForceCancelSettings.Check).
func forceCancelSettingsFromEnv() (engine.ForceCancelSettings, error) {
	s := engine.DefaultForceCancelSettings()
	err := errors.Join(
		concurrencyDuration("ORK_FORCE_CANCEL_GRACE", "engine.runs.force_cancel_grace", &s.Grace),
		concurrencyDuration("ORK_MAX_FORCE_CANCEL_GRACE", "engine.runs.max_force_cancel_grace", &s.MaxGrace),
		concurrencyDuration("ORK_FORCE_CANCEL_CEILING", "engine.runs.force_cancel_ceiling", &s.Ceiling),
		concurrencyDuration("ORK_FORCE_CANCEL_RECORD_TIMEOUT", "engine.runs.force_cancel_record_timeout", &s.RecordTimeout),
		concurrencyInt("ORK_FORCE_CANCEL_SCAN_EVENTS", "engine.runs.force_cancel_scan_events", &s.ScanEvents),
		concurrencyInt("ORK_FORCE_CANCEL_PAGE_SIZE", "engine.runs.force_cancel_page_size", &s.PageSize),
		concurrencyDuration("ORK_FORCE_CANCEL_EXECUTION_TIMEOUT", "engine.runs.force_cancel_execution_timeout", &s.ExecutionTimeout),
		concurrencyDuration("ORK_FORCE_CANCEL_ATTACH_PROGRESS_SLACK", "engine.runs.force_cancel_attach_progress_slack", &s.AttachProgressSlack),
		concurrencyInt("ORK_FORCE_CANCEL_CASCADE_MAX", "engine.runs.force_cancel_cascade_max", &s.Cascade.Max),
		concurrencyInt("ORK_FORCE_CANCEL_CASCADE_SCAN", "engine.runs.force_cancel_cascade_scan", &s.Cascade.Scan),
		concurrencyInt("ORK_FORCE_CANCEL_CASCADE_WALK_SCAN", "engine.runs.force_cancel_cascade_walk_scan", &s.Cascade.WalkScan),
		concurrencyInt("ORK_FORCE_CANCEL_CASCADE_BATCH", "engine.runs.force_cancel_cascade_batch", &s.Cascade.Batch),
		concurrencyInt("ORK_FORCE_CANCEL_CASCADE_STARTS_PER_TASK", "engine.runs.force_cancel_cascade_starts_per_task", &s.Cascade.StartsPerTask),
		concurrencyDuration("ORK_FORCE_CANCEL_CASCADE_POLL", "engine.runs.force_cancel_cascade_poll", &s.Cascade.Poll),
		concurrencyDuration("ORK_FORCE_CANCEL_CASCADE_LIST_TIMEOUT", "engine.runs.force_cancel_cascade_list_timeout", &s.Cascade.ListTimeout),
		concurrencyDuration("ORK_FORCE_CANCEL_CASCADE_SETTLE_SLACK", "engine.runs.force_cancel_cascade_settle_slack", &s.Cascade.SettleSlack),
	)
	if err == nil {
		err = s.Check()
	}
	if err != nil {
		return engine.DefaultForceCancelSettings(), err
	}
	return s, nil
}
