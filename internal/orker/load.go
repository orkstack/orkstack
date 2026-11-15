package orker

// LoadReading is one Orker's load as its gauges export it
// (ork.orker.running_tasks, open_pins, max_concurrent and cordoned; docs/DEPLOY.md
// "Metrics"): the same counters and cordon DescribeOrker answers from.
type LoadReading struct {
	// RunningTasks is the ExecuteTask attempts in flight (running_tasks).
	RunningTasks int64
	// OpenPins is the pins this Orker accepted and has not completed.
	OpenPins int64
	// MaxConcurrent is how many tasks it runs at once.
	MaxConcurrent int64
	// Operator, StoreOutage and Stopping are the cordon's causes, each held
	// or not; any of them stops it taking new work (AGENTS.md §7, Cordon).
	Operator, StoreOutage, Stopping bool
}

// Load reads this Orker's load: one lock read of each counter and two of the
// cordon, as DescribeOrker reads them, and no filesystem access, so a scrape
// costs nothing a task would notice. The reads are separate, so a cause set
// or cleared between them may show in one gauge one collection late.
func (a *Activities) Load() LoadReading {
	operator, outage := a.cordon.Causes()
	_, stopping := a.cordon.Stopping()
	return LoadReading{
		RunningTasks:  int64(a.running.load()),
		OpenPins:      int64(a.pins.count()),
		MaxConcurrent: int64(a.maxConc),
		Operator:      operator != nil,
		StoreOutage:   outage != nil,
		Stopping:      stopping,
	}
}
