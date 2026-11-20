package servicehost

import "time"

// The reach probe's clock (14 §7): once an hour at the Orker's own offset,
// two minutes after an answer that was open or a probe that could not run;
// hosting goes on on the last clean answer for two hours without a probe
// that ran, and ends, its services evicted, after a day.
const (
	ReachEvery       = time.Hour
	ReachRetry       = 2 * time.Minute
	reachKeepHosting = 2 * time.Hour
	reachEvictAfter  = 24 * time.Hour
	// reachMinGap keeps the hourly probe at least this far from the last one,
	// so the first after a boot is not minutes after it.
	reachMinGap = 30 * time.Minute
)

// ProbeResult is one run of the reach probe, as the Orker saw it.
type ProbeResult struct {
	// Ran: the probe and its canaries ran and answered; Fitness is the
	// verdict on that answer.
	Ran     bool
	Fitness Fitness
	// ImageRefused: the probe's image was refused by the image policy or
	// could not be pulled, a fault of configuration with its own reason.
	ImageRefused bool
	// Err says why a probe that did not run did not.
	Err string
}

// ReachVerdict is what the probe's history allows now.
type ReachVerdict struct {
	// MayHost: the Orker may take a new service.
	MayHost bool
	// Evict: the services it hosts must be stopped (SERVICE_HOST_UNFIT).
	Evict bool
	// Reasons say why it may not host, each one sentence.
	Reasons []string
}

// ReachWatch keeps an Orker's reach-probe history and judges it.
type ReachWatch struct {
	offset    time.Duration
	seen      bool
	last      ProbeResult
	lastAt    time.Time
	lastClean time.Time
	openRun   int
	// unclean is the failures of the last probe that ran and was not
	// clean, nil once one that ran is clean.
	unclean []string
}

// NewReachWatch starts a history whose hourly probe runs at offset past the
// hour; an offset outside the hour is folded into it.
func NewReachWatch(offset time.Duration) *ReachWatch {
	offset %= ReachEvery
	if offset < 0 {
		offset = 0
	}
	return &ReachWatch{offset: offset}
}

// Record takes a probe's result at now.
func (w *ReachWatch) Record(r ProbeResult, now time.Time) {
	w.seen, w.last, w.lastAt = true, r, now
	switch {
	case r.Ran && r.Fitness.Fit:
		w.lastClean, w.openRun, w.unclean = now, 0, nil
	case r.Ran:
		w.openRun++
		w.unclean = r.Fitness.Failures
	}
}

// Verdict judges the history at now.
func (w *ReachWatch) Verdict(now time.Time) ReachVerdict {
	if !w.seen {
		return ReachVerdict{Reasons: []string{"no reach probe has run"}}
	}
	hosting := !w.lastClean.IsZero()
	since := now.Sub(w.lastClean)
	r := w.last
	switch {
	case r.Ran && r.Fitness.Fit:
		return ReachVerdict{MayHost: true}
	case r.Ran:
		if hosting && w.openRun < 2 {
			// Open once: asked again in two minutes; nothing changes yet.
			return ReachVerdict{MayHost: true}
		}
		return ReachVerdict{Evict: hosting, Reasons: prefixed("its reach probe is not clean: ", r.Fitness.Failures)}
	case r.ImageRefused && len(w.unclean) == 0:
		return ReachVerdict{Evict: hosting && since > reachEvictAfter, Reasons: []string{"its reach probe's image: " + r.Err}}
	}
	reasons := []string{"its reach probe could not be run: " + r.Err}
	if len(w.unclean) > 0 {
		// The last probe that ran found the host unclean, and none since has
		// said otherwise: that answer stands.
		return ReachVerdict{Evict: hosting && w.openRun >= 2,
			Reasons: append(prefixed("its reach probe is not clean: ", w.unclean), reasons...)}
	}
	switch {
	case !hosting:
		return ReachVerdict{Reasons: reasons}
	case since > reachEvictAfter:
		return ReachVerdict{Evict: true, Reasons: reasons}
	case since > reachKeepHosting:
		return ReachVerdict{Reasons: reasons}
	}
	return ReachVerdict{MayHost: true}
}

// Next is when the probe runs next; the zero time before the first.
func (w *ReachWatch) Next() time.Time {
	if !w.seen {
		return time.Time{}
	}
	if !w.last.Ran || !w.last.Fitness.Fit {
		return w.lastAt.Add(ReachRetry)
	}
	slot := w.lastAt.Truncate(ReachEvery).Add(w.offset)
	for slot.Before(w.lastAt.Add(reachMinGap)) {
		slot = slot.Add(ReachEvery)
	}
	return slot
}

func prefixed(prefix string, list []string) []string {
	out := make([]string, 0, len(list))
	for _, s := range list {
		out = append(out, prefix+s)
	}
	return out
}
