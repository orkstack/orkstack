package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// passSpacing is the walk's rate: ten sources a second.
	passSpacing = 100 * time.Millisecond
	// passStarts is how many workflows one walk starts.
	passStarts = 20
	// passHealthy is how long an instance is ready before it is healthy.
	passHealthy = 10 * time.Minute
	// passConfirmed bounds how many attempts are remembered as confirmed.
	passConfirmed = 4096
	// copyAgain is how often what lasts is said.
	copyAgain = time.Hour
)

// What an instance shows of itself in its heartbeat (internal/orker/sourcehost).
const (
	instanceActive  = "active"
	instanceWaiting = "waiting"
	instanceInvalid = "invalid"
)

// PassDid is what a walk did for one source.
type PassDid string

const (
	PassDescribed       PassDid = "described"
	PassDescribeFailed  PassDid = "describe_failed"
	PassRemoved         PassDid = "removed"
	PassStarted         PassDid = "started"
	PassStartUnanswered PassDid = "start_unanswered"
	PassSync            PassDid = "sync"
	PassReroute         PassDid = "reroute"
	PassReport          PassDid = "report"
)

// PassDocuments are the documents a walk is over.
type PassDocuments interface {
	Names(ctx context.Context) ([]eventsources.Key, error)
	Get(ctx context.Context, namespace, name string) (*orkstackv1.EventSource, error)
	Remove(ctx context.Context, namespace, name, etag string) error
}

// SourcePass makes good what a signal can miss: a write whose workflow was
// not told, a report that was not relayed, a workflow that is gone. It decides
// nothing about a source that the source's own workflow would not decide, and
// a walk over what has not changed sends nothing.
type SourcePass struct {
	Starter   *SourceStarter
	Documents PassDocuments
	// Confirmed reads the engine's record that an attempt confirmed.
	Confirmed func(ctx context.Context, a artifacts.InstanceAttempt) (artifacts.ConfirmRecord, bool, error)
	// Interval is how often a walk begins, and Readmit how often an instance
	// confirms.
	Interval, Readmit time.Duration
	// Did is told what a walk did, from more than one goroutine, and Walked
	// when one ended; both may be nil.
	Did    func(PassDid)
	Walked func(at time.Time)
	Logger *slog.Logger

	// Handover moves sources to the current release; nil moves none.
	Handover *SourceHandover

	// watching is how many sources that were handed over are looked at.
	watching atomic.Int64
	watchers sync.WaitGroup
	// copies is when what lasts was last said of an id: a deleted source's
	// document that is back, a workflow that is no source's.
	copies map[string]time.Time
	// confirmed are the attempts seen confirmed: a record never goes.
	confirmed map[artifacts.InstanceAttempt]bool
}

// Run walks until ctx ends.
func (p *SourcePass) Run(ctx context.Context) {
	if !p.Starter.Versioned {
		return
	}
	defer p.watchers.Wait()
	for ctx.Err() == nil {
		began := time.Now()
		if err := p.walk(ctx); err != nil && ctx.Err() == nil {
			p.Logger.WarnContext(ctx, "the event sources could not be walked; the next walk tries again", "error", err)
		}
		sleep(ctx, time.Until(began.Add(p.Interval)))
	}
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// passWalk is one walk.
type passWalk struct {
	*SourcePass
	starts     int
	deferred   int
	namespaces map[string]*orkstackv1.Namespace
	// seen is what a hand-over counts by, and blind the sources whose pool
	// could not be told.
	seen     []seenSource
	blind    []string
	blindErr error
}

// namespace is the document the walk judges a namespace's sources by.
func (w *passWalk) namespace(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	ns, ok := w.namespaces[name]
	if !ok {
		var err error
		if ns, err = w.Starter.Namespaces.Get(ctx, name); err != nil {
			return nil, err
		}
		w.namespaces[name] = ns
	}
	return ns, nil
}

// walk looks at every source once, from a point of its own.
func (p *SourcePass) walk(ctx context.Context) error {
	if p.confirmed == nil || len(p.confirmed) > passConfirmed {
		p.confirmed = map[artifacts.InstanceAttempt]bool{}
	}
	if p.copies == nil || len(p.copies) > passConfirmed {
		p.copies = map[string]time.Time{}
	}
	keys, err := p.Documents.Names(ctx)
	if err != nil {
		return err
	}
	w := &passWalk{SourcePass: p, namespaces: map[string]*orkstackv1.Namespace{}}
	if n := len(keys); n > 0 {
		from := rand.IntN(n)
		keys = slices.Concat(keys[from:], keys[:from])
	}
	for _, key := range keys {
		began := time.Now()
		w.look(ctx, key)
		if sleep(ctx, time.Until(began.Add(passSpacing))); ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if w.deferred > 0 {
		p.Logger.WarnContext(ctx, "more event sources have no workflow than one walk starts; the next walks start the rest", "started", w.starts, "waiting", w.deferred)
	}
	if p.Walked != nil {
		p.Walked(time.Now())
	}
	if err := w.handOver(ctx); err != nil {
		return fmt.Errorf("hand-overs: %w", err)
	}
	return nil
}

// blindOf keeps that a source's document could not be read for now. One this
// release cannot read is none: an open workflow of it is found by the index.
func (w *passWalk) blindOf(err error, key eventsources.Key) {
	_, unreadable := errors.AsType[*eventsources.UnreadableError](err)
	if w.Handover != nil && !unreadable && !errors.Is(err, eventsources.ErrNotFound) {
		w.blind, w.blindErr = append(w.blind, key.Namespace+"/"+key.Name), err
	}
}

// sayOnce reports whether what lasts is said of id now: once an hour.
func (p *SourcePass) sayOnce(id string) bool {
	if time.Since(p.copies[id]) < copyAgain {
		return false
	}
	p.copies[id] = time.Now()
	return true
}

func (p *SourcePass) did(what PassDid) {
	if p.Did != nil {
		p.Did(what)
	}
}

// look is one source: a source that cannot be seen is passed over, and
// nothing else is.
func (w *passWalk) look(ctx context.Context, key eventsources.Key) {
	log := w.Logger.With("namespace", key.Namespace, "event_source", key.Name)
	src, err := w.Documents.Get(ctx, key.Namespace, key.Name)
	if err != nil {
		w.blindOf(err, key)
		if !errors.Is(err, eventsources.ErrNotFound) && ctx.Err() == nil {
			log.WarnContext(ctx, "an event source's document could not be read; it is passed over in this walk", "error", err)
		}
		return
	}
	log = log.With("event_source_id", src.GetId())
	e, err := w.Starter.describe(ctx, src.GetId())
	w.see(ctx, src, e, err)
	if err != nil {
		w.did(PassDescribeFailed)
		log.WarnContext(ctx, "an event source's workflow could not be described; it is passed over in this walk", "error", err)
		return
	}
	w.did(PassDescribed)
	if err := w.act(ctx, src, e); err != nil && ctx.Err() == nil {
		log.WarnContext(ctx, "an event source could not be put right; the next walk tries again", "error", err)
	}
}

// act is the rows, in their order, from the one describe.
func (w *passWalk) act(ctx context.Context, src *orkstackv1.EventSource, e *sourceExecution) error {
	var carry eventsource.Carry
	if e != nil && len(e.carry) > 0 {
		var err error
		if carry, err = eventsource.DecodeCarry(e.carry); err != nil {
			if errors.Is(err, eventsource.ErrCarryFormat) {
				// That release's engines look after it.
				return nil
			}
			if src.GetDeletingSince() == nil {
				return fmt.Errorf("its carry is damaged, and nothing is started from it: %w", err)
			}
			// What it kept is removed whatever it remembered, where nothing runs.
			carry = eventsource.Carry{}
		}
	}
	if !e.open() {
		if src.GetDeletingSince() != nil && carry.Continued != nil && carry.Continued.How == eventsource.EndedPurged {
			return w.remove(ctx, src)
		}
		return w.start(ctx, src)
	}
	if e.task || carry.Format == 0 {
		// It has not read what it was last told.
		return nil
	}
	differs, err := w.differs(ctx, src, &carry, e)
	if err != nil {
		return err
	}
	if differs {
		w.did(PassSync)
		return w.Starter.signal(ctx, src.GetId(), e.runID, SignalSourceSync, nil)
	}
	return w.instances(ctx, src.GetId(), &carry, e)
}

// remove removes the document of a source whose workflow has removed what it
// kept. Nothing else removes one.
func (w *passWalk) remove(ctx context.Context, src *orkstackv1.EventSource) error {
	switch err := w.Documents.Remove(ctx, src.GetNamespace(), src.GetName(), src.GetEtag()); {
	case errors.Is(err, eventsources.ErrChanged):
		return nil
	case err != nil:
		return err
	}
	w.did(PassRemoved)
	w.Logger.InfoContext(ctx, "a deleted event source is gone", "namespace", src.GetNamespace(), "event_source", src.GetName(), "event_source_id", src.GetId())
	return nil
}

func (w *passWalk) start(ctx context.Context, src *orkstackv1.EventSource) error {
	if w.starts >= passStarts {
		w.deferred++
		return nil
	}
	got, err := w.Starter.Start(ctx, src)
	switch got {
	case SourceStarted:
		w.starts++
		w.did(PassStarted)
		w.Logger.InfoContext(ctx, "an event source's workflow was started: it had none, or its last had ended", "namespace", src.GetNamespace(),
			"event_source", src.GetName(), "event_source_id", src.GetId())
	case SourceUnanswered:
		w.starts++
		w.did(PassStartUnanswered)
	case SourceDeletedCopy:
		if !w.sayOnce(src.GetId()) {
			return err
		}
		w.Logger.WarnContext(ctx, "an event source's document is a deleted one's that came back into the store; it is not run", "reason", eventsources.ReasonDeletedDocumentReappeared,
			"namespace", src.GetNamespace(), "event_source", src.GetName(), "event_source_id", src.GetId())
	}
	return err
}

// differs reports whether what the workflow last loaded is not what the
// document says. One whose load fails tries again by itself.
func (w *passWalk) differs(ctx context.Context, src *orkstackv1.EventSource, carry *eventsource.Carry, e *sourceExecution) (bool, error) {
	l := carry.Loaded
	if l == nil || e.now.Load != eventsource.LoadIsOK {
		return false, nil
	}
	if marked := src.GetDeletingSince() != nil; marked || l.Deleting {
		return marked && !l.Deleting, nil
	}
	doc := eventsources.Effective(src, eventsources.Limits{})
	if l.Revision != src.GetServiceRevision() || l.Paused != (src.GetPause() != nil) || l.Instances != doc.GetInstances() || l.ResumeSeq != src.GetResumeSeq() {
		return true, nil
	}
	ns, err := w.namespace(ctx, src.GetNamespace())
	if err != nil {
		return false, err
	}
	if held := ns.GetHold() != nil; l.Held != held || held || l.Paused {
		return l.Held != held, nil
	}
	// The namespace's default pool is a source's that names none.
	if doc.GetPool() != "" || e.now.Source.Pool == "" {
		return false, nil
	}
	_, pool, err := sourcePool("", ns, w.Starter.DefaultQueue)
	if err != nil {
		return false, err
	}
	return pool != e.now.Source.Pool, nil
}

// instances sends what an instance's report, or what holds one without
// confirming, was not told of.
func (w *passWalk) instances(ctx context.Context, id string, carry *eventsource.Carry, e *sourceExecution) error {
	byID := make(map[string]*workflowpb.PendingActivityInfo, len(e.pending))
	for _, a := range e.pending {
		byID[a.GetActivityId()] = a
	}
	for _, index := range slices.Sorted(maps.Keys(e.now.Slots)) {
		slot := e.now.Slots[index]
		epochs := []eventsource.SlotEpoch{slot.Current}
		if slot.Replacement != nil {
			epochs = append(epochs, *slot.Replacement)
		}
		for _, of := range epochs {
			a := byID[held.ActivityID(index, of.Epoch)]
			if a == nil || a.GetState() != enumspb.PENDING_ACTIVITY_STATE_STARTED {
				continue
			}
			confirmed, err := w.isConfirmed(ctx, artifacts.InstanceAttempt{SourceID: id, Index: int(index), Epoch: of.Epoch, Attempt: int(a.GetAttempt())})
			if err != nil {
				return err
			}
			if !confirmed {
				if err := w.reroute(ctx, id, carry, e, index, of, a); err != nil {
					return err
				}
				continue
			}
			for _, r := range reported(a, of.Epoch) {
				if !awaits(e.now, index, of.Epoch, r.what) {
					continue
				}
				w.did(PassReport)
				if err := w.Starter.signal(ctx, id, e.runID, SignalSourceReport, SourceReport{Index: index, Epoch: of.Epoch, What: r.what, Reason: r.reason}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// reroute asks for an activity to be scheduled again that something took more
// than two confirmations ago and never confirmed.
func (w *passWalk) reroute(ctx context.Context, id string, carry *eventsource.Carry, e *sourceExecution, index int32, of eventsource.SlotEpoch, a *workflowpb.PendingActivityInfo) error {
	taken := a.GetLastStartedTime()
	if of.Phase != eventsource.PhaseScheduled || taken == nil || time.Since(taken.AsTime()) <= 2*w.Readmit || freshToday(carry, index) >= carry.Pinned.FreshStarts {
		return nil
	}
	w.did(PassReroute)
	w.Logger.WarnContext(ctx, "an event source's instance was taken and never confirmed; it is scheduled again", "event_source_id", id, "index", index,
		"epoch", of.Epoch, "orker", eventsources.SafeText(a.GetLastWorkerIdentity(), maxReportedReason))
	return w.Starter.signal(ctx, id, e.runID, SignalSourceReroute, SourceReroute{Index: index, Epoch: of.Epoch})
}

func freshToday(carry *eventsource.Carry, index int32) int {
	n := 0
	for _, at := range carry.Index[index].Fresh {
		if time.Since(at) < carry.Pinned.FreshWindow {
			n++
		}
	}
	return n
}

// isConfirmed reports whether the engine has the attempt's confirmation on
// record; a read that failed is neither.
func (w *passWalk) isConfirmed(ctx context.Context, a artifacts.InstanceAttempt) (bool, error) {
	if w.confirmed[a] {
		return true, nil
	}
	_, found, err := w.Confirmed(ctx, a)
	if found && err == nil {
		w.confirmed[a] = true
	}
	return found, err
}

type reportedOf struct{ what, reason string }

// reported is what a confirmed instance has reported, as its beat shows it.
func reported(a *workflowpb.PendingActivityInfo, epoch int64) []reportedOf {
	out := []reportedOf{{what: eventsource.ReportStarted}}
	payloads := a.GetHeartbeatDetails().GetPayloads()
	if len(payloads) != 1 {
		return out
	}
	d, ok := eventsources.ReadHeartbeatDetails(payloads[0].GetData())
	if !ok || d.Epoch != epoch {
		return out
	}
	switch d.State {
	case instanceInvalid:
		out = append(out, reportedOf{what: eventsource.ReportInvalid, reason: d.Reason})
	case instanceActive, instanceWaiting:
		out = append(out, reportedOf{what: eventsource.ReportReady})
		if a.GetAttempt() > 1 && !d.Since.IsZero() && time.Since(d.Since) >= passHealthy {
			out = append(out, reportedOf{what: eventsource.ReportHealthy})
		}
	}
	return out
}
