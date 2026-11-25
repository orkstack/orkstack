package api

import (
	"context"
	"errors"
	"sync"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	// resumeHistoryTTL is how long a closed execution's reading is kept.
	resumeHistoryTTL = time.Minute
	// maxResumePreviews bounds the previews one process makes at once, of
	// whichever runs: each reads a whole history and asks about every saved
	// result.
	maxResumePreviews = 16
	// maxResumeReadings bounds the readings kept.
	maxResumeReadings = 32
	// maxResumeGates bounds the runs whose requests are counted; past it, the
	// idle ones are forgotten.
	maxResumeGates = 4096
)

// causeThrottled: the run's resume is asked for too often.
const causeThrottled = "throttled"

// resumeGate bounds what requests to resume a run cost this process: one
// preview of a run at a time and maxResumePreviews of all, its confirms a minute, and one reading a minute
// of an execution's history. Nothing is decided by it that a request without
// it would decide otherwise. The zero value is ready to use.
type resumeGate struct {
	mu       sync.Mutex
	runs     map[string]*resumeGateRun
	readings map[string]resumeReading
	// previews holds a token of each preview being made.
	previews chan struct{}
}

type resumeGateRun struct {
	// preview holds one token while a preview runs.
	preview  chan struct{}
	waiting  int
	confirms []time.Time
}

type resumeReading struct {
	base *resumeBase
	at   time.Time
}

// enter admits one request: a preview waits for the one before it, and a
// confirm past its run's share of the last minute is refused. release ends it.
func (g *resumeGate) enter(ctx context.Context, msg *orkstackv1.ResumeRunRequest, now time.Time, perMinute int) (release func(), err error) {
	g.mu.Lock()
	run := g.run(artifacts.ResolveNamespace(msg.GetNamespace())+"\x00"+msg.GetRunId(), now)
	if !msg.GetDryRun() {
		defer g.mu.Unlock()
		kept := run.confirms[:0]
		for _, at := range run.confirms {
			if now.Sub(at) < time.Minute {
				kept = append(kept, at)
			}
		}
		run.confirms = kept
		if len(run.confirms) >= perMinute {
			r := refuseResume(engine.RefuseCheckUnavailable, engine.RefusalWait, "run %s's resume was confirmed %d times in the last minute; try again shortly",
				msg.GetRunId(), len(run.confirms))
			r.Cause, r.RetryAfter = causeThrottled, time.Minute-now.Sub(run.confirms[0])
			return nil, r
		}
		run.confirms = append(run.confirms, now)
		return func() {}, nil
	}
	run.waiting++
	if g.previews == nil {
		g.previews = make(chan struct{}, maxResumePreviews)
	}
	previews := g.previews
	g.mu.Unlock()
	done := func() {
		g.mu.Lock()
		run.waiting--
		g.mu.Unlock()
	}
	select {
	case run.preview <- struct{}{}:
	case <-ctx.Done():
		done()
		return nil, ctxError(ctx)
	}
	select {
	case previews <- struct{}{}:
		return func() { <-previews; <-run.preview; done() }, nil
	case <-ctx.Done():
		<-run.preview
		done()
		return nil, ctxError(ctx)
	}
}

// run is a run's entry. Locked.
func (g *resumeGate) run(key string, now time.Time) *resumeGateRun {
	if g.runs == nil {
		g.runs = map[string]*resumeGateRun{}
	}
	run := g.runs[key]
	if run == nil {
		if len(g.runs) >= maxResumeGates {
			for k, other := range g.runs {
				if other.waiting == 0 && (len(other.confirms) == 0 || now.Sub(other.confirms[len(other.confirms)-1]) >= time.Minute) {
					delete(g.runs, k)
				}
			}
			// Every entry counted in the last minute: the count of some run
			// is forgotten rather than the process's memory spent.
			for k, other := range g.runs {
				if len(g.runs) < maxResumeGates {
					break
				}
				if other.waiting == 0 {
					delete(g.runs, k)
				}
			}
		}
		run = &resumeGateRun{preview: make(chan struct{}, 1)}
		g.runs[key] = run
	}
	return run
}

// read is the last minute's reading of key, or nil.
func (g *resumeGate) read(key string, now time.Time) *resumeBase {
	g.mu.Lock()
	defer g.mu.Unlock()
	hit, ok := g.readings[key]
	if !ok || now.Sub(hit.at) >= resumeHistoryTTL {
		return nil
	}
	return hit.base
}

// keep remembers what b read, and forgets the readings that are stale, and
// then any one, over the bound.
func (g *resumeGate) keep(key string, b *resumeBase, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.readings == nil {
		g.readings = map[string]resumeReading{}
	}
	for k, r := range g.readings {
		if now.Sub(r.at) >= resumeHistoryTTL || len(g.readings) >= maxResumeReadings {
			delete(g.readings, k)
		}
	}
	g.readings[key] = resumeReading{at: now, base: &resumeBase{
		history: b.history, start: b.start, definition: b.definition, resumesUsed: b.resumesUsed, previous: b.previous, scope: b.scope,
	}}
}

// ctxError is a caller's own end.
func ctxError(ctx context.Context) error {
	code := connect.CodeCanceled
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		code = connect.CodeDeadlineExceeded
	}
	return connect.NewError(code, ctx.Err())
}
