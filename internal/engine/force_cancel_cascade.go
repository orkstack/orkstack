package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The cascade of a force cancel: every open run below its run has a force
// cancel of its own, which asks it to cancel, gives it its grace and ends it
// only if it stalls. One decision covers them all (AGENTS.md §7, "Force
// cancel"); each is started under a decision derived from it, which names that
// one run and the one execution it is a child of.

// The cascade's own local activities, run on the engine worker only.
const (
	ActivityFCBelow   = "ForceCancelBelow"
	ActivityFCDerive  = "ForceCancelDerive"
	ActivityFCOpen    = "ForceCancelOpen"
	ActivityFCAfter   = "ForceCancelAfter"
	ActivityFCSummary = "ForceCancelSummary"
)

const forceCancelCascaded = "run.cancel.force.cascade"

// The states of a cascade.
const (
	FCCascadeRunning  = "running"
	FCCascadeComplete = "complete"
	FCCascadePartial  = "partial"
	FCCascadeStopped  = "stopped"
)

// The states of a run below beside a force cancel's own.
const (
	FCBelowWaiting  = "waiting"
	FCBelowPending  = "pending"
	FCBelowAttached = "attached"
)

// The causes of a run below not being force-canceled, and of not knowing what
// is left open.
const (
	FCCauseChainUnreadable = "chain_unreadable"
	FCCauseNotBelow        = "not_below"
	FCCauseIDOccupied      = "id_occupied"
	FCCauseTooManyRuns     = "too_many_runs"
	FCCauseTookTooLong     = "took_too_long"
	FCCauseCouldNotRead    = "could_not_read"
)

// FCBelowList is a search's answer: the runs to cover, at most the cascade's
// own most, and how many more there are.
type FCBelowList struct {
	Rows []FCBelow `json:"rows,omitempty"`
	// Reached and Unreached are the runs past the most: those their parent's
	// cancel reaches, and those it does not.
	Reached   int               `json:"reached,omitempty"`
	Unreached int               `json:"unreached,omitempty"`
	Truncated string            `json:"truncated,omitempty"`
	Excluded  map[string]string `json:"excluded,omitempty"`
	ByRoot    int               `json:"by_root,omitempty"`
}

// FCAfter is what is still open below once a cascade has done what it does.
type FCAfter struct {
	Count       int      `json:"count,omitempty"`
	RunIDs      []string `json:"run_ids,omitempty"`
	Unconfirmed string   `json:"unconfirmed,omitempty"`
}

// FCBelowResult is what became of one run below.
type FCBelowResult struct {
	FCBelow
	State     string `json:"state"`
	RequestID string `json:"request_id,omitempty"`
	// Attached: another caller's force cancel of it was in flight.
	Attached bool   `json:"attached,omitempty"`
	Cause    string `json:"cause,omitempty"`
}

// final reports whether r needs nothing more.
func (r FCBelowResult) final() bool {
	switch r.State {
	case FCStateForceCanceled, FCStateCanceled, FCStateEnded, FCStateAlreadyEnded:
		return true
	}
	return false
}

// ForceCancelCascade is a cascade as it stands.
type ForceCancelCascade struct {
	State   string          `json:"state"`
	AsOf    time.Time       `json:"as_of"`
	Results []FCBelowResult `json:"results,omitempty"`
	// Covered are the runs past the most whose parent's cancel reaches them;
	// NotStarted, those it does not.
	Covered    int               `json:"covered,omitempty"`
	NotStarted int               `json:"not_started,omitempty"`
	ByRoot     int               `json:"by_root,omitempty"`
	Truncated  string            `json:"truncated,omitempty"`
	Excluded   map[string]string `json:"excluded,omitempty"`
	// Rerun: a later search found more, or was cut short.
	Rerun bool `json:"rerun,omitempty"`
	// Unrecorded: its record could not be written.
	Unrecorded bool `json:"unrecorded,omitempty"`
	// Cause is why a search could not be made.
	Cause string `json:"cause,omitempty"`
	// Unread: where it stands could not be asked.
	Unread bool     `json:"unread,omitempty"`
	After  *FCAfter `json:"after,omitempty"`
}

// verdict is the state of a cascade that reached its end.
func (c ForceCancelCascade) verdict(stopped bool) string {
	if stopped {
		return FCCascadeStopped
	}
	if c.After == nil || c.After.Count > 0 || c.After.Unconfirmed != "" || c.NotStarted > 0 || len(c.Excluded) > 0 {
		return FCCascadePartial
	}
	for _, r := range c.Results {
		if !r.final() {
			return FCCascadePartial
		}
	}
	return FCCascadeComplete
}

// FCBelowStep is a cascade's step on one run below.
type FCBelowStep struct {
	Input ForceCancelInput `json:"input"`
	RunID string           `json:"run_id"`
}

// FCOpenStep asks which of some runs are open.
type FCOpenStep struct {
	Input  ForceCancelInput `json:"input"`
	RunIDs []string         `json:"run_ids"`
}

// FCSummaryStep is a cascade's record.
type FCSummaryStep struct {
	Input   ForceCancelInput   `json:"input"`
	Cascade ForceCancelCascade `json:"cascade"`
}

// FCDerived is how to force-cancel a run below: the input and the id of a
// force cancel of its own, or what is true of it already.
type FCDerived struct {
	Input      ForceCancelInput `json:"input"`
	WorkflowID string           `json:"workflow_id,omitempty"`
	RequestID  string           `json:"request_id,omitempty"`
	Timeout    time.Duration    `json:"timeout,omitempty"`
	// Bound is how long it is waited for.
	Bound  time.Duration `json:"bound,omitempty"`
	Closed bool          `json:"closed,omitempty"`
	// Attached: one of it is in flight, under RequestID, for Bound at most.
	Attached bool   `json:"attached,omitempty"`
	Cause    string `json:"cause,omitempty"`
}

// cascading is acting for a step of a cascade: its decision has one.
func (a *ForceCancelActivities) cascading(ctx context.Context, in ForceCancelInput) (ForceCancelDecision, forceCancelSelf, error) {
	self, err := a.self(ctx)
	if err != nil {
		return ForceCancelDecision{}, self, err
	}
	d, err := a.actingAs(ctx, in, self)
	if err == nil && (!d.Cascade || in.Settings == nil) {
		err = errForceCancelRefused(FCCauseDecisionUnverified)
	}
	return d, self, err
}

// Below finds the open runs below the cascade's.
func (a *ForceCancelActivities) Below(ctx context.Context, in ForceCancelInput) (FCBelowList, error) {
	d, _, err := a.cascading(ctx, in)
	if err != nil {
		return FCBelowList{}, err
	}
	cascade := in.Settings.Cascade.bounded()
	found, err := a.reader.Subtree(ctx, d.Namespace, d.RunID, cascade)
	if err != nil {
		return FCBelowList{}, err
	}
	out := FCBelowList{Truncated: found.Truncated, ByRoot: found.ByRoot, Excluded: firstByName(found.Excluded, forceCancelExcludedMost)}
	for i, b := range found.Open {
		switch {
		case i < cascade.Max:
			out.Rows = append(out.Rows, b)
		case b.Reached:
			out.Reached++
		default:
			out.Unreached++
		}
	}
	return out, nil
}

// firstByName is at most n of m, the first by name.
func firstByName(m map[string]string, n int) map[string]string {
	if len(m) <= n {
		return m
	}
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := map[string]string{}
	for _, id := range ids[:n] {
		out[id] = m[id]
	}
	return out
}

// Derive is how to force-cancel one run below: it is held to be below by its
// own start and its parents', which nobody can change, never by the list it
// was named from; and the decision it answers is of this cascade, names that
// run as it is now and no other, and is good only in this execution's child.
func (a *ForceCancelActivities) Derive(ctx context.Context, in FCBelowStep) (FCDerived, error) {
	d, self, err := a.cascading(ctx, in.Input)
	if err != nil {
		return FCDerived{}, err
	}
	now, err := a.describe(ctx, in.RunID)
	var gone *serviceerror.NotFound
	if errors.As(err, &gone) {
		return FCDerived{Closed: true}, nil
	}
	if err != nil {
		return FCDerived{}, err
	}
	if cause, err := a.isBelow(ctx, d, in.RunID, now.ExecutionRunID); err != nil || cause != "" {
		return FCDerived{Cause: cause}, err
	}
	if !now.Open {
		return FCDerived{Closed: true}, nil
	}
	// Its own settings, not those of an input anyone may have written.
	s := a.starting()
	found, err := s.find(ctx, d.Namespace, in.RunID)
	if err != nil {
		slog.Warn("force cancel: whether one is of a run below could not be told; this one starts beside any", "run_id", in.RunID, "error", err)
	} else {
		for _, f := range found.join {
			if current, err := s.current(ctx, f.decision); err == nil && current {
				return FCDerived{Attached: true, RequestID: f.decision.RequestID, Bound: s.bound(f.decision)}, nil
			}
		}
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return FCDerived{}, fmt.Errorf("force cancel: a nonce: %w", err)
	}
	own, err := s.derived(d, in.RunID, now.ExecutionRunID, a.longestGrace(ctx, in.RunID))
	if err != nil {
		return FCDerived{}, err
	}
	if !own.IssuedAt.Before(own.ExpiresAt) {
		return FCDerived{Cause: FCCauseDecisionStale}, nil // the cascade's own is over
	}
	own.RequestID = d.RequestID + "-" + hex.EncodeToString(nonce)
	own.Of = &ForceCancelOf{RunID: d.RunID, WorkflowID: self.WorkflowID, Execution: self.RunID}
	input, err := s.input(own)
	if err != nil {
		return FCDerived{}, err
	}
	return FCDerived{Input: input, WorkflowID: own.WorkflowID(), RequestID: own.RequestID, Timeout: s.timeout(own), Bound: s.bound(own)}, nil
}

// bound is how long a force cancel under d is waited for: what it takes when
// nothing fails, and something over.
func (s *ForceCancelStarter) bound(d ForceCancelDecision) time.Duration {
	return d.Ceiling() + 2*d.Grace() + forceCancelSettleLeast
}

// starting is a starter over the activities' own dependencies.
func (a *ForceCancelActivities) starting() *ForceCancelStarter {
	return &ForceCancelStarter{client: a.client, namespace: a.namespace, reader: a.reader, audit: a.audit, starter: a.starter,
		keys: a.keys, mode: a.mode, settings: a.settings, now: a.now}
}

// isBelow is why runID is not a run below d's, "" when it is. Each run's start
// says which run started it, and the cluster which execution did, of which it
// is the child: that one is read, never another under the same id, which
// anyone may have started since to say what it likes.
func (a *ForceCancelActivities) isBelow(ctx context.Context, d ForceCancelDecision, runID, execution string) (string, error) {
	for hop := 0; hop < forceCancelTreeDepth; hop++ {
		first, err := a.firstEvent(ctx, runID, execution)
		var gone *serviceerror.NotFound
		if errors.As(err, &gone) {
			return FCCauseChainUnreadable, nil
		}
		if err != nil {
			return "", err
		}
		namespace, origin, err := a.reader.startedBy(first)
		if err != nil {
			return FCCauseChainUnreadable, nil
		}
		started := first.GetWorkflowExecutionStartedEventAttributes()
		parent := started.GetParentWorkflowExecution()
		if started.GetWorkflowType().GetName() != RunWorkflowType || artifacts.ResolveNamespace(namespace) != artifacts.ResolveNamespace(d.Namespace) ||
			origin == nil || origin.Kind != RunOriginKindParentRun || origin.ID == "" || parent.GetWorkflowId() != origin.ID || parent.GetRunId() == "" {
			return FCCauseNotBelow, nil
		}
		if origin.ID == d.RunID {
			return "", nil
		}
		runID, execution = parent.GetWorkflowId(), parent.GetRunId()
	}
	return FCCauseNotBelow, nil
}

// longestGrace is the longest cancellation grace of a run's own tasks, by the
// definition it was started with.
func (a *ForceCancelActivities) longestGrace(ctx context.Context, runID string) time.Duration {
	ctx, cancel := context.WithTimeout(ctx, forceCancelDefinitionTimeout)
	defer cancel()
	return a.reader.longestTaskGrace(ctx, runID)
}

// Open is which of some runs are open, or may be.
func (a *ForceCancelActivities) Open(ctx context.Context, in FCOpenStep) ([]string, error) {
	if _, _, err := a.cascading(ctx, in.Input); err != nil {
		return nil, err
	}
	// One that cannot be told is not known to have closed.
	open, failed := a.openOf(ctx, in.RunIDs)
	open = append(open, failed...)
	sort.Strings(open)
	return open, nil
}

// forceCancelDescribes is how many runs are described at once.
const forceCancelDescribes = 20

// openOf is which of ids are open, and which could not be told.
func (a *ForceCancelActivities) openOf(ctx context.Context, ids []string) (open, failed []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	slots := make(chan struct{}, forceCancelDescribes)
	for _, id := range ids {
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer func() { <-slots; wg.Done() }()
			now, err := a.describe(ctx, id)
			var gone *serviceerror.NotFound
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.As(err, &gone):
			case err != nil:
				failed = append(failed, id)
			case now.Open:
				open = append(open, id)
			}
		}()
	}
	wg.Wait()
	sort.Strings(open)
	return open, failed
}

// After is what is open below now: what a search finds, each confirmed by the
// cluster's own record of it, which the index may lag. It never fails: what
// cannot be told is said.
func (a *ForceCancelActivities) After(ctx context.Context, in FCOpenStep) (FCAfter, error) {
	d, _, err := a.cascading(ctx, in.Input)
	if err != nil {
		return FCAfter{}, err
	}
	cascade := in.Input.Settings.Cascade.bounded()
	found, err := a.reader.Subtree(ctx, d.Namespace, d.RunID, cascade)
	if err != nil {
		return FCAfter{Unconfirmed: FCCauseCouldNotRead}, nil
	}
	ids, seen := []string{}, map[string]bool{}
	for _, b := range found.Open {
		ids, seen[b.RunID] = append(ids, b.RunID), true
	}
	for _, id := range in.RunIDs {
		if !seen[id] {
			ids, seen[id] = append(ids, id), true
		}
	}
	open, failed := a.openOf(ctx, ids)
	out := FCAfter{Count: len(open), RunIDs: open[:min(len(open), cascade.Max)]}
	switch {
	case found.Truncated == "timeout", ctx.Err() != nil:
		out.Unconfirmed = FCCauseTookTooLong
	case found.Truncated != "":
		out.Unconfirmed = FCCauseTooManyRuns
	case len(failed) > 0:
		out.Unconfirmed = FCCauseCouldNotRead
	}
	return out, nil
}

// Summary records a cascade, once.
func (a *ForceCancelActivities) Summary(ctx context.Context, in FCSummaryStep) error {
	d, _, err := a.cascading(ctx, in.Input)
	if err != nil {
		return err
	}
	in.Cascade = *in.Cascade.shown()
	if a.audit == nil {
		return errors.New("force cancel: no audit store")
	}
	return a.put(ctx, &orkstackv1.AuditRecord{
		Id: forceCancelAuditID(forceCancelCascaded, d.Namespace, d.RunID, d.RequestID, d.IssuedAt), At: timestamppb.New(d.IssuedAt),
		Principal: d.Principal, PrincipalVerified: d.PrincipalVerified, AuthMethod: d.AuthMethod, PrincipalIssuer: d.PrincipalIssuer,
		Action: forceCancelCascaded, Namespace: d.Namespace, Target: d.RunID, Permission: d.Permission, Scope: d.Scope, Rule: d.Rule,
		Succeeded: in.Cascade.State == FCCascadeComplete, Detail: "request " + d.RequestID + ", at " + a.now().UTC().Format(time.RFC3339) + ": " + in.Cascade.detail(),
	})
}

// detail is a cascade in one line, for its record.
func (c ForceCancelCascade) detail() string {
	counts := map[string]int{}
	for _, r := range c.Results {
		switch r.State {
		case FCStateForceCanceled, FCStateCanceled, FCStateEnded, FCStateAlreadyEnded, FCStateFailed, FCBelowPending:
			counts[r.State]++
		default:
			counts["unknown"]++
		}
	}
	states := make([]string, 0, len(counts))
	for state := range counts {
		states = append(states, state)
	}
	sort.Strings(states)
	parts := []string{"unknown"}
	switch c.State {
	case FCCascadeComplete, FCCascadePartial, FCCascadeStopped:
		parts[0] = c.State
	}
	for _, state := range states {
		parts = append(parts, fmt.Sprintf("%s=%d", state, counts[state]))
	}
	for _, p := range []struct {
		name string
		n    int
	}{{"left to their parents' cancel", c.Covered}, {"not started", c.NotStarted}, {"excluded", len(c.Excluded)}} {
		if p.n > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", p.name, p.n))
		}
	}
	switch {
	case c.After == nil:
		parts = append(parts, "what is open after it was not looked at")
	case c.After.Unconfirmed != "":
		parts = append(parts, fmt.Sprintf("open after it=%d, unconfirmed (%s)", c.After.Count, c.After.Unconfirmed))
	default:
		parts = append(parts, fmt.Sprintf("open after it=%d", c.After.Count))
	}
	return strings.Join(parts, ", ")
}

// bounded is s within what one answer of a step may hold, whatever the input
// says.
func (s ForceCancelCascadeSettings) bounded() ForceCancelCascadeSettings {
	s.Max = min(max(s.Max, 1), forceCancelCascadeMost)
	s.Scan = min(max(s.Scan, 1), forceCancelScanMax)
	s.WalkScan = min(max(s.WalkScan, 1), forceCancelScanMax)
	s.Batch = min(max(s.Batch, 1), 100)
	s.StartsPerTask = min(max(s.StartsPerTask, 1), 100)
	s.Poll = min(max(s.Poll, time.Second), time.Minute)
	s.ListTimeout = min(max(s.ListTimeout, time.Second), time.Minute)
	s.SettleSlack = min(max(s.SettleSlack, 0), 10*time.Minute)
	return s
}

// forceCancelCascadeMost is the most runs one cascade may cover: a search's
// answer names each, and stays a small payload.
const (
	forceCancelCascadeMost  = 300
	forceCancelExcludedMost = 50
)
