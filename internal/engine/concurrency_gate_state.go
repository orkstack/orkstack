package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// The concurrency gate's state and decisions (E88 §5.1.2–§5.1.5), as pure
// functions of the state and the instant they are given. Nothing here reads
// a clock, issues a command or touches the workflow API:
// ConcurrencyGateWorkflow passes workflow.Now and turns what these return
// into signals, timers and local activities, so every rule is unit-tested and
// fuzzed apart from Temporal.

// gateProto is the gate protocol's version (§5.1.3). A payload naming a newer
// one is refused; every earlier one is accepted.
const gateProto = 1

// The strategies a limit declares (§3). P2's gate decides queue, refuse and
// skip; cancel_oldest and replace are refused by the validator until the
// gate supersedes (E88 P8).
const (
	gateStrategyQueue        = "queue"
	gateStrategyRefuse       = "refuse"
	gateStrategySkip         = "skip"
	gateStrategyCancelOldest = "cancel_oldest"
	gateStrategyReplace      = "replace"
)

// The answers to an acquire (§5.1.4).
const (
	gateAnswerGranted     = "granted"
	gateAnswerQueued      = "queued"
	gateAnswerFenced      = "fenced"
	gateAnswerSuperseded  = "superseded"
	gateAnswerUnavailable = "unavailable"
	gateAnswerFull        = "full"
	gateAnswerFullBound   = "full_bound"
)

// gateRunLevel is a run-level entry's task index.
const gateRunLevel = -1

// The gate's rebuild states (§5.1.6).
const (
	gateRebuildNone     = ""         // no rebuild: a clean start, or one finished
	gateRebuildListing  = "listing"  // ListConcurrencyEntries is being read
	gateRebuildSettling = "settling" // restored; grants wait out rebuild_settle
	gateRebuildFailing  = "failing"  // past rebuild_timeout, still retrying
)

// The gate's phases, as its OrkflowGatePhase attribute says them.
const (
	gatePhaseActive    = "active"
	gatePhaseLingering = "lingering"
)

// gateCarryBound is the most a gate's encoded state may take (§5.1.9): the
// boot refuses tunables whose worst case is larger, so a continue-as-new
// carry stays inside the 256 KiB payload bound with room for its envelope.
const gateCarryBound = 224 << 10

// GateTunables are the gate's own input (§5.1.8, §9.6 "gate input"): decided
// by the acquire that starts it and carried by every continue-as-new, so a
// gate never reads configuration.
type GateTunables struct {
	MaxEntries          int           `json:"max_entries"`
	MaxTombstones       int           `json:"max_tombstones"`
	MaxLabels           int           `json:"max_labels"`
	AcquireTimeout      time.Duration `json:"acquire_timeout"`
	ClockGrace          time.Duration `json:"clock_grace"`
	TombstoneMargin     time.Duration `json:"tombstone_margin"`
	VerifyMinAge        time.Duration `json:"verify_min_age"`
	VerifyDebounce      time.Duration `json:"verify_debounce"`
	VerifyAwait         time.Duration `json:"verify_await"`
	VerifyParallel      int           `json:"verify_parallel"`
	VerifyBatch         int           `json:"verify_batch"`
	VerifyTimeout       time.Duration `json:"verify_timeout"`
	TaskAckGrace        time.Duration `json:"task_ack_grace"`
	TaskConfirmBatch    int           `json:"task_confirm_batch"`
	StalledAfter        time.Duration `json:"stalled_after"`
	HolderSweepInterval time.Duration `json:"holder_sweep_interval"`
	GrantBackoffMin     time.Duration `json:"grant_backoff_min"`
	GrantBackoffMax     time.Duration `json:"grant_backoff_max"`
	RebuildPageTimeout  time.Duration `json:"rebuild_page_timeout"`
	RebuildSettle       time.Duration `json:"rebuild_settle"`
	RebuildBackoffMin   time.Duration `json:"rebuild_backoff_min"`
	RebuildBackoffMax   time.Duration `json:"rebuild_backoff_max"`
	RebuildTimeout      time.Duration `json:"rebuild_timeout"`
	FreezeTimeout       time.Duration `json:"freeze_timeout"`
	DrainMargin         time.Duration `json:"drain_margin"`
	ContinueEvents      int           `json:"gate_continue_events"`
	ContinueUpdates     int           `json:"gate_continue_updates"`
	Linger              time.Duration `json:"gate_linger"`
}

// DefaultGateTunables are §9.6's defaults.
func DefaultGateTunables() GateTunables {
	return GateTunables{
		MaxEntries:          500,
		MaxTombstones:       1000,
		MaxLabels:           32,
		AcquireTimeout:      2 * time.Minute,
		ClockGrace:          time.Minute,
		TombstoneMargin:     time.Minute,
		VerifyMinAge:        10 * time.Second,
		VerifyDebounce:      5 * time.Second,
		VerifyAwait:         10 * time.Second,
		VerifyParallel:      16,
		VerifyBatch:         64,
		VerifyTimeout:       30 * time.Second,
		TaskAckGrace:        2 * time.Minute,
		TaskConfirmBatch:    8,
		StalledAfter:        10 * time.Minute,
		HolderSweepInterval: 10 * time.Minute,
		GrantBackoffMin:     time.Second,
		GrantBackoffMax:     30 * time.Second,
		RebuildPageTimeout:  30 * time.Second,
		RebuildSettle:       30 * time.Second,
		RebuildBackoffMin:   time.Second,
		RebuildBackoffMax:   5 * time.Minute,
		RebuildTimeout:      10 * time.Minute,
		FreezeTimeout:       time.Minute,
		DrainMargin:         5 * time.Second,
		ContinueEvents:      2000,
		ContinueUpdates:     500,
		Linger:              5 * time.Minute,
	}
}

// tombstoneLife is how long a tombstone fences a late acquire (§5.1.4).
func (t GateTunables) tombstoneLife() time.Duration {
	return t.AcquireTimeout + t.ClockGrace + t.TombstoneMargin
}

// drainBound is how long a drain waits for its handlers (§5.1.7, G2).
func (t GateTunables) drainBound() time.Duration { return t.VerifyAwait + t.DrainMargin }

// gateDigest is the lowercase hex SHA-256 of parts joined by a NUL byte
// (decision G6), so no part can run into the next.
func gateDigest(parts ...string) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// GateID is the workflow id of one namespace incarnation's gate for key
// (§5.1.1). incarnation is the namespace's created_at in RFC 3339 UTC, empty
// for default, so a namespace deleted and created again starts with fresh
// gates.
func GateID(namespace, incarnation, key string) string {
	return "ork-conc-" + gateDigest(namespace, incarnation, key)[:32]
}

// slotTombKey and runTombKey are a tombstone's identity (§5.1.2).
func slotTombKey(run string, seqBase int64, taskIx int) string {
	return gateDigest(run, strconv.FormatInt(seqBase, 10), strconv.Itoa(taskIx))[:16]
}

func runTombKey(run string) string { return gateDigest(run)[:16] }

// gateEntry is one run's or task's place at a key: a holder or a queued
// entry (§5.1.2).
type gateEntry struct {
	Run       string    `json:"run"`
	TaskIx    int       `json:"task_ix"`
	SeqBase   int64     `json:"seq_base"`
	Limit     int       `json:"limit"`
	Strategy  string    `json:"strategy"`
	Proto     int       `json:"proto"`
	LabelIx   int       `json:"label_ix"`
	ArrivedAt time.Time `json:"arrived_at"`
	Seq       int64     `json:"seq"`
	// Queued entries only.
	WaitUntil time.Time `json:"wait_until,omitzero"`
	// Holders only.
	GrantedAt     time.Time `json:"granted_at,omitzero"`
	VerifyPending bool      `json:"verify_pending,omitempty"`
	Adopted       bool      `json:"adopted,omitempty"`
	// GrantPending: granted from the line, and the run not yet signalled.
	GrantPending bool `json:"grant_pending,omitempty"`
	// Liveness marks (§5.1.5): when its memo key was first seen missing,
	// and whether a pass found it stalled.
	MissingSince time.Time `json:"missing_since,omitzero"`
	Stalled      bool      `json:"stalled,omitempty"`
}

func (e gateEntry) same(run string, seqBase int64, taskIx int) bool {
	return e.Run == run && e.SeqBase == seqBase && e.TaskIx == taskIx
}

// before orders the line: by arrival, then by the gate's own sequence.
func (e gateEntry) before(arrivedAt time.Time, seq int64) bool {
	if !e.ArrivedAt.Equal(arrivedAt) {
		return e.ArrivedAt.Before(arrivedAt)
	}
	return e.Seq < seq
}

// gateTomb fences a released slot, or (RunLevel) a run's incarnations below
// Below (§5.1.2).
type gateTomb struct {
	Key       string    `json:"key"`
	RunLevel  bool      `json:"run,omitempty"`
	Below     int64     `json:"below,omitempty"`
	Until     time.Time `json:"until"`
	RemovedBy string    `json:"removed_by,omitempty"`
}

// gateRebuild is the rebuild's progress (§5.1.6).
type gateRebuild struct {
	State    string    `json:"state,omitempty"`
	Since    time.Time `json:"since,omitzero"`
	Attempts int       `json:"attempts,omitempty"`
}

// gateState is everything a gate carries across continue-as-new (§5.1.2).
type gateState struct {
	Labels             []string     `json:"labels,omitempty"`
	Holders            []gateEntry  `json:"holders,omitempty"`
	Queue              []gateEntry  `json:"queue,omitempty"`
	Tombstones         []gateTomb   `json:"tombstones,omitempty"`
	Seq                int64        `json:"seq"`
	Epoch              int64        `json:"epoch"`
	Phase              string       `json:"phase,omitempty"`
	Draining           bool         `json:"draining,omitempty"`
	FrozenUntil        time.Time    `json:"frozen_until,omitzero"`
	Rebuild            gateRebuild  `json:"rebuild,omitzero"`
	FenceDegradedUntil time.Time    `json:"fence_degraded_until,omitzero"`
	ResetRequestID     string       `json:"reset_request_id,omitempty"`
	LastPassAt         time.Time    `json:"last_pass_at,omitzero"`
	Tunables           GateTunables `json:"tunables"`
}

// gateAcquire is an acquire's payload (§5.1.4). ArrivedAt and Seq are set
// only on a re-assert, from the answer the run was given first.
type gateAcquire struct {
	Proto     int       `json:"proto"`
	Run       string    `json:"run"`
	SeqBase   int64     `json:"seq_base"`
	TaskIx    int       `json:"task_ix"`
	Limit     int       `json:"limit"`
	Strategy  string    `json:"strategy"`
	Label     string    `json:"label"`
	WaitUntil time.Time `json:"wait_until"`
	IssuedAt  time.Time `json:"issued_at"`
	ArrivedAt time.Time `json:"arrived_at,omitzero"`
	Seq       int64     `json:"seq,omitempty"`
}

// gateAnswer is an acquire's reply.
type gateAnswer struct {
	Proto        int       `json:"proto"`
	Answer       string    `json:"answer"`
	Seq          int64     `json:"seq,omitempty"`
	ArrivedAt    time.Time `json:"arrived_at,omitzero"`
	Position     int       `json:"position,omitempty"`
	HoldersAhead int       `json:"holders_ahead,omitempty"`
	By           string    `json:"by,omitempty"`
	Strategy     string    `json:"strategy,omitempty"`
	Epoch        int64     `json:"epoch"`
}

// gateStrategyDecided reports a strategy this gate decides.
func gateStrategyDecided(s string) bool {
	return s == gateStrategyQueue || s == gateStrategyRefuse || s == gateStrategySkip
}

// gateRejection is a validator's refusal: Protocol is never retried,
// Retryable is retried by the caller's backoff.
type gateRejection struct {
	Protocol  bool
	Retryable bool
	Reason    string
}

func (r *gateRejection) Error() string { return r.Reason }

// validateAcquire is the update validator's rule (§5.1.3): it rejects before
// anything is written to history.
func (g *gateState) validateAcquire(req gateAcquire, now time.Time) error {
	switch {
	case req.Proto > gateProto:
		return &gateRejection{Protocol: true, Reason: fmt.Sprintf("acquire speaks protocol %d; this gate speaks %d", req.Proto, gateProto)}
	case req.Proto < 1:
		return &gateRejection{Protocol: true, Reason: fmt.Sprintf("acquire names no protocol (%d)", req.Proto)}
	case !gateStrategyDecided(req.Strategy):
		return &gateRejection{Protocol: true, Reason: fmt.Sprintf("strategy %q is not decided by this gate", req.Strategy)}
	case len(req.Run) > gateWorstRunIDBytes:
		return &gateRejection{Protocol: true, Reason: fmt.Sprintf("acquire's run id is %d bytes; at most %d are carried", len(req.Run), gateWorstRunIDBytes)}
	case gateEncodedLen(req.Label) > gateWorstLabelBytes:
		return &gateRejection{Protocol: true, Reason: fmt.Sprintf("acquire's limit template takes %d bytes encoded; at most %d are carried", gateEncodedLen(req.Label), gateWorstLabelBytes)}
	case req.Run == "" || req.SeqBase < 1 || req.Limit < 1 || req.TaskIx < gateRunLevel:
		return &gateRejection{Protocol: true, Reason: fmt.Sprintf("acquire for run %q, seq_base %d, task %d, limit %d is malformed", req.Run, req.SeqBase, req.TaskIx, req.Limit)}
	case g.Draining:
		return &gateRejection{Retryable: true, Reason: "the gate is carrying its state on"}
	case g.frozen(now):
		return &gateRejection{Retryable: true, Reason: "the gate is frozen for a reset"}
	}
	return nil
}

func (g *gateState) frozen(now time.Time) bool {
	return !g.FrozenUntil.IsZero() && now.Before(g.FrozenUntil)
}

func (g *gateState) rebuilding() bool {
	return g.Rebuild.State == gateRebuildListing || g.Rebuild.State == gateRebuildFailing
}

// settling reports that grants wait out rebuild_settle (§5.1.6).
func (g *gateState) settling(now time.Time) bool {
	return g.Rebuild.State == gateRebuildSettling && now.Before(g.Rebuild.Since.Add(g.Tunables.RebuildSettle))
}

// grantsOff reports that nothing may be granted now.
func (g *gateState) grantsOff(now time.Time) bool {
	return g.rebuilding() || g.settling(now) || g.frozen(now)
}

// capacity is how many holders the key admits for an entry declaring limit:
// the strictest of its own and every holder's (§5.1.4 step 7).
func (g *gateState) capacity(limit int) int {
	c := limit
	for _, h := range g.Holders {
		c = min(c, h.Limit)
	}
	return c
}

func (g *gateState) holderIx(run string, seqBase int64, taskIx int) int {
	return slices.IndexFunc(g.Holders, func(e gateEntry) bool { return e.same(run, seqBase, taskIx) })
}

func (g *gateState) queueIx(run string, seqBase int64, taskIx int) int {
	return slices.IndexFunc(g.Queue, func(e gateEntry) bool { return e.same(run, seqBase, taskIx) })
}

// tombFor is the live tombstone that fences (run, seqBase, taskIx), if any.
func (g *gateState) tombFor(run string, seqBase int64, taskIx int, now time.Time) (gateTomb, bool) {
	slot, whole := slotTombKey(run, seqBase, taskIx), runTombKey(run)
	for _, t := range g.Tombstones {
		if !now.Before(t.Until) {
			continue
		}
		if (!t.RunLevel && t.Key == slot) || (t.RunLevel && t.Key == whole && t.Below > seqBase) {
			return t, true
		}
	}
	return gateTomb{}, false
}

// checkDue reports that a check pass is due (§5.1.4 step 8).
func (g *gateState) checkDue(now time.Time) bool {
	if !g.LastPassAt.IsZero() && now.Sub(g.LastPassAt) < g.Tunables.VerifyDebounce {
		return false
	}
	for _, h := range g.Holders {
		if h.VerifyPending || now.Sub(h.GrantedAt) > g.Tunables.VerifyMinAge {
			return true
		}
	}
	return false
}

// labelIx is label's index, adding it while there is room; past max_labels
// it is _other's, the index after the last (§5.1.2).
func (g *gateState) labelIx(label string) int {
	if i := slices.Index(g.Labels, label); i >= 0 {
		return i
	}
	if len(g.Labels) < g.Tunables.MaxLabels {
		g.Labels = append(g.Labels, label)
		return len(g.Labels) - 1
	}
	return g.Tunables.MaxLabels
}

// gateDecision is decideAcquire's outcome: an answer, or a check pass the
// gate runs before deciding again with afterPass set.
type gateDecision struct {
	Answer    gateAnswer
	WantsPass bool
	// Granted: this decision moved the acquirer into the holders. The run
	// learns it from the answer, not from a grant signal.
	Granted bool
}

// decideAcquire is §5.1.4's order. A pass request leaves g unchanged; the
// gate runs the pass and decides again with afterPass set.
func (g *gateState) decideAcquire(req gateAcquire, now time.Time, afterPass bool) gateDecision {
	answer := func(a string) gateDecision {
		return gateDecision{Answer: gateAnswer{Proto: gateProto, Answer: a, Epoch: g.Epoch}}
	}
	// 1. Stale.
	if now.Sub(req.IssuedAt) > g.Tunables.AcquireTimeout+g.Tunables.ClockGrace {
		return answer(gateAnswerFenced)
	}
	// 2. Tombstoned.
	if t, ok := g.tombFor(req.Run, req.SeqBase, req.TaskIx, now); ok {
		if t.RemovedBy != "" {
			d := answer(gateAnswerSuperseded)
			d.Answer.By, d.Answer.Strategy = t.RemovedBy, req.Strategy
			return d
		}
		return answer(gateAnswerFenced)
	}
	// 3. Already a holder.
	if i := g.holderIx(req.Run, req.SeqBase, req.TaskIx); i >= 0 {
		d := answer(gateAnswerGranted)
		d.Answer.Seq, d.Answer.ArrivedAt = g.Holders[i].Seq, g.Holders[i].ArrivedAt
		return d
	}
	// 4. Already queued.
	if i := g.queueIx(req.Run, req.SeqBase, req.TaskIx); i >= 0 {
		d := g.queuedAnswer(i)
		d.WantsPass = !afterPass && g.checkDue(now)
		return d
	}
	reassert := !req.ArrivedAt.IsZero() && req.Seq > 0
	// 5. Rebuilding, or settling after one: refuse and skip cannot be told
	// the key is full when the gate does not yet know.
	if g.rebuilding() || g.settling(now) {
		if req.Strategy == gateStrategyRefuse || req.Strategy == gateStrategySkip {
			return answer(gateAnswerUnavailable)
		}
		return g.enqueue(req, now, reassert)
	}
	admissible := len(g.Holders) < g.capacity(req.Limit) && !g.grantsOff(now)
	// 6. A re-assert takes its place in the line, then step 7.
	if reassert {
		if admissible && g.placeOf(req.ArrivedAt, req.Seq) == 0 {
			return g.grant(req, now, req.ArrivedAt, req.Seq)
		}
		return g.enqueue(req, now, true)
	}
	// 7. Admissible, with nobody ahead.
	if admissible && len(g.Queue) == 0 {
		return g.grant(req, now, gateArrival(now), g.Seq+1)
	}
	// 8. Full, with a check due.
	if !afterPass && len(g.Holders) >= g.capacity(req.Limit) && g.checkDue(now) {
		return gateDecision{WantsPass: true}
	}
	// 9. Still full.
	if req.Strategy == gateStrategyRefuse || req.Strategy == gateStrategySkip {
		return answer(gateAnswerFull)
	}
	return g.enqueue(req, now, false)
}

// placeOf is where an entry arriving at (arrivedAt, seq) goes in the line.
func (g *gateState) placeOf(arrivedAt time.Time, seq int64) int {
	for i, q := range g.Queue {
		if !q.before(arrivedAt, seq) {
			return i
		}
	}
	return len(g.Queue)
}

func (g *gateState) grant(req gateAcquire, now, arrivedAt time.Time, seq int64) gateDecision {
	g.Holders = append(g.Holders, gateEntry{
		Run: req.Run, TaskIx: req.TaskIx, SeqBase: req.SeqBase, Limit: req.Limit,
		Strategy: req.Strategy, Proto: req.Proto, LabelIx: g.labelIx(req.Label),
		ArrivedAt: arrivedAt, Seq: seq, GrantedAt: now,
		VerifyPending: now.Before(g.FenceDegradedUntil),
	})
	g.Seq = max(g.Seq, seq)
	return gateDecision{
		Answer:  gateAnswer{Proto: gateProto, Answer: gateAnswerGranted, Seq: seq, ArrivedAt: arrivedAt, Epoch: g.Epoch},
		Granted: true,
	}
}

// enqueue puts req in the line: a new arrival at the back, bounded by
// max_entries; a re-assert at its place, unbounded (§5.1.4 steps 6, 9).
func (g *gateState) enqueue(req gateAcquire, now time.Time, reassert bool) gateDecision {
	arrivedAt, seq := req.ArrivedAt, req.Seq
	if !reassert {
		if len(g.Holders)+len(g.Queue) >= g.Tunables.MaxEntries {
			return gateDecision{Answer: gateAnswer{Proto: gateProto, Answer: gateAnswerFullBound, Epoch: g.Epoch}}
		}
		arrivedAt, seq = gateArrival(now), g.Seq+1
	}
	at := g.placeOf(arrivedAt, seq)
	g.Queue = slices.Insert(g.Queue, at, gateEntry{
		Run: req.Run, TaskIx: req.TaskIx, SeqBase: req.SeqBase, Limit: req.Limit,
		Strategy: req.Strategy, Proto: req.Proto, LabelIx: g.labelIx(req.Label),
		ArrivedAt: arrivedAt, Seq: seq, WaitUntil: req.WaitUntil,
	})
	g.Seq = max(g.Seq, seq)
	return g.queuedAnswer(at)
}

func (g *gateState) queuedAnswer(i int) gateDecision {
	e := g.Queue[i]
	return gateDecision{Answer: gateAnswer{
		Proto: gateProto, Answer: gateAnswerQueued, Seq: e.Seq, ArrivedAt: e.ArrivedAt,
		Position: i + 1, HoldersAhead: len(g.Holders), Epoch: g.Epoch,
	}}
}

// admitHeads moves every admissible head of the line into the holders
// (§5.1.4 "Grant") and returns them, for the gate to signal.
func (g *gateState) admitHeads(now time.Time) []gateEntry {
	if g.grantsOff(now) {
		return nil
	}
	var granted []gateEntry
	for len(g.Queue) > 0 && len(g.Holders) < g.capacity(g.Queue[0].Limit) {
		h := g.Queue[0]
		g.Queue = g.Queue[1:]
		h.WaitUntil, h.GrantedAt, h.GrantPending = time.Time{}, now, true
		h.VerifyPending = now.Before(g.FenceDegradedUntil)
		g.Holders = append(g.Holders, h)
		granted = append(granted, h)
	}
	return granted
}

// release removes (run, seqBase, taskIx) and fences it (§5.1.4). It is
// idempotent: a second release only refreshes the tombstone.
func (g *gateState) release(run string, seqBase int64, taskIx int, now time.Time) {
	g.Holders = slices.DeleteFunc(g.Holders, func(e gateEntry) bool { return e.same(run, seqBase, taskIx) })
	g.Queue = slices.DeleteFunc(g.Queue, func(e gateEntry) bool { return e.same(run, seqBase, taskIx) })
	g.addTomb(gateTomb{Key: slotTombKey(run, seqBase, taskIx), Until: now.Add(g.Tunables.tombstoneLife())}, now)
}

// releaseAll removes every entry of run whose seq_base is below below, and
// writes or raises the run's tombstone, keeping the largest below.
func (g *gateState) releaseAll(run string, below int64, now time.Time) {
	drop := func(e gateEntry) bool { return e.Run == run && e.SeqBase < below }
	g.Holders = slices.DeleteFunc(g.Holders, drop)
	g.Queue = slices.DeleteFunc(g.Queue, drop)
	key := runTombKey(run)
	if i := slices.IndexFunc(g.Tombstones, func(t gateTomb) bool { return t.RunLevel && t.Key == key && now.Before(t.Until) }); i >= 0 {
		below = max(below, g.Tombstones[i].Below)
	}
	g.addTomb(gateTomb{Key: key, RunLevel: true, Below: below, Until: now.Add(g.Tunables.tombstoneLife())}, now)
}

// addTomb writes t in place of any tombstone of the same identity, dropping
// expired ones first and then the oldest while over max_tombstones;
// evicting a live one degrades the fence until it would have expired
// (§5.1.4).
func (g *gateState) addTomb(t gateTomb, now time.Time) {
	g.Tombstones = slices.DeleteFunc(g.Tombstones, func(o gateTomb) bool {
		return !now.Before(o.Until) || (o.Key == t.Key && o.RunLevel == t.RunLevel)
	})
	g.Tombstones = append(g.Tombstones, t)
	for len(g.Tombstones) > g.Tunables.MaxTombstones {
		evicted := g.Tombstones[0]
		g.Tombstones = g.Tombstones[1:]
		if evicted.Until.After(g.FenceDegradedUntil) {
			g.FenceDegradedUntil = evicted.Until
		}
	}
}

// empty reports nothing held or queued.
func (g *gateState) empty() bool { return len(g.Holders) == 0 && len(g.Queue) == 0 }

// phaseNow is the phase the gate's attribute should say.
func (g *gateState) phaseNow() string {
	if g.empty() {
		return gatePhaseLingering
	}
	return gatePhaseActive
}

// dropExpiredQueued removes queued entries whose wait_until has passed
// (§5.1.5); their runs learn it from their own deadline, not from the gate.
func (g *gateState) dropExpiredQueued(now time.Time) {
	g.Queue = slices.DeleteFunc(g.Queue, func(e gateEntry) bool {
		return !e.WaitUntil.IsZero() && !now.Before(e.WaitUntil)
	})
}

// gatePassEvents is what one check pass costs a gate's history (§5.1.10):
// the boot refuses a continuation threshold below four of them.
const gatePassEvents = 12

// gateWorstRunIDBytes and gateWorstLabelBytes size the worst-case carry and
// are what the acquire validator holds a run id and a template to: a run id
// at its longest (a new one is 30 bytes, child runs included; one an earlier
// release minted, ork-{timestamp}-{sequence}, a little more) and a declared
// template's JSON encoding at its longest, escapes included.
const (
	gateWorstRunIDBytes = 48
	gateWorstLabelBytes = 512
)

// Check is the boot's refusal of tunables a gate could not run on (§5.1.8,
// §5.1.9, G13), each naming its setting: a bound below one, a threshold a
// pass would cross, a linger within a tombstone's life (a clean start would
// then grant past a live fence), and a worst-case carry over the bound.
func (t GateTunables) Check() error {
	for _, c := range []struct {
		name string
		v    int
	}{
		{"max_entries", t.MaxEntries}, {"max_tombstones", t.MaxTombstones}, {"max_labels", t.MaxLabels},
		{"verify_parallel", t.VerifyParallel}, {"verify_batch", t.VerifyBatch}, {"task_confirm_batch", t.TaskConfirmBatch},
	} {
		if c.v < 1 {
			return fmt.Errorf("engine.concurrency.%s is %d; it must be at least 1", c.name, c.v)
		}
	}
	durations := map[string]time.Duration{
		"acquire_timeout": t.AcquireTimeout, "clock_grace": t.ClockGrace, "tombstone_margin": t.TombstoneMargin,
		"verify_min_age": t.VerifyMinAge, "verify_debounce": t.VerifyDebounce, "verify_await": t.VerifyAwait,
		"verify_timeout": t.VerifyTimeout, "task_ack_grace": t.TaskAckGrace, "stalled_after": t.StalledAfter,
		"holder_sweep_interval": t.HolderSweepInterval, "grant_backoff_min": t.GrantBackoffMin,
		"grant_backoff_max": t.GrantBackoffMax, "rebuild_page_timeout": t.RebuildPageTimeout,
		"rebuild_settle": t.RebuildSettle, "rebuild_backoff_min": t.RebuildBackoffMin,
		"rebuild_backoff_max": t.RebuildBackoffMax, "rebuild_timeout": t.RebuildTimeout,
		"freeze_timeout": t.FreezeTimeout, "drain_margin": t.DrainMargin, "gate_linger": t.Linger,
	}
	for _, name := range slices.Sorted(maps.Keys(durations)) {
		if v := durations[name]; v <= 0 {
			return fmt.Errorf("engine.concurrency.%s is %s; it must be above zero", name, v)
		}
	}
	switch {
	case t.GrantBackoffMax < t.GrantBackoffMin:
		return fmt.Errorf("engine.concurrency.grant_backoff_max (%s) is below grant_backoff_min (%s)", t.GrantBackoffMax, t.GrantBackoffMin)
	case t.RebuildBackoffMax < t.RebuildBackoffMin:
		return fmt.Errorf("engine.concurrency.rebuild_backoff_max (%s) is below rebuild_backoff_min (%s)", t.RebuildBackoffMax, t.RebuildBackoffMin)
	case t.ContinueEvents < 4*gatePassEvents:
		return fmt.Errorf("engine.concurrency.gate_continue_events is %d; it must be at least %d, four check passes", t.ContinueEvents, 4*gatePassEvents)
	case t.ContinueUpdates < 4:
		return fmt.Errorf("engine.concurrency.gate_continue_updates is %d; it must be at least 4", t.ContinueUpdates)
	case t.Linger <= t.tombstoneLife():
		return fmt.Errorf("engine.concurrency.gate_linger (%s) must be above acquire_timeout + clock_grace + tombstone_margin (%s): "+
			"a gate closed sooner could reopen clean while a release's fence still stands", t.Linger, t.tombstoneLife())
	}
	size, err := worstCaseGateBytes(t)
	if err != nil {
		return err
	}
	if size > gateCarryBound {
		return fmt.Errorf("a gate full at these settings would carry %d bytes, above the %d-byte bound: "+
			"lower engine.concurrency.max_entries, max_tombstones or max_labels", size, gateCarryBound)
	}
	return nil
}

// worstCaseGateBytes is the encoded size of a gate full at every bound of t:
// max_entries entries with the longest run ids, max_tombstones tombstones and
// max_labels labels at their longest.
//
// It encodes a gate with one of each and multiplies, never building the
// whole state, so a setting far too large is refused rather than allocated.
func worstCaseGateBytes(t GateTunables) (int, error) {
	one := t
	one.MaxEntries, one.MaxTombstones, one.MaxLabels = 1, 1, 1
	base, err := stablejson.Marshal(worstCaseGateState(one))
	if err != nil {
		return 0, fmt.Errorf("encode the worst-case gate state: %w", err)
	}
	g := worstCaseGateState(one)
	entry, err := stablejson.Marshal(g.Holders[0])
	if err != nil {
		return 0, fmt.Errorf("encode the worst-case gate entry: %w", err)
	}
	tomb, err := stablejson.Marshal(g.Tombstones[0])
	if err != nil {
		return 0, fmt.Errorf("encode the worst-case gate tombstone: %w", err)
	}
	label, err := stablejson.Marshal(g.Labels[0])
	if err != nil {
		return 0, fmt.Errorf("encode the worst-case gate label: %w", err)
	}
	// Each further element adds itself and a comma; the one queued entry
	// of the base state stands for a queue that may be empty.
	extra := func(n, size int) int64 { return int64(max(0, n-1)) * int64(size+1) }
	total := int64(len(base)) + extra(t.MaxEntries, len(entry)) + extra(t.MaxTombstones, len(tomb)) + extra(t.MaxLabels, len(label))
	if total > 1<<31 {
		return 1 << 31, nil
	}
	return int(total), nil
}

// worstCaseGateState is a gate full at every bound of t, every field set.
func worstCaseGateState(t GateTunables) gateState {
	at := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	runID := strings.Repeat("r", gateWorstRunIDBytes)
	// The label is sized by its encoding, so it is measured with characters
	// JSON escapes: the validator holds a template's encoding to this bound.
	g := gateState{Tunables: t, Seq: 1 << 62, Epoch: 1 << 62, Phase: gatePhaseActive,
		FenceDegradedUntil: at, LastPassAt: at, FrozenUntil: at, ResetRequestID: strings.Repeat("x", 64),
		Rebuild: gateRebuild{State: gateRebuildSettling, Since: at, Attempts: 1 << 30}}
	for range t.MaxLabels {
		g.Labels = append(g.Labels, strings.Repeat("<", gateWorstLabelBytes/len(`\u003c`)))
	}
	full := gateEntry{Run: runID, TaskIx: 1 << 20, SeqBase: 1 << 62, Limit: 1 << 30, Strategy: gateStrategyCancelOldest,
		Proto: gateProto, LabelIx: t.MaxLabels, ArrivedAt: at, Seq: 1 << 62, WaitUntil: at, GrantedAt: at,
		VerifyPending: true, Adopted: true, GrantPending: true, MissingSince: at, Stalled: true}
	g.Holders = append(g.Holders, full)
	for range max(1, t.MaxEntries-1) {
		g.Queue = append(g.Queue, full)
	}
	for range t.MaxTombstones {
		g.Tombstones = append(g.Tombstones, gateTomb{Key: strings.Repeat("k", 16), RunLevel: true, Below: 1 << 62, Until: at, RemovedBy: runID})
	}
	g.Draining = true
	return g
}

// gateEncodedLen is how many bytes s takes inside a JSON string, escapes
// included and its quotes not.
func gateEncodedLen(s string) int {
	b, err := stablejson.Marshal(s)
	if err != nil {
		return len(s) * 6
	}
	return len(b) - 2
}

// gateArrival is the instant an entry arrives, as the gate records it and
// answers it: to the millisecond its carry keeps, so the line's order is the
// same before and after a continue-as-new and in every run's own record.
func gateArrival(now time.Time) time.Time { return now.Truncate(time.Millisecond) }
