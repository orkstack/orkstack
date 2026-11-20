package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The admission record at submit (docs/TASK_IDENTITY.md, "The admission
// record"; workload-identity §3.1 rules 2 to 5). A run of a tree in which any
// run holds an identity is admitted: before its key is claimed or it is
// started, an engine writes, under the candidate run id, a record of what
// each of its tasks may hold and who may start it, named by its own SHA-256.
// The run's plan carries that digest, so the record and the plan are one
// value. Its children are admitted the same way, each naming its parent's
// run id and record, top-down.
//
// prepare decides everything but the ids (prepareAdmission); admitRun draws
// a nonce for one candidate id, builds the tree's records, writes them, and
// sets each plan's digest. It is the one step the submit paths and a fire
// call, right after they draw an id and before the claim or the start.

// AdmissionRecords is the store of the admission and chain records: the
// deployment's identity record store.
type AdmissionRecords interface {
	WriteAdmission(ctx context.Context, a artifacts.Admission) (sha256Hex string, created bool, err error)
	DeleteAdmission(ctx context.Context, namespace, runID, sha256Hex string) error
	CompleteChain(ctx context.Context, namespace, runID string, c artifacts.Chain) error
}

// admissionWriteBound is rule 3's bound on rule 4's writes, on the engine's
// own clock.
const admissionWriteBound = 10 * time.Second

// The reasons of a tree that cannot be admitted (docs/REASONS.md).
const (
	reasonIdentityOff       = "IDENTITY_OFF"
	reasonAttemptsOverLimit = "IDENTITY_ATTEMPTS_OVER_LIMIT"
	reasonTableTooLarge     = "IDENTITY_TABLE_TOO_LARGE"
)

// admissionTree is a tree's admissions as prepare decides them: everything
// but each run's id, nonce, starter and time, which admitRun fills for one
// candidate.
type admissionTree struct {
	root *admittedRun
}

// admittedRun is one run's admission before its ids, and the plan that
// will carry its digest.
type admittedRun struct {
	template artifacts.Admission
	plan     *engine.RunPlan
	children []admittedChild
}

// admittedChild is a composed task's child run: its plan entry, whose run
// id mintChildren draws after prepareAdmission, and its admission.
type admittedChild struct {
	taskID string
	entry  *engine.ChildRunPlan
	run    *admittedRun
}

// placeholderAdmission is the digest a plan carries while prepare measures
// it: the same length as the one admitRun sets.
func placeholderAdmission() string { return strings.Repeat("0", 64) }

// prepareAdmission decides the admissions of a tree that holds an
// identity, and nil for one that holds none. It reads the namespace's
// policy and ids, and refuses before anything is minted: an engine with no
// issuer, a task whose attempts would pass identity.attempts.max. Each plan
// of the tree is given a placeholder digest, so the plan's size is measured
// with it.
func (s *Service) prepareAdmission(ctx context.Context, ns *orkstackv1.Namespace, planned *orkstackv1.Orkflow,
	plan *engine.RunPlan, origin *orkstackv1.RunOrigin, binder *artifacts.AdmittedBinder, holds bool) (*admissionTree, error) {
	if !holds {
		return nil, nil
	}
	if s.issuer == nil || s.identityAdmissions == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: Orkflow %s@%s holds a task identity, and this engine issues none (identity.issuer is unset)",
			reasonIdentityOff, planned.GetName(), planned.GetVersion()))
	}
	policy, err := s.namespacePolicy(ctx, ns)
	if err != nil {
		return nil, policyReadError(ns.GetName(), err)
	}
	extra := int32(s.identityAttemptsExtra)
	if policy.AttemptsExtra != nil {
		extra = *policy.AttemptsExtra
	}
	nsID, err := s.namespaceID(ctx, ns.GetName(), true)
	if err != nil {
		return nil, err
	}
	common := artifacts.Admission{
		Version:       1,
		Namespace:     artifacts.ResolveNamespace(ns.GetName()),
		Origin:        admittedOrigin(origin, binder),
		EngineBuild:   s.engineBuild,
		Issuer:        s.issuer.URL(),
		SubjectFormat: string(s.issuer.SubjectFormat()),
		TrustDomain:   s.issuer.TrustDomain(),
		NamespaceID:   nsID,
		TokenTTL:      int32(s.identityTokenTTL / time.Second),
		AttemptsExtra: extra,
		SpecVersion:   int(task.IdentitySpecVersion),
	}
	root, err := s.admittedRunOf(ctx, common, planned, plan)
	if err != nil {
		return nil, err
	}
	return &admissionTree{root: root}, nil
}

// admittedRunOf is one run's admission and, recursively, its children's.
func (s *Service) admittedRunOf(ctx context.Context, common artifacts.Admission, wf *orkstackv1.Orkflow, plan *engine.RunPlan) (*admittedRun, error) {
	a := common
	a.Orkflow, a.OrkflowVersion, a.DefinitionDigest = wf.GetName(), wf.GetVersion(), wf.GetDefinitionDigest()
	owner := common.Namespace
	if o := wf.GetNamespace(); o != "" && artifacts.ResolveNamespace(o) != common.Namespace {
		owner = artifacts.ResolveNamespace(o)
		a.OrkflowNamespace = owner
	}
	orkflowID, err := s.orkflowID(ctx, owner, wf.GetName(), true)
	if err != nil {
		return nil, err
	}
	a.OrkflowID = orkflowID
	tasks, sets, err := s.admittedTasks(wf, plan, common.AttemptsExtra)
	if err != nil {
		return nil, err
	}
	a.Tasks, a.Sets = tasks, sets
	if err := checkAdmissionTemplate(a); err != nil {
		return nil, err
	}
	plan.Identity = identityPlanOf(a, placeholderAdmission())
	run := &admittedRun{template: a, plan: plan}
	for _, t := range wf.GetTasks() {
		entry := plan.Children[t.GetId()]
		if t.GetOrkflow() == nil || entry == nil {
			continue
		}
		def, err := entry.Definition()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("task %q's child run: %w", t.GetId(), err))
		}
		child, err := s.admittedRunOf(ctx, common, def, &entry.Plan)
		if err != nil {
			return nil, err
		}
		run.children = append(run.children, admittedChild{taskID: t.GetId(), entry: entry, run: child})
	}
	return run, nil
}

// admittedTasks is the table of every task of wf that runs a container,
// and the distinct identity sets they name. A task whose attempts would
// pass identity.attempts.max is refused, naming it.
func (s *Service) admittedTasks(wf *orkstackv1.Orkflow, plan *engine.RunPlan, extra int32) (map[string]artifacts.AdmittedTask, []artifacts.IdentitySet, error) {
	tasks := map[string]artifacts.AdmittedTask{}
	var sets []artifacts.IdentitySet
	for _, t := range wf.GetTasks() {
		if t.GetAwait() != nil || t.GetSleep() != nil || t.GetOrkflow() != nil {
			continue
		}
		digest, _, err := task.IdentitySpecDigest(task.IdentitySpecVersion, task.IdentitySpecOfTask(t))
		if err != nil {
			return nil, nil, connect.NewError(connect.CodeInternal, fmt.Errorf("task %q: %w", t.GetId(), err))
		}
		pool := t.GetPool()
		if pool == "" {
			pool = task.DefaultPool
		}
		row := artifacts.AdmittedTask{Pool: pool, SpecSHA256: digest}
		if identity.TaskHoldsIdentity(t) {
			slots, err := identity.Slots(int(t.GetRetry().GetMaximumAttempts()), int(extra), s.identityAttemptsMax)
			var over *identity.AttemptsOverLimitError
			switch {
			case errors.As(err, &over):
				return nil, nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
					"%s: task %q of Orkflow %s@%s: %d attempts may hold its identities (max(1, retry.maximum_attempts) + attempts_extra %d), over identity.attempts.max (%d)",
					reasonAttemptsOverLimit, t.GetId(), wf.GetName(), wf.GetVersion(), over.Slots, extra, over.Limit))
			case err != nil:
				return nil, nil, connect.NewError(connect.CodeInternal, fmt.Errorf("task %q: %w", t.GetId(), err))
			}
			row.Set = setIndex(&sets, admittedSet(t))
			row.IdentitySlots = int32(slots)
			deadline := engine.AttemptDeadlineSeconds(t, plan.TransferBudgetSeconds)
			if deadline > math.MaxInt32 {
				return nil, nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
					"task %q of Orkflow %s@%s: an attempt's deadline of %d s cannot be admitted", t.GetId(), wf.GetName(), wf.GetVersion(), deadline))
			}
			row.AttemptDeadline = int32(deadline)
			row.MaxItems = t.GetEach().GetMaxItems()
		}
		tasks[t.GetId()] = row
	}
	return tasks, sets, nil
}

// admittedSet is t's identities as the admission holds them.
func admittedSet(t *orkstackv1.Task) artifacts.IdentitySet {
	var set artifacts.IdentitySet
	for _, id := range t.GetIdentities() {
		refresh := artifacts.RefreshRequired
		if id.GetRefresh() == orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT {
			refresh = artifacts.RefreshBestEffort
		}
		set.Identities = append(set.Identities, artifacts.AdmittedIdentity{
			Name: id.GetName(), Audience: id.GetAudience(), TTLSeconds: id.GetTtlSeconds(), Refresh: refresh})
	}
	return set
}

// setIndex is set's 1-based position in sets, appended when it is new:
// tasks that declare the same identities share one entry.
func setIndex(sets *[]artifacts.IdentitySet, set artifacts.IdentitySet) int {
	for i, s := range *sets {
		if slices.Equal(s.Identities, set.Identities) {
			return i + 1
		}
	}
	*sets = append(*sets, set)
	return len(*sets)
}

// admittedOrigin is what started the run, as admitted, with a stored
// starter's binder.
func admittedOrigin(o *orkstackv1.RunOrigin, binder *artifacts.AdmittedBinder) artifacts.AdmittedOrigin {
	kind := strings.ToLower(strings.TrimPrefix(o.GetKind().String(), "RUN_ORIGIN_KIND_"))
	return artifacts.AdmittedOrigin{
		Kind:      strings.ReplaceAll(kind, "_", "-"),
		ID:        o.GetId(),
		Principal: o.GetPrincipal(),
		Verified:  o.GetPrincipalVerified(),
		Binder:    binder,
	}
}

// admittedRecords is what one admitRun created: the only records a cleanup
// may delete.
type admittedRecords struct {
	namespace string
	created   []admittedRecord
}

type admittedRecord struct{ runID, sha256Hex string }

// admitRun admits the tree for the candidate runID: it draws a nonce for
// each run, builds the root's record and then each child's, each naming its
// parent's run id and digest, writes them create-only, children first, and
// sets each plan's digest. A tree with no admission writes nothing and
// returns nil. A write that fails is unavailable, and what this call
// created is deleted on the engine's own clock before it answers (rule 5).
func (s *Service) admitRun(ctx context.Context, sub *submission, runID string, starter artifacts.Starter) (*admittedRecords, error) {
	if sub.admission == nil {
		return nil, nil
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), admissionWriteBound)
	defer cancel()
	at := s.now().UTC().Truncate(time.Millisecond)
	built, err := buildAdmissions(sub.admission.root, runID, "", starter, at)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("build the run's admission: %w", err))
	}
	records := &admittedRecords{namespace: artifacts.ResolveNamespace(sub.namespace)}
	// Children first, the run's own last: no record is written before a
	// record it names.
	for i := len(built) - 1; i >= 0; i-- {
		b := built[i]
		sha, created, err := s.identityAdmissions.WriteAdmission(work, b.admission)
		if err == nil && sha != b.sha256Hex {
			if created {
				records.created = append(records.created, admittedRecord{runID: b.admission.RunID, sha256Hex: sha})
			}
			err = errors.New("the store named it by another digest")
		}
		if err != nil {
			// A write that failed may still have landed: it is deleted with
			// the rest, and a delete of what is not there is no error.
			records.created = append(records.created, admittedRecord{runID: b.admission.RunID, sha256Hex: b.sha256Hex})
			s.deleteAdmissions(ctx, records)
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"write the admission record of run %s: %w; %s", b.admission.RunID, withoutDigest(err, b.sha256Hex), submitAgain(*sub)))
		}
		if created {
			records.created = append(records.created, admittedRecord{runID: b.admission.RunID, sha256Hex: sha})
		}
	}
	for _, b := range built {
		// The root's plan is the submission's own; a child's is its entry
		// in that plan's tree.
		plan := b.run.plan
		if b.run == sub.admission.root {
			plan = &sub.plan
		}
		plan.Identity = identityPlanOf(b.admission, b.sha256Hex)
	}
	return records, nil
}

// builtAdmission is one run's record, its digest, and the run it was built
// for.
type builtAdmission struct {
	run       *admittedRun
	admission artifacts.Admission
	sha256Hex string
}

// buildAdmissions builds run's record for runID and then, recursively, each
// child's after its parent's. rootRunID is "" for the root itself.
func buildAdmissions(run *admittedRun, runID, rootRunID string, starter artifacts.Starter, at time.Time) ([]builtAdmission, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("draw the admission's nonce: %w", err)
	}
	a := run.template
	a.RunID, a.Starter, a.RootRunID, a.Nonce, a.AdmittedAt = runID, starter, rootRunID, hex.EncodeToString(nonce), at
	// Each composed task's child run, by id: what ties a child's start to
	// the task of its parent that composes it, not only to the parent run.
	for _, c := range run.children {
		if c.entry.RunID == "" {
			return nil, fmt.Errorf("task %q's child run has no id yet", c.taskID)
		}
		if a.Children == nil {
			a.Children = map[string]string{}
		}
		a.Children[c.taskID] = c.entry.RunID
	}
	_, sha, err := artifacts.EncodeAdmission(a)
	if err != nil {
		return nil, err
	}
	out := []builtAdmission{{run: run, admission: a, sha256Hex: sha}}
	root := rootRunID
	if root == "" {
		root = runID
	}
	for _, c := range run.children {
		below, err := buildAdmissions(c.run, c.entry.RunID, root, artifacts.Starter{
			Kind: artifacts.StarterParentRun, RunID: runID, AdmissionSHA256: sha, TaskID: c.taskID}, at)
		if err != nil {
			return nil, err
		}
		out = append(out, below...)
	}
	return out, nil
}

// deleteAdmissions deletes what one admitRun created, for a candidate no
// start was attempted for, on the engine's own clock. What it cannot delete
// is logged and counted: an orphan the operator's sweep removes.
func (s *Service) deleteAdmissions(ctx context.Context, records *admittedRecords) {
	if records == nil || len(records.created) == 0 {
		return
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), admissionWriteBound)
	defer cancel()
	for _, r := range records.created {
		if err := s.identityAdmissions.DeleteAdmission(work, records.namespace, r.runID, r.sha256Hex); err != nil {
			s.logger.Warn("an admission record of a run that never started could not be deleted; the operator's sweep removes it",
				"run_id", r.runID, "err", withoutDigest(err, r.sha256Hex))
			if s.onAdmissionOrphan != nil {
				s.onAdmissionOrphan(ctx)
			}
		}
	}
	records.created = nil
}

// admitCandidate admits the tree for a candidate an engine is about to claim
// or start itself, and writes the run's definition, when it is passed by
// reference, and its tree's run-context object under the id too: everything
// the start will reference is
// written before the key is claimed (rule 4). It returns nil records for a
// tree with no admission, and writes nothing then.
func (s *Service) admitCandidate(ctx context.Context, sub *submission, runID string) (*admittedRecords, error) {
	records, err := s.admitRun(ctx, sub, runID, artifacts.Starter{Kind: artifacts.StarterEngine})
	if err != nil || records == nil {
		return records, err
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), admissionWriteBound)
	defer cancel()
	// Its definition, when passed by reference, and its tree's run-context
	// object (E86), so an admitted tree's tasks get their run context.
	if err := s.writeRunObjects(work, runID, *sub); err != nil {
		s.deleteAdmissions(ctx, records)
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("%w; %s", err, submitAgain(*sub)))
	}
	return records, nil
}

// chainWriteBound bounds the eager write of a root's chain record after its
// start: any engine that needs the record later completes it.
const chainWriteBound = 5 * time.Second

// completeChain writes a root's chain record from its start's answer: the
// execution the start opened and the admission its plan names. A failure is
// logged and nothing more: whichever engine first needs the record writes
// it from the start itself (docs/TASK_IDENTITY.md, "The start").
func (s *Service) completeChain(ctx context.Context, sub submission, runID, executionID string) {
	if sub.plan.Identity == nil || s.identityAdmissions == nil {
		return
	}
	if executionID == "" {
		s.logger.Debug("a started run's chain record is left to the first engine that needs it: the start named no execution", "run_id", runID)
		return
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), chainWriteBound)
	defer cancel()
	err := s.identityAdmissions.CompleteChain(work, artifacts.ResolveNamespace(sub.namespace), runID,
		artifacts.Chain{FirstExecutionID: executionID, AdmissionSHA256: sub.plan.Identity.AdmissionSHA256})
	switch {
	case err == nil:
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		s.logger.Warn("a started run's chain record was not written in time; the first engine that needs it writes it",
			"run_id", runID, "err", err)
	default:
		s.logger.Error("a started run's chain record was not written; the first engine that needs it writes it from the run's first start",
			"run_id", runID, "err", err)
	}
}

// identityPlanOf is what a run's plan carries of its admission a, named
// sha256Hex: derived from the record, never built beside it.
func identityPlanOf(a artifacts.Admission, sha256Hex string) *engine.IdentityPlan {
	p := &engine.IdentityPlan{AdmissionSHA256: sha256Hex, Issuer: a.Issuer, TokenTTLSeconds: a.TokenTTL,
		SpecVersion: uint32(a.SpecVersion), AttemptsExtra: a.AttemptsExtra}
	if len(a.Grants) > 0 {
		p.Grants = maps.Clone(a.Grants)
	}
	return p
}

// withoutDigest is err with an admission's digest taken out of its text: a
// store's error names the object it failed on, and a record's name is a
// secret until its run starts. Its chain is kept for errors.Is and As.
func withoutDigest(err error, sha256Hex string) error {
	if err == nil || sha256Hex == "" || !strings.Contains(err.Error(), sha256Hex) {
		return err
	}
	return &scrubbedError{msg: strings.ReplaceAll(err.Error(), sha256Hex, "<admission>"), err: err}
}

type scrubbedError struct {
	msg string
	err error
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.err }

// checkAdmissionTemplate refuses, before any id is drawn, a run whose
// record could never be written: what it holds is held to the record's
// rules, and its table to the record's bound with room for the ids,
// starter and nonce admitRun adds (IDENTITY_TABLE_TOO_LARGE, a property of
// the definition, so every submit and fire of it would fail alike).
func checkAdmissionTemplate(a artifacts.Admission) error {
	probe := a
	probe.RunID = "ork-" + strings.Repeat("0", 26)
	probe.Nonce = strings.Repeat("0", 32)
	probe.AdmittedAt = time.Unix(0, 0).UTC()
	probe.Starter = artifacts.Starter{Kind: artifacts.StarterEngine}
	raw, _, err := artifacts.EncodeAdmission(probe)
	if errors.Is(err, artifacts.ErrAdmissionTooLarge) || (err == nil && len(raw) > artifacts.MaxAdmissionBytes-admissionHeadroom) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: Orkflow %s@%s's tasks do not fit one admission record of %d KiB; split it into composed Orkflows",
			reasonTableTooLarge, a.Orkflow, a.OrkflowVersion, artifacts.MaxAdmissionBytes>>10))
	}
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("the Orkflow %s@%s cannot be admitted: %w", a.Orkflow, a.OrkflowVersion, err))
	}
	return nil
}

// admissionHeadroom is the room checkAdmissionTemplate leaves for what a
// record's ids and starter add to the template it measures.
const admissionHeadroom = 1 << 10

// unadmittedPlan says whether plan would start an admitted run before its
// record was written: the placeholder prepare measured with is still there.
func unadmittedPlan(plan engine.RunPlan) bool {
	return plan.Identity != nil && plan.Identity.AdmissionSHA256 == placeholderAdmission()
}
