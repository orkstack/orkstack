package identity

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The engine's records of a child run's outputs and of the composed task that
// takes them (docs/TASK_IDENTITY.md, "The values of a composed task"): each is
// made of records alone, and what the run says of its own tasks is held to
// them.

// The reasons of a record the engine did not make because of what it read.
const (
	ReasonChildValuesNotRecorded = "CHILD_VALUES_NOT_RECORDED"
	ReasonOutputsNotRecorded     = "OUTPUTS_NOT_RECORDED"
)

// ValuesRefusal is a record not made because of what the store and the
// cluster answered. Any other error of a ValuesRecorder is a step that was
// not answered, and is asked again, or the render's own.
type ValuesRefusal struct{ Reason, Message string }

func (e *ValuesRefusal) Error() string { return e.Reason + ": " + e.Message }

// ValuesRecords is what a ValuesRecorder reads and writes.
type ValuesRecords interface {
	FollowsStore
	TasksOnRecord(ctx context.Context, class artifacts.SlotClass, namespace, runID string, tasks []string) (map[string]bool, error)
	WriteRecordedValues(ctx context.Context, rec *orkstackv1.RecordedValues) (string, error)
}

// OutputsRender is one render of a run's declared outputs.
type OutputsRender struct {
	Definition *orkstackv1.Orkflow
	// Input is the run's start input (StartInput).
	Input []byte
	// Upstream is where each task the outputs read keeps its values, and the
	// digest they must have.
	Upstream map[string]*orkstackv1.ArtifactRef
	MaxBytes int64
}

// ValuesRecorder records a child run's outputs and a composed task's values.
type ValuesRecorder struct {
	Records ValuesRecords
	// Spacing is the waits of one read; nil is the reader's own.
	Spacing  *artifacts.RecordSpacings
	Starts   StartReader
	Describe func(ctx context.Context, runID string) (ExecutionFacts, error)
	// Definition is the definition a run was admitted with.
	Definition func(ctx context.Context, adm artifacts.Admission) (*orkstackv1.Orkflow, error)
	// Render answers the outputs as the parent canonicalises them.
	Render func(ctx context.Context, r OutputsRender) ([]byte, error)
	// Store keeps a run's outputs where a task's values are kept.
	Store       func(ctx context.Context, adm artifacts.Admission, raw []byte) (*orkstackv1.ArtifactRef, error)
	EngineBuild string
	Now         func() time.Time
}

// OutputsAsk is a child run that succeeded, and what it says of its tasks.
type OutputsAsk struct {
	Namespace, RunID, AdmissionSHA256 string
	Scopes                            Scopes
	// Input is the run's start input (StartInput).
	Input []byte
	// Offered is, for each task that succeeded, the values the run holds.
	Offered  map[string]*orkstackv1.ArtifactRef
	MaxBytes int64
	// Spacing is the waits of one read; nil is the recorder's.
	Spacing *artifacts.RecordSpacings
}

// ComposedAsk is a composed task whose child run ended, and the digest of the
// values it returned.
type ComposedAsk struct {
	Namespace, RunID, AdmissionSHA256, TaskID string
	Scopes                                    Scopes
	// Input is the run's start input (StartInput).
	Input                              []byte
	ChildAdmissionSHA256, ValuesSHA256 string
	// Spacing is the waits of one read; nil is the recorder's.
	Spacing *artifacts.RecordSpacings
	// Closed: a resume asks, before it resets the run, for the values its
	// child's outputs give a task the resume keeps, so the run's execution
	// may be closed. Only ResumeRun sets it. A run the cluster no longer holds
	// is still refused, and every other check stands.
	Closed bool
}

// RecordedOutcome is a record made, or found as it would have been made.
type RecordedOutcome struct {
	RecordSHA256 string
	// ValuesRef is where the values are; Values are their bytes, of a run's
	// outputs.
	ValuesRef *orkstackv1.ArtifactRef
	Values    []byte
	Artifacts []*orkstackv1.ArtifactRef
}

// recording is one record being made.
type recording struct {
	r      *ValuesRecorder
	reason string
	check  *FollowsCheck
	adm    artifacts.Admission
	sha    string
	root   uint64
}

func (c *recording) refuse(format string, args ...any) error {
	return &ValuesRefusal{Reason: c.reason, Message: fmt.Sprintf(format, args...)}
}

// refused is a check's error: a refusal of the record's, also for a record
// that is there and cannot be used, or the step that was not answered.
func (c *recording) refused(err error) error {
	var m *MintError
	if errors.As(err, &m) && (!m.Unavailable || m.invalid) {
		return &ValuesRefusal{Reason: c.reason, Message: m.Message}
	}
	return err
}

// begin is step 1: the run's admission, its start, and its root's resumes.
func (r *ValuesRecorder) begin(ctx context.Context, reason, namespace, runID, sha string, scopes Scopes, spacing *artifacts.RecordSpacings, closed bool) (*recording, error) {
	if spacing == nil {
		spacing = r.Spacing
	}
	c := &recording{r: r, reason: reason, check: &FollowsCheck{Store: r.Records, Spacing: spacing}, sha: sha}
	adm, err := c.admission(ctx, namespace, runID, sha)
	if err != nil {
		return nil, err
	}
	c.adm, c.root = adm, scopes.ofRoot(adm)
	switch {
	case adm.ValueRecords == 0:
		return nil, c.refuse("run %q was admitted with no record format", runID)
	case !artifacts.WritesRecordFormat(adm.ValueRecords):
		// A replica of a newer release may be the one that tries next.
		return nil, unavailableMint("run %q expects a record format this engine does not write", runID)
	}
	facts, err := r.Describe(ctx, runID)
	switch {
	case errors.Is(err, ErrNoSuchExecution), err == nil && !facts.Open && !closed:
		return nil, c.refuse("run %q is not executing", runID)
	case err != nil:
		return nil, unavailableMint("run %q could not be described: %v", runID, err)
	}
	switch start := CheckStart(ctx, r.Starts, adm, sha, facts); start.Verdict {
	case StartAdmitted:
	case StartNotAdmitted:
		return nil, c.refuse("%s", start.Reason)
	default:
		return nil, start.unavailableMint()
	}
	return c, nil
}

// admission is the admission record of run named sha.
func (c *recording) admission(ctx context.Context, namespace, runID, sha string) (artifacts.Admission, error) {
	read, err := c.r.Records.ReadAdmission(ctx, namespace, runID, sha)
	switch {
	case errors.Is(err, artifacts.ErrInvalidRecordKey):
		return artifacts.Admission{}, c.refuse("run %q names no admission record", runID)
	case err != nil:
		return artifacts.Admission{}, unavailableMint("the admission record of run %q could not be read: %v", runID, err)
	case read.Answer == artifacts.RecordNone:
		return artifacts.Admission{}, c.refuse("run %q has no admission record of that name", runID)
	case read.Answer == artifacts.RecordInvalid:
		return artifacts.Admission{}, c.refuse("the admission record of run %q cannot be used (%s: %s)", runID, read.Object, read.Reason)
	case read.Answer != artifacts.RecordFound:
		return artifacts.Admission{}, unavailableMint("the admission record of run %q is in a format this engine does not read", runID)
	case read.Value.RunID != runID || read.Value.Namespace != namespace:
		return artifacts.Admission{}, c.refuse("the admission record named for run %q is another run's", runID)
	}
	return read.Value, nil
}

// values is what a record of kind has in common, bound to the run's input.
func (c *recording) values(kind orkstackv1.RecordedValuesKind, input []byte) (*orkstackv1.RecordedValues, error) {
	nonce, err := hex.DecodeString(c.adm.Nonce)
	if err != nil {
		return nil, unavailableMint("the admission record of run %q cannot be trusted: %v", c.adm.RunID, err)
	}
	return &orkstackv1.RecordedValues{SchemaVersion: int32(c.adm.ValueRecords), Kind: kind, Namespace: c.adm.Namespace, RunId: c.adm.RunID,
		AdmissionSha256: c.sha, DefinitionDigest: "sha256:" + c.adm.DefinitionDigest, InputBinding: InputBinding(nonce, input),
		Recorded: &orkstackv1.CommitRecording{AdmissionSha256: c.sha, EngineBuild: c.r.EngineBuild, Time: timestamppb.New(c.r.Now().UTC())}}, nil
}

func wantOf(rec *orkstackv1.RecordedValues) artifacts.RecordWant {
	return artifacts.RecordWant{Kind: rec.GetKind(), Namespace: rec.GetNamespace(), RunID: rec.GetRunId(), TaskID: rec.GetTaskId(),
		Scope: rec.GetResumeScope(), AdmissionSHA256: rec.GetAdmissionSha256()}
}

// write makes rec, create-only, and answers its digest; or the record that
// was there first, for the caller to compare.
func (c *recording) write(ctx context.Context, rec *orkstackv1.RecordedValues, key string) (string, *artifacts.RecordedFound, error) {
	sha, err := c.r.Records.WriteRecordedValues(ctx, rec)
	if err == nil {
		return sha, nil, nil
	}
	if errors.Is(err, artifacts.ErrRecordTooLarge) || errors.Is(err, artifacts.ErrInvalidRecordKey) {
		return "", nil, c.refuse("it cannot be recorded: %v", err)
	}
	if !errors.Is(err, artifacts.ErrRecordExists) {
		return "", nil, unavailableMint("the record could not be made: %v", err)
	}
	read, err := c.r.Records.ReadRecordedValues(ctx, artifacts.SlotRun, c.check.Spacing, key, wantOf(rec))
	switch {
	case err != nil:
		return "", nil, unavailableMint("the record already there could not be read: %v", err)
	case read.Answer == artifacts.RecordInvalid:
		return "", nil, c.refuse("a record is already there and cannot be used (%s: %s)", read.Object, read.Reason)
	case read.Answer != artifacts.RecordFound:
		return "", nil, unavailableMint("the record already there could not be read (%s)", read.Answer)
	}
	return read.Value.RecordSHA256(), read.Value, nil
}

// RecordComposed records a composed task's values: its child's recorded
// outputs, whose values have the digest the child returned.
func (r *ValuesRecorder) RecordComposed(ctx context.Context, q ComposedAsk) (RecordedOutcome, error) {
	none := RecordedOutcome{}
	c, err := r.begin(ctx, ReasonChildValuesNotRecorded, q.Namespace, q.RunID, q.AdmissionSHA256, q.Scopes, q.Spacing, q.Closed)
	if err != nil {
		return none, err
	}
	if err := c.check.CheckScope(ctx, c.adm, q.TaskID, q.Scopes); err != nil {
		return none, c.refused(err)
	}
	childID, ok := c.adm.Children[q.TaskID]
	if !ok {
		return none, c.refuse("task %q composes no run", q.TaskID)
	}
	// 2. What the task follows.
	rows, err := c.check.CheckFollows(ctx, c.adm, c.sha, q.TaskID, c.root)
	if err != nil {
		return none, c.refused(err)
	}
	// 3. The child, and its outputs' record.
	child, err := c.admission(ctx, q.Namespace, childID, q.ChildAdmissionSHA256)
	if err != nil {
		return none, err
	}
	if child.Starter != (artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: q.RunID, AdmissionSHA256: q.AdmissionSHA256, TaskID: q.TaskID}) {
		return none, c.refuse("child run %s is not this task's", childID)
	}
	out, err := c.outputsOf(ctx, childID, q)
	if err != nil {
		return none, err
	}
	// 4. The record.
	rec, err := c.values(orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, q.Input)
	if err != nil {
		return none, err
	}
	rec.TaskId, rec.ResumeScope = q.TaskID, q.Scopes.Resume
	rec.Values, rec.Artifacts = out.ValuesRef(), out.Artifacts()
	rec.ChildRunId, rec.ChildAdmissionSha256, rec.ChildOutputsSha256 = childID, q.ChildAdmissionSHA256, out.RecordSHA256()
	for _, row := range rows {
		rec.Follows = append(rec.Follows, &orkstackv1.RecordedSource{TaskId: row.Ref.TaskID, RecordSha256: row.RecordSHA256, ResultSha256: row.ResultSHA256})
	}
	key, err := artifacts.RecordKey(artifacts.RecordComposed, q.Namespace, q.RunID, q.TaskID, q.Scopes.Resume)
	if err != nil {
		return none, c.refuse("task %q has no record key: %v", q.TaskID, err)
	}
	sha, first, err := c.write(ctx, rec, key)
	if err != nil {
		return none, err
	}
	if first != nil {
		run, adm, outputs := first.Child()
		if run != rec.ChildRunId || adm != rec.ChildAdmissionSha256 || outputs != rec.ChildOutputsSha256 {
			return none, c.refuse("the task's record names another child run, or other outputs of it")
		}
	}
	return RecordedOutcome{RecordSHA256: sha, ValuesRef: rec.Values, Artifacts: rec.Artifacts}, nil
}

// outputsOf is the outputs' record of child run childID, whose values are the
// ones it returned.
func (c *recording) outputsOf(ctx context.Context, childID string, q ComposedAsk) (*artifacts.RecordedFound, error) {
	key, err := artifacts.OutputsRecordKey(q.Namespace, childID)
	if err != nil {
		return nil, c.refuse("child run %s has no record key: %v", childID, err)
	}
	read, err := c.r.Records.ReadRecordedValues(ctx, artifacts.SlotRun, c.check.Spacing, key, artifacts.RecordWant{
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, Namespace: q.Namespace, RunID: childID, AdmissionSHA256: q.ChildAdmissionSHA256})
	switch {
	case err != nil:
		return nil, unavailableMint("the outputs' record of child run %s could not be read: %v", childID, err)
	case read.Answer == artifacts.RecordNone:
		return nil, c.refuse("child run %s ended, and no engine recorded the outputs it returned", childID)
	case read.Answer == artifacts.RecordInvalid:
		return nil, c.refuse("child run %s ended: a record is there and is not this child's outputs (%s: %s)", childID, read.Object, read.Reason)
	case read.Answer != artifacts.RecordFound:
		return nil, unavailableMint("the outputs' record of child run %s is in a format this engine does not read", childID)
	}
	if _, values, _ := read.Value.Standing(); values != q.ValuesSHA256 {
		return nil, c.refuse("child run %s returned other values than the outputs on its record", childID)
	}
	return read.Value, nil
}

// RecordOutputs records a child run's outputs, rendered from its admitted
// definition over the values its tasks' records name.
func (r *ValuesRecorder) RecordOutputs(ctx context.Context, q OutputsAsk) (RecordedOutcome, error) {
	none := RecordedOutcome{}
	c, err := r.begin(ctx, ReasonOutputsNotRecorded, q.Namespace, q.RunID, q.AdmissionSHA256, q.Scopes, q.Spacing, false)
	if err != nil {
		return none, err
	}
	if c.adm.Starter.Kind != artifacts.StarterParentRun {
		return none, c.refuse("run %q has no parent to take its outputs", q.RunID)
	}
	if _, err := c.check.Establish(ctx, c.adm, q.Scopes); err != nil {
		return none, c.refused(err)
	}
	wf, err := r.Definition(ctx, c.adm)
	switch {
	case errors.Is(err, ErrDefinitionNotAdmitted):
		return none, c.refuse("the definition run %q was admitted with cannot be read: %v", q.RunID, err)
	case err != nil:
		return none, unavailableMint("the definition of run %q could not be read: %v", q.RunID, err)
	}
	// 2. What the run's start follows, and what it always runs.
	if _, err := c.check.records(ctx, c.adm, c.sha, c.adm.StartFollows, c.root); err != nil {
		return none, c.refused(err)
	}
	if err := c.completed(ctx, wf); err != nil {
		return none, err
	}
	// 3. The outputs, from the records.
	src, err := c.sources(ctx, wf, q)
	if err != nil {
		return none, err
	}
	produced, err := c.outputArtifacts(ctx, wf, src)
	if err != nil {
		return none, err
	}
	raw := []byte("{}")
	if len(wf.GetOutputs()) > 0 {
		if raw, err = r.Render(ctx, OutputsRender{Definition: wf, Input: q.Input, Upstream: src.upstream, MaxBytes: q.MaxBytes}); err != nil {
			return none, err
		}
	}
	// 4. Their values, then their record.
	rec, err := c.values(orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, q.Input)
	if err != nil {
		return none, err
	}
	if rec.Values, err = r.Store(ctx, c.adm, raw); err != nil {
		return none, unavailableMint("the outputs of run %q could not be stored: %v", q.RunID, err)
	}
	rec.Sources, rec.Artifacts = src.rows, produced
	key, err := artifacts.OutputsRecordKey(q.Namespace, q.RunID)
	if err != nil {
		return none, c.refuse("run %q has no record key: %v", q.RunID, err)
	}
	sha, first, err := c.write(ctx, rec, key)
	if err != nil {
		return none, err
	}
	if first != nil {
		same, err := sameOutputs(first, rec)
		if err != nil {
			return none, unavailableMint("the outputs on record of run %q cannot be trusted: %v", q.RunID, err)
		}
		if !same {
			return none, c.refuse("other outputs of run %q are already on record", q.RunID)
		}
	}
	return RecordedOutcome{RecordSHA256: sha, ValuesRef: rec.Values, Values: raw, Artifacts: produced}, nil
}

// sameOutputs says first has the result rec does.
func sameOutputs(first *artifacts.RecordedFound, rec *orkstackv1.RecordedValues) (bool, error) {
	_, values, _ := first.Standing()
	was, err := ResultSHA256(values, resultArtifacts(first.Artifacts()))
	if err != nil {
		return false, err
	}
	is, err := ResultSHA256(rec.GetValues().GetSha256(), resultArtifacts(rec.GetArtifacts()))
	return was == is, err
}

// kindOf is the kind of record task id of wf has; "" for one that has none.
func kindOf(wf *orkstackv1.Orkflow, id string) string {
	for _, t := range wf.GetTasks() {
		if t.GetId() == id {
			return RecordKind(t)
		}
	}
	return ""
}

// completed is that every task a successful run completes is on record: by
// the listing, whose names of a container or a composed task's records only a
// success leaves, at the one scope a run below a root has (Establish refuses
// it any other); and by its key for one the listing does not show, and for
// an awaited task, whose record may be a rejection.
func (c *recording) completed(ctx context.Context, wf *orkstackv1.Orkflow) error {
	if len(c.adm.Completes) == 0 {
		return nil
	}
	on, err := c.r.Records.TasksOnRecord(ctx, artifacts.SlotRun, c.adm.Namespace, c.adm.RunID, c.adm.Completes)
	if err != nil {
		return unavailableMint("the records of run %q could not be listed: %v", c.adm.RunID, err)
	}
	kinds := make(map[string]string, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		kinds[t.GetId()] = RecordKind(t)
	}
	var byKey []artifacts.FollowRef
	for _, id := range c.adm.Completes {
		if kind := kinds[id]; !on[id] || kind == artifacts.FollowAwaited {
			byKey = append(byKey, artifacts.FollowRef{TaskID: id, Kind: kind})
		}
	}
	_, err = c.recordsOf(ctx, byKey)
	return err
}

// recordsOf reads the records of refs, tasks of this run.
func (c *recording) recordsOf(ctx context.Context, refs []artifacts.FollowRef) ([]FollowedRecord, error) {
	for _, ref := range refs {
		if ref.Kind == "" {
			return nil, c.refuse("task %q of run %q has no record of its own", ref.TaskID, c.adm.RunID)
		}
	}
	rows, err := c.check.records(ctx, c.adm, c.sha, refs, c.root)
	if err != nil {
		return nil, c.refused(err)
	}
	return rows, nil
}

// outputSources are the records a run's outputs are taken from.
type outputSources struct {
	byTask   map[string]FollowedRecord
	upstream map[string]*orkstackv1.ArtifactRef
	rows     []*orkstackv1.RecordedSource
}

// sources reads the record of each task the outputs read and the run says
// succeeded. One it says did not is absent by its word, unless every
// successful run completes it or an artifact of it is returned.
func (c *recording) sources(ctx context.Context, wf *orkstackv1.Orkflow, q OutputsAsk) (*outputSources, error) {
	read, err := dsl.ReferencedUpstreamIn(wf.GetOutputs())
	if err != nil {
		return nil, c.refuse("the outputs of run %q cannot be read: %v", q.RunID, err)
	}
	producers := map[string]bool{}
	for _, a := range wf.GetOutputArtifacts() {
		if a.GetTaskId() != dsl.InputsProducer {
			producers[a.GetTaskId()] = true
		}
	}
	names := maps.Clone(producers)
	for _, id := range read {
		names[id] = true
	}
	src := &outputSources{byTask: map[string]FollowedRecord{}, upstream: map[string]*orkstackv1.ArtifactRef{}}
	ids := slices.Sorted(maps.Keys(names))
	var refs []artifacts.FollowRef
	for _, id := range ids {
		kind := kindOf(wf, id)
		_, offered := q.Offered[id]
		switch {
		case kind == "":
			return nil, c.refuse("the outputs of run %q read task %q, which has no record of its own", q.RunID, id)
		case offered:
			refs = append(refs, artifacts.FollowRef{TaskID: id, Kind: kind})
		case producers[id] || slices.Contains(c.adm.Completes, id):
			return nil, c.refuse("run %q holds no result of task %q, which it cannot succeed without", q.RunID, id)
		}
	}
	found, err := c.recordsOf(ctx, refs)
	if err != nil {
		return nil, err
	}
	for _, f := range found {
		src.byTask[f.Ref.TaskID] = f
	}
	for _, id := range ids {
		f, ok := src.byTask[id]
		if !ok {
			src.rows = append(src.rows, &orkstackv1.RecordedSource{TaskId: id, Absent: true})
			continue
		}
		offered := q.Offered[id]
		if f.valuesSHA256 != offered.GetSha256() {
			return nil, c.refuse("run %q holds other values of task %q than its record", q.RunID, id)
		}
		src.rows = append(src.rows, &orkstackv1.RecordedSource{TaskId: id, RecordSha256: f.RecordSHA256, ResultSha256: f.ResultSHA256})
		if slices.Contains(read, id) {
			// A resolution's record does not say where its values are. The
			// digest is the record's either way, and the render holds the
			// bytes to it.
			at := f.values
			if at == nil {
				at = offered
			}
			at = proto.Clone(at).(*orkstackv1.ArtifactRef)
			at.Sha256 = f.valuesSHA256
			src.upstream[id] = at
		}
	}
	return src, nil
}

// outputArtifacts are the artifacts the run returns, each under its output's
// name, as the record of the task that produced it names it.
func (c *recording) outputArtifacts(ctx context.Context, wf *orkstackv1.Orkflow, src *outputSources) ([]*orkstackv1.ArtifactRef, error) {
	returned := wf.GetOutputArtifacts()
	if len(returned) == 0 {
		return nil, nil
	}
	passed := map[string]bool{}
	for _, a := range returned {
		if a.GetTaskId() == dsl.InputsProducer {
			passed[a.GetName()] = true
		}
	}
	received, err := c.check.received(ctx, c.adm, c.sha, slices.Sorted(maps.Keys(passed)), c.root)
	if err != nil {
		return nil, c.refused(err)
	}
	// One a failure handler passed, with no record, is returned by no record.
	received = slices.DeleteFunc(received, func(a *orkstackv1.ArtifactRef) bool { return a == nil })
	out := make([]*orkstackv1.ArtifactRef, 0, len(returned))
	for _, name := range slices.Sorted(maps.Keys(returned)) {
		spec, from := returned[name], received
		if spec.GetTaskId() != dsl.InputsProducer {
			from = src.byTask[spec.GetTaskId()].artifacts
		}
		at := slices.IndexFunc(from, func(a *orkstackv1.ArtifactRef) bool { return a.GetName() == spec.GetName() })
		if at < 0 {
			return nil, c.refuse("no record names the artifact %s/%s that output %q returns", spec.GetTaskId(), spec.GetName(), name)
		}
		ref := proto.Clone(from[at]).(*orkstackv1.ArtifactRef)
		ref.Name = name
		out = append(out, ref)
	}
	return out, nil
}
