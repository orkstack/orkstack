package artifacts

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// The admission record and a root's chain record (docs/TASK_IDENTITY.md,
// "The admission record, and the start"): what an admitted run's tasks may
// hold and who may start it, named by the SHA-256 of its own bytes and
// written once under the run's identity prefix; and, for a run an engine
// started itself, the first execution of its chain and the admission it was
// started with. Neither is execution state: Temporal alone says whether a
// run is alive, and a record's presence is never read as that.
const (
	// MaxAttemptSlots is the most slots one task or one item may have: the
	// upper bound of identity.attempts.max.
	MaxAttemptSlots = 256
	// MaxAdmissionBytes bounds one admission record: it holds the 4,000
	// tasks an Orkflow may declare by default. A larger table is
	// IDENTITY_TABLE_TOO_LARGE.
	MaxAdmissionBytes = 2 << 20
	// MaxAttemptsExtra is the upper bound of identity.attempts.max_extra.
	MaxAttemptsExtra = 16
	// MaxIdentitiesPerTask is the most identities one task may declare.
	MaxIdentitiesPerTask = 8

	admissionFormat = 1
	chainFormat     = 1
	chainObjectName = "chain.json"
	maxChainBytes   = 512
	maxAdmittedText = 1024
	// maxAdmittedSeconds bounds a duration the record pins: a week.
	maxAdmittedSeconds = 7 * 24 * 3600
)

// The starters an admission may name, and no other.
const (
	// StarterEngine: an engine starts this run itself.
	StarterEngine = "engine"
	// StarterFire: the schedule fire the record names prepared it.
	StarterFire = "fire"
	// StarterParentRun: its parent run, under the parent's admission, by
	// the task that composes it.
	StarterParentRun = "parent-run"
)

// The subject formats of a task's token (docs/TASK_IDENTITY.md, "Claims").
const (
	SubjectFormatOrk    = "ork"
	SubjectFormatSPIFFE = "spiffe"
)

// The refresh policies of a declared identity.
const (
	RefreshRequired   = "required"
	RefreshBestEffort = "best_effort"
)

// The kinds of task a FollowRef names.
const (
	FollowContainer = "container"
	FollowAwaited   = "awaited"
	FollowComposed  = "composed"
)

var (
	// ErrAdmissionTooLarge is IDENTITY_TABLE_TOO_LARGE: the record would
	// exceed MaxAdmissionBytes.
	ErrAdmissionTooLarge = errors.New("IDENTITY_TABLE_TOO_LARGE")
	// ErrChainDiffers is a chain record already there that names another
	// first execution or another admission: IDENTITY_NOT_ADMITTED.
	ErrChainDiffers = errors.New("artifacts: the run's chain record names another start")

	nonceHex = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// Admission is one admitted run's record.
type Admission struct {
	Version          int                       `json:"v"`
	RunID            string                    `json:"run_id"`
	Namespace        string                    `json:"namespace"`
	Orkflow          string                    `json:"orkflow"`
	OrkflowNamespace string                    `json:"orkflow_namespace,omitzero"`
	OrkflowVersion   string                    `json:"orkflow_version"`
	DefinitionDigest string                    `json:"definition_digest"`
	Starter          Starter                   `json:"starter"`
	RootRunID        string                    `json:"root_run_id,omitzero"`
	Origin           AdmittedOrigin            `json:"origin"`
	Nonce            string                    `json:"nonce"`
	AdmittedAt       time.Time                 `json:"admitted_at"`
	EngineBuild      string                    `json:"engine_build"`
	Issuer           string                    `json:"issuer,omitzero"`
	SubjectFormat    string                    `json:"subject_format"`
	TrustDomain      string                    `json:"trust_domain,omitzero"`
	NamespaceID      string                    `json:"namespace_id,omitzero"`
	OrkflowID        string                    `json:"orkflow_id,omitzero"`
	TokenTTL         int32                     `json:"token_ttl_seconds,omitzero"`
	AttemptsExtra    int32                     `json:"attempts_extra"`
	CommitWait       int32                     `json:"commit_wait_seconds"`
	SpecVersion      int                       `json:"spec_v"`
	Sets             []IdentitySet             `json:"sets"`
	Tasks            map[string]AdmittedTask   `json:"tasks"`
	Grants           map[string]GrantSelection `json:"grants,omitempty"`
	Follows          map[string][]FollowRef    `json:"follows,omitempty"`
	StartFollows     []FollowRef               `json:"start_follows,omitempty"`
	Completes        []string                  `json:"completes,omitempty"`
	Children         map[string]string         `json:"children,omitempty"`
}

// Starter is who may start the run: one of the three kinds, with exactly
// the fields its kind names.
type Starter struct {
	Kind string `json:"kind"`
	// A fire's: the fire's workflow id and execution id.
	WorkflowID  string `json:"workflow_id,omitzero"`
	ExecutionID string `json:"execution_id,omitzero"`
	// A parent run's: its run id, its admission's digest, and the task of
	// it that composes this run.
	RunID           string `json:"run_id,omitzero"`
	AdmissionSHA256 string `json:"admission_sha256,omitzero"`
	TaskID          string `json:"task_id,omitzero"`
}

// AdmittedOrigin is what started the run, as admitted.
type AdmittedOrigin struct {
	Kind      string `json:"kind"`
	ID        string `json:"id,omitzero"`
	Principal string `json:"principal,omitzero"`
	Verified  bool   `json:"verified"`
	// Binder is a stored starter's binder (a schedule's or a trigger's).
	Binder *AdmittedBinder `json:"binder,omitzero"`
}

// AdmittedBinder is a stored starter's binder and the binding that let it
// bind.
type AdmittedBinder struct {
	Principal string        `json:"principal"`
	Verified  bool          `json:"verified"`
	Binding   BinderBinding `json:"binding,omitzero"`
}

// BinderBinding is the binding that granted the binder run.submit on the
// Orkflow, as the save recorded it.
type BinderBinding struct {
	Name     string `json:"name"`
	Selector string `json:"selector"`
	Role     string `json:"role"`
	Scope    string `json:"scope"`
}

// IdentitySet is one distinct declaration of identities, which tasks name
// by its 1-based position.
type IdentitySet struct {
	Identities []AdmittedIdentity `json:"identities"`
}

// AdmittedIdentity is one declared identity, as admitted.
type AdmittedIdentity struct {
	Name       string `json:"name"`
	Audience   string `json:"audience"`
	TTLSeconds int32  `json:"ttl_seconds"`
	Refresh    string `json:"refresh"`
}

// AdmittedTask is one task that runs a container. Set is the 1-based
// position of its identity set in Sets, 0 when it holds no identity; the
// slots, deadline and item maximum are pinned for a task that holds one.
type AdmittedTask struct {
	Set             int    `json:"set,omitzero"`
	Pool            string `json:"pool"`
	SpecSHA256      string `json:"spec_sha256"`
	IdentitySlots   int32  `json:"identity_slots,omitzero"`
	AttemptDeadline int32  `json:"attempt_deadline_seconds,omitzero"`
	MaxItems        int32  `json:"max_items,omitzero"`
}

// GrantSelection is a grant as admitted for the run.
type GrantSelection struct {
	Provider    string            `json:"provider"`
	Principal   string            `json:"principal"`
	Audience    string            `json:"audience,omitzero"`
	SessionTags map[string]string `json:"session_tags,omitempty"`
}

// FollowRef names a task whose record another needs: in this run when
// RunID is empty.
type FollowRef struct {
	RunID  string `json:"run_id,omitzero"`
	TaskID string `json:"task_id"`
	Kind   string `json:"kind"`
}

// SetOf answers the identities task holds, or nil when it holds none or is
// not in the table.
func (a Admission) SetOf(task string) []AdmittedIdentity {
	t, ok := a.Tasks[task]
	if !ok || t.Set < 1 || t.Set > len(a.Sets) {
		return nil
	}
	return a.Sets[t.Set-1].Identities
}

// AdmissionKey renders the key of a run's admission:
// metadata/snapshots/{ns}/{run-id}/identity/admission-{sha256}.json.
func AdmissionKey(namespace, runID, sha256Hex string) (string, error) {
	if !lowerHex64(sha256Hex) {
		return "", fmt.Errorf("artifacts: admission key: %q is not a SHA-256 in lowercase hex: %w", sha256Hex, ErrInvalidRecordKey)
	}
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return prefix + "admission-" + sha256Hex + ".json", nil
}

// ChainKey renders the key of a root's chain record:
// metadata/snapshots/{ns}/{run-id}/identity/chain.json.
func ChainKey(namespace, runID string) (string, error) {
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return prefix + chainObjectName, nil
}

// EncodeAdmission answers the record's one canonical form and the SHA-256
// that names it. A record that would mislead the mint is
// ErrInvalidRecordKey, and one over MaxAdmissionBytes is
// ErrAdmissionTooLarge.
func EncodeAdmission(a Admission) ([]byte, string, error) {
	a.Version = admissionFormat
	a.Namespace = ResolveNamespace(a.Namespace)
	a.AdmittedAt = a.AdmittedAt.UTC()
	if reason := checkAdmission(a); reason != "" {
		return nil, "", fmt.Errorf("artifacts: admission: %s: %w", reason, ErrInvalidRecordKey)
	}
	raw, err := json.Marshal(a, json.Deterministic(true))
	if err != nil {
		return nil, "", fmt.Errorf("artifacts: admission: %w", err)
	}
	raw = append(raw, '\n')
	if len(raw) > MaxAdmissionBytes {
		return nil, "", fmt.Errorf("artifacts: admission of %d bytes is over %d: %w", len(raw), MaxAdmissionBytes, ErrAdmissionTooLarge)
	}
	return raw, hexDigest(raw), nil
}

// decodeAdmission reads a record. Only the exact bytes EncodeAdmission
// writes are a record: anything else of format 1 is invalid, a later
// format is unsupported.
func decodeAdmission(raw []byte) (Admission, RecordAnswer, string) {
	if len(raw) > MaxAdmissionBytes {
		return Admission{}, RecordInvalid, "too large"
	}
	version, bad := peekVersion(raw)
	switch {
	case bad != "":
		return Admission{}, RecordInvalid, bad
	case version > admissionFormat:
		return Admission{}, RecordUnsupported, ""
	case version != admissionFormat:
		return Admission{}, RecordInvalid, fmt.Sprintf("format %d is no format", version)
	}
	var a Admission
	if err := json.Unmarshal(raw, &a, json.RejectUnknownMembers(true)); err != nil {
		return Admission{}, RecordInvalid, "does not decode: " + err.Error()
	}
	again, _, err := EncodeAdmission(a)
	if err != nil {
		return Admission{}, RecordInvalid, err.Error()
	}
	if !bytes.Equal(again, raw) {
		return Admission{}, RecordInvalid, "is not as an admission is written"
	}
	return a, RecordFound, ""
}

// peekVersion answers raw's "v" when raw is one JSON object.
func peekVersion(raw []byte) (int64, string) {
	var head struct {
		V jsontext.Value `json:"v"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return 0, "does not decode: " + err.Error()
	}
	if len(head.V) == 0 {
		return 0, `no "v"`
	}
	var version int64
	if err := json.Unmarshal(head.V, &version); err != nil {
		return 0, `"v" is not a whole number`
	}
	return version, ""
}

// checkAdmission says why a can mislead the mint, or "".
func checkAdmission(a Admission) string {
	if err := validNamespaceName(a.Namespace); err != nil {
		return "namespace: " + err.Error()
	}
	if _, err := encodeArchiveSegment(a.RunID); err != nil || a.RunID == "" {
		return "a run id is needed"
	}
	if validOrkflowComponent("name", a.Orkflow) != nil || validOrkflowComponent("version", a.OrkflowVersion) != nil {
		return "an Orkflow name and version are needed"
	}
	if a.OrkflowNamespace != "" && validNamespaceName(a.OrkflowNamespace) != nil {
		return "the Orkflow's namespace is no namespace"
	}
	if !lowerHex64(a.DefinitionDigest) {
		return "the definition digest is not a SHA-256"
	}
	if reason := checkStarter(a.Starter); reason != "" {
		return reason
	}
	if a.RootRunID != "" {
		if a.Starter.Kind != StarterParentRun {
			return "only a child run names a root"
		}
		if _, err := encodeArchiveSegment(a.RootRunID); err != nil {
			return "the root run id is no run id"
		}
	}
	if !text(a.Origin.Kind) || len(a.Origin.ID) > maxAdmittedText || len(a.Origin.Principal) > maxAdmittedText {
		return "the origin needs a kind"
	}
	if b := a.Origin.Binder; b != nil && (!text(b.Principal) || len(b.Binding.Name)+len(b.Binding.Selector)+len(b.Binding.Role)+len(b.Binding.Scope) > maxAdmittedText) {
		return "a binder needs a principal"
	}
	if !nonceHex.MatchString(a.Nonce) {
		return "the nonce is not 128 bits in lowercase hex"
	}
	if a.AdmittedAt.IsZero() || !text(a.EngineBuild) {
		return "admitted_at and the engine's build are needed"
	}
	switch a.SubjectFormat {
	case SubjectFormatOrk:
	case SubjectFormatSPIFFE:
		if !text(a.TrustDomain) {
			return "a spiffe subject needs a trust domain"
		}
	default:
		return fmt.Sprintf("subject format %q is not ork or spiffe", a.SubjectFormat)
	}
	if a.SpecVersion < 1 {
		return "the spec digest's projection is needed"
	}
	if a.AttemptsExtra < 0 || a.AttemptsExtra > MaxAttemptsExtra || a.CommitWait < 0 || a.CommitWait > maxAdmittedSeconds ||
		a.TokenTTL < 0 || a.TokenTTL > maxAdmittedSeconds {
		return "attempts_extra, commit_wait and the token's life are out of bounds"
	}
	if len(a.Sets) > 0 && (!text(a.Issuer) || !text(a.NamespaceID) || !text(a.OrkflowID) || a.TokenTTL < 1) {
		return "a run that holds an identity needs the issuer, the ids and the token's life"
	}
	for i, set := range a.Sets {
		if reason := checkSet(set); reason != "" {
			return fmt.Sprintf("set %d: %s", i+1, reason)
		}
	}
	// A run whose tasks all compose, wait or sleep runs no container itself.
	if a.Tasks == nil {
		return "no table of tasks"
	}
	if len(a.Tasks) == 0 && (len(a.Sets) > 0 || len(a.Grants) > 0) {
		return "it holds an identity no task uses"
	}
	for id, t := range a.Tasks {
		if reason := checkAdmittedTask(id, t, len(a.Sets)); reason != "" {
			return fmt.Sprintf("task %q: %s", id, reason)
		}
	}
	if a.Grants != nil && len(a.Grants) == 0 {
		return "an empty grants table is written as none"
	}
	for name, g := range a.Grants {
		if !text(name) || !text(g.Provider) || !text(g.Principal) {
			return "a grant needs a name, a provider and a principal"
		}
	}
	for id, refs := range a.Follows {
		if validCommitTaskID(id) != nil || len(refs) == 0 {
			return "a follow is of a task, and names one"
		}
		if reason := checkFollows(refs); reason != "" {
			return reason
		}
	}
	if reason := checkFollows(a.StartFollows); reason != "" {
		return reason
	}
	for _, id := range a.Completes {
		if validCommitTaskID(id) != nil {
			return "a completion names a task"
		}
	}
	for id, child := range a.Children {
		if validCommitTaskID(id) != nil || child == "" {
			return "a child is a composed task's run"
		}
		if _, err := encodeArchiveSegment(child); err != nil {
			return "a child's run id is no run id"
		}
	}
	return ""
}

func checkStarter(s Starter) string {
	switch s.Kind {
	case StarterEngine:
		if s != (Starter{Kind: StarterEngine}) {
			return "an engine starter names nothing else"
		}
	case StarterFire:
		if !text(s.WorkflowID) || !text(s.ExecutionID) || s.RunID != "" || s.AdmissionSHA256 != "" || s.TaskID != "" {
			return "a fire starter names its workflow and execution, and nothing else"
		}
	case StarterParentRun:
		if _, err := encodeArchiveSegment(s.RunID); err != nil || s.RunID == "" || !lowerHex64(s.AdmissionSHA256) || validCommitTaskID(s.TaskID) != nil || s.WorkflowID != "" || s.ExecutionID != "" {
			return "a parent-run starter names the parent's run, admission and task, and nothing else"
		}
	default:
		return fmt.Sprintf("starter %q is not engine, fire or parent-run", s.Kind)
	}
	return ""
}

func checkSet(set IdentitySet) string {
	if len(set.Identities) < 1 || len(set.Identities) > MaxIdentitiesPerTask {
		return fmt.Sprintf("%d identities is not 1 to %d", len(set.Identities), MaxIdentitiesPerTask)
	}
	seen := map[string]bool{}
	for _, id := range set.Identities {
		if validCommitTaskID(id.Name) != nil || seen[id.Name] {
			return fmt.Sprintf("identity %q is not a name, or is named twice", id.Name)
		}
		seen[id.Name] = true
		if !text(id.Audience) || id.TTLSeconds < 1 {
			return fmt.Sprintf("identity %q needs an audience and a life", id.Name)
		}
		if id.Refresh != RefreshRequired && id.Refresh != RefreshBestEffort {
			return fmt.Sprintf("identity %q's refresh %q is not required or best_effort", id.Name, id.Refresh)
		}
	}
	return ""
}

func checkAdmittedTask(id string, t AdmittedTask, sets int) string {
	if err := validCommitTaskID(id); err != nil {
		return err.Error()
	}
	if err := validPoolName("pool", t.Pool); err != nil {
		return err.Error()
	}
	if !lowerHex64(t.SpecSHA256) {
		return "its spec digest is not a SHA-256"
	}
	if t.Set < 0 || t.Set > sets || t.MaxItems < 0 {
		return "it names no set, or a negative item maximum"
	}
	if t.Set == 0 {
		if t.IdentitySlots != 0 || t.AttemptDeadline != 0 {
			return "a task that holds no identity has no slots"
		}
		return ""
	}
	if t.IdentitySlots < 1 || t.IdentitySlots > MaxAttemptSlots || t.AttemptDeadline < 1 {
		return fmt.Sprintf("a task that holds an identity needs 1 to %d slots and a deadline", MaxAttemptSlots)
	}
	return ""
}

func checkFollows(refs []FollowRef) string {
	for _, r := range refs {
		if validCommitTaskID(r.TaskID) != nil {
			return "a follow names a task"
		}
		if r.Kind != FollowContainer && r.Kind != FollowAwaited && r.Kind != FollowComposed {
			return fmt.Sprintf("a follow's kind %q is not container, awaited or composed", r.Kind)
		}
		if r.RunID != "" {
			if _, err := encodeArchiveSegment(r.RunID); err != nil {
				return "a follow's run id is no run id"
			}
		}
	}
	return ""
}

func text(s string) bool { return s != "" && len(s) <= maxAdmittedText }

// WriteAdmission writes a's record create-only and answers its digest, and
// whether this call created it. The same bytes already there are success,
// not created: the key is the bytes' hash, so there are no other bytes, and
// an object found there is not the caller's to delete. Its writer is the
// submit path (docs/TASK_IDENTITY.md, "The admission record").
func (s *RecordStore) WriteAdmission(ctx context.Context, a Admission) (string, bool, error) {
	raw, sha, err := EncodeAdmission(a)
	if err != nil {
		return "", false, err
	}
	key, err := AdmissionKey(a.Namespace, a.RunID, sha)
	if err != nil {
		return "", false, err
	}
	err = s.createIn(ctx, key, raw)
	if err == nil {
		return sha, true, nil
	}
	if !errors.Is(err, errBlobExists) {
		return "", false, err
	}
	if err := s.createOrSame(ctx, key, raw, MaxAdmissionBytes); err != nil {
		if errors.Is(err, errBlobExists) {
			return "", false, &RecordStoreUnanswered{Class: UnansweredUnavailable, Err: fmt.Errorf("artifacts: admission %s holds other bytes than its name", key)}
		}
		return "", false, err
	}
	return sha, false, nil
}

// DeleteAdmission removes the admission of run named sha256Hex: only ever
// one the caller's own WriteAdmission created, for a run no start was
// attempted for. A missing object is not an error.
func (s *RecordStore) DeleteAdmission(ctx context.Context, namespace, runID, sha256Hex string) error {
	key, err := AdmissionKey(namespace, runID, sha256Hex)
	if err != nil {
		return err
	}
	if err := s.b.remove(ctx, key); err != nil {
		return unansweredIn(ctx, err)
	}
	return nil
}

// ReadAdmission reads the admission of run named sha256Hex. RecordNone
// when the store said "no such key", confirmed (IDENTITY_NOT_ADMITTED);
// RecordInvalid when the bytes do not hash to the name, do not decode, are
// not canonical or are another run's; RecordUnsupported for a later format.
// A store that does not answer is RECORD_STORE_UNANSWERED.
func (s *RecordStore) ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (RecordRead[Admission], error) {
	key, err := AdmissionKey(namespace, runID, sha256Hex)
	if err != nil {
		return RecordRead[Admission]{}, err
	}
	raw, _, found, err := s.reader.readObject(ctx, SlotRun, key, MaxAdmissionBytes+1)
	if err != nil {
		return RecordRead[Admission]{}, err
	}
	if !found {
		return RecordRead[Admission]{Answer: RecordNone}, nil
	}
	if hexDigest(raw) != sha256Hex {
		return invalidRead[Admission](key, "does not hash to its name"), nil
	}
	a, answer, reason := decodeAdmission(raw)
	switch answer {
	case RecordUnsupported:
		v, _ := peekVersion(raw)
		return RecordRead[Admission]{Answer: RecordUnsupported, Object: key, Version: v}, nil
	case RecordInvalid:
		return invalidRead[Admission](key, "%s", reason), nil
	}
	if a.Namespace != ResolveNamespace(namespace) || a.RunID != runID {
		return invalidRead[Admission](key, "is the record of another run"), nil
	}
	return RecordRead[Admission]{Answer: RecordFound, Value: a}, nil
}

// Chain is a root's chain record: the first execution of its chain and
// the digest of the admission it was started with.
type Chain struct {
	FirstExecutionID string
	AdmissionSHA256  string
}

// executionIDPattern is what a cluster's execution id may hold: a UUID,
// and nothing JSON or a key would have to escape.
var executionIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)

func (c Chain) check() string {
	if !executionIDPattern.MatchString(c.FirstExecutionID) || !lowerHex64(c.AdmissionSHA256) {
		return "a chain record names a first execution and an admission's digest"
	}
	return ""
}

// ChainInvalidError is a chain record already there that is not one this
// binary can trust: IDENTITY_UNAVAILABLE, never "absent" or "differs".
type ChainInvalidError struct {
	Object, Reason string
}

func (e *ChainInvalidError) Error() string {
	return fmt.Sprintf("artifacts: chain record %s: %s", e.Object, e.Reason)
}

// CompleteChain creates the run's chain record. One already there that is
// the same is success; one that names another start is ErrChainDiffers;
// one this binary cannot read is *ChainInvalidError. A store that does not
// answer is RECORD_STORE_UNANSWERED.
func (s *RecordStore) CompleteChain(ctx context.Context, namespace, runID string, c Chain) error {
	if reason := c.check(); reason != "" {
		return fmt.Errorf("artifacts: %s: %w", reason, ErrInvalidRecordKey)
	}
	key, err := ChainKey(namespace, runID)
	if err != nil {
		return err
	}
	err = s.createIn(ctx, key, encodeChain(c))
	if err == nil || !errors.Is(err, errBlobExists) {
		return err
	}
	raw, found, err := s.getIn(ctx, key, maxChainBytes+1)
	if err != nil {
		return err
	}
	if !found {
		return &RecordStoreUnanswered{Class: UnansweredUnavailable, Err: fmt.Errorf("artifacts: chain record %s was there and is gone", key)}
	}
	there, answer, reason := decodeChain(raw)
	if answer != RecordFound {
		if reason == "" {
			reason = "a format this binary does not read"
		}
		return &ChainInvalidError{Object: key, Reason: reason}
	}
	if there != c {
		return fmt.Errorf("%w: %s names first execution %s and admission %s", ErrChainDiffers, key, there.FirstExecutionID, there.AdmissionSHA256)
	}
	return nil
}

// ReadChain reads a root's chain record: RecordNone when the store said
// "no such key", confirmed.
func (s *RecordStore) ReadChain(ctx context.Context, namespace, runID string) (RecordRead[Chain], error) {
	key, err := ChainKey(namespace, runID)
	if err != nil {
		return RecordRead[Chain]{}, err
	}
	raw, _, found, err := s.reader.readObject(ctx, SlotRun, key, maxChainBytes+1)
	if err != nil {
		return RecordRead[Chain]{}, err
	}
	if !found {
		return RecordRead[Chain]{Answer: RecordNone}, nil
	}
	c, answer, reason := decodeChain(raw)
	switch answer {
	case RecordUnsupported:
		v, _ := peekVersion(raw)
		return RecordRead[Chain]{Answer: RecordUnsupported, Object: key, Version: v}, nil
	case RecordInvalid:
		return invalidRead[Chain](key, "%s", reason), nil
	}
	return RecordRead[Chain]{Answer: RecordFound, Value: c}, nil
}

var chainKeys = []string{"v", "first_execution_id", "admission_sha256"}

type chainWire struct {
	V                int64  `json:"v"`
	FirstExecutionID string `json:"first_execution_id"`
	AdmissionSHA256  string `json:"admission_sha256"`
}

func encodeChain(c Chain) []byte {
	raw, err := json.Marshal(chainWire{V: chainFormat, FirstExecutionID: c.FirstExecutionID, AdmissionSHA256: c.AdmissionSHA256}, json.Deterministic(true))
	if err != nil {
		// A struct of two strings and an integer always encodes.
		panic(fmt.Sprintf("artifacts: encode chain record: %v", err))
	}
	return append(raw, '\n')
}

func decodeChain(raw []byte) (Chain, RecordAnswer, string) {
	if len(raw) > maxChainBytes {
		return Chain{}, RecordInvalid, "too large"
	}
	version, bad := decodeVersioned(raw, chainFormat, chainKeys)
	switch {
	case bad != "":
		return Chain{}, RecordInvalid, bad
	case version > chainFormat:
		return Chain{}, RecordUnsupported, ""
	case version != chainFormat:
		return Chain{}, RecordInvalid, fmt.Sprintf("format %d is no format", version)
	}
	var w chainWire
	if err := json.Unmarshal(raw, &w, json.RejectUnknownMembers(true)); err != nil {
		return Chain{}, RecordInvalid, "does not decode: " + err.Error()
	}
	c := Chain{FirstExecutionID: w.FirstExecutionID, AdmissionSHA256: w.AdmissionSHA256}
	if reason := c.check(); reason != "" {
		return Chain{}, RecordInvalid, reason
	}
	if !bytes.Equal(encodeChain(c), raw) {
		return Chain{}, RecordInvalid, "is not as a chain record is written"
	}
	return c, RecordFound, ""
}
