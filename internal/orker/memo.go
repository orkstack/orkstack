package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The memo step (docs/DSL.md "Caching", docs/OCI_ACTIVITY_CONTRACT.md §5.4
// "Memoized completion"). For an attempt of a CACHED task, after the commit
// check, the image policy and the staging of the upstream values.json files
// the render needs — but before any artifact download, secret, pull or
// container — the Orker computes a memo key over exactly what the task is and
// is given, and looks for a memo: a pointer to a SUCCEEDED commit another run
// already durably wrote.
//
// A hit writes THIS run's own commit.json, whose value and artifact
// references are the source's content-addressed objects, and returns it: no
// image is pulled, no secret is read, no container runs. Anything else is a
// miss, never a failure — the only way the memo step can fail an attempt is
// its own commit write, exactly as a fresh commit's write can.

// MemoKeyVersion is the key document's "v". Any change to the component set
// or to how one is computed bumps it, which turns every existing memo into a
// miss — safe by construction.
const MemoKeyVersion = 1

// Miss reasons, recorded on CacheResult.miss_reason. Never failure codes: a
// miss runs the task.
const (
	MemoMissAbsent             = "MEMO_ABSENT"
	MemoMissUnreadable         = "MEMO_UNREADABLE"
	MemoMissMismatch           = "MEMO_MISMATCH"
	MemoMissExpired            = "MEMO_EXPIRED"
	MemoMissSourceGone         = "SOURCE_COMMIT_GONE"
	MemoMissSourceChanged      = "SOURCE_COMMIT_CHANGED"
	MemoMissSourceUnreadable   = "SOURCE_COMMIT_UNREADABLE"
	MemoMissSourceMismatch     = "SOURCE_COMMIT_MISMATCH"
	MemoMissSourceOverLimits   = "SOURCE_OVER_LIMITS"
	MemoMissSourceObjectAbsent = "SOURCE_OBJECT_UNAVAILABLE"
	MemoMissInputUnhashed      = "INPUT_UNHASHED"
	MemoMissStoreAbsent        = "MEMO_STORE_ABSENT"
)

// memoLookupTimeout bounds the whole lookup — memo read, source commit read,
// value refetch and object checks. Exceeding it is a MISS (MEMO_UNREADABLE,
// "timed out"): the lookup can cost bounded time and never more. A variable
// only so a test can shorten it.
var memoLookupTimeout = 30 * time.Second

// memoWriteTimeout bounds the best-effort memo write, on a context detached
// from cancellation (the task has already committed). A variable only so a
// test can shorten it.
var memoWriteTimeout = 10 * time.Second

// memoMissDetailMax bounds CacheResult.miss_detail.
const memoMissDetailMax = 1 << 10

// memoKey is a computed key and its components.
type memoKey struct {
	Key        string
	Components map[string]string
}

// short renders the key for a log line.
func (k memoKey) short() string {
	if len(k.Key) <= 12 {
		return k.Key
	}
	return k.Key[:12] + "…"
}

// memoInputFile is one staged-or-deferred input file, as the key covers it:
// its container path, its name as task.json names it, and its content
// digest.
type memoInputFile struct {
	Path    string `json:"path"`
	NameB64 string `json:"name_b64"`
	SHA256  string `json:"sha256"`
}

// memoEachUpstream is one fan-out upstream of a direct dependent: a zero-item
// fan-in stages no file, so its count is what the key sees.
type memoEachUpstream struct {
	TaskID string `json:"task_id"`
	Count  int32  `json:"count"`
}

// deferredFetch is one artifact download stageInputs recorded instead of
// performing (deferBytes), in the order it would have performed it.
type deferredFetch struct {
	ref      *orkstackv1.ArtifactRef
	hostPath string
}

// resolvedEachItem is what each-fan-out's resolveEachItem resolved for one
// item: the element's exact JSON bytes as task.json's each.item carries them,
// and the name, position and count task.json hands the workload.
type resolvedEachItem struct {
	Bytes json.RawMessage
	As    string
	Index int32
	Count int32
}

// memoTrace is one attempt's memo decision, declared in Execute beside
// beat/result/failure so the attempt record and the observer see it on every
// exit path. result is nil until the memo step decides.
type memoTrace struct {
	result *orkstackv1.CacheResult
	// emit writes one [ork] line to the attempt's log; nil writes nothing.
	emit func(line string)
}

// decision is the trace's result, nil-safe.
func (t *memoTrace) decision() *orkstackv1.CacheResult {
	if t == nil {
		return nil
	}
	return t.result
}

func (t *memoTrace) log(format string, args ...any) {
	if t != nil && t.emit != nil {
		t.emit(fmt.Sprintf(format, args...))
	}
}

// miss records a MISS with its token and sentence.
func (t *memoTrace) miss(key memoKey, reason, format string, args ...any) {
	detail := fmt.Sprintf(format, args...)
	if len(detail) > memoMissDetailMax {
		detail = truncateString(detail, memoMissDetailMax)
	}
	t.result = &orkstackv1.CacheResult{
		Outcome:       orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS,
		Key:           key.Key,
		MissReason:    reason,
		MissDetail:    detail,
		KeyComponents: key.Components,
	}
}

// truncateString cuts s to at most max bytes on a rune boundary, ending in
// "…" when anything was removed.
func truncateString(s string, max int) string {
	const marker = "…"
	if len(s) <= max {
		return s
	}
	keep := max - len(marker)
	for keep > 0 && keep < len(s) && s[keep]&0xC0 == 0x80 {
		keep--
	}
	return s[:keep] + marker
}

// cachedInput reports whether this attempt takes the memo step: a task that
// declares cache, and is not pinned (a hand-built pinned input is treated as
// uncached; docs/DSL.md rule 36 makes it unreachable through apply).
func cachedInput(in *orkstackv1.ExecuteTaskInput) bool {
	return in.GetCache() != nil && in.GetPin() == nil
}

// resultCache is what ExecuteTaskResult.cache carries for a trace: the
// decision without its key_components, so history stays small.
func resultCache(c *orkstackv1.CacheResult) *orkstackv1.CacheResult {
	if c == nil {
		return nil
	}
	out := proto.Clone(c).(*orkstackv1.CacheResult)
	out.KeyComponents = nil
	return out
}

// commitCache is what ActivityCommit.cache carries: the decision of the
// attempt that committed, with none of the fields decided after the commit
// (memo_written, memo_write_error) and no key_components.
func commitCache(c *orkstackv1.CacheResult) *orkstackv1.CacheResult {
	out := resultCache(c)
	if out == nil {
		return nil
	}
	out.MemoWritten = false
	out.MemoWriteError = ""
	return out
}

func (d *OCIDispatcher) clock() time.Time {
	if d.now != nil {
		return d.now().UTC()
	}
	return time.Now().UTC()
}

// sha256Hex is the lowercase hex SHA-256 of b.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// sha256JSON is the lowercase hex SHA-256 of json.Marshal(v). Every value
// hashed here is built of strings, ints, slices and string-keyed maps, which
// encoding/json cannot fail to encode.
func sha256JSON(v any) string {
	encoded, _ := json.Marshal(v)
	return sha256Hex(encoded)
}

// memoKeyDocument is the document a key is the SHA-256 of. Field order is
// declaration order; components' keys are sorted by encoding/json.
type memoKeyDocument struct {
	V          int               `json:"v"`
	Namespace  string            `json:"namespace"`
	Scope      string            `json:"scope"`
	Orkflow    string            `json:"orkflow"`
	Task       string            `json:"task"`
	Components map[string]string `json:"components"`
}

// memoScopeName is the scope's word in the key document.
func memoScopeName(scope orkstackv1.CacheScope) string {
	if scope == orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE {
		return "namespace"
	}
	return "orkflow"
}

// computeMemoKey is the memo key (docs/DSL.md "What the key covers"), and nothing else reads an
// input to build a key. config and runJSON are encodeInputFile's bytes; item
// is what resolveEachItem resolved, nil for a task that is not a fan-out item.
// ok is false when an input reference had no digest (INPUT_UNHASHED). Pure.
func computeMemoKey(in *orkstackv1.ExecuteTaskInput, config, runJSON []byte,
	staged *stagedInputs, item *resolvedEachItem) (key memoKey, ok bool) {
	if staged != nil && staged.inputUnhashed {
		return memoKey{}, false
	}
	command := in.GetCommand()
	if command == nil {
		command = []string{}
	}
	env := in.GetEnv()
	if env == nil {
		env = map[string]string{}
	}
	type secret struct {
		Name   string `json:"name"`
		Source string `json:"source"`
	}
	secrets := make([]secret, 0, len(in.GetSecrets()))
	for _, s := range in.GetSecrets() {
		secrets = append(secrets, secret{Name: s.GetName(), Source: s.GetSource()})
	}
	network := "default"
	if in.GetNetwork() == orkstackv1.NetworkMode_NETWORK_MODE_NONE {
		network = "none"
	}
	files := []memoInputFile{}
	eachUpstream := []memoEachUpstream{}
	if staged != nil {
		files = append(files, staged.inputFiles...)
		for _, up := range staged.eachUpstream {
			eachUpstream = append(eachUpstream, memoEachUpstream{TaskID: up.TaskID, Count: up.Count})
		}
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	sort.SliceStable(eachUpstream, func(i, j int) bool { return eachUpstream[i].TaskID < eachUpstream[j].TaskID })
	components := map[string]string{
		"image":             in.GetImage().GetIndexDigest(),
		"command":           sha256JSON(command),
		"env":               sha256JSON(env),
		"secrets":           sha256JSON(secrets),
		"network":           network,
		"run_as_orker_user": strconv.FormatBool(in.GetRunAsOrkerUser()),
		"config":            sha256Hex(config),
		"run_input":         sha256Hex(runJSON),
		"inputs": sha256JSON(struct {
			Files        []memoInputFile    `json:"files"`
			EachUpstream []memoEachUpstream `json:"each_upstream"`
		}{files, eachUpstream}),
	}
	if item != nil {
		components["item"] = sha256Hex(item.Bytes)
		components["item_as"] = item.As
		components["item_index"] = strconv.FormatInt(int64(item.Index), 10)
		components["item_count"] = strconv.FormatInt(int64(item.Count), 10)
	}
	scope := in.GetCache().GetScope()
	orkflow := ""
	if scope != orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE {
		orkflow = in.GetOrkflowName()
	}
	// Strings only: encoding/json cannot fail on this document.
	document, _ := json.Marshal(memoKeyDocument{
		V:          MemoKeyVersion,
		Namespace:  artifacts.ResolveNamespace(in.GetNamespace()),
		Scope:      memoScopeName(scope),
		Orkflow:    orkflow,
		Task:       in.GetTaskId(),
		Components: components,
	})
	return memoKey{Key: sha256Hex(document), Components: components}, true
}

// memoRefFor addresses key's memo for this input.
func memoRefFor(in *orkstackv1.ExecuteTaskInput, key memoKey) artifacts.MemoRef {
	ref := artifacts.MemoRef{
		MemoScopeRef: artifacts.MemoScopeRef{
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			TaskID:    in.GetTaskId(),
		},
		Key: key.Key,
	}
	if in.GetCache().GetScope() == orkstackv1.CacheScope_CACHE_SCOPE_NAMESPACE {
		ref.NamespaceScoped = true
	} else {
		ref.Orkflow = in.GetOrkflowName()
	}
	return ref
}

// memoHit is what a successful lookup produced: the source commit, its
// refetched values bytes, the source to name, and the commit this run will
// write (already measured against commit_max_bytes).
type memoHit struct {
	record artifacts.CommitRecord
	values []byte
	source *orkstackv1.CacheSource
	commit *orkstackv1.ActivityCommit
}

// lookupMemo decides HIT or MISS for key, in the order of the miss-reason
// table (docs/DSL.md "Miss reasons"). On a HIT it returns the source commit, its refetched values bytes
// and the hit commit to write; every other outcome is a MISS with its reason
// and sentence set on trace. It never returns a Failure.
func (d *OCIDispatcher) lookupMemo(ctx context.Context, in *orkstackv1.ExecuteTaskInput,
	attempt int32, startedAt *timestamppb.Timestamp, key memoKey, trace *memoTrace) *memoHit {
	// Row 1.
	if d.Memos == nil || d.Commits == nil || in.GetCommitUri() == "" {
		trace.miss(key, MemoMissStoreAbsent, "this orker has no memo store or commit store, so it reuses nothing")
		return nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, memoLookupTimeout)
	defer cancel()
	timedOut := func() bool {
		return errors.Is(lookupCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
	}
	// Row 2.
	ref := memoRefFor(in, key)
	memo, err := d.Memos.GetMemo(lookupCtx, ref)
	switch {
	case err == nil:
	case timedOut():
		trace.miss(key, MemoMissUnreadable, "the memo lookup for key %s timed out after %s", key.short(), memoLookupTimeout)
		return nil
	case errors.Is(err, artifacts.ErrNotFound):
		trace.miss(key, MemoMissAbsent, "no memo for key %s", key.short())
		return nil
	default:
		trace.miss(key, MemoMissUnreadable, "the memo for key %s could not be read: %v", key.short(), err)
		return nil
	}
	// Row 3.
	if memo.GetSchemaVersion() != artifacts.MemoSchemaVersion {
		trace.miss(key, MemoMissUnreadable, "the memo for key %s has schema_version %d; this orker reads %d",
			key.short(), memo.GetSchemaVersion(), artifacts.MemoSchemaVersion)
		return nil
	}
	namespace := artifacts.ResolveNamespace(in.GetNamespace())
	scope := in.GetCache().GetScope()
	if scope == orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED {
		scope = orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW
	}
	switch {
	case memo.GetKey() != key.Key:
		trace.miss(key, MemoMissMismatch, "the memo stored at key %s names key %q", key.short(), memo.GetKey())
		return nil
	case artifacts.ResolveNamespace(memo.GetNamespace()) != namespace:
		trace.miss(key, MemoMissMismatch, "the memo names namespace %q, not %q", memo.GetNamespace(), namespace)
		return nil
	case memo.GetScope() != scope:
		trace.miss(key, MemoMissMismatch, "the memo's scope is %s, this task's is %s", memo.GetScope(), scope)
		return nil
	case memo.GetTaskId() != in.GetTaskId():
		trace.miss(key, MemoMissMismatch, "the memo names task %q, not %q", memo.GetTaskId(), in.GetTaskId())
		return nil
	case scope == orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW && memo.GetOrkflowName() != in.GetOrkflowName():
		trace.miss(key, MemoMissMismatch, "the memo names orkflow %q, not %q", memo.GetOrkflowName(), in.GetOrkflowName())
		return nil
	case memo.GetSource().GetFinishedAt() == nil || memo.GetSource().GetCommitUri() == "":
		trace.miss(key, MemoMissMismatch, "the memo names no source commit or no producing time")
		return nil
	}
	source := memo.GetSource()
	// Row 4.
	now := d.clock()
	ttl := time.Duration(in.GetCache().GetTtlSeconds()) * time.Second
	if age := now.Sub(source.GetFinishedAt().AsTime()); ttl <= 0 || age >= ttl {
		trace.miss(key, MemoMissExpired, "the memo's result was committed %s ago, at %s; this task reuses results for %s",
			age.Round(time.Second), source.GetFinishedAt().AsTime().UTC().Format(time.RFC3339), ttl)
		return nil
	}
	// Row 5.
	record, err := d.Commits.Get(lookupCtx, source.GetCommitUri())
	switch {
	case err == nil:
	case timedOut():
		trace.miss(key, MemoMissUnreadable, "the memo lookup for key %s timed out after %s", key.short(), memoLookupTimeout)
		return nil
	case errors.Is(err, artifacts.ErrNotFound):
		trace.miss(key, MemoMissSourceGone, "the memo points at %s, which no longer exists (its run was retired)",
			source.GetCommitUri())
		return nil
	default:
		trace.miss(key, MemoMissSourceUnreadable, "the source commit %s could not be read: %v", source.GetCommitUri(), err)
		return nil
	}
	commit := record.Commit
	if commit.GetSchemaVersion() != artifacts.CommitSchemaVersion {
		trace.miss(key, MemoMissSourceUnreadable, "the source commit %s has schema_version %d; this orker reads %d",
			source.GetCommitUri(), commit.GetSchemaVersion(), artifacts.CommitSchemaVersion)
		return nil
	}
	// Row 6.
	if record.SHA256 != source.GetCommitSha256() {
		trace.miss(key, MemoMissSourceChanged, "the source commit %s hashes to %s, the memo recorded %s",
			source.GetCommitUri(), record.SHA256, source.GetCommitSha256())
		return nil
	}
	// Row 7.
	if mismatch := sourceCommitMismatch(commit, in, source, scope); mismatch != "" {
		trace.miss(key, MemoMissSourceMismatch, "the source commit %s is not this task's result: %s",
			source.GetCommitUri(), mismatch)
		return nil
	}
	// Row 8.
	limits := in.GetLimits()
	if max := limits.GetValuesMaxBytes(); max > 0 && commit.GetValues().GetSizeBytes() > max {
		trace.miss(key, MemoMissSourceOverLimits, "the source values.json is %d bytes, over this run's %d-byte limit",
			commit.GetValues().GetSizeBytes(), max)
		return nil
	}
	if max := limits.GetArtifactMaxCount(); max > 0 && int32(len(commit.GetArtifacts())) > max {
		trace.miss(key, MemoMissSourceOverLimits, "the source has %d artifacts, over this run's limit of %d",
			len(commit.GetArtifacts()), max)
		return nil
	}
	for _, artifact := range commit.GetArtifacts() {
		if max := limits.GetArtifactMaxBytes(); max > 0 && artifact.GetSizeBytes() > max {
			trace.miss(key, MemoMissSourceOverLimits, "the source artifact %q is %d bytes, over this run's %d-byte limit",
				artifact.GetName(), artifact.GetSizeBytes(), max)
			return nil
		}
	}
	// Row 9.
	valuesLimit := limits.GetValuesMaxBytes()
	if valuesLimit <= 0 {
		valuesLimit = math.MaxInt32
	}
	values, failure := d.refetchCommittedValues(lookupCtx, commit.GetValues(), valuesLimit)
	if failure != nil {
		if timedOut() {
			trace.miss(key, MemoMissUnreadable, "the memo lookup for key %s timed out after %s", key.short(), memoLookupTimeout)
			return nil
		}
		trace.miss(key, MemoMissSourceObjectAbsent, "the source values.json is unavailable: %s", failure.Message)
		return nil
	}
	if d.Store == nil {
		trace.miss(key, MemoMissSourceObjectAbsent, "this orker has no artifact store to confirm the source's objects")
		return nil
	}
	for _, artifact := range commit.GetArtifacts() {
		if _, err := d.Store.Stat(lookupCtx, artifact.GetUri()); err != nil {
			if timedOut() {
				trace.miss(key, MemoMissUnreadable, "the memo lookup for key %s timed out after %s", key.short(), memoLookupTimeout)
				return nil
			}
			trace.miss(key, MemoMissSourceObjectAbsent, "the source artifact %q at %s is unavailable: %v",
				artifact.GetName(), artifact.GetUri(), err)
			return nil
		}
	}
	// A memo pointing at a HIT commit is followed once: cache.source always
	// names the run whose container produced the bytes, and no chain forms.
	named := &orkstackv1.CacheSource{
		RunId:          commit.GetRunId(),
		TaskId:         commit.GetTaskId(),
		OrkflowName:    commit.GetOrkflowName(),
		OrkflowVersion: commit.GetOrkflowVersion(),
		CommitUri:      source.GetCommitUri(),
		CommitSha256:   record.SHA256,
		StartedAt:      commit.GetStartedAt(),
		FinishedAt:     commit.GetFinishedAt(),
		Item:           source.Item,
		Attempt:        commit.GetAttempt(),
	}
	if prior := commit.GetCache(); prior.GetOutcome() == orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT && prior.GetSource() != nil {
		named = proto.Clone(prior.GetSource()).(*orkstackv1.CacheSource)
	}
	// Row 10: this run's own commit, measured as it will be written.
	hit := d.hitCommit(in, attempt, startedAt, now, key, commit, named)
	document, err := artifacts.MarshalCommit(hit)
	if err != nil {
		trace.miss(key, MemoMissSourceOverLimits, "the commit reusing it could not be encoded: %v", err)
		return nil
	}
	if max := limits.GetCommitMaxBytes(); max > 0 && int64(len(document)) > max {
		trace.miss(key, MemoMissSourceOverLimits, "the commit reusing it would be %d bytes, over this run's %d-byte limit",
			len(document), max)
		return nil
	}
	return &memoHit{record: record, values: values, source: named, commit: hit}
}

// sourceCommitMismatch is row 7: the commit a memo points at must be this
// namespace's, this task's, the memo's run's, as old as the memo says, of this
// pinned image, a success, and — under orkflow scope — this Orkflow's.
func sourceCommitMismatch(commit *orkstackv1.ActivityCommit, in *orkstackv1.ExecuteTaskInput,
	source *orkstackv1.CacheSource, scope orkstackv1.CacheScope) string {
	checks := []struct {
		field, stored, wanted string
	}{
		{"namespace", commit.GetNamespace(), artifacts.ResolveNamespace(in.GetNamespace())},
		{"task_id", commit.GetTaskId(), in.GetTaskId()},
		{"run_id", commit.GetRunId(), source.GetRunId()},
		{"finished_at", timestampString(commit.GetFinishedAt()), timestampString(source.GetFinishedAt())},
		{"image.index_digest", commit.GetImage().GetIndexDigest(), in.GetImage().GetIndexDigest()},
		{"exit_code", strconv.Itoa(int(commit.GetExitCode())), "0"},
	}
	if scope == orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW {
		checks = append(checks, struct{ field, stored, wanted string }{
			"orkflow_name", commit.GetOrkflowName(), in.GetOrkflowName()})
	}
	for _, check := range checks {
		if check.stored != check.wanted {
			return fmt.Sprintf("%s is %q, want %q", check.field, check.stored, check.wanted)
		}
	}
	return ""
}

func timestampString(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}

// hitCommit is this activity's own commit for a reused result: identity
// fields this activity's (so a retry's §5.4 adoption is the ordinary one),
// content fields the source's references (so no byte is copied).
func (d *OCIDispatcher) hitCommit(in *orkstackv1.ExecuteTaskInput, attempt int32, startedAt *timestamppb.Timestamp,
	now time.Time, key memoKey, source *orkstackv1.ActivityCommit, named *orkstackv1.CacheSource) *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{
		SchemaVersion:           artifacts.CommitSchemaVersion,
		Namespace:               artifacts.ResolveNamespace(in.GetNamespace()),
		RunId:                   in.GetRunId(),
		TaskId:                  in.GetTaskId(),
		OrkflowName:             in.GetOrkflowName(),
		OrkflowVersion:          in.GetOrkflowVersion(),
		OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
		IdempotencyKey:          in.GetIdempotencyKey(),
		Image:                   source.GetImage(),
		ExitCode:                0,
		Values:                  source.GetValues(),
		Artifacts:               source.GetArtifacts(),
		SystemArtifacts:         source.GetSystemArtifacts(),
		StartedAt:               startedAt,
		FinishedAt:              timestamppb.New(now),
		Attempt:                 attempt,
		OrkerId:                 d.OrkerID,
		BuildId:                 d.BuildID,
		Cache: &orkstackv1.CacheResult{
			Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT,
			Key:     key.Key,
			Source:  named,
		},
	}
}

// refetchCommittedValues reads a committed values.json of at most limit bytes
// and verifies its digest. refetchValues is this with InlineValuesLimit.
func (d *OCIDispatcher) refetchCommittedValues(ctx context.Context, ref *orkstackv1.ArtifactRef, limit int64) ([]byte, *Failure) {
	if d.Store == nil {
		return nil, Fail(ReasonValuesRefetchFailed, "this orker has no artifact store")
	}
	if ref.GetUri() == "" {
		return nil, Fail(ReasonValuesRefetchFailed, "the commit records no values.json")
	}
	reader, err := d.Store.Get(ctx, ref.GetUri())
	if err != nil {
		return nil, FailCause(ReasonValuesRefetchFailed, err,
			"refetch the committed values.json at %s: %v", ref.GetUri(), err)
	}
	defer func() { _ = reader.Close() }()
	raw, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, FailCause(ReasonValuesRefetchFailed, err,
			"refetch the committed values.json at %s: %v", ref.GetUri(), err)
	}
	if got := HashBytes(raw); got != ref.GetSha256() {
		// The store returned bytes that are not what the commit recorded.
		// That is the store lying, and it is terminal: no retry can make the
		// digest match.
		return nil, Fail(ReasonValuesCorrupt,
			"the committed values.json at %s hashes to %s but the commit records %s",
			ref.GetUri(), got, ref.GetSha256())
	}
	return raw, nil
}

// commitHit writes this activity's own commit for a reused result and
// returns the ExecuteTaskResult. It is the §4.1 sequence of AGENTS.md with
// the upload replaced by a check: the value was re-verified by digest, the
// objects confirmed present, then THIS run's commit.json is created, and only
// then is success reported.
func (d *OCIDispatcher) commitHit(ctx context.Context, in *orkstackv1.ExecuteTaskInput,
	attempt int32, startedAt *timestamppb.Timestamp, key memoKey,
	hit *memoHit, trace *memoTrace) (*orkstackv1.ExecuteTaskResult, *Failure) {
	commit := hit.commit
	record, err := d.Commits.Create(ctx, in.GetCommitUri(), commit)
	switch {
	case err == nil:
		trace.result = &orkstackv1.CacheResult{
			Outcome:       orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT,
			Key:           key.Key,
			Source:        hit.source,
			KeyComponents: key.Components,
		}
		trace.log("[ork] cache hit: reusing task %q of run %s (committed %s by %s, attempt %d); key %s; no container ran",
			hit.source.GetTaskId(), hit.source.GetRunId(),
			hit.source.GetFinishedAt().AsTime().UTC().Format(time.RFC3339),
			orkerOf(hit.record.Commit), hit.source.GetAttempt(), key.short())
		result := &orkstackv1.ExecuteTaskResult{
			TaskId: in.GetTaskId(),
			Commit: &orkstackv1.CommitRef{
				Uri:       in.GetCommitUri(),
				Sha256:    record.SHA256,
				Etag:      record.ETag,
				CreatedAt: commit.GetFinishedAt(),
			},
			Artifacts:       commit.GetArtifacts(),
			SystemArtifacts: commit.GetSystemArtifacts(),
			ValuesRef:       commit.GetValues(),
			Image:           commit.GetImage(),
			StartedAt:       startedAt,
			FinishedAt:      commit.GetFinishedAt(),
			OrkerId:         d.OrkerID,
			Attempt:         attempt,
			Cache:           resultCache(trace.result),
		}
		if len(hit.values) <= InlineValuesLimit {
			result.ValuesJson = hit.values
			result.ValuesInlined = true
		}
		return result, nil

	case errors.Is(err, artifacts.ErrPreconditionFailed):
		// Another attempt committed first: adopt it, as commit does, and the
		// trace takes the adopted commit's cache.
		adopted, failure := d.adopt(ctx, in, attempt)
		if failure != nil {
			return nil, failure
		}
		if adopted == nil {
			return nil, FailCause(ReasonCommitWriteFailed, err,
				"the commit at %s was refused as already present, but reading it found nothing",
				in.GetCommitUri())
		}
		if adopted.GetCache() != nil {
			trace.result = proto.Clone(adopted.GetCache()).(*orkstackv1.CacheResult)
		}
		return adopted, nil

	default:
		// The commit store is the store every task needs, and a storage
		// failure can never look like success (AGENTS.md §15). This is the
		// only way the memo step can fail an attempt.
		trace.result = &orkstackv1.CacheResult{
			Outcome:       orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT,
			Key:           key.Key,
			Source:        hit.source,
			KeyComponents: key.Components,
		}
		return nil, FailCause(ReasonCommitWriteFailed, err,
			"write the commit at %s reusing run %s's result: %v", in.GetCommitUri(), hit.source.GetRunId(), err)
	}
}

func orkerOf(commit *orkstackv1.ActivityCommit) string {
	if id := commit.GetOrkerId(); id != "" {
		return id
	}
	return "an unnamed orker"
}

// shouldWriteMemo reports whether a fresh commit under this trace refreshes
// its memo: a MISS with a key, or a BYPASSED.
func shouldWriteMemo(c *orkstackv1.CacheResult) bool {
	switch c.GetOutcome() {
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED:
		return c.GetKey() != ""
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS:
		return c.GetKey() != ""
	}
	return false
}

// writeMemo records the fresh commit as key's memo. Best-effort: a failure is
// logged (warn: run, task, key, error), set on trace.memo_write_error and
// never changes the attempt's outcome. The context is detached from
// cancellation and bounded by memoWriteTimeout.
//
// It is reachable only from commit's SUCCESSFUL create: a memo always points
// at a commit that already exists.
func (d *OCIDispatcher) writeMemo(ctx context.Context, in *orkstackv1.ExecuteTaskInput,
	key memoKey, commit *orkstackv1.ActivityCommit, record artifacts.CommitRecord, trace *memoTrace) {
	if trace == nil || trace.result == nil {
		return
	}
	if d.Memos == nil {
		trace.result.MemoWriteError = "this orker has no memo store"
		trace.log("[ork] cache memo could not be written: %s", trace.result.MemoWriteError)
		return
	}
	scope := in.GetCache().GetScope()
	if scope == orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED {
		scope = orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW
	}
	source := &orkstackv1.CacheSource{
		RunId:          commit.GetRunId(),
		TaskId:         commit.GetTaskId(),
		OrkflowName:    commit.GetOrkflowName(),
		OrkflowVersion: commit.GetOrkflowVersion(),
		CommitUri:      in.GetCommitUri(),
		CommitSha256:   record.SHA256,
		StartedAt:      commit.GetStartedAt(),
		FinishedAt:     commit.GetFinishedAt(),
		Attempt:        commit.GetAttempt(),
	}
	if each := in.GetEach(); each != nil {
		index := each.GetIndex()
		source.Item = &index
	}
	memo := &orkstackv1.CacheMemo{
		SchemaVersion: artifacts.MemoSchemaVersion,
		Key:           key.Key,
		Namespace:     artifacts.ResolveNamespace(in.GetNamespace()),
		Scope:         scope,
		OrkflowName:   in.GetOrkflowName(),
		TaskId:        in.GetTaskId(),
		Source:        source,
		KeyComponents: key.Components,
		CreatedAt:     timestamppb.New(d.clock()),
		OrkerId:       d.OrkerID,
		BuildId:       d.BuildID,
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memoWriteTimeout)
	defer cancel()
	if err := d.Memos.PutMemo(writeCtx, memo); err != nil {
		trace.result.MemoWriteError = truncateString(err.Error(), memoMissDetailMax)
		d.logger().Warn("cache memo not written",
			"run_id", in.GetRunId(), "task_id", in.GetTaskId(), "key", key.Key, "error", err)
		trace.log("[ork] cache memo could not be written: %v", err)
		return
	}
	trace.result.MemoWritten = true
	trace.log("[ork] cache memo written for key %s", key.short())
}

// completeStaging performs what stageInputs deferred: every recorded fetch,
// in recorded order, through fetchInto (so a digest mismatch is still
// INPUT_ARTIFACT_CORRUPT and a missing object INPUT_ARTIFACT_UNAVAILABLE),
// then the secrets exactly as stageInputs resolves them. A no-op when nothing
// was deferred.
func (d *OCIDispatcher) completeStaging(ctx context.Context, in *orkstackv1.ExecuteTaskInput,
	ws *Workspace, staged *stagedInputs) *Failure {
	for _, fetch := range staged.deferred {
		if err := os.MkdirAll(filepath.Dir(fetch.hostPath), 0o755); err != nil {
			return FailCause(ReasonWorkdirFailed, err, "stage input artifact %q: %v", fetch.ref.GetName(), err)
		}
		if failure := d.fetchInto(ctx, fetch.ref, fetch.hostPath); failure != nil {
			return failure
		}
	}
	staged.deferred = nil
	if staged.secretsDeferred {
		staged.secretsDeferred = false
		return d.stageSecrets(in, ws, staged)
	}
	return nil
}
