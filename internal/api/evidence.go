package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RunBodyReader is the archive's raw half: the JSONL body as stored.
// engine.RunArchiver implements it; ExportRunEvidence asserts it at call time
// from the RunArchive seam, so a fake archive need not.
type RunBodyReader interface {
	LoadRunBody(ctx context.Context, runID string) ([]byte, error)
}

// ExportRunEvidence streams one finished run's evidence bundle
// (docs/RUN_EVIDENCE.md): MANIFEST.json, then every present member in
// manifest order, as a tar.gz in chunks of at most artifactChunkSize, ending
// with an empty chunk carrying `last`.
//
// It is a READ assembled per request and kept nowhere. Every coordinate comes
// off the archived run — never from the caller — and every digest in the
// manifest is of the bytes as read: a member with a recorded reference is
// verified as it streams and a mismatch ends the stream with DataLoss and no
// `last`. A store that cannot answer is Unavailable, never a reclaimed member
// and never a smaller bundle that looks whole.
func (s *Service) ExportRunEvidence(
	ctx context.Context,
	req *connect.Request[orkstackv1.ExportRunEvidenceRequest],
	stream *connect.ServerStream[orkstackv1.EvidenceChunk],
) error {
	msg := req.Msg
	// 1.
	if msg.GetRunId() == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	// 2. A partial deployment is told, not handed a smaller bundle.
	bodies, ok := s.archive.(RunBodyReader)
	if s.archive == nil || !ok {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("run archive is not configured"))
	}
	if s.artifactStore == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("artifact store is not configured"))
	}
	documents, ok := s.attempts.(artifacts.AttemptDocumentReader)
	if s.attempts == nil || !ok {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("task attempt store is not configured"))
	}
	if s.commits == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("commit store is not configured"))
	}
	// 3. Nothing is read before the namespace check.
	run, err := s.LookupRunIn(ctx, msg.GetRunId(), msg.GetNamespace())
	if err != nil {
		return err
	}
	// 4.
	taskID := msg.GetTaskId()
	if taskID != "" && !hasTask(run, taskID) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("task %q not found in run %q", taskID, run.GetId()))
	}
	// 5.
	if !isTerminalStatus(run.GetStatus()) {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("run %q is %s; evidence is exported for finished runs only", run.GetId(), shortStatus(run.GetStatus())))
	}
	export := &evidenceExport{s: s, small: map[string][]byte{}}
	manifest, err := export.build(ctx, run, bodies, documents, msg)
	if err != nil {
		return err
	}
	chunks := &evidenceChunker{stream: stream}
	if msg.GetManifestOnly() {
		// 10. Nothing large was read.
		document, err := manifest.MarshalJSON()
		if err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
		if _, err := chunks.Write(document); err != nil {
			return err
		}
	} else if err := evidence.Write(ctx, chunks, manifest, export); err != nil {
		// 11. Any error ends the stream without `last`.
		if chunks.err != nil {
			return chunks.err
		}
		return evidenceStreamError(err)
	}
	if err := chunks.flush(); err != nil {
		return err
	}
	// 12.
	return stream.Send(&orkstackv1.EvidenceChunk{Last: true})
}

// evidenceExport is one request's manifest builder and the Source the writer
// reads present members from. small holds the members already read to build
// the manifest, so the bytes hashed and the bytes written are the same bytes.
type evidenceExport struct {
	s     *Service
	small map[string][]byte
}

// build is steps 6 to 9: every member, from the archived snapshot.
func (e *evidenceExport) build(
	ctx context.Context,
	run *orkstackv1.Run,
	bodies RunBodyReader,
	documents artifacts.AttemptDocumentReader,
	msg *orkstackv1.ExportRunEvidenceRequest,
) (*evidence.Manifest, error) {
	namespace := artifacts.ResolveNamespace(run.GetNamespace())
	// 6. The archived body is the snapshot every member is resolved from: it
	// is the run the body attests.
	body, err := bodies.LoadRunBody(ctx, run.GetId())
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("run %q is finished but not yet archived; retry shortly", run.GetId()))
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read archive of run %q: %w", run.GetId(), err))
	}
	archived, _, err := engine.UnmarshalRunArchive(body)
	if err != nil || archived.GetId() != run.GetId() {
		return nil, connect.NewError(connect.CodeDataLoss,
			fmt.Errorf("archive body of run %q does not decode as that run", run.GetId()))
	}
	runKey, err := artifacts.RunArchiveKey(run.GetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	m := &evidence.Manifest{
		SchemaVersion:    evidence.SchemaVersion,
		RunID:            archived.GetId(),
		Namespace:        namespace,
		OrkflowName:      archived.GetOrkflowName(),
		OrkflowVersion:   archived.GetOrkflowVersion(),
		DefinitionDigest: archived.GetOrkflowDefinitionDigest(),
		RunStatus:        shortStatus(archived.GetStatus()),
		RunCreatedAt:     evidenceTime(archived.GetCreatedAt().AsTime(), archived.GetCreatedAt() != nil),
		RunFinishedAt:    evidenceTime(archived.GetFinishedAt().AsTime(), archived.GetFinishedAt() != nil),
		ExportedAt:       e.s.now().UTC().Truncate(time.Second),
		EngineVersion:    e.s.system.EngineVersion,
		BuildID:          e.s.system.BuildID,
		Scope: evidence.Scope{
			TaskID:    msg.GetTaskId(),
			Logs:      !msg.GetExcludeLogs(),
			Artifacts: !msg.GetExcludeArtifacts(),
		},
		Tasks:   []evidence.TaskEntry{},
		Members: []evidence.Member{},
	}
	m.Members = append(m.Members, e.smallMember("run.jsonl", evidence.KindRun, "", 0, runKey, body))

	// 7. The definition, addressed by the digest the run pinned.
	if err := e.definition(ctx, m, namespace, archived); err != nil {
		return nil, err
	}

	// 8. Per task, in snapshot order.
	for _, task := range archived.GetTaskRuns() {
		if msg.GetTaskId() != "" && task.GetTaskId() != msg.GetTaskId() {
			continue
		}
		if err := e.task(ctx, m, archived, task, documents, msg); err != nil {
			return nil, err
		}
	}
	// 9.
	m.Counts = evidence.CountMembers(m.Members)
	return m, nil
}

func (e *evidenceExport) definition(ctx context.Context, m *evidence.Manifest, namespace string, archived *orkstackv1.Run) error {
	origin, err := artifacts.OrkflowKey(namespace, archived.GetOrkflowName(), archived.GetOrkflowVersion())
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("definition key of run %q: %w", archived.GetId(), err))
	}
	origin += "#source_yaml"
	const sourcePath = "orkflow/source.yaml"
	doc, err := e.s.orkflows.Get(ctx, namespace, archived.GetOrkflowName(), archived.GetOrkflowVersion())
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		m.Definition = evidence.Definition{State: evidence.StateReclaimed, Reason: evidence.ReasonDefinitionDeleted}
		m.Members = append(m.Members, evidence.Member{Path: sourcePath, Kind: evidence.KindSource, Origin: origin,
			State: evidence.StateReclaimed, Reason: evidence.ReasonDefinitionDeleted})
		return nil
	case err != nil:
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("read definition %s@%s of run %q: %w",
			archived.GetOrkflowName(), archived.GetOrkflowVersion(), archived.GetId(), err))
	}
	digest, err := dsl.DefinitionDigest(doc)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("digest definition %s@%s of run %q: %w",
			archived.GetOrkflowName(), archived.GetOrkflowVersion(), archived.GetId(), err))
	}
	if digest != archived.GetOrkflowDefinitionDigest() {
		// Content hashes over implicit identity (AGENTS.md §14): a document
		// that does not hash to what the run pinned is not the run's
		// definition, whatever its coordinates say.
		m.Definition = evidence.Definition{State: evidence.StateRejected,
			Reason: evidence.ReasonDefinitionDigestMismatch, StoredDefinitionDigest: digest}
		m.Members = append(m.Members, evidence.Member{Path: sourcePath, Kind: evidence.KindSource, Origin: origin,
			State: evidence.StateRejected, Reason: evidence.ReasonDefinitionDigestMismatch})
		return nil
	}
	m.Definition = evidence.Definition{State: evidence.StatePresent}
	m.Members = append(m.Members, e.smallMember(sourcePath, evidence.KindSource, "", 0, origin, []byte(doc.GetSourceYaml())))
	return nil
}

func (e *evidenceExport) task(
	ctx context.Context,
	m *evidence.Manifest,
	archived *orkstackv1.Run,
	task *orkstackv1.TaskRun,
	documents artifacts.AttemptDocumentReader,
	msg *orkstackv1.ExportRunEvidenceRequest,
) error {
	id := task.GetTaskId()
	if !evidenceTaskID(id) {
		// Cannot happen with today's compiler; a tar path must not be
		// composed from anything else.
		return connect.NewError(connect.CodeInternal, fmt.Errorf("task id %q of run %q is not a slug", id, archived.GetId()))
	}
	prefix := "tasks/" + id + "/"
	entry := evidence.TaskEntry{
		TaskID:         id,
		Status:         shortStatus(task.GetStatus()),
		OutcomeAttempt: task.GetAttempt(),
		ExitCode:       task.GetExitCode(),
		Attempts:       []evidence.AttemptEntry{},
	}

	// commit
	if ref := task.GetCommit(); ref != nil {
		member, err := e.commit(ctx, prefix+"commit.json", id, archived.GetId(), ref)
		if err != nil {
			return err
		}
		m.Members = append(m.Members, member)
	}
	// values
	if ref := task.GetValuesRef(); ref != nil {
		member, err := e.stat(ctx, prefix+"values.json", evidence.KindValues, id, 0, ref)
		if err != nil {
			return err
		}
		m.Members = append(m.Members, member)
	}
	// artifacts, in TaskRun.artifacts order
	if !msg.GetExcludeArtifacts() {
		for _, ref := range task.GetArtifacts() {
			member, err := e.stat(ctx, prefix+"artifacts/"+evidence.MemberName(ref.GetName()), evidence.KindArtifact, id, 0, ref)
			if err != nil {
				return err
			}
			member.Name = ref.GetName()
			m.Members = append(m.Members, member)
		}
	}
	// attempts, each record before its logs
	docs, err := documents.ReadAttemptDocuments(ctx, archived.GetId(), id)
	if err != nil {
		return connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read attempt records of task %q in run %q: %w", id, archived.GetId(), err))
	}
	byAttempt := make(map[int32]artifacts.AttemptDocument, len(docs))
	highest := task.GetAttempt()
	for _, doc := range docs {
		if doc.Record.GetRunId() != archived.GetId() || doc.Record.GetTaskId() != id || doc.Ref.RunID != archived.GetId() || doc.Ref.TaskID != id {
			return connect.NewError(connect.CodeDataLoss,
				fmt.Errorf("attempt record %s does not belong to task %q of run %q", doc.Key, id, archived.GetId()))
		}
		if _, seen := byAttempt[doc.Ref.Attempt]; seen {
			continue
		}
		byAttempt[doc.Ref.Attempt] = doc
		highest = max(highest, doc.Ref.Attempt)
	}
	for n := int32(1); n <= highest; n++ {
		recordPath := fmt.Sprintf("%sattempts/%d.json", prefix, n)
		doc, recorded := byAttempt[n]
		if recorded {
			m.Members = append(m.Members, e.smallMember(recordPath, evidence.KindAttempt, id, n, doc.Key, doc.Raw))
			entry.Attempts = append(entry.Attempts, evidence.AttemptEntry{
				Attempt:       n,
				State:         evidence.StatePresent,
				Outcome:       strings.TrimPrefix(doc.Record.GetOutcome().String(), "ATTEMPT_OUTCOME_"),
				CommitAdopted: doc.Record.GetCommitAdopted(),
			})
		} else {
			key, err := artifacts.AttemptKey(artifacts.AttemptRef{RunID: archived.GetId(), TaskID: id, Attempt: n})
			if err != nil {
				return connect.NewError(connect.CodeInternal, err)
			}
			m.Members = append(m.Members, evidence.Member{Path: recordPath, Kind: evidence.KindAttempt, TaskID: id,
				Attempt: n, Origin: key, State: evidence.StateUnrecorded, Reason: evidence.ReasonAttemptRecordMissing})
			entry.Attempts = append(entry.Attempts, evidence.AttemptEntry{Attempt: n, State: evidence.StateUnrecorded})
		}
		if msg.GetExcludeLogs() {
			continue
		}
		// The snapshot is authoritative for the outcome attempt and survives
		// the loss of its record; every other attempt's logs are what its
		// own record says it uploaded.
		var refs []*orkstackv1.ArtifactRef
		switch {
		case n == task.GetAttempt():
			refs = task.GetSystemArtifacts()
		case recorded:
			refs = doc.Record.GetSystemArtifacts()
		}
		for _, stream := range []struct {
			name string
			kind evidence.Kind
		}{{"stdout.log", evidence.KindStdout}, {"stderr.log", evidence.KindStderr}} {
			ref := refNamed(refs, stream.name)
			if ref == nil {
				continue
			}
			member, err := e.stat(ctx, fmt.Sprintf("%sattempts/%d/%s", prefix, n, stream.name), stream.kind, id, n, ref)
			if err != nil {
				return err
			}
			m.Members = append(m.Members, member)
		}
	}
	m.Tasks = append(m.Tasks, entry)
	return nil
}

// commit reads the commit record the snapshot's CommitRef names, verbatim.
func (e *evidenceExport) commit(ctx context.Context, memberPath, taskID, runID string, ref *orkstackv1.CommitRef) (evidence.Member, error) {
	recorded := evidence.NormalizeDigest(ref.GetSha256())
	member := evidence.Member{Path: memberPath, Kind: evidence.KindCommit, TaskID: taskID, Origin: ref.GetUri()}
	rec, err := e.s.commits.Get(ctx, ref.GetUri())
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		member.State, member.Reason, member.SHA256 = evidence.StateReclaimed, evidence.ReasonCommitMissing, recorded
		return member, nil
	case errors.Is(err, artifacts.ErrCommitUndecodable):
		// The snapshot's CommitRef was minted from decodable bytes; bytes that
		// no longer decode are corruption, not absence.
		return member, connect.NewError(connect.CodeDataLoss,
			fmt.Errorf("commit of task %q in run %q is undecodable", taskID, runID))
	case err != nil:
		return member, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read commit of task %q in run %q: %w", taskID, runID, err))
	}
	if evidence.NormalizeDigest(rec.SHA256) != recorded {
		// An immutable record under a create-if-absent key cannot
		// legitimately differ from the digest the run recorded.
		return member, connect.NewError(connect.CodeDataLoss,
			fmt.Errorf("commit of task %q in run %q does not hash to the recorded digest", taskID, runID))
	}
	return e.smallMember(memberPath, evidence.KindCommit, taskID, 0, ref.GetUri(), rec.Raw), nil
}

// stat is a large member: the recorded reference, confirmed to exist. The
// bytes are read and verified only while streaming.
func (e *evidenceExport) stat(ctx context.Context, memberPath string, kind evidence.Kind, taskID string, attempt int32, ref *orkstackv1.ArtifactRef) (evidence.Member, error) {
	member := evidence.Member{Path: memberPath, Kind: kind, TaskID: taskID, Attempt: attempt, Origin: ref.GetUri(),
		SizeBytes: ref.GetSizeBytes(), SHA256: evidence.NormalizeDigest(ref.GetSha256())}
	if member.SHA256 == "" {
		// Cannot happen: the Orker hashes before it uploads. A bundle cannot
		// attest a byte it has no digest for.
		return member, connect.NewError(connect.CodeDataLoss,
			fmt.Errorf("recorded %s %q of task %q has no sha256", kind, ref.GetName(), taskID))
	}
	_, err := e.s.artifactStore.Stat(ctx, ref.GetUri())
	switch {
	case err == nil:
		member.State = evidence.StatePresent
	case errors.Is(err, artifacts.ErrNotFound):
		member.State, member.Reason = evidence.StateReclaimed, evidence.ReasonObjectMissing
	default:
		return member, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("stat recorded artifact %q of task %q: %w", ref.GetName(), taskID, err))
	}
	return member, nil
}

// smallMember records a member whose bytes are already in hand.
func (e *evidenceExport) smallMember(memberPath string, kind evidence.Kind, taskID string, attempt int32, origin string, content []byte) evidence.Member {
	sum := sha256.Sum256(content)
	e.small[memberPath] = content
	return evidence.Member{Path: memberPath, Kind: kind, TaskID: taskID, Attempt: attempt, Origin: origin,
		SizeBytes: int64(len(content)), SHA256: hex.EncodeToString(sum[:]), State: evidence.StatePresent}
}

// Open serves small members from the copy the manifest was built from, and
// large members from the artifact store by their recorded URI.
func (e *evidenceExport) Open(ctx context.Context, m evidence.Member) (io.ReadCloser, error) {
	if content, ok := e.small[m.Path]; ok {
		return io.NopCloser(bytes.NewReader(content)), nil
	}
	return e.s.artifactStore.Get(ctx, m.Origin)
}

// evidenceStreamError maps a writer failure to the code the stream ends with.
func evidenceStreamError(err error) error {
	var memberErr *evidence.MemberError
	if !errors.As(err, &memberErr) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return connect.NewError(connect.CodeCanceled, err)
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	member := memberErr.Member
	name := member.Name
	if name == "" {
		name = path.Base(member.Path)
	}
	switch {
	case errors.Is(err, evidence.ErrMemberVanished):
		// A lifecycle rule ran between stat and stream. The manifest already
		// said present, and a bundle must never contradict its own manifest.
		return connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("artifact %q of task %q disappeared during export", name, member.TaskID))
	case errors.Is(err, evidence.ErrMemberMismatch):
		return connect.NewError(connect.CodeDataLoss, fmt.Errorf("member %s failed integrity verification", member.Path))
	default:
		return connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read recorded artifact %q of task %q: %w", name, member.TaskID, memberErr.Err))
	}
}

// evidenceChunker is the io.Writer the bundle is written through: one
// EvidenceChunk per artifactChunkSize bytes, the remainder on flush. err is
// the stream's own failure, kept apart from the writer's.
type evidenceChunker struct {
	stream *connect.ServerStream[orkstackv1.EvidenceChunk]
	buf    []byte
	err    error
}

func (c *evidenceChunker) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	written := len(p)
	for len(p) > 0 {
		room := artifactChunkSize - len(c.buf)
		take := min(room, len(p))
		c.buf = append(c.buf, p[:take]...)
		p = p[take:]
		if len(c.buf) == artifactChunkSize {
			if err := c.send(); err != nil {
				return 0, err
			}
		}
	}
	return written, nil
}

func (c *evidenceChunker) flush() error {
	if c.err != nil {
		return c.err
	}
	if len(c.buf) == 0 {
		return nil
	}
	return c.send()
}

func (c *evidenceChunker) send() error {
	data := c.buf
	c.buf = make([]byte, 0, artifactChunkSize)
	if err := c.stream.Send(&orkstackv1.EvidenceChunk{Data: data}); err != nil {
		c.err = err
		return err
	}
	return nil
}

// refNamed is the reference named name, or nil.
func refNamed(refs []*orkstackv1.ArtifactRef, name string) *orkstackv1.ArtifactRef {
	for _, ref := range refs {
		if ref.GetName() == name {
			return ref
		}
	}
	return nil
}

// evidenceTaskID holds a task id to the DSL slug rule before it becomes a tar
// path segment.
func evidenceTaskID(id string) bool {
	if id == "" || len(id) > 200 || id[0] < 'a' || id[0] > 'z' {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// shortStatus is a Status in the short dialect of docs/JSON.md.
func shortStatus(status orkstackv1.Status) string {
	return strings.TrimPrefix(status.String(), "STATUS_")
}

// evidenceTime is a snapshot timestamp in UTC, or the zero time when unset.
func evidenceTime(t time.Time, set bool) time.Time {
	if !set {
		return time.Time{}
	}
	return t.UTC()
}
