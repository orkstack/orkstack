package orker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Fan-out on the Orker (docs/DSL.md "Fan-out"): an ITEM attempt resolves its
// own element of the array the engine counted, and a DIRECT DEPENDENT of a
// fan-out stages every item from that item's own commit. Neither needs any
// state of its own: the commit store is the record, and the engine sent the
// two facts — count and commit root — that address every item.

// itemSegmentOf renders one item index as its five-wide directory segment,
// the same spelling the store keys use, so a lexicographic `ls` is index
// order.
func itemSegmentOf(index int32) string {
	seg, err := artifacts.ItemSegment(index)
	if err != nil {
		return fmt.Sprintf("%d", index)
	}
	return seg
}

// stageEachUpstream stages every item of one fan-out upstream for a direct
// dependent: each item's commit is read from the commit URI the engine would
// have dispatched it at, its identity checked, and its values and artifacts
// queued for a digest-verified fetch (into now, or deferred) into
// upstream/<task>/<item>/ and artifacts/<task>/<item>/.
//
// The commit reads run transferLimit at a time; what they yield is then walked
// in index order, so task.json's lists are exactly a serial staging's, and the
// failure reported is the lowest-index item's.
func (d *OCIDispatcher) stageEachUpstream(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	ws *Workspace,
	up *orkstackv1.UpstreamRef,
	staged *stagedInputs,
	now *[]deferredFetch,
	deferBytes bool,
) *Failure {
	producer := up.GetTaskId()
	each := up.GetEach()
	safeProducer := safeComponent(producer)
	upstreamDir := filepath.Join(ws.Input, "upstream", safeProducer)
	artifactsDir := filepath.Join(ws.Input, "artifacts", safeProducer)
	// Present even for count == 0, so a workload's `ls` works.
	for _, dir := range []string{upstreamDir, artifactsDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return FailCause(ReasonWorkdirFailed, err, "stage the items of fan-out task %q: %v", producer, err)
		}
	}
	staged.eachUpstream = append(staged.eachUpstream, taskJSONEachUpstream{TaskID: producer, Count: each.GetCount()})
	if each.GetCount() == 0 {
		return nil
	}
	if d.Commits == nil {
		return Fail(ReasonInputUnavailable,
			"this orker has no commit store, so it cannot read the items of fan-out task %q", producer)
	}
	commits := make([]*orkstackv1.ActivityCommit, each.GetCount())
	failures := make([]*Failure, each.GetCount())
	at, err := eachBounded(ctx, int(each.GetCount()), d.transferLimit(), func(ctx context.Context, i int) error {
		commit, failure := d.readItemCommit(ctx, in, producer, each.GetCommitRoot(), int32(i))
		if failure != nil {
			failures[i] = failure
			return failure
		}
		commits[i] = commit
		return nil
	})
	if err != nil {
		if failures[at] != nil {
			return failures[at]
		}
		return FailCause(ReasonInputUnavailable, err, "stage the items of fan-out task %q: %v", producer, err)
	}
	for i := int32(0); i < each.GetCount(); i++ {
		commit := commits[i]
		segment := itemSegmentOf(i)
		index := i
		if values := commit.GetValues(); values.GetUri() != "" {
			dir := filepath.Join(upstreamDir, segment)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return FailCause(ReasonWorkdirFailed, err, "stage fan-out task %q item %d: %v", producer, i, err)
			}
			fetchOrDefer(staged, now, deferBytes, values, filepath.Join(dir, "values.json"))
			valuesPath := ContainerUpstream + "/" + safeProducer + "/" + segment + "/values.json"
			staged.noteInputFile(valuesPath, "values.json", values.GetSha256())
			staged.upstream = append(staged.upstream, taskJSONUpstream{
				TaskID:     producer,
				ValuesPath: valuesPath,
				Item:       &index,
			})
		}
		if len(commit.GetArtifacts()) > 0 {
			dir := filepath.Join(artifactsDir, segment)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return FailCause(ReasonWorkdirFailed, err, "stage fan-out task %q item %d: %v", producer, i, err)
			}
			for _, ref := range commit.GetArtifacts() {
				safe := safeComponent(ref.GetName())
				fetchOrDefer(staged, now, deferBytes, ref, filepath.Join(dir, safe))
				containerPath := ContainerInputArtifacts + "/" + safeProducer + "/" + segment + "/" + safe
				staged.noteInputFile(containerPath, ref.GetName(), ref.GetSha256())
				entry := taskJSONInputArtifact{
					Producer:    producer,
					NameB64:     EncodeArtifactName(ref.GetName()),
					Path:        containerPath,
					SizeBytes:   ref.GetSizeBytes(),
					SHA256:      ref.GetSha256(),
					ContentType: ref.GetContentType(),
					Item:        &index,
				}
				if utf8.ValidString(ref.GetName()) {
					entry.Name = ref.GetName()
				}
				staged.artifacts = append(staged.artifacts, entry)
			}
		}
		// Nothing is added to the renderer's .tasks or paths: rule 24 makes
		// both unreachable for a fan-out.
	}
	return nil
}

// readItemCommit reads and checks one item's commit of a fan-out upstream.
func (d *OCIDispatcher) readItemCommit(
	ctx context.Context, in *orkstackv1.ExecuteTaskInput, producer, commitRoot string, i int32,
) (*orkstackv1.ActivityCommit, *Failure) {
	if err := ctx.Err(); err != nil {
		return nil, FailCause(ReasonInputUnavailable, err, "stage the items of fan-out task %q: %v", producer, err)
	}
	key, err := artifacts.ItemCommitKey(in.GetNamespace(), in.GetRunId(), producer, i)
	if err != nil {
		return nil, FailCause(ReasonInputUnavailable, err,
			"fan-out task %q item %d: compose its commit key: %v", producer, i, err)
	}
	uri := strings.TrimSuffix(commitRoot, "/") + "/" + key
	record, err := d.Commits.Get(ctx, uri)
	switch {
	case err == nil:
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, FailCause(ReasonInputUnavailable, err,
			"fan-out task %q item %d has no commit at %s", producer, i, uri)
	case errors.Is(err, artifacts.ErrCommitUndecodable):
		return nil, FailCause(ReasonCommitUndecodable, err,
			"fan-out task %q item %d: the object at %s is not a valid ActivityCommit", producer, i, uri)
	default:
		return nil, FailCause(ReasonCommitReadFailed, err,
			"fan-out task %q item %d: read the commit at %s: %v", producer, i, uri, err)
	}
	commit := record.Commit
	if commit.GetSchemaVersion() != artifacts.CommitSchemaVersion {
		return nil, Fail(ReasonCommitSchema,
			"fan-out task %q item %d: the commit at %s has schema_version %d; this orker understands %d",
			producer, i, uri, commit.GetSchemaVersion(), artifacts.CommitSchemaVersion)
	}
	if mismatch := itemCommitIdentityMismatch(commit, in, producer, i); mismatch != "" {
		return nil, Fail(ReasonCommitIdentity,
			"fan-out task %q item %d: the commit at %s is for a different activity: %s",
			producer, i, uri, mismatch)
	}
	return commit, nil
}

// itemCommitIdentityMismatch is commitIdentityMismatch for one item of a
// fan-out upstream: the commit must be that item's, of this run and this
// definition.
func itemCommitIdentityMismatch(commit *orkstackv1.ActivityCommit, in *orkstackv1.ExecuteTaskInput, producer string, index int32) string {
	checks := []struct {
		field, stored, wanted string
	}{
		{"namespace", commit.GetNamespace(), artifacts.ResolveNamespace(in.GetNamespace())},
		{"run_id", commit.GetRunId(), in.GetRunId()},
		{"task_id", commit.GetTaskId(), producer},
		{"idempotency_key", commit.GetIdempotencyKey(),
			task.ItemIdempotencyKeyFor(in.GetNamespace(), in.GetRunId(), producer, index)},
		{"orkflow_definition_digest", commit.GetOrkflowDefinitionDigest(), in.GetOrkflowDefinitionDigest()},
	}
	for _, check := range checks {
		if check.stored != check.wanted {
			return fmt.Sprintf("%s is %q, the item's is %q", check.field, check.stored, check.wanted)
		}
	}
	return ""
}

// resolveEachItem returns the exact JSON text of this item's element: for a
// task source, the staged values.json walked by source.path, decoded level by
// level into json.RawMessage so the element's bytes survive; for an input
// source, the protojson rendering of the run input's value. The array must be
// the one the engine counted — length each.count, element [each.index] —
// otherwise the attempt fails EACH_ITEM_UNAVAILABLE.
func (d *OCIDispatcher) resolveEachItem(in *orkstackv1.ExecuteTaskInput, ws *Workspace) (json.RawMessage, *Failure) {
	each := in.GetEach()
	source := each.GetSource()
	unavailable := func(found string) *Failure {
		return Fail(ReasonEachItemUnavailable,
			"task %q item %d: each.over no longer resolves to the %d-item array the engine counted (%s)",
			in.GetTaskId(), each.GetIndex(), each.GetCount(), found)
	}
	if source.GetTaskId() != "" {
		hostPath := filepath.Join(ws.Input, "upstream", safeComponent(source.GetTaskId()), "values.json")
		raw, err := os.ReadFile(hostPath)
		if err != nil {
			return nil, unavailable(fmt.Sprintf("the values of %q were not staged: %v", source.GetTaskId(), err))
		}
		return itemOfRaw(raw, source.GetPath(), each.GetIndex(), each.GetCount(), unavailable)
	}
	value := structpb.NewStructValue(in.GetRunInput().GetData())
	if in.GetRunInput().GetData() == nil {
		value = structpb.NewStructValue(&structpb.Struct{})
	}
	fields := value.GetStructValue().GetFields()
	cur, ok := fields[source.GetInput()]
	if !ok {
		return nil, unavailable(fmt.Sprintf("the run input has no %q", source.GetInput()))
	}
	for _, key := range source.GetPath() {
		obj := cur.GetStructValue()
		if obj == nil {
			return nil, unavailable(fmt.Sprintf("the value above %q is not an object", key))
		}
		next, ok := obj.GetFields()[key]
		if !ok {
			return nil, unavailable(fmt.Sprintf("key %q is absent", key))
		}
		cur = next
	}
	list := cur.GetListValue()
	if list == nil {
		return nil, unavailable("found a value that is not an array")
	}
	if int32(len(list.GetValues())) != each.GetCount() {
		return nil, unavailable(fmt.Sprintf("found %d items", len(list.GetValues())))
	}
	if each.GetIndex() < 0 || each.GetIndex() >= each.GetCount() {
		return nil, unavailable(fmt.Sprintf("item %d is outside the array", each.GetIndex()))
	}
	encoded, err := protojson.Marshal(list.GetValues()[each.GetIndex()])
	if err != nil {
		return nil, FailCause(ReasonEachItemUnavailable, err, "task %q item %d: encode the item: %v",
			in.GetTaskId(), each.GetIndex(), err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return nil, FailCause(ReasonEachItemUnavailable, err, "task %q item %d: encode the item: %v",
			in.GetTaskId(), each.GetIndex(), err)
	}
	return json.RawMessage(compact.Bytes()), nil
}

// itemOfRaw walks raw by path, one level at a time as json.RawMessage, and
// returns element [index] of the array it reaches, byte for byte.
func itemOfRaw(raw []byte, path []string, index, count int32, unavailable func(string) *Failure) (json.RawMessage, *Failure) {
	cur := json.RawMessage(raw)
	for _, key := range path {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(cur, &obj); err != nil || obj == nil {
			return nil, unavailable(fmt.Sprintf("the value above %q is not an object", key))
		}
		next, ok := obj[key]
		if !ok {
			return nil, unavailable(fmt.Sprintf("key %q is absent", key))
		}
		cur = next
	}
	var list []json.RawMessage
	if err := json.Unmarshal(cur, &list); err != nil || list == nil {
		return nil, unavailable("found a value that is not an array")
	}
	if int32(len(list)) != count {
		return nil, unavailable(fmt.Sprintf("found %d items", len(list)))
	}
	if index < 0 || index >= count {
		return nil, unavailable(fmt.Sprintf("item %d is outside the array", index))
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, list[index]); err != nil {
		return nil, unavailable("the item is not valid JSON")
	}
	return json.RawMessage(compact.Bytes()), nil
}

// eachRenderData is the `.each` render root for one item: {<as>: the item
// decoded with UseNumber, "index": i, "count": n}, so a large integer renders
// as written.
func eachRenderData(each *orkstackv1.EachContext, item json.RawMessage) (map[string]any, error) {
	if each == nil {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode the item: %w", err)
	}
	return map[string]any{
		each.GetAs(): value,
		"index":      each.GetIndex(),
		"count":      each.GetCount(),
	}, nil
}
