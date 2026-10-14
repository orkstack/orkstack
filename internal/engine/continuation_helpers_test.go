package engine

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The helpers below build continuation objects the way a switch writes them
// (spec 2.7), so Stage 1's readers can be tested against chains before any
// writer exists. They are test-only on purpose: the writer is Stage 2's.

// gzipForTest compresses data as one gzip member.
func gzipForTest(t testing.TB, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// gzipObjectForTest marshals msg, gzips it and returns the stored bytes with
// the reference that names them at their content-addressed key.
func gzipObjectForTest(t testing.TB, msg proto.Message, namespace, runID string, generation int64, kind string) ([]byte, *orkstackv1.ContinuationObject) {
	t.Helper()
	plain, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	data := gzipForTest(t, plain)
	return data, &orkstackv1.ContinuationObject{
		Uri:       continuationURIForTest(t, namespace, runID, generation, kind, data),
		Sha256:    artifacts.ContinuationSHA256(data),
		SizeBytes: int64(len(data)),
		RawBytes:  int64(len(plain)),
	}
}

// checkpointPartsForTest splits run into RunCheckpointParts: part 0 holds the
// header, then rowsPerPart task rows (items cleared) per part, then the item
// rows, rowsPerPart per part.
func checkpointPartsForTest(run *orkstackv1.Run, rowsPerPart int) []*orkstackv1.RunCheckpointPart {
	header := proto.Clone(run).(*orkstackv1.Run)
	header.TaskRuns = nil
	parts := []*orkstackv1.RunCheckpointPart{{Header: header}}
	current := parts[0]
	add := func(row *orkstackv1.TaskRun, item bool) {
		if len(current.GetTaskRuns())+len(current.GetItems()) >= rowsPerPart {
			current = &orkstackv1.RunCheckpointPart{}
			parts = append(parts, current)
		}
		if item {
			current.Items = append(current.Items, row)
		} else {
			current.TaskRuns = append(current.TaskRuns, row)
		}
	}
	for _, row := range run.GetTaskRuns() {
		clone := proto.Clone(row).(*orkstackv1.TaskRun)
		clone.Items = nil
		add(clone, false)
	}
	for _, row := range run.GetTaskRuns() {
		for _, item := range row.GetItems() {
			add(proto.Clone(item).(*orkstackv1.TaskRun), true)
		}
	}
	return parts
}

// checkpointObjectForTest encodes parts as concatenated gzip members and
// returns the stored bytes with the reference that names them at their
// content-addressed key.
func checkpointObjectForTest(t testing.TB, parts []*orkstackv1.RunCheckpointPart, namespace, runID string, generation int64) ([]byte, *orkstackv1.ContinuationObject) {
	t.Helper()
	var data []byte
	obj := &orkstackv1.ContinuationObject{}
	for _, part := range parts {
		plain, err := proto.Marshal(part)
		if err != nil {
			t.Fatal(err)
		}
		member := gzipForTest(t, plain)
		obj.Parts = append(obj.Parts, &orkstackv1.ContinuationPart{
			Offset:    int64(len(data)),
			SizeBytes: int64(len(member)),
			RawBytes:  int64(len(plain)),
			Sha256:    artifacts.ContinuationSHA256(member),
		})
		obj.RawBytes += int64(len(plain))
		data = append(data, member...)
	}
	obj.SizeBytes = int64(len(data))
	obj.Sha256 = artifacts.ContinuationSHA256(data)
	obj.Uri = continuationURIForTest(t, namespace, runID, generation, artifacts.ContinuationKindCheckpoint, data)
	return data, obj
}

// continuationURIForTest is the s3 URI a store rooted at bucket "test" would
// return for the object.
func continuationURIForTest(t testing.TB, namespace, runID string, generation int64, kind string, data []byte) string {
	t.Helper()
	key, err := artifacts.RunContinuationKey(namespace, runID, generation, kind, artifacts.ContinuationSHA256(data))
	if err != nil {
		t.Fatal(err)
	}
	return "s3://test/" + key
}

// memSnapshotStore is an in-memory SnapshotStore that records every read, so
// a test can prove a reader read nothing (or exactly what it should).
type memSnapshotStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	reads   []string
	// fail, when set for a URI, is returned by ReadSnapshot instead of the
	// object: a transient store failure.
	fail map[string]error
}

func newMemSnapshotStore() *memSnapshotStore {
	return &memSnapshotStore{objects: map[string][]byte{}, fail: map[string]error{}}
}

func (s *memSnapshotStore) WriteSnapshot(_ context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	key, err := artifacts.RunSnapshotKey(namespace, runID, sequence)
	if err != nil {
		return "", err
	}
	uri := "s3://test/" + key
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[uri] = bytes.Clone(data)
	return uri, nil
}

func (s *memSnapshotStore) ReadSnapshot(_ context.Context, uri string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, uri)
	if err := s.fail[uri]; err != nil {
		return nil, err
	}
	data, ok := s.objects[uri]
	if !ok {
		return nil, fmt.Errorf("mem snapshot %q: %w", uri, artifacts.ErrNotFound)
	}
	return bytes.Clone(data), nil
}

func (s *memSnapshotStore) put(uri string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[uri] = bytes.Clone(data)
}

func (s *memSnapshotStore) get(uri string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[uri]
	return data, ok
}

func (s *memSnapshotStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.reads)
}
