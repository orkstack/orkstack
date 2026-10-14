package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

type testSnapshotStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	next    int64
}

func newTestSnapshotStore() *testSnapshotStore {
	return &testSnapshotStore{objects: map[string][]byte{}}
}

func (s *testSnapshotStore) WriteSnapshot(_ context.Context, namespace, runID string, sequence int64, data []byte) (string, error) {
	uri := fmt.Sprintf("memory://snapshots/%s/%s/%d.pb", namespace, runID, sequence)
	s.mu.Lock()
	s.objects[uri] = append([]byte(nil), data...)
	s.mu.Unlock()
	return uri, nil
}

func (s *testSnapshotStore) ReadSnapshot(_ context.Context, uri string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[uri]
	if !ok {
		return nil, artifacts.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

// WriteContinuation stores data create-if-absent at its content-addressed
// key under an s3:// root, as the real stores name it, so a carry naming it
// passes artifacts.ContinuationURIMatches.
func (s *testSnapshotStore) WriteContinuation(_ context.Context, namespace, runID string, generation int64, kind string, data []byte) (string, error) {
	key, err := artifacts.RunContinuationKey(namespace, runID, generation, kind, artifacts.ContinuationSHA256(data))
	if err != nil {
		return "", err
	}
	uri := "s3://test/" + key
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.objects[uri]; ok && !bytes.Equal(existing, data) {
		return "", fmt.Errorf("continuation %q exists with other bytes", uri)
	}
	s.objects[uri] = append([]byte(nil), data...)
	return uri, nil
}

// ReadContinuationRange reads a range of a stored continuation object.
func (s *testSnapshotStore) ReadContinuationRange(_ context.Context, uri string, wantSize, offset, length int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[uri]
	if !ok {
		return nil, fmt.Errorf("continuation %q: %w", uri, artifacts.ErrNotFound)
	}
	if int64(len(data)) != wantSize {
		return nil, fmt.Errorf("continuation %q is %d bytes, want %d: %w", uri, len(data), wantSize, artifacts.ErrContinuationSize)
	}
	if offset < 0 || length < 0 || offset+length > int64(len(data)) {
		return nil, fmt.Errorf("continuation %q: range %d+%d outside %d bytes", uri, offset, length, len(data))
	}
	return append([]byte(nil), data[offset:offset+length]...), nil
}

func (s *testSnapshotStore) putRun(t *testing.T, run *orkstackv1.Run) string {
	t.Helper()
	data, err := proto.Marshal(run)
	if err != nil {
		t.Fatalf("marshal run snapshot: %v", err)
	}
	s.mu.Lock()
	s.next++
	sequence := s.next
	s.mu.Unlock()
	uri, err := s.WriteSnapshot(context.Background(), run.GetNamespace(), run.GetId(), sequence, data)
	if err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	return uri
}

func eventActivitiesWithSnapshots(activities *engine.EventActivities) (*engine.EventActivities, *testSnapshotStore) {
	store := newTestSnapshotStore()
	return activities.WithSnapshotStore(store), store
}

// keys is every URI the store holds, sorted.
func (s *testSnapshotStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.objects))
	for uri := range s.objects {
		out = append(out, uri)
	}
	sort.Strings(out)
	return out
}

// get is the bytes at uri, if any.
func (s *testSnapshotStore) get(uri string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[uri]
	return append([]byte(nil), data...), ok
}

// put stores data at uri, overwriting.
func (s *testSnapshotStore) put(uri string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[uri] = append([]byte(nil), data...)
}
