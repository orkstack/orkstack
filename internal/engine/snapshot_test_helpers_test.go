package engine_test

import (
	"context"
	"fmt"
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
