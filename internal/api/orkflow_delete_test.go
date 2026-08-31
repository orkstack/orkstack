package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

type deleteRecordingStore struct {
	inner       *engine.MemoryOrkflowStore
	deleteCalls int
}

func (s *deleteRecordingStore) Put(ctx context.Context, wf *orkstackv1.Orkflow, expected artifacts.HeadExpectation) (*orkstackv1.Orkflow, bool, error) {
	return s.inner.Put(ctx, wf, expected)
}
func (s *deleteRecordingStore) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	return s.inner.Get(ctx, namespace, name, version)
}
func (s *deleteRecordingStore) List(ctx context.Context, namespace string) ([]*orkstackv1.Orkflow, error) {
	return s.inner.List(ctx, namespace)
}
func (s *deleteRecordingStore) Versions(ctx context.Context, namespace, name string) (artifacts.OrkflowVersions, error) {
	return s.inner.Versions(ctx, namespace, name)
}
func (s *deleteRecordingStore) SetCurrentVersion(ctx context.Context, namespace, name, version string) (string, error) {
	return s.inner.SetCurrentVersion(ctx, namespace, name, version)
}
func (s *deleteRecordingStore) Delete(ctx context.Context, namespace, name string) error {
	s.deleteCalls++
	return s.inner.Delete(ctx, namespace, name)
}

func deletionService(t *testing.T, now time.Time, runs []*orkstackv1.Run, readerErr error) (*api.Service, *deleteRecordingStore) {
	t.Helper()
	store := &deleteRecordingStore{inner: engine.NewMemoryOrkflowStore()}
	for index, version := range []string{"1.0.0", "2.0.0"} {
		_, _, err := store.Put(context.Background(), &orkstackv1.Orkflow{
			Name:      "deploy",
			Version:   version,
			CreatedAt: timestamppb.New(now.Add(time.Duration(index) * time.Second)),
		}, artifacts.NoHeadExpectation())
		if err != nil {
			t.Fatalf("seed deploy@%s: %v", version, err)
		}
	}
	byID := make(map[string]*orkstackv1.Run, len(runs))
	for _, run := range runs {
		byID[run.GetId()] = run
	}
	return api.NewService(api.Config{
		Orkflows: store,
		Reader:   &fakeReader{runs: byID, listErr: readerErr},
		Now:      func() time.Time { return now },
	}), store
}

func deleteDeploy(svc *api.Service) (*orkstackv1.DeleteOrkflowResponse, error) {
	response, err := svc.DeleteOrkflow(context.Background(), connect.NewRequest(
		&orkstackv1.DeleteOrkflowRequest{Name: "deploy"}))
	if err != nil {
		return nil, err
	}
	return response.Msg, nil
}

func assertRunSafetyResult(
	t *testing.T,
	store *deleteRecordingStore,
	err error,
	wantCode connect.Code,
) {
	t.Helper()
	if wantCode == connect.CodeUnknown {
		if err != nil {
			t.Fatalf("DeleteOrkflow with old run: %v", err)
		}
		if store.deleteCalls != 1 {
			t.Errorf("store Delete calls = %d, want 1", store.deleteCalls)
		}
		return
	}
	if got := connect.CodeOf(err); got != wantCode {
		t.Fatalf("DeleteOrkflow code = %s, want %s (err: %v)", got, wantCode, err)
	}
	if store.deleteCalls != 0 {
		t.Errorf("store Delete called %d time(s) after safety refusal, want zero", store.deleteCalls)
	}
	if _, getErr := store.Get(context.Background(), "", "deploy", ""); getErr != nil {
		t.Errorf("orkflow disappeared after safety refusal: %v", getErr)
	}
	if !strings.Contains(err.Error(), "deploy") || !strings.Contains(err.Error(), "run(s) block deletion") {
		t.Errorf("refusal is not actionable: %v", err)
	}
}

func TestDeleteOrkflow_DeletesAllVersionsAndResolvesAnEmptyNamespaceToDefault(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	svc, store := deletionService(t, now, nil, nil)
	if _, err := store.SetCurrentVersion(context.Background(), "", "deploy", "1.0.0"); err != nil {
		t.Fatalf("SetCurrentVersion: %v", err)
	}

	response, err := deleteDeploy(svc)
	if err != nil {
		t.Fatalf("DeleteOrkflow: %v", err)
	}
	if response.GetName() != "deploy" || response.GetNamespace() != "default" || !response.GetDeletedAt().AsTime().Equal(now) {
		t.Errorf("response = %+v, want deploy/default deleted at %s", response, now)
	}
	for _, version := range []string{"", "1.0.0", "2.0.0"} {
		if _, err := store.Get(context.Background(), "default", "deploy", version); !errors.Is(err, artifacts.ErrNotFound) {
			t.Errorf("Get(deploy@%q) after delete = %v, want ErrNotFound", version, err)
		}
	}
	versions, err := store.Versions(context.Background(), "default", "deploy")
	if err != nil || len(versions.All) != 0 || versions.Current != "" || versions.Head != "" {
		t.Errorf("Versions after delete = %+v, %v; want empty catalog", versions, err)
	}
}

func TestDeleteOrkflow_ValidatesCoordinatesAndReportsAMissingOrkflow(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	svc, _ := deletionService(t, now, nil, nil)
	tests := []struct {
		name     string
		request  *orkstackv1.DeleteOrkflowRequest
		wantCode connect.Code
	}{
		{"uppercase name", &orkstackv1.DeleteOrkflowRequest{Name: "Deploy"}, connect.CodeInvalidArgument},
		{"leading digit", &orkstackv1.DeleteOrkflowRequest{Name: "1deploy"}, connect.CodeInvalidArgument},
		{"name over 200 bytes", &orkstackv1.DeleteOrkflowRequest{Name: strings.Repeat("a", 201)}, connect.CodeInvalidArgument},
		{"invalid namespace", &orkstackv1.DeleteOrkflowRequest{Name: "deploy", Namespace: "Payments"}, connect.CodeInvalidArgument},
		{"unknown orkflow", &orkstackv1.DeleteOrkflowRequest{Name: "unknown"}, connect.CodeNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := svc.DeleteOrkflow(context.Background(), connect.NewRequest(test.request))
			if got := connect.CodeOf(err); got != test.wantCode {
				t.Errorf("DeleteOrkflow(%+v) code = %s, want %s (err: %v)", test.request, got, test.wantCode, err)
			}
		})
	}
}

func TestDeleteOrkflow_RunSafetyWindowBlocksActiveAndRecentButAllowsOldRuns(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		run      *orkstackv1.Run
		wantCode connect.Code
	}{
		{
			name: "pending run remains active regardless of age",
			run: &orkstackv1.Run{Id: "pending", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_PENDING,
				CreatedAt: timestamppb.New(now.Add(-48 * time.Hour))},
			wantCode: connect.CodeFailedPrecondition,
		},
		{
			name: "run completed one hour ago is recent",
			run: &orkstackv1.Run{Id: "recent", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED,
				FinishedAt: timestamppb.New(now.Add(-time.Hour))},
			wantCode: connect.CodeFailedPrecondition,
		},
		{
			name: "run completed twenty five hours ago is outside the window",
			run: &orkstackv1.Run{Id: "old", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED,
				FinishedAt: timestamppb.New(now.Add(-25 * time.Hour))},
			wantCode: connect.CodeUnknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			svc, store := deletionService(t, now, []*orkstackv1.Run{test.run}, nil)
			_, err := deleteDeploy(svc)
			assertRunSafetyResult(t, store, err, test.wantCode)
		})
	}
}

func TestDeleteOrkflow_RefusesAnOlderActiveRunBeyondTheFirstListPage(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	runs := make([]*orkstackv1.Run, 0, engine.DefaultRunPageSize+1)
	runs = append(runs, &orkstackv1.Run{
		Id: "older-active", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_RUNNING,
		CreatedAt: timestamppb.New(now.Add(-72 * time.Hour)),
	})
	for i := 0; i < engine.DefaultRunPageSize; i++ {
		runs = append(runs, &orkstackv1.Run{
			Id: fmt.Sprintf("newer-old-%03d", i), OrkflowName: "deploy",
			Status:     orkstackv1.Status_STATUS_SUCCEEDED,
			CreatedAt:  timestamppb.New(now.Add(-48*time.Hour + time.Duration(i)*time.Second)),
			FinishedAt: timestamppb.New(now.Add(-48*time.Hour + time.Duration(i)*time.Second)),
		})
	}
	svc, store := deletionService(t, now, runs, nil)
	_, err := deleteDeploy(svc)
	assertRunSafetyResult(t, store, err, connect.CodeFailedPrecondition)
}

func TestDeleteOrkflow_TerminalRunWithoutFinishedAtFailsClosed(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	svc, store := deletionService(t, now, []*orkstackv1.Run{{
		Id: "missing-finished-at", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(now.Add(-time.Minute)),
	}}, nil)
	_, err := deleteDeploy(svc)
	assertRunSafetyResult(t, store, err, connect.CodeFailedPrecondition)
}

func TestDeleteOrkflow_RunReaderFailureFailsClosedAndLeavesTheOrkflowStored(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	svc, store := deletionService(t, now, nil, fmt.Errorf("temporal unavailable"))
	_, err := deleteDeploy(svc)
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("DeleteOrkflow code = %s, want Unavailable (err: %v)", got, err)
	}
	if store.deleteCalls != 0 {
		t.Errorf("store Delete called %d time(s), want zero when safety cannot be evaluated", store.deleteCalls)
	}
	if _, getErr := store.Get(context.Background(), "", "deploy", ""); getErr != nil {
		t.Errorf("orkflow disappeared after reader failure: %v", getErr)
	}
}
