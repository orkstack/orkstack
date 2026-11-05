package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// scriptedCommitStore answers the check with what a store's outage looks like,
// and carries the probe key the real backends expose.
type scriptedCommitStore struct {
	createErr, getErr error
	creates, gets     int
}

func (s *scriptedCommitStore) commitProbeURI() string { return "s3://b/" + CommitProbeKey }

func (s *scriptedCommitStore) Create(context.Context, string, *orkstackv1.ActivityCommit) (CommitRecord, error) {
	s.creates++
	return CommitRecord{}, s.createErr
}

func (s *scriptedCommitStore) Get(context.Context, string) (CommitRecord, error) {
	s.gets++
	return CommitRecord{}, s.getErr
}

func TestCheckCommitStore_FailsOnlyWhenTheStoreDoesNotAnswer(t *testing.T) {
	down := errors.New("connection refused")
	cases := []struct {
		name              string
		createErr, getErr error
		wantErr           error
	}{
		{"the probe's winner refuses the create", ErrPreconditionFailed, nil, nil},
		{"the key was gone and the create wrote it again", nil, nil, nil},
		{"the winner vanished between the two calls", ErrPreconditionFailed, ErrNotFound, nil},
		{"an undecodable object is still an answer", ErrPreconditionFailed, ErrCommitUndecodable, nil},
		{"the write path is down", down, nil, down},
		{"the read path is down", ErrPreconditionFailed, down, down},
		{"a timeout", context.DeadlineExceeded, nil, context.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &scriptedCommitStore{createErr: tc.createErr, getErr: tc.getErr}
			err := CheckCommitStore(context.Background(), store)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("CheckCommitStore = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCheckCommitStore_RefusesNoStore(t *testing.T) {
	if err := CheckCommitStore(context.Background(), nil); err == nil {
		t.Fatal("a nil store passed the check")
	}
}

// The local backend: a healthy directory passes and adds nothing but the probe
// object, and a directory it cannot write fails, because a local store on a
// full or read-only disk loses completions exactly as an unreachable bucket does.
func TestCheckCommitStore_LocalBackend(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ProbeCommitStore(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := CheckCommitStore(context.Background(), store); err != nil {
			t.Fatalf("a healthy local store failed the check: %v", err)
		}
	}
	var files int
	if err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			files++
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if files != 1 {
		t.Fatalf("three checks left %d files, want only the probe object", files)
	}

	if err := os.Chmod(root, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })
	if err := CheckCommitStore(context.Background(), store); err == nil {
		t.Fatal("a local store it cannot reach passed the check")
	}
}

func TestCommitProbeKeyTracksCommitSchemaVersion(t *testing.T) {
	want := fmt.Sprintf("%s/schema-v%d/conditional-create/%s", CommitProbePrefix, CommitSchemaVersion, CommitObjectName)
	if CommitProbeKey != want {
		t.Fatalf("CommitProbeKey = %q, want %q; a schema bump must move the durable probe key", CommitProbeKey, want)
	}
}

func TestProbeCommitStoreRepeatedCallsUseOneObjectOutsideCommitPrefix(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := ProbeCommitStore(context.Background(), store); err != nil {
			t.Fatalf("probe %d: %v", i+1, err)
		}
	}

	var files []string
	if err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			rel, err := filepath.Rel(root, name)
			if err != nil {
				return err
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != CommitProbeKey {
		t.Fatalf("files after three probes = %v, want only %q", files, CommitProbeKey)
	}
	if strings.HasPrefix(files[0], CommitPrefix+"/") {
		t.Fatalf("probe object %q polluted completion prefix %q", files[0], CommitPrefix)
	}
}

func TestProbeCommitStoreStillRejectsBackendWithoutConditionalCreate(t *testing.T) {
	store := &nonConditionalProbeStore{}
	err := ProbeCommitStore(context.Background(), store)
	if err == nil {
		t.Fatal("ProbeCommitStore accepted a backend where both writers overwrite")
	}
	if errors.Is(err, ErrCommitStoreUnavailable) {
		t.Fatalf("two winners are the store's answer, not a store that could not answer: %v", err)
	}
}

// s3Answer is the error an S3 call returns when the store answers with status.
func s3Answer(status int) error {
	return &smithy.OperationError{ServiceID: "S3", OperationName: "PutObject", Err: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      fmt.Errorf("status %d", status),
	}}
}

// TestProbeCommitStoreSaysWhetherTheStoreAnswered: a probe that could not use
// the store has proved nothing about conditional writes, and the first start of
// a stack is exactly that case: its store refuses a write with 403 until it has
// created its bucket. A store that answers the conditional write with a refusal
// of the request itself may simply not support it, so that stays its own answer.
func TestProbeCommitStoreSaysWhetherTheStoreAnswered(t *testing.T) {
	conflict := fmt.Errorf("create: %w", ErrPreconditionFailed)
	tests := []struct {
		name            string
		createErr       error
		getErr          error
		wantUnavailable bool
	}{
		{name: "a write refused 403, as before the store has its bucket", createErr: s3Answer(http.StatusForbidden), wantUnavailable: true},
		{name: "a write refused 401", createErr: s3Answer(http.StatusUnauthorized), wantUnavailable: true},
		{name: "no such bucket", createErr: s3Answer(http.StatusNotFound), wantUnavailable: true},
		{name: "a server error", createErr: s3Answer(http.StatusInternalServerError), wantUnavailable: true},
		{name: "a store that is overloaded", createErr: s3Answer(http.StatusServiceUnavailable), wantUnavailable: true},
		{
			name:            "no response at all",
			createErr:       &smithy.OperationError{ServiceID: "S3", OperationName: "PutObject", Err: errors.New("connection refused")},
			wantUnavailable: true,
		},
		{name: "the winner cannot be read back", createErr: conflict, getErr: s3Answer(http.StatusForbidden), wantUnavailable: true},

		{name: "If-None-Match not implemented", createErr: s3Answer(http.StatusNotImplemented)},
		{name: "If-None-Match refused as a bad request", createErr: s3Answer(http.StatusBadRequest)},
		{name: "a method the store does not allow", createErr: s3Answer(http.StatusMethodNotAllowed)},
		{name: "a failure that is not the S3 client's", createErr: errors.New("link: operation not supported")},
		{name: "both writers refused, and no winner stored", createErr: conflict, getErr: ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &failingProbeStore{createErr: tt.createErr, getErr: tt.getErr}
			err := ProbeCommitStore(context.Background(), store)
			if err == nil {
				t.Fatal("ProbeCommitStore accepted the store")
			}
			if got := errors.Is(err, ErrCommitStoreUnavailable); got != tt.wantUnavailable {
				t.Errorf("unavailable = %v, want %v: %v", got, tt.wantUnavailable, err)
			}
			// The store's own answer stays in the message, whichever it was.
			cause := tt.getErr
			if cause == nil {
				cause = tt.createErr
			}
			if !errors.Is(err, cause) {
				t.Errorf("the refusal lost the store's answer %v: %v", cause, err)
			}
		})
	}
}

type failingProbeStore struct {
	createErr error
	getErr    error
}

func (s *failingProbeStore) commitProbeURI() string {
	return "memory://probe/" + CommitProbeKey
}

func (s *failingProbeStore) Create(context.Context, string, *orkstackv1.ActivityCommit) (CommitRecord, error) {
	return CommitRecord{}, s.createErr
}

func (s *failingProbeStore) Get(context.Context, string) (CommitRecord, error) {
	return CommitRecord{}, s.getErr
}

type nonConditionalProbeStore struct {
	mu     sync.Mutex
	commit *orkstackv1.ActivityCommit
}

func (s *nonConditionalProbeStore) commitProbeURI() string {
	return "memory://probe/" + CommitProbeKey
}

func (s *nonConditionalProbeStore) Create(_ context.Context, _ string, commit *orkstackv1.ActivityCommit) (CommitRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commit = commit
	return CommitRecord{Commit: commit}, nil
}

func (s *nonConditionalProbeStore) Get(_ context.Context, _ string) (CommitRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commit == nil {
		return CommitRecord{}, ErrNotFound
	}
	return CommitRecord{Commit: s.commit}, nil
}

func TestLocalCommitStoreAcceptsOnlyTheFixedProbeKeyOutsideCommitPrefix(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	probeURI := store.commitProbeURI()
	candidate := commitProbeCandidate()
	if _, err := store.Create(context.Background(), probeURI, candidate); err != nil {
		t.Fatalf("fixed probe Create: %v", err)
	}

	bad := (&url.URL{Scheme: "file", Path: filepath.Join(root, "metadata", "probes", "other", CommitObjectName)}).String()
	if _, err := store.Create(context.Background(), bad, candidate); !errors.Is(err, ErrInvalidURI) {
		t.Fatalf("Create outside fixed probe key = %v, want ErrInvalidURI", err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(CommitProbeKey))); err != nil {
		t.Fatalf("fixed probe object: %v", err)
	}
}
