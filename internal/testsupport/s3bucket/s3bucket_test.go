package s3bucket_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// validBucket is S3's bucket naming rule for the subset Name produces:
// 3–63 bytes of lowercase letters, digits and hyphens, starting and ending
// with a letter or digit.
var validBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func TestName_IsAValidBucketUniqueToTheProcess(t *testing.T) {
	cases := []struct {
		name   string
		label  string
		pid    int
		suffix int64
		want   string
	}{
		{"ordinary", "orkctl", 89965, 1791094506683583630, "orkstack-orkctl-89965-683583630"},
		{"negative suffix", "ork", 1, -42, "orkstack-ork-1-42"},
		{"longest label, largest pid", "abcdefghij-klmnopqrs", 4194304, 999_999_999, "orkstack-abcdefghij-klmnopqrs-4194304-999999999"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := s3bucket.Name(tc.label, tc.pid, tc.suffix)
			if got != tc.want {
				t.Fatalf("Name(%q, %d, %d) = %q, want %q", tc.label, tc.pid, tc.suffix, got, tc.want)
			}
			if !validBucket.MatchString(got) {
				t.Fatalf("Name(%q, %d, %d) = %q is not a valid S3 bucket name", tc.label, tc.pid, tc.suffix, got)
			}
		})
	}
	if a, b := s3bucket.Name("orkctl", 7, 1), s3bucket.Name("orkctl", 8, 1); a == b {
		t.Fatalf("two processes share bucket %q", a)
	}
}

func TestName_RefusesALabelThatCannotFormABucket(t *testing.T) {
	for _, label := range []string{"", "Orkctl", "1orkctl", "ork_ctl", "a-label-far-too-long-for-it"} {
		t.Run(label, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Name(%q, ...) did not refuse the label", label)
				}
			}()
			s3bucket.Name(label, 1, 1)
		})
	}
}

// fakeStore is the part of an S3-compatible store Remove talks to, over
// path-style HTTP: delete a bucket, list one, delete an object. It records
// every request so a test can say how many a removal cost.
type fakeStore struct {
	mu       sync.Mutex
	requests []string
	// objects are the keys the bucket holds; gone means the bucket does not
	// exist at all.
	objects []string
	gone    bool
	// deletesNonEmpty makes DeleteBucket remove a bucket together with its
	// objects, as SeaweedFS does with its non-empty-delete setting; without it
	// the store answers BucketNotEmpty, as AWS S3 does.
	deletesNonEmpty bool
}

func (f *fakeStore) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func (f *fakeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// The request's identity: its method and path, and for a listing the one
	// query parameter that makes it one (the SDK adds x-id=… to the others).
	line := r.Method + " " + r.URL.Path
	if r.URL.Query().Get("list-type") == "2" {
		line += "?list-type=2"
	}
	f.requests = append(f.requests, line)

	apiError := func(status int, code string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
	}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	bucket, key := parts[0], ""
	if len(parts) == 2 {
		key = parts[1]
	}
	if f.gone {
		apiError(http.StatusNotFound, "NoSuchBucket")
		return
	}
	switch {
	case r.Method == http.MethodGet && key == "":
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>%s</Name><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`, bucket, len(f.objects))
		for _, k := range f.objects {
			fmt.Fprintf(w, `<Contents><Key>%s</Key></Contents>`, k)
		}
		fmt.Fprint(w, `</ListBucketResult>`)
	case r.Method == http.MethodDelete && key != "":
		kept := f.objects[:0]
		for _, k := range f.objects {
			if k != key {
				kept = append(kept, k)
			}
		}
		f.objects = kept
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete:
		if len(f.objects) > 0 && !f.deletesNonEmpty {
			apiError(http.StatusConflict, "BucketNotEmpty")
			return
		}
		f.objects, f.gone = nil, true
		w.WriteHeader(http.StatusNoContent)
	default:
		apiError(http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func newFakeStore(t *testing.T, f *fakeStore) *s3.Client {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	client, err := s3bucket.Client(context.Background(), artifacts.Config{
		S3Endpoint: server.URL, S3Region: "us-east-1",
		S3AccessKey: "access", S3SecretKey: "secret", S3PathStyle: true,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return client
}

func TestRemove_IsOneRequestWhenTheStoreDeletesANonEmptyBucket(t *testing.T) {
	store := &fakeStore{objects: []string{"metadata/a.json", "runs/b/commit.json"}, deletesNonEmpty: true}
	client := newFakeStore(t, store)

	if err := s3bucket.Remove(context.Background(), client, "orkstack-ork-1-2"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got, want := store.log(), []string{"DELETE /orkstack-ork-1-2"}; !slices.Equal(got, want) {
		t.Fatalf("requests = %q, want exactly %q: a bucket a store can delete whole is not walked object by object", got, want)
	}
}

func TestRemove_EmptiesTheBucketFirstWhenTheStoreRefusesANonEmptyOne(t *testing.T) {
	store := &fakeStore{objects: []string{"metadata/a.json", "runs/b/commit.json"}}
	client := newFakeStore(t, store)

	if err := s3bucket.Remove(context.Background(), client, "orkstack-ork-1-2"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	want := []string{
		"DELETE /orkstack-ork-1-2",                    // refused: BucketNotEmpty
		"GET /orkstack-ork-1-2?list-type=2",           // so it lists
		"DELETE /orkstack-ork-1-2/metadata/a.json",    // deletes each object
		"DELETE /orkstack-ork-1-2/runs/b/commit.json", //
		"DELETE /orkstack-ork-1-2",                    // and then the bucket
	}
	if got := store.log(); !slices.Equal(got, want) {
		t.Fatalf("requests =\n%q\nwant\n%q", got, want)
	}
	if !store.gone {
		t.Fatal("the bucket is still there")
	}
}

func TestRemove_ABucketAlreadyGoneIsRemoved(t *testing.T) {
	store := &fakeStore{gone: true}
	client := newFakeStore(t, store)

	if err := s3bucket.Remove(context.Background(), client, "orkstack-ork-1-2"); err != nil {
		t.Fatalf("Remove of a bucket that is already gone: %v", err)
	}
	if got := store.log(); len(got) != 1 {
		t.Fatalf("requests = %q, want the one delete", got)
	}
}

func TestRemove_AnyOtherRefusalIsReturned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>no</Message></Error>`)
	}))
	t.Cleanup(server.Close)
	client, err := s3bucket.Client(context.Background(), artifacts.Config{
		S3Endpoint: server.URL, S3Region: "us-east-1", S3AccessKey: "a", S3SecretKey: "s", S3PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s3bucket.Remove(context.Background(), client, "orkstack-ork-1-2")
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("Remove = %v, want the store's AccessDenied", err)
	}
}

func TestFinish(t *testing.T) {
	const bucket = "orkstack-ork-1-2"
	errBoom := errors.New("boom")

	cases := []struct {
		name string
		code int
		// leftFile is "" for none, "ok" for a writable path, "unwritable" for
		// one under a directory that does not exist.
		leftFile    string
		removeErr   error
		wantRemoved bool
		wantLeft    bool
		wantStderr  []string
	}{
		{
			name: "a failure keeps the bucket and nothing else", code: 1, leftFile: "ok",
			wantStderr: []string{"kept s3://" + bucket + " for the failure's post-mortem"},
		},
		{
			name: "a failure with no sweeper keeps it too", code: 1,
			wantStderr: []string{"kept s3://" + bucket + " for the failure's post-mortem"},
		},
		{name: "a pass with no sweeper removes the bucket", code: 0, wantRemoved: true},
		{
			name: "a pass whose removal fails says so", code: 0, removeErr: errBoom, wantRemoved: true,
			wantStderr: []string{"remove s3://" + bucket + ": boom"},
		},
		{
			name: "a pass leaves the bucket to the sweeper", code: 0, leftFile: "ok", wantLeft: true,
			wantStderr: []string{"left s3://" + bucket + " for removal after the run"},
		},
		{
			name: "an unwritable hand-off removes it here", code: 0, leftFile: "unwritable", wantRemoved: true,
			wantStderr: []string{"cannot hand s3://" + bucket + " to its sweeper", "removing it here"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			leftFile := ""
			switch tc.leftFile {
			case "ok":
				leftFile = filepath.Join(dir, "job.bucket")
			case "unwritable":
				leftFile = filepath.Join(dir, "missing-dir", "job.bucket")
			}
			var removed int
			var stderr bytes.Buffer
			s3bucket.Finish(context.Background(), tc.code, bucket, leftFile, func(context.Context) error {
				removed++
				return tc.removeErr
			}, &stderr)

			if got := removed == 1; got != tc.wantRemoved || removed > 1 {
				t.Errorf("remove called %d times, want removed=%v", removed, tc.wantRemoved)
			}
			for _, want := range tc.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr is missing %q:\n%s", want, stderr.String())
				}
			}
			data, err := os.ReadFile(leftFile)
			switch {
			case tc.wantLeft:
				if err != nil || string(data) != bucket+"\n" {
					t.Errorf("hand-off file = %q, %v; want %q", data, err, bucket+"\n")
				}
				if !strings.Contains(stderr.String(), leftFile) {
					t.Errorf("stderr does not name the hand-off file %s:\n%s", leftFile, stderr.String())
				}
			case leftFile != "" && tc.leftFile == "ok":
				if err == nil {
					t.Errorf("a hand-off file was written when the bucket was not left: %q", data)
				}
			}
		})
	}
}

// IsName is what lets a sweeper trust a name it reads from a file: only a
// bucket of the shape Name yields is a test process's own, and the shared
// development bucket never is.
func TestIsName_AcceptsOnlyABucketNameYields(t *testing.T) {
	for _, bucket := range []string{
		s3bucket.Name("orkctl", 89965, 1791094506683583630),
		s3bucket.Name("abcdefghij-klmnopqrs", 4194304, 999_999_999),
		s3bucket.Name("ork", 1, -42),
	} {
		if !s3bucket.IsName(bucket) {
			t.Errorf("IsName(%q) = false, want true", bucket)
		}
	}
	for _, bucket := range []string{
		"",
		"orkstack-artifacts", // the development stack's own bucket
		"orkstack-ork-1",     // no suffix
		"orkstack-ork-x-2",   // a pid that is not a number
		"orkstack--1-2",      // no label
		"orkstack-ork-1-2\n", // a line, not a name
		"orkstack-ork-1-2/x", // a key, not a bucket
		"prod-ork-1-2",
		"ORKSTACK-ork-1-2",
	} {
		if s3bucket.IsName(bucket) {
			t.Errorf("IsName(%q) = true, want false", bucket)
		}
	}
}
