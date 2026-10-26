package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// fakeStore answers the requests cleanup makes, over path-style HTTP, and
// records them. objects are what the bucket holds; gone means it does not
// exist. A store that refuses a non-empty bucket answers BucketNotEmpty, as AWS
// S3 does; one with deletesNonEmpty removes the bucket and its objects at once.
type fakeStore struct {
	mu              sync.Mutex
	requests        []string
	objects         []string
	gone            bool
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
	// A request is its method and path, plus the query that makes a GET a
	// listing or a POST a batch delete (the SDK adds x-id=… to the rest).
	line := r.Method + " " + r.URL.Path
	if r.URL.Query().Get("list-type") == "2" {
		line += "?list-type=2"
	}
	if _, ok := r.URL.Query()["delete"]; ok {
		line += "?delete"
	}
	f.requests = append(f.requests, line)

	apiError := func(status int, code string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
	}
	if f.gone {
		apiError(http.StatusNotFound, "NoSuchBucket")
		return
	}
	switch {
	case r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`, len(f.objects))
		for _, k := range f.objects {
			fmt.Fprintf(w, `<Contents><Key>%s</Key></Contents>`, k)
		}
		fmt.Fprint(w, `</ListBucketResult>`)
	case r.Method == http.MethodPost:
		f.objects = nil
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`)
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

// bucketsStore is a store of several empty buckets, over path-style HTTP. It
// lists only the buckets in listed (SeaweedFS 4.48 with its auth off lists
// none), answers HEAD for every bucket it holds, and records each DELETE.
type bucketsStore struct {
	mu      sync.Mutex
	held    map[string]bool
	listed  []string
	deleted []string
}

func (f *bucketsStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := strings.TrimPrefix(r.URL.Path, "/")
	switch {
	case r.Method == http.MethodGet && name == "":
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID></ID></Owner><Buckets>`)
		for _, b := range f.listed {
			fmt.Fprintf(w, `<Bucket><Name>%s</Name></Bucket>`, b)
		}
		fmt.Fprint(w, `</Buckets></ListAllMyBucketsResult>`)
	case !f.held[name]:
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		if r.Method != http.MethodHead {
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>NoSuchBucket</Message></Error>`)
		}
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete:
		delete(f.held, name)
		f.deleted = append(f.deleted, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// writeFile writes data at root/rel, making its directories.
func writeFile(t *testing.T, root, rel, data string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A proof a signal stopped leaves its passing processes' buckets named in its
// hand-off files, and a failing one's in its log: on a store whose listing
// shows none of them, those recorded names are what scripts/proof-clean.sh
// finds and removes. A bucket whose process is alive is left alone, a recorded
// bucket the store no longer holds is not listed, and the development bucket
// is never touched.
func TestLeftovers_FindsWhatAStoppedProofRecordedAndLeavesALiveProcesssBucket(t *testing.T) {
	const live = 22 // dead pids are 11, 12, 13 and 15
	root := t.TempDir()
	writeFile(t, root, "stopped/integration/engine-001.bucket", "orkstack-engine-11-1\n")
	writeFile(t, root, "stopped/integration/engine-002.log",
		"--- FAIL: TestX\ns3bucket: kept s3://orkstack-engine-12-2 for the failure's post-mortem\n")
	writeFile(t, root, "stopped/integration.log",
		"bucket sweep: left s3://orkstack-engine-13-3 of engine-003: store unavailable\n")
	writeFile(t, root, "running/integration/engine-004.bucket", "orkstack-engine-22-4\n")
	writeFile(t, root, "old/integration/engine-005.bucket", "orkstack-engine-15-5\n")
	writeFile(t, root, "old/integration/engine-006.bucket", "orkstack-artifacts\n")

	cases := []struct {
		name        string
		remove      bool
		listed      []string
		wantDeleted []string
	}{
		{
			name:        "listed only",
			wantDeleted: nil,
		},
		{
			name:        "removed",
			remove:      true,
			wantDeleted: []string{"orkstack-engine-11-1", "orkstack-engine-12-2", "orkstack-engine-13-3"},
		},
		{
			// A store whose listing works (S3, MinIO) adds what no proof here
			// recorded.
			name:        "a listing store adds its own",
			remove:      true,
			listed:      []string{"orkstack-artifacts", "orkstack-orkctl-11-9"},
			wantDeleted: []string{"orkstack-engine-11-1", "orkstack-engine-12-2", "orkstack-engine-13-3", "orkstack-orkctl-11-9"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &bucketsStore{held: map[string]bool{
				"orkstack-artifacts":   true,
				"orkstack-engine-11-1": true,
				"orkstack-engine-12-2": true,
				"orkstack-engine-13-3": true,
				"orkstack-engine-22-4": true,
				"orkstack-orkctl-11-9": true,
			}, listed: tc.listed}
			server := httptest.NewServer(store)
			t.Cleanup(server.Close)
			t.Setenv("ORK_S3_ENDPOINT", server.URL)
			t.Setenv("ORK_S3_ACCESS_KEY", "access")
			t.Setenv("ORK_S3_SECRET_KEY", "secret")
			ctx := context.Background()
			client, endpoint, err := newBareClient(ctx)
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			var out strings.Builder
			alive := func(pid int) bool { return pid == live }
			if err := findLeftovers(ctx, client, endpoint, root, tc.remove, alive, &out); err != nil {
				t.Fatalf("findLeftovers: %v", err)
			}

			store.mu.Lock()
			deleted := append([]string(nil), store.deleted...)
			store.mu.Unlock()
			slices.Sort(deleted)
			if !slices.Equal(deleted, tc.wantDeleted) {
				t.Errorf("deleted %q, want %q", deleted, tc.wantDeleted)
			}
			got := out.String()
			for _, want := range append(tc.wantDeleted,
				"  orkstack-engine-11-1\n",
				"  orkstack-engine-22-4  (in use: pid 22 is alive; left alone)\n") {
				if !strings.Contains(got, want) {
					t.Errorf("output is missing %q:\n%s", want, got)
				}
			}
			for _, never := range []string{"orkstack-engine-15-5", "orkstack-artifacts"} {
				if strings.Contains(got, never) {
					t.Errorf("output names %s, which is no leftover:\n%s", never, got)
				}
			}
		})
	}
}

func TestCleanup(t *testing.T) {
	const bucket = "orkstack-admin-1-2"
	cases := []struct {
		name         string
		store        *fakeStore
		wantRequests []string
		wantOut      string
	}{
		{
			name:         "one request when the store deletes a non-empty bucket",
			store:        &fakeStore{objects: []string{"a", "b"}, deletesNonEmpty: true},
			wantRequests: []string{"DELETE /" + bucket},
			wantOut:      `removed "` + bucket + `"`,
		},
		{
			name:  "refused as not empty, it is emptied and then deleted",
			store: &fakeStore{objects: []string{"a", "b"}},
			wantRequests: []string{
				"DELETE /" + bucket,
				"GET /" + bucket + "?list-type=2",
				"POST /" + bucket + "?delete",
				"DELETE /" + bucket,
			},
			wantOut: `removed "` + bucket + `"`,
		},
		{
			name:         "a bucket already gone is not an error",
			store:        &fakeStore{gone: true},
			wantRequests: []string{"DELETE /" + bucket},
			wantOut:      `"` + bucket + `" is already gone`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.store)
			t.Cleanup(server.Close)
			t.Setenv("ORK_S3_ENDPOINT", server.URL)
			t.Setenv("ORK_S3_ACCESS_KEY", "access")
			t.Setenv("ORK_S3_SECRET_KEY", "secret")
			ctx := context.Background()
			client, endpoint, err := newBareClient(ctx)
			if err != nil {
				t.Fatalf("client: %v", err)
			}

			var out strings.Builder
			if err := cleanupBucket(ctx, client, bucket, endpoint, &out); err != nil {
				t.Fatalf("cleanupBucket: %v", err)
			}
			if got := tc.store.log(); !slices.Equal(got, tc.wantRequests) {
				t.Errorf("requests =\n%q\nwant\n%q", got, tc.wantRequests)
			}
			if !strings.Contains(out.String(), tc.wantOut) {
				t.Errorf("output %q does not contain %q", out.String(), tc.wantOut)
			}
		})
	}
}
