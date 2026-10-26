package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
