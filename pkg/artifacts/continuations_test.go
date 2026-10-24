package artifacts

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

const (
	testSHA  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	otherSHA = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
)

func TestRunContinuationKey(t *testing.T) {
	cases := []struct {
		name       string
		namespace  string
		runID      string
		generation int64
		kind       string
		sha        string
		want       string
		wantErr    bool
	}{
		{name: "checkpoint", namespace: "team-a", runID: "ork-1-2", generation: 0, kind: ContinuationKindCheckpoint, sha: testSHA,
			want: "metadata/snapshots/team-a/ork-1-2/continuations/000000-checkpoint-" + testSHA + ".pb.gz"},
		{name: "events", namespace: "team-a", runID: "ork-1-2", generation: 3, kind: ContinuationKindEvents, sha: testSHA,
			want: "metadata/snapshots/team-a/ork-1-2/continuations/000003-events-" + testSHA + ".pb.gz"},
		{name: "snapshot", namespace: "team-a", runID: "ork-1-2", generation: 12, kind: ContinuationKindSnapshot, sha: testSHA,
			want: "metadata/snapshots/team-a/ork-1-2/continuations/000012-snapshot-" + testSHA + ".pb.gz"},
		{name: "wide generation", namespace: "", runID: "ork-1-2", generation: 1234567, kind: ContinuationKindEvents, sha: testSHA,
			want: "metadata/snapshots/default/ork-1-2/continuations/1234567-events-" + testSHA + ".pb.gz"},
		{name: "negative generation", runID: "ork-1-2", generation: -1, kind: ContinuationKindEvents, sha: testSHA, wantErr: true},
		{name: "definition", runID: "ork-1-2", kind: ContinuationKindDefinition, sha: testSHA,
			want: "metadata/snapshots/default/ork-1-2/continuations/000000-definition-" + testSHA + ".pb.gz"},
		{name: "unknown kind", runID: "ork-1-2", kind: "ledger", sha: testSHA, wantErr: true},
		{name: "empty run", runID: "", kind: ContinuationKindEvents, sha: testSHA, wantErr: true},
		{name: "uppercase digest", runID: "ork-1-2", kind: ContinuationKindEvents, sha: strings.ToUpper(testSHA), wantErr: true},
		{name: "short digest", runID: "ork-1-2", kind: ContinuationKindEvents, sha: testSHA[:63], wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RunContinuationKey(tc.namespace, tc.runID, tc.generation, tc.kind, tc.sha)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("RunContinuationKey = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("RunContinuationKey: %v", err)
			}
			if got != tc.want {
				t.Fatalf("RunContinuationKey = %q, want %q", got, tc.want)
			}
			// A continuation key is never a {seq}.pb key (spec 1.2).
			if strings.HasSuffix(got, snapshotSuffix) {
				t.Fatalf("continuation key %q ends like a sequence snapshot", got)
			}
		})
	}
}

func TestContinuationURIMatches(t *testing.T) {
	key, err := RunContinuationKey("team-a", "ork-1-2", 2, ContinuationKindCheckpoint, testSHA)
	if err != nil {
		t.Fatal(err)
	}
	s3URI := "s3://bucket/" + key
	fileURI := "file:///var/lib/ork/" + key
	cases := []struct {
		name string
		uri  string
		ns   string
		run  string
		gen  int64
		kind string
		sha  string
		want bool
	}{
		{name: "s3 root", uri: s3URI, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA, want: true},
		{name: "file root", uri: fileURI, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA, want: true},
		{name: "another run", uri: s3URI, ns: "team-a", run: "ork-1-3", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "another namespace", uri: s3URI, ns: "team-b", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "another generation", uri: s3URI, ns: "team-a", run: "ork-1-2", gen: 1, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "another kind", uri: s3URI, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindSnapshot, sha: testSHA},
		{name: "another digest", uri: s3URI, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: otherSHA},
		{name: "s3 without bucket", uri: "s3:///" + key, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "s3 key below a sub-prefix", uri: "s3://bucket/x/" + key, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "file climbing out", uri: "file:///var/lib/../" + key, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "file host", uri: "file://host/var/" + key, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "query", uri: s3URI + "?x=1", ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "other scheme", uri: "https://bucket/" + key, ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "a run whose id ends the same", uri: "s3://bucket/" + strings.Replace(key, "/ork-1-2/", "/xork-1-2/", 1), ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
		{name: "empty", uri: "", ns: "team-a", run: "ork-1-2", gen: 2, kind: ContinuationKindCheckpoint, sha: testSHA},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ContinuationURIMatches(tc.uri, tc.ns, tc.run, tc.gen, tc.kind, tc.sha); got != tc.want {
				t.Fatalf("ContinuationURIMatches(%q) = %v, want %v", tc.uri, got, tc.want)
			}
		})
	}
}

func TestLocalWriteContinuation_CreateIfAbsentIsIdempotent(t *testing.T) {
	store, _ := newLocalSnapshotStore(t)
	ctx := context.Background()
	data := []byte("gzip bytes of a checkpoint")
	first, err := store.WriteContinuation(ctx, "team-a", "ork-1-2", 1, ContinuationKindCheckpoint, data)
	if err != nil {
		t.Fatalf("first WriteContinuation: %v", err)
	}
	second, err := store.WriteContinuation(ctx, "team-a", "ork-1-2", 1, ContinuationKindCheckpoint, data)
	if err != nil {
		t.Fatalf("second WriteContinuation of the same bytes: %v", err)
	}
	if first != second {
		t.Fatalf("URIs differ: %q then %q", first, second)
	}
	if !ContinuationURIMatches(first, "team-a", "ork-1-2", 1, ContinuationKindCheckpoint, ContinuationSHA256(data)) {
		t.Fatalf("URI %q does not match its own key", first)
	}
	got, err := store.ReadSnapshot(ctx, first)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %q, want %q", got, data)
	}
}

func TestLocalWriteContinuation_RefusesOtherBytesAtTheKey(t *testing.T) {
	store, _ := newLocalSnapshotStore(t)
	ctx := context.Background()
	data := []byte("the real object")
	key, err := RunContinuationKey("team-a", "ork-1-2", 1, ContinuationKindEvents, ContinuationSHA256(data))
	if err != nil {
		t.Fatal(err)
	}
	// Something else already sits at the content key: a corrupted object.
	if err := store.root.MkdirAll(key[:strings.LastIndex(key, "/")], localDirPerm); err != nil {
		t.Fatal(err)
	}
	if err := store.root.WriteFile(key, []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteContinuation(ctx, "team-a", "ork-1-2", 1, ContinuationKindEvents, data); err == nil {
		t.Fatal("WriteContinuation accepted a key holding other bytes")
	}
	existing, err := store.root.ReadFile(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(existing) != "tampered" {
		t.Fatalf("WriteContinuation overwrote the existing object: %q", existing)
	}
}

func TestLocalReadContinuationRange_RefusesASizeMismatch(t *testing.T) {
	store, _ := newLocalSnapshotStore(t)
	ctx := context.Background()
	data := []byte("0123456789")
	uri, err := store.WriteContinuation(ctx, "team-a", "ork-1-2", 0, ContinuationKindCheckpoint, data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadContinuationRange(ctx, uri, 10, 3, 4)
	if err != nil {
		t.Fatalf("ReadContinuationRange: %v", err)
	}
	if string(got) != "3456" {
		t.Fatalf("range = %q, want 3456", got)
	}
	if _, err := store.ReadContinuationRange(ctx, uri, 11, 0, 4); !errors.Is(err, ErrContinuationSize) {
		t.Fatalf("size mismatch err = %v, want ErrContinuationSize", err)
	}
	if _, err := store.ReadContinuationRange(ctx, uri, 10, 8, 4); err == nil || errors.Is(err, ErrContinuationSize) {
		t.Fatalf("a range past the recorded size = %v, want an argument error", err)
	}
	missing := strings.Replace(uri, ContinuationSHA256(data), otherSHA, 1)
	if _, err := store.ReadContinuationRange(ctx, missing, 10, 0, 4); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object err = %v, want ErrNotFound", err)
	}
	seq, err := store.WriteSnapshot(ctx, "team-a", "ork-1-2", 1, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadContinuationRange(ctx, seq, 10, 0, 4); !errors.Is(err, ErrInvalidURI) {
		t.Fatalf("a {seq}.pb URI = %v, want ErrInvalidURI", err)
	}
}

func TestParseURI_AcceptsContinuationKeysOnly(t *testing.T) {
	local, root := newLocalSnapshotStore(t)
	s3 := &S3RunSnapshotStore{bucket: "bucket"}
	contKey, err := RunContinuationKey("team-a", "ork-1-2", 1, ContinuationKindSnapshot, testSHA)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		key  string
		ok   bool
	}{
		{name: "sequence snapshot", key: "metadata/snapshots/team-a/ork-1-2/7.pb", ok: true},
		{name: "continuation", key: contKey, ok: true},
		{name: "gzip outside continuations", key: "metadata/snapshots/team-a/ork-1-2/7.pb.gz"},
		{name: "continuations without suffix", key: "metadata/snapshots/team-a/ork-1-2/continuations/x.json"},
		{name: "outside the prefix", key: "metadata/runs/team-a/ork-1-2/continuations/x.pb.gz"},
		{name: "workload namespace", key: "orkflows/a/continuations/x.pb.gz"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, backend := range []struct {
				name  string
				uri   string
				parse func(string) (string, error)
			}{
				{name: "local", uri: "file://" + root + "/" + tc.key, parse: local.parseURI},
				{name: "s3", uri: "s3://bucket/" + tc.key, parse: s3.parseURI},
			} {
				got, err := backend.parse(backend.uri)
				if tc.ok {
					if err != nil || got != tc.key {
						t.Fatalf("%s parseURI(%q) = %q, %v; want %q", backend.name, backend.uri, got, err, tc.key)
					}
					continue
				}
				if !errors.Is(err, ErrInvalidURI) {
					t.Fatalf("%s parseURI(%q) = %q, %v; want ErrInvalidURI", backend.name, backend.uri, got, err)
				}
			}
		})
	}
}

// TestContinuationURI_IsWhatTheWriteReturned: a reader that knows a
// definition object only by its run id and digest resolves the same URI the
// write returned, without touching the backend, and a malformed coordinate
// is refused, never composed.
func TestContinuationURI_IsWhatTheWriteReturned(t *testing.T) {
	store, _ := newLocalSnapshotStore(t)
	ctx := context.Background()
	data := []byte("gzip members of a planned definition")
	sum := ContinuationSHA256(data)
	written, err := store.WriteContinuation(ctx, "team-a", "ork-1-2", 0, ContinuationKindDefinition, data)
	if err != nil {
		t.Fatalf("WriteContinuation: %v", err)
	}
	resolved, err := store.ContinuationURI("team-a", "ork-1-2", 0, ContinuationKindDefinition, sum)
	if err != nil {
		t.Fatalf("ContinuationURI: %v", err)
	}
	if resolved != written {
		t.Fatalf("ContinuationURI = %q, the write returned %q", resolved, written)
	}
	if !ContinuationURIMatches(resolved, "team-a", "ork-1-2", 0, ContinuationKindDefinition, sum) {
		t.Fatalf("URI %q does not match its own key", resolved)
	}
	s3, err := (&S3RunSnapshotStore{bucket: "runs"}).ContinuationURI("team-a", "ork-1-2", 0, ContinuationKindDefinition, sum)
	if err != nil || !ContinuationURIMatches(s3, "team-a", "ork-1-2", 0, ContinuationKindDefinition, sum) {
		t.Fatalf("s3 ContinuationURI = %q (%v), want a URI matching its key", s3, err)
	}
	for _, bad := range []struct{ kind, sum string }{{"ledger", sum}, {ContinuationKindDefinition, "not-hex"}} {
		if uri, err := store.ContinuationURI("team-a", "ork-1-2", 0, bad.kind, bad.sum); err == nil {
			t.Fatalf("ContinuationURI(%q, %q) = %q, want a refusal", bad.kind, bad.sum, uri)
		}
	}
}
