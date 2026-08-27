package artifacts

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// sampleCommit is a minimally realistic commit: enough set that an encoding
// bug shows up, not so much that the fixture becomes the test.
func sampleCommit() *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{
		SchemaVersion:           CommitSchemaVersion,
		Namespace:               "payments",
		RunId:                   "ork-1700000000-1",
		TaskId:                  "migrate",
		OrkflowName:             "billing",
		OrkflowVersion:          "v3",
		OrkflowDefinitionDigest: "sha256:" + strings.Repeat("a", 64),
		IdempotencyKey:          "payments/ork-1700000000-1/migrate",
		Image: &orkstackv1.ImageRef{
			Reference:   "ghcr.io/acme/migrate:v3",
			Registry:    "ghcr.io",
			Repository:  "acme/migrate",
			IndexDigest: "sha256:" + strings.Repeat("b", 64),
			Pinned:      "ghcr.io/acme/migrate@sha256:" + strings.Repeat("b", 64),
		},
		Values: &orkstackv1.ArtifactRef{
			Name:      "values.json",
			Uri:       "s3://bucket/orkflows/billing/ork-1700000000-1/migrate/values.json/" + strings.Repeat("c", 64),
			SizeBytes: 42,
			Sha256:    strings.Repeat("c", 64),
		},
		Attempt: 1,
		OrkerId: "orker-a",
		BuildId: "build-7",
	}
}

// TestCommitKey_RendersTheDocumentedLayout pins the key shape §5.2 specifies,
// including where each coordinate is validated and where one is encoded.
func TestCommitKey_RendersTheDocumentedLayout(t *testing.T) {
	longRun := strings.Repeat("z", 300)

	tests := []struct {
		name      string
		namespace string
		runID     string
		taskID    string
		want      string
		wantErr   error
	}{
		{
			name:      "clean coordinates are used verbatim",
			namespace: "payments",
			runID:     "ork-1700000000-1",
			taskID:    "migrate",
			want:      "metadata/commits/payments/ork-1700000000-1/migrate/commit.json",
		},
		{
			name:      "an absent namespace resolves to the built-in default",
			namespace: "",
			runID:     "ork-1700000000-1",
			taskID:    "migrate",
			want:      "metadata/commits/" + DefaultNamespace + "/ork-1700000000-1/migrate/commit.json",
		},
		{
			// AGENTS.md §7: nothing may parse a run id. The tests elsewhere in
			// this repo inject "run-001", "ghost" and friends, so the key must
			// take whatever it is handed.
			name:      "an opaque run id is one segment, verbatim when clean",
			namespace: "payments",
			runID:     "ghost",
			taskID:    "migrate",
			want:      "metadata/commits/payments/ghost/migrate/commit.json",
		},
		{
			// A run id that is not a clean component is escaped reversibly
			// rather than rejected — encoded, never parsed.
			name:      "a run id with separators is escaped into one segment",
			namespace: "payments",
			runID:     "run/../../etc",
			taskID:    "migrate",
			want: "metadata/commits/payments/" +
				archiveEncodedMarker + "cnVuLy4uLy4uL2V0Yw" + "/migrate/commit.json",
		},
		{
			name:      "an empty run id is refused",
			namespace: "payments",
			runID:     "",
			taskID:    "migrate",
			wantErr:   ErrInvalidCommitKey,
		},
		{
			name:      "an over-long run id is refused rather than truncated",
			namespace: "payments",
			runID:     longRun,
			taskID:    "migrate",
			wantErr:   ErrInvalidCommitKey,
		},
		{
			name:      "a namespace that is not a slug is refused, never sanitized",
			namespace: "Payments",
			runID:     "ork-1700000000-1",
			taskID:    "migrate",
			wantErr:   ErrInvalidCommitKey,
		},
		{
			name:      "a traversal task id is refused, never sanitized",
			namespace: "payments",
			runID:     "ork-1700000000-1",
			taskID:    "../../namespaces/default",
			wantErr:   ErrInvalidCommitKey,
		},
		{
			name:      "an empty task id is refused",
			namespace: "payments",
			runID:     "ork-1700000000-1",
			taskID:    "",
			wantErr:   ErrInvalidCommitKey,
		},
		{
			name:      "an upper-case task id is refused",
			namespace: "payments",
			runID:     "ork-1700000000-1",
			taskID:    "Migrate",
			wantErr:   ErrInvalidCommitKey,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CommitKey(tc.namespace, tc.runID, tc.taskID)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("CommitKey(%q,%q,%q) error = %v, want %v",
						tc.namespace, tc.runID, tc.taskID, err, tc.wantErr)
				}
				if got != "" {
					t.Fatalf("a refused key still rendered %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("CommitKey(%q,%q,%q): %v", tc.namespace, tc.runID, tc.taskID, err)
			}
			if got != tc.want {
				t.Fatalf("CommitKey(%q,%q,%q) = %q, want %q",
					tc.namespace, tc.runID, tc.taskID, got, tc.want)
			}
		})
	}
}

// TestCommitKey_CannotCollideWithAWorkloadArtifact is the §5.2 claim that a
// workload naming an artifact "commit.json" cannot reach a commit record.
func TestCommitKey_CannotCollideWithAWorkloadArtifact(t *testing.T) {
	key, err := CommitKey("payments", "ork-1700000000-1", "migrate")
	if err != nil {
		t.Fatalf("CommitKey: %v", err)
	}
	if !strings.HasPrefix(key, "metadata/") {
		t.Fatalf("commit key %q does not live under metadata/", key)
	}

	// The two namespaces a workload artifact can land in, per backend.
	artifactKey := objectKey("commit.json", strings.Repeat("d", 64))
	if strings.HasPrefix(artifactKey, "metadata/") {
		t.Fatalf("a local artifact key %q reaches the metadata prefix", artifactKey)
	}
	s3Key := s3ObjectKey(Provenance{Orkflow: "billing", RunID: "ork-1700000000-1", TaskID: "migrate"}, "commit.json", strings.Repeat("0", 64))
	if strings.HasPrefix(s3Key, "metadata/") {
		t.Fatalf("an s3 artifact key %q reaches the metadata prefix", s3Key)
	}
}

// TestMemoryCommitStore_CreateIsCreateIfAbsent is the core of the contract: the
// second writer loses, and loses without changing what the winner stored.
func TestMemoryCommitStore_CreateIsCreateIfAbsent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCommitStore()
	uri := "mem://commits/metadata/commits/payments/ork-1700000000-1/migrate/commit.json"

	winner := sampleCommit()
	first, err := store.Create(ctx, uri, winner)
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if first.SHA256 == "" || first.Bytes == 0 {
		t.Fatalf("first Create returned an unpopulated record: %+v", first)
	}

	loser := sampleCommit()
	loser.OrkerId = "orker-b"
	loser.Attempt = 2
	if _, err := store.Create(ctx, uri, loser); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("second Create error = %v, want ErrPreconditionFailed", err)
	}

	// The loser must not have moved a byte.
	after, err := store.Get(ctx, uri)
	if err != nil {
		t.Fatalf("Get after the losing Create: %v", err)
	}
	if after.SHA256 != first.SHA256 || after.Bytes != first.Bytes {
		t.Fatalf("stored bytes changed: sha %q->%q, size %d->%d",
			first.SHA256, after.SHA256, first.Bytes, after.Bytes)
	}
	if got := after.Commit.GetOrkerId(); got != "orker-a" {
		t.Fatalf("stored commit orker_id = %q, want the winner's %q", got, "orker-a")
	}
	if got := after.Commit.GetAttempt(); got != 1 {
		t.Fatalf("stored commit attempt = %d, want the winner's 1", got)
	}
}

// TestMemoryCommitStore_TwoWritersRaceToOneWinner is the two-writer race §5.3
// requires of every backend, run against the in-memory one so the seam itself
// is held to it.
func TestMemoryCommitStore_TwoWritersRaceToOneWinner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCommitStore()
	uri := "mem://commits/probe/commit.json"

	const writers = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		wins     int
		refused  int
		unwanted []error
	)
	start := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := sampleCommit()
			c.Attempt = int32(i + 1)
			<-start
			_, err := store.Create(ctx, uri, c)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrPreconditionFailed):
				refused++
			default:
				unwanted = append(unwanted, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(unwanted) != 0 {
		t.Fatalf("unexpected errors from the race: %v", unwanted)
	}
	if wins != 1 {
		t.Fatalf("winners = %d, want exactly 1", wins)
	}
	if refused != writers-1 {
		t.Fatalf("refusals = %d, want %d", refused, writers-1)
	}
}

// TestMemoryCommitStore_GetReportsMissingAndUndecodableDifferently is the §5.4
// step-0 branch split: "not found" means proceed, "undecodable" must never.
func TestMemoryCommitStore_GetReportsMissingAndUndecodableDifferently(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCommitStore()

	if _, err := store.Get(ctx, "mem://commits/absent/commit.json"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get of a missing commit = %v, want ErrNotFound", err)
	}

	torn := "mem://commits/torn/commit.json"
	store.PutRaw(torn, []byte(`{"schemaVersion":`))
	rec, err := store.Get(ctx, torn)
	if !errors.Is(err, ErrCommitUndecodable) {
		t.Fatalf("Get of a torn commit = %v, want ErrCommitUndecodable", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("a torn commit must not read as missing: %v", err)
	}
	if rec.Bytes != int64(len(`{"schemaVersion":`)) {
		t.Fatalf("undecodable record reported %d bytes, want %d",
			rec.Bytes, len(`{"schemaVersion":`))
	}
}

// TestMarshalCommit_IsStableWithinABuildAndRoundTrips guards the one property
// §14.10 lets us have: protojson's random whitespace is normalized away, so two
// marshals in one build agree. It deliberately does NOT claim cross-build
// stability, which is why a record's SHA256 comes from stored bytes.
func TestMarshalCommit_IsStableWithinABuildAndRoundTrips(t *testing.T) {
	first, err := MarshalCommit(sampleCommit())
	if err != nil {
		t.Fatalf("MarshalCommit: %v", err)
	}
	for i := range 50 {
		again, err := MarshalCommit(sampleCommit())
		if err != nil {
			t.Fatalf("MarshalCommit #%d: %v", i, err)
		}
		if string(again) != string(first) {
			t.Fatalf("marshal #%d differs from the first:\n%s\n%s", i, first, again)
		}
	}

	back, err := UnmarshalCommit(first)
	if err != nil {
		t.Fatalf("UnmarshalCommit: %v", err)
	}
	if back.GetSchemaVersion() != CommitSchemaVersion {
		t.Fatalf("round-tripped schema_version = %d, want %d",
			back.GetSchemaVersion(), CommitSchemaVersion)
	}
	if back.GetImage().GetIndexDigest() != sampleCommit().GetImage().GetIndexDigest() {
		t.Fatalf("round-tripped index digest = %q", back.GetImage().GetIndexDigest())
	}
}

// TestMarshalCommit_RejectsNil keeps a library-level nil out of the store
// rather than letting it become an empty commit object.
func TestMarshalCommit_RejectsNil(t *testing.T) {
	if _, err := MarshalCommit(nil); !errors.Is(err, ErrInvalidCommitKey) {
		t.Fatalf("MarshalCommit(nil) = %v, want ErrInvalidCommitKey", err)
	}
	store := NewMemoryCommitStore()
	if _, err := store.Create(context.Background(), "mem://x", nil); !errors.Is(err, ErrInvalidCommitKey) {
		t.Fatalf("Create(nil) = %v, want ErrInvalidCommitKey", err)
	}
}

// TestCommitRecord_DigestNamesTheStoredBytes is the §14.10 consequence stated
// as a test: what Get reports must be the digest of what is on the wire, which
// is exactly what Create reported.
func TestCommitRecord_DigestNamesTheStoredBytes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCommitStore()
	uri := "mem://commits/digest/commit.json"

	created, err := store.Create(ctx, uri, sampleCommit())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := store.Get(ctx, uri)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SHA256 != created.SHA256 {
		t.Fatalf("Get sha %q != Create sha %q", got.SHA256, created.SHA256)
	}
	if got.ETag != created.ETag {
		t.Fatalf("Get etag %q != Create etag %q", got.ETag, created.ETag)
	}
	if got.Bytes != created.Bytes {
		t.Fatalf("Get size %d != Create size %d", got.Bytes, created.Bytes)
	}
}
