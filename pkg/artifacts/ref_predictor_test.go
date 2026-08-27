package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// TestMemoryRefPredictor_ProjectsTheContentAddressedLayout pins the projection
// itself: a key names bytes, not a slot, so the same name with different
// content predicts a different uri and the same content predicts the same one.
func TestMemoryRefPredictor_ProjectsTheContentAddressedLayout(t *testing.T) {
	shaA := strings.Repeat("a", 64)
	shaB := strings.Repeat("b", 64)

	tests := []struct {
		name        string
		predictor   MemoryRefPredictor
		prov        Provenance
		artifact    string
		sha         string
		size        int64
		contentType string
		wantURI     string
	}{
		{
			name:        "a plain name under the default root",
			artifact:    "values.json",
			sha:         shaA,
			size:        42,
			contentType: "application/json",
			wantURI:     DefaultMemoryRefRoot + "/" + objectKey("values.json", shaA),
		},
		{
			name:      "the same name with different bytes predicts a different uri",
			artifact:  "values.json",
			sha:       shaB,
			wantURI:   DefaultMemoryRefRoot + "/" + objectKey("values.json", shaB),
			prov:      Provenance{Orkflow: "billing"},
			size:      7,
			predictor: MemoryRefPredictor{},
		},
		{
			name:      "a custom root, with its trailing slash normalized away",
			predictor: MemoryRefPredictor{Root: "mem://elsewhere/"},
			artifact:  "report.txt",
			sha:       shaA,
			wantURI:   "mem://elsewhere/" + objectKey("report.txt", shaA),
		},
		{
			// The layout must survive a name that is not path-safe, because
			// objectKey encodes rather than sanitizes.
			name:     "a name with separators and dot segments stays one encoded head",
			artifact: "../../etc/passwd",
			sha:      shaA,
			wantURI:  DefaultMemoryRefRoot + "/" + objectKey("../../etc/passwd", shaA),
		},
		{
			name:     "an empty name is representable",
			artifact: "",
			sha:      shaA,
			wantURI:  DefaultMemoryRefRoot + "/" + objectKey("", shaA),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.predictor.RefFor(tc.prov, tc.artifact, tc.sha, tc.size, tc.contentType)
			if got.URI != tc.wantURI {
				t.Fatalf("uri = %q, want %q", got.URI, tc.wantURI)
			}
			if got.Name != tc.artifact {
				t.Fatalf("name = %q, want %q", got.Name, tc.artifact)
			}
			if got.SHA256 != tc.sha {
				t.Fatalf("sha256 = %q, want %q", got.SHA256, tc.sha)
			}
			if got.SizeBytes != tc.size {
				t.Fatalf("size = %d, want %d", got.SizeBytes, tc.size)
			}
			// contentType is used verbatim: a prediction performs no I/O and
			// therefore cannot sniff.
			if got.ContentType != tc.contentType {
				t.Fatalf("content type = %q, want %q", got.ContentType, tc.contentType)
			}
		})
	}
}

// TestMemoryRefPredictor_CarriesProvenanceInMetadata keeps a predicted ref
// carrying the same coordinates a real Put's ref does, since the Orker passes
// provenance through PutOptions.Metadata.
func TestMemoryRefPredictor_CarriesProvenanceInMetadata(t *testing.T) {
	prov := Provenance{Orkflow: "billing", RunID: "ork-1700000000-1", TaskID: "migrate"}
	got := MemoryRefPredictor{}.RefFor(prov, "values.json", strings.Repeat("a", 64), 1, "application/json")

	want := prov.Metadata(nil)
	if len(got.Metadata) != len(want) {
		t.Fatalf("metadata = %v, want %v", got.Metadata, want)
	}
	for k, v := range want {
		if got.Metadata[k] != v {
			t.Fatalf("metadata[%q] = %q, want %q", k, got.Metadata[k], v)
		}
	}
	if ProvenanceFromMetadata(got.Metadata) != prov {
		t.Fatalf("provenance did not round-trip: %v", ProvenanceFromMetadata(got.Metadata))
	}
}

// TestRefPrediction_AgreesWithWhatPutActuallyReturns is the contract's real
// claim — "MUST agree byte-for-byte with Put's result" — measured against the
// live local backend rather than asserted.
//
// The predictor under test composes a different SCHEME (it is the in-memory
// one), so the comparison is on everything a projection can be wrong about: the
// key, the name, the digest, the size and the content type. Phase 2's
// LocalStore.RefFor and S3Store.RefFor inherit this test's shape for the uri
// too.
func TestRefPrediction_AgreesWithWhatPutActuallyReturns(t *testing.T) {
	ctx := context.Background()
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	prov := Provenance{Orkflow: "billing", RunID: "ork-1700000000-1", TaskID: "migrate"}
	names := []string{"values.json", "workspace.tar.gz", "../../etc/passwd", "", strings.Repeat("n", 300)}

	for _, name := range names {
		t.Run(strings.ReplaceAll(name, "/", "_"), func(t *testing.T) {
			body := []byte("payload for " + name)
			sum := sha256.Sum256(body)
			digest := hex.EncodeToString(sum[:])

			// An explicit content type on both sides: a prediction cannot
			// sniff, and every §5.4 caller knows what it is uploading.
			const contentType = "application/octet-stream"
			actual, err := store.Put(ctx, name, strings.NewReader(string(body)), PutOptions{
				ContentType: contentType,
				Metadata:    prov.Metadata(nil),
			})
			if err != nil {
				t.Fatalf("Put(%q): %v", name, err)
			}

			predicted := MemoryRefPredictor{}.RefFor(prov, name, digest, int64(len(body)), contentType)

			if predicted.SHA256 != actual.SHA256 {
				t.Fatalf("sha256: predicted %q, Put returned %q", predicted.SHA256, actual.SHA256)
			}
			if predicted.Name != actual.Name {
				t.Fatalf("name: predicted %q, Put returned %q", predicted.Name, actual.Name)
			}
			if predicted.SizeBytes != actual.SizeBytes {
				t.Fatalf("size: predicted %d, Put returned %d", predicted.SizeBytes, actual.SizeBytes)
			}
			if predicted.ContentType != actual.ContentType {
				t.Fatalf("content type: predicted %q, Put returned %q",
					predicted.ContentType, actual.ContentType)
			}

			// The key is the part of the uri a projection has to get right.
			wantKey := objectKey(name, actual.SHA256)
			if !strings.HasSuffix(actual.URI, wantKey) {
				t.Fatalf("Put uri %q does not end in the content-addressed key %q", actual.URI, wantKey)
			}
			if !strings.HasSuffix(predicted.URI, wantKey) {
				t.Fatalf("predicted uri %q does not end in the key Put used, %q", predicted.URI, wantKey)
			}
		})
	}
}
