package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// fingerprintVersion is the "v" of a submission's fingerprint. The encoding
// is fixed: changing it moves every key's fingerprint, and a claim made before
// the change would refuse its own retry `already_exists`.
const fingerprintVersion = 1

// submissionFingerprint is what an idempotency key is bound to beside its run
// id: which submission the key was used for (docs/API.md, "Idempotency
// keys").
//
// It is the SHA-256 of the canonical JSON of the Orkflow's name, the version
// AS THE CALLER WROTE IT (empty when it wrote none, so a retry that crosses a
// promotion is the same submission), the run it asks to run again, and the
// SHA-256 of the canonical JSON of the input as sent, before any declared
// default is applied. Canonical JSON sorts keys and drops spacing, so a retry
// that re-serialises the same input is the same submission, and fields are
// encoded as JSON strings, so no two submissions compose alike however their
// strings end.
//
// Left out on purpose: allow_empty_pool, no_cache and origin_reference say
// how a run is started or labelled, not what was asked for; the namespace is
// already in the claim's id.
func submissionFingerprint(msg *orkstackv1.SubmitRunRequest) (string, error) {
	var input any
	if data := msg.GetInput().GetData(); data != nil {
		input = data.AsMap()
	}
	inputBytes, err := stablejson.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("encode input for the idempotency fingerprint: %w", err)
	}
	inputSum := sha256.Sum256(inputBytes)
	doc, err := stablejson.Marshal(map[string]any{
		"v":             fingerprintVersion,
		"orkflow":       msg.GetOrkflowName(),
		"version":       msg.GetOrkflowVersion(),
		"source_run_id": msg.GetSourceRunId(),
		"input_sha256":  hex.EncodeToString(inputSum[:]),
	})
	if err != nil {
		return "", fmt.Errorf("encode the idempotency fingerprint: %w", err)
	}
	sum := sha256.Sum256(doc)
	return hex.EncodeToString(sum[:]), nil
}
