package artifacts

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
)

// A run that continues as new into a fresh Temporal execution writes immutable
// objects under its own snapshot prefix, in a continuations/ directory: the
// event segment of the generation that closed, a full snapshot of the run's
// exact state at the switch, and the checkpoint the next generation starts
// from. Each is content-addressed by its SHA-256, so a retried or re-decided
// write lands on its own key and create-if-absent can never conflict. They
// are platform records (AGENTS.md §2.2), never mutable state: Temporal decided
// everything they hold. Run cleanup reclaims them with the run's snapshot
// prefix; a bucket lifecycle rule must never expire them, because a live run
// reads its segments for its whole life and its archive is built from them.
const (
	// ContinuationDir is the directory under a run's snapshot prefix that
	// holds its continuation objects.
	ContinuationDir = "continuations"

	// continuationSuffix is the fixed leaf suffix of every continuation key:
	// every continuation object is gzip-compressed protobuf.
	continuationSuffix = ".pb.gz"

	// ContinuationContentType is what a continuation object is served as.
	ContinuationContentType = "application/gzip"

	// ContinuationKindCheckpoint names a checkpoint: the run's state at a
	// switch, as gzip members of orkstack.v1.RunCheckpointPart.
	ContinuationKindCheckpoint = "checkpoint"
	// ContinuationKindEvents names an event segment: the gzipped
	// orkstack.v1.RunEventSegment of one closed generation.
	ContinuationKindEvents = "events"
	// ContinuationKindSnapshot names a switch snapshot: the gzipped full
	// orkstack.v1.Run at a switch.
	ContinuationKindSnapshot = "snapshot"
	// ContinuationKindDefinition names a run's planned definition when it
	// travels by reference: gzip members of orkstack.v1.Orkflow, written by
	// the submitting engine before the start that uses it, always at
	// generation 0.
	ContinuationKindDefinition = "definition"
)

// ErrContinuationSize reports a continuation object whose stored size is not
// the size its reference records. It is never retryable: an immutable object
// of the wrong size will not become the right one.
var ErrContinuationSize = errors.New("continuation object size mismatch")

// continuationKinds is the closed set of kinds a continuation key may name.
var continuationKinds = map[string]bool{
	ContinuationKindCheckpoint: true,
	ContinuationKindEvents:     true,
	ContinuationKindSnapshot:   true,
	ContinuationKindDefinition: true,
}

// RunContinuationKey renders the content-addressed key of one continuation
// object:
//
//	metadata/snapshots/{ns}/{run}/continuations/{gen:06d}-{kind}-{sha256}.pb.gz
//
// Content-addressed: a retried or re-decided write lands on its own key, so
// create-if-absent can never conflict, and an orphan (a switch that a signal,
// a cancel or the size cap turned back) is reclaimed by run cleanup with the
// prefix.
func RunContinuationKey(namespace, runID string, generation int64, kind, sha256hex string) (string, error) {
	prefix, err := RunSnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	if generation < 0 {
		return "", fmt.Errorf("artifacts: run continuation key: generation %d is negative", generation)
	}
	if !continuationKinds[kind] {
		return "", fmt.Errorf("artifacts: run continuation key: unknown kind %q", kind)
	}
	if !isSHA256Hex(sha256hex) {
		return "", fmt.Errorf("artifacts: run continuation key: %q is not a lowercase hex SHA-256", sha256hex)
	}
	return fmt.Sprintf("%s%s/%06d-%s-%s%s", prefix, ContinuationDir, generation, kind, sha256hex, continuationSuffix), nil
}

// continuationURI renders the URI a store gives the key of one continuation
// object, through that store's own uri function: what ContinuationURI
// returns, validated as RunContinuationKey validates it.
func continuationURI(uri func(string) string, namespace, runID string, generation int64, kind, sha256hex string) (string, error) {
	key, err := RunContinuationKey(namespace, runID, generation, kind, sha256hex)
	if err != nil {
		return "", err
	}
	return uri(key), nil
}

// ContinuationURIMatches reports whether uri names exactly the key
// RunContinuationKey(namespace, runID, generation, kind, sha256hex) builds,
// under any store root ("s3://<bucket>/" or "file:///<dir>/"). Pure: workflow
// code and readers both use it to refuse a carry naming another run's object
// BEFORE reading anything.
func ContinuationURIMatches(uri, namespace, runID string, generation int64, kind, sha256hex string) bool {
	key, err := RunContinuationKey(namespace, runID, generation, kind, sha256hex)
	if err != nil {
		return false
	}
	u, err := url.Parse(uri)
	if err != nil || u.RawQuery != "" || u.Fragment != "" || u.User != nil || u.Opaque != "" {
		return false
	}
	switch u.Scheme {
	case "s3":
		return u.Host != "" && u.Path == "/"+key
	case "file":
		// The store root is any directory; the key is the whole tail. A path
		// that is not already clean could climb out of the root, so it never
		// matches.
		return u.Host == "" && path.Clean(u.Path) == u.Path && strings.HasSuffix(u.Path, "/"+key)
	default:
		return false
	}
}

// ContinuationSHA256 is the lowercase hex SHA-256 a continuation object is
// addressed by.
func ContinuationSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// isContinuationKey reports whether key is under the snapshot prefix, inside a
// continuations/ directory, with the continuation suffix.
func isContinuationKey(key string) bool {
	return strings.HasPrefix(key, SnapshotPrefix+"/") &&
		strings.Contains(key, "/"+ContinuationDir+"/") &&
		strings.HasSuffix(key, continuationSuffix)
}

// isSnapshotObjectKey is what ReadSnapshot and DeleteSnapshot accept: a
// sequence snapshot ({seq}.pb), a snapshot overlay ({seq}.ovl) or a
// continuation object.
func isSnapshotObjectKey(key string) bool {
	if !strings.HasPrefix(key, SnapshotPrefix+"/") {
		return false
	}
	return strings.HasSuffix(key, snapshotSuffix) || strings.HasSuffix(key, snapshotOverlaySuffix) ||
		isContinuationKey(key)
}

// checkContinuationRange validates a range read's arguments against the size
// the caller's reference records.
func checkContinuationRange(uri string, wantSize, offset, length int64) error {
	if wantSize <= 0 || offset < 0 || length <= 0 || offset > wantSize-length {
		return fmt.Errorf("artifacts: continuation %q: range [%d, %d) is outside an object of %d bytes",
			uri, offset, offset+length, wantSize)
	}
	return nil
}

func isSHA256Hex(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
