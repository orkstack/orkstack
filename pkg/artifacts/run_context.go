package artifacts

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
)

// The run-context object (docs/STORAGE.md): what a run tree's tasks may read
// about the runs they belong to — the root's Orkflow and origin, and each
// composed child's place in the tree. It is written once per run tree at
// submit, named by the SHA-256 of its own bytes, under the root run's
// snapshot prefix, and reclaimed with it. It holds no run id of the tree:
// those are minted after its content is fixed and travel in each task's
// input (ExecuteTaskInput.run_context). It is never execution state.

const (
	// MaxRunContextObjectBytes bounds one object. The run plan's 256 KiB
	// bounds how many composed runs a tree holds, and each entry is far
	// smaller than the plan's share for it.
	MaxRunContextObjectBytes = 1 << 20
	runContextFormat         = 1
	runContextObjectPrefix   = "run-context-"
	runContextObjectSuffix   = ".json"
)

var (
	// ErrRunContextMissing is a run-context object the store says is not
	// there.
	ErrRunContextMissing = errors.New("run context object missing")
	// ErrRunContextInvalid is a run-context object whose bytes are not what
	// its name says, or are no object of a known format.
	ErrRunContextInvalid = errors.New("run context object invalid")
)

// RunContextObject is the object's one form.
type RunContextObject struct {
	Format int            `json:"format"`
	Root   RunContextRoot `json:"root"`
	// Entries are the tree's runs: entries[0] the root, then a pre-order walk
	// of the composed children, task ids in order (RunPlan.RunContextIndex).
	Entries []RunContextEntry `json:"entries"`
}

// RunContextRoot is the tree's root run.
type RunContextRoot struct {
	Orkflow RunContextOrkflow `json:"orkflow"`
	Origin  RunContextOrigin  `json:"origin"`
	// OriginShared is Origin without its principal and reference: what a
	// child run of an Orkflow another namespace shared reads as its root's
	// origin. Present exactly when an entry is shared.
	OriginShared *RunContextOrigin `json:"originShared,omitzero"`
}

// RunContextOrkflow names an Orkflow version and its definition digest.
type RunContextOrkflow struct {
	Name             string `json:"name,omitzero"`
	Version          string `json:"version,omitzero"`
	DefinitionDigest string `json:"definitionDigest,omitzero"`
}

// RunContextOrigin is the root's origin as submit decided it (RunOrigin).
type RunContextOrigin struct {
	Kind              string `json:"kind,omitzero"`
	ID                string `json:"id,omitzero"`
	Reference         string `json:"reference,omitzero"`
	Description       string `json:"description,omitzero"`
	Principal         string `json:"principal,omitzero"`
	PrincipalVerified bool   `json:"principalVerified,omitzero"`
}

// RunContextEntry is one run of the tree. The root's entry is empty.
type RunContextEntry struct {
	// Parent is the entry of the run that started this one.
	Parent *int32 `json:"parent,omitzero"`
	// ParentTaskID is the composed task of the parent that started it.
	ParentTaskID string             `json:"parentTaskId,omitzero"`
	Orkflow      *RunContextOrkflow `json:"orkflow,omitzero"`
	// Shared says the child's Orkflow is another namespace's, shared through
	// composition, or runs below one that is: its tasks read OriginShared as
	// the root's origin.
	Shared bool `json:"shared,omitzero"`
}

// EncodeRunContextObject answers o's one canonical form and the SHA-256 that
// names it, refusing a tree that is not consistent.
func EncodeRunContextObject(o RunContextObject) ([]byte, string, error) {
	o.Format = runContextFormat
	if reason := checkRunContextObject(o); reason != "" {
		return nil, "", fmt.Errorf("artifacts: run context: %s", reason)
	}
	raw, err := json.Marshal(o, json.Deterministic(true))
	if err != nil {
		return nil, "", fmt.Errorf("artifacts: run context: %w", err)
	}
	if len(raw) > MaxRunContextObjectBytes {
		return nil, "", fmt.Errorf("artifacts: run context of %d bytes is over %d", len(raw), MaxRunContextObjectBytes)
	}
	return raw, hexDigest(raw), nil
}

// DecodeRunContextObject reads an object. Only the exact bytes
// EncodeRunContextObject writes are an object; anything else, a later format
// included, is ErrRunContextInvalid.
func DecodeRunContextObject(raw []byte) (RunContextObject, error) {
	if len(raw) > MaxRunContextObjectBytes {
		return RunContextObject{}, fmt.Errorf("%w: over %d bytes", ErrRunContextInvalid, MaxRunContextObjectBytes)
	}
	var o RunContextObject
	if err := json.Unmarshal(raw, &o, json.RejectUnknownMembers(true)); err != nil {
		return RunContextObject{}, fmt.Errorf("%w: does not decode: %v", ErrRunContextInvalid, err)
	}
	if o.Format != runContextFormat {
		return RunContextObject{}, fmt.Errorf("%w: format %d is not %d", ErrRunContextInvalid, o.Format, runContextFormat)
	}
	again, _, err := EncodeRunContextObject(o)
	if err != nil {
		return RunContextObject{}, fmt.Errorf("%w: %v", ErrRunContextInvalid, err)
	}
	if !bytes.Equal(again, raw) {
		return RunContextObject{}, fmt.Errorf("%w: not as a run context is written", ErrRunContextInvalid)
	}
	return o, nil
}

func checkRunContextObject(o RunContextObject) string {
	if len(o.Entries) == 0 {
		return "no entries"
	}
	if o.Entries[0] != (RunContextEntry{}) {
		return "the root's entry holds a parent"
	}
	shared := false
	for i, e := range o.Entries[1:] {
		i++
		switch {
		case e.Parent == nil || *e.Parent < 0 || int(*e.Parent) >= i:
			return fmt.Sprintf("entry %d's parent is not an entry before it", i)
		case e.ParentTaskID == "":
			return fmt.Sprintf("entry %d names no parent task", i)
		case e.Orkflow == nil:
			return fmt.Sprintf("entry %d names no orkflow", i)
		}
		shared = shared || e.Shared
	}
	switch s := o.Root.OriginShared; {
	case shared && s == nil:
		return "a shared entry with no shared origin"
	case !shared && s != nil:
		return "a shared origin with no shared entry"
	case s != nil && (s.Principal != "" || s.PrincipalVerified || s.Reference != ""):
		return "the shared origin names a principal or reference"
	}
	return ""
}

// RunContextKey renders the key of a run tree's run-context object:
// metadata/snapshots/{ns}/{root-run-id}/run-context-{sha256}.json.
func RunContextKey(namespace, rootRunID, sha256Hex string) (string, error) {
	if !lowerHex64(sha256Hex) {
		return "", fmt.Errorf("artifacts: run context key: %q is not a SHA-256 in lowercase hex", sha256Hex)
	}
	prefix, err := RunSnapshotPrefix(namespace, rootRunID)
	if err != nil {
		return "", err
	}
	return prefix + runContextObjectPrefix + sha256Hex + runContextObjectSuffix, nil
}

// WriteRunContext writes a run tree's object under its root run,
// create-if-absent at the key its bytes name. The same bytes already there
// are success, so a retried or reissued write converges; other bytes at the
// key are an error.
func (s *BlobRunSnapshotStore) WriteRunContext(ctx context.Context, namespace, rootRunID string, data []byte) error {
	if _, err := DecodeRunContextObject(data); err != nil {
		return fmt.Errorf("artifacts: put run context: %w", err)
	}
	sum := hexDigest(data)
	key, err := RunContextKey(namespace, rootRunID, sum)
	if err != nil {
		return err
	}
	uri := s.b.locate(key)
	err = s.b.create(ctx, key, "application/json", data)
	if err == nil {
		return nil
	}
	if !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: put run context %q: %w", uri, err)
	}
	existing, _, readErr := s.b.get(ctx, key, MaxRunContextObjectBytes+1)
	if readErr != nil {
		return fmt.Errorf("artifacts: put run context %q: read existing object: %w", uri, readErr)
	}
	if hexDigest(existing) != sum {
		return fmt.Errorf("artifacts: put run context %q: an object with other bytes is already at the key", uri)
	}
	return nil
}

// ReadRunContext reads and verifies the object sha256Hex names under the root
// run: ErrRunContextMissing when the store says it is not there,
// ErrRunContextInvalid when sha256Hex names no object or the bytes do not hash
// to the name or are no object; any other error is the store's. A reader
// never retries an invalid object: its bytes cannot change.
func (s *BlobRunSnapshotStore) ReadRunContext(ctx context.Context, namespace, rootRunID, sha256Hex string) (RunContextObject, error) {
	key, err := RunContextKey(namespace, rootRunID, sha256Hex)
	if err != nil {
		// A run's reference that names no object is as invalid as one that
		// names other bytes: no read can change it.
		return RunContextObject{}, fmt.Errorf("%w: %v", ErrRunContextInvalid, err)
	}
	raw, _, err := s.b.get(ctx, key, MaxRunContextObjectBytes+1)
	if errors.Is(err, errBlobNotFound) {
		return RunContextObject{}, fmt.Errorf("%w: %s", ErrRunContextMissing, s.b.locate(key))
	}
	if err != nil {
		return RunContextObject{}, fmt.Errorf("artifacts: read run context %q: %w", s.b.locate(key), err)
	}
	if hexDigest(raw) != sha256Hex {
		return RunContextObject{}, fmt.Errorf("%w: %s does not hash to its name", ErrRunContextInvalid, s.b.locate(key))
	}
	return DecodeRunContextObject(raw)
}
