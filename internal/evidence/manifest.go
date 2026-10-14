// Package evidence owns the shape of a run evidence bundle: the MANIFEST.json
// document, the member path rule, the tar.gz writer, and the offline verifier.
//
// A bundle is a READ of objects that already exist — the archived run, the
// pinned definition, each task's commit.json, values.json, output artifacts,
// attempt records and uploaded logs — assembled per request and kept nowhere
// (docs/RUN_EVIDENCE.md). The package holds no store: internal/api builds the
// manifest from the stores and hands the writer a Source, and cmd/orkctl runs
// the verifier over a file with no server involved.
package evidence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion is the manifest schema every bundle whose run lost nothing
// is written with.
const SchemaVersion = 1

// SchemaVersionEventGaps is the schema of a bundle whose run lost part of its
// event stream from object storage before it was archived: the same members,
// plus event_gaps naming every lost range. It is a new version, not an
// additive key, so a tool older than it refuses the bundle with its own
// message instead of verifying it silently. These are the two versions this
// build reads; a newer bundle is not verified by an older tool.
const SchemaVersionEventGaps = 2

// ManifestPath is the name of a bundle's first member.
const ManifestPath = "MANIFEST.json"

// Kind is what a member is.
type Kind string

// The member kinds, complete.
const (
	KindRun      Kind = "run"
	KindSource   Kind = "source"
	KindCommit   Kind = "commit"
	KindValues   Kind = "values"
	KindArtifact Kind = "artifact"
	KindAttempt  Kind = "attempt"
	KindStdout   Kind = "stdout"
	KindStderr   Kind = "stderr"
)

// State is whether a member's bytes are in the bundle, and if not, why.
type State string

// The member states, complete (docs/RUN_EVIDENCE.md, "States").
const (
	// StatePresent: the bytes were read from the store, hashed, and are in the
	// tar; for a member with a recorded reference they equal the reference.
	StatePresent State = "present"
	// StateReclaimed: the store answered NotFound for an object the run
	// recorded. The recorded digest is preserved.
	StateReclaimed State = "reclaimed"
	// StateUnrecorded: an attempt number for which the attempt store holds no
	// record.
	StateUnrecorded State = "unrecorded"
	// StateRejected: an object exists at the coordinates but is not what the
	// run pinned (today only the definition).
	StateRejected State = "rejected"
)

// The reason tokens, complete. A client prints the token's sentence
// (ReasonText); an unknown token is printed as itself.
const (
	ReasonDefinitionDeleted        = "definition_deleted"
	ReasonDefinitionDigestMismatch = "definition_digest_mismatch"
	ReasonObjectMissing            = "object_missing"
	ReasonCommitMissing            = "commit_missing"
	ReasonAttemptRecordMissing     = "attempt_record_missing"
)

var reasonSentences = map[string]string{
	ReasonDefinitionDeleted:        "the pinned definition was deleted from the orkflow store; the run's definition digest still attests it",
	ReasonDefinitionDigestMismatch: "a document exists at the pinned coordinates but does not hash to the digest this run pinned",
	ReasonCommitMissing:            "the commit record was reclaimed by cleanup; its recorded digest is preserved",
	ReasonObjectMissing:            "the object was removed from the store by a lifecycle rule or cleanup; its recorded digest is preserved",
	ReasonAttemptRecordMissing:     "no record: the orker died before filing one, or it was reclaimed",
}

// ReasonText is the documented sentence for a reason token; an unknown token
// is returned as itself.
func ReasonText(token string) string {
	if sentence, ok := reasonSentences[token]; ok {
		return sentence
	}
	return token
}

// Manifest is MANIFEST.json. It is the whole contract of a bundle: every
// member's path, origin, size, digest and state, and enough about the run to
// read the bundle without the engine. Its JSON is the wire form; the Go type
// exists so the writer and the verifier cannot disagree about a key.
type Manifest struct {
	SchemaVersion    int         `json:"schema_version"`
	RunID            string      `json:"run_id"`
	Namespace        string      `json:"namespace"`
	OrkflowName      string      `json:"orkflow_name"`
	OrkflowVersion   string      `json:"orkflow_version"`
	DefinitionDigest string      `json:"definition_digest"`
	RunStatus        string      `json:"run_status"`
	RunCreatedAt     time.Time   `json:"run_created_at"`
	RunFinishedAt    time.Time   `json:"run_finished_at"`
	ExportedAt       time.Time   `json:"exported_at"`
	EngineVersion    string      `json:"engine_version"`
	BuildID          string      `json:"build_id"`
	Scope            Scope       `json:"scope"`
	Definition       Definition  `json:"definition"`
	Tasks            []TaskEntry `json:"tasks"`
	Members          []Member    `json:"members"`
	Counts           Counts      `json:"counts"`
	// EventGaps is set, and only set, on a schema_version 2 bundle: every
	// range of the run's event stream lost from object storage before the
	// run was archived. The archive body (run.jsonl) records the same ranges
	// as gap lines.
	EventGaps []EventGap `json:"event_gaps,omitempty"`
}

// EventGap is one lost range of a run's event sequence, inclusive.
type EventGap struct {
	First  int64  `json:"first"`
	Last   int64  `json:"last"`
	Reason string `json:"reason"`
}

// Sentence is the notice a reader prints for the gap.
func (g EventGap) Sentence() string {
	return fmt.Sprintf("events %d–%d of this run were lost from object storage before it was archived (%s)",
		g.First, g.Last, g.Reason)
}

// Scope is what the request asked for.
type Scope struct {
	TaskID    string `json:"task_id"`
	Logs      bool   `json:"logs"`
	Artifacts bool   `json:"artifacts"`
	// Item is set when the request narrowed task_id to one item of a
	// fan-out. Additive: schema_version stays 1.
	Item *int32 `json:"item,omitempty"`
}

// Definition is the state of the pinned definition's source.
type Definition struct {
	State                  State  `json:"state"`
	Reason                 string `json:"reason"`
	StoredDefinitionDigest string `json:"stored_definition_digest"`
}

// TaskEntry is one task of the run, members or not.
type TaskEntry struct {
	TaskID string `json:"task_id"`
	// Item is set on the entry of one item of a fan-out task. A fan-out's own
	// entry (no item) carries its status and no attempts; its items follow it.
	Item           *int32         `json:"item,omitempty"`
	Status         string         `json:"status"`
	OutcomeAttempt int32          `json:"outcome_attempt"`
	ExitCode       int32          `json:"exit_code"`
	Attempts       []AttemptEntry `json:"attempts"`
}

// AttemptEntry is one attempt number of a task.
type AttemptEntry struct {
	Attempt       int32  `json:"attempt"`
	State         State  `json:"state"`
	Outcome       string `json:"outcome,omitempty"`
	CommitAdopted bool   `json:"commit_adopted,omitempty"`
}

// Member is one file of the bundle, or one file the bundle would have held.
type Member struct {
	Path   string `json:"path"`
	Kind   Kind   `json:"kind"`
	TaskID string `json:"task_id,omitempty"`
	// Item is set on a member one item of a fan-out recorded; such a member
	// lives under tasks/{task}/items/{item:05}/.
	Item    *int32 `json:"item,omitempty"`
	Attempt int32  `json:"attempt,omitempty"`
	// Name is the exact recorded artifact name for kind "artifact"; Path's
	// last segment is derived from it and may be encoded (MemberName).
	Name string `json:"name,omitempty"`
	// Origin is the object the bytes came from: the recorded URI for an
	// artifact, values, log or commit; the store key for the archive body
	// and an attempt record; the version document key plus "#source_yaml"
	// for the definition. It names a location, never a credential.
	Origin    string `json:"origin"`
	SizeBytes int64  `json:"size_bytes"`
	// SHA256 is lowercase hex of the bytes as read (present), or the recorded
	// digest (reclaimed). Empty only for kind "source" when reclaimed or
	// rejected, and for "unrecorded".
	SHA256 string `json:"sha256"`
	State  State  `json:"state"`
	Reason string `json:"reason,omitempty"`
}

// Counts tallies the members by state.
type Counts struct {
	Present      int   `json:"present"`
	Reclaimed    int   `json:"reclaimed"`
	Unrecorded   int   `json:"unrecorded"`
	Rejected     int   `json:"rejected"`
	BytesPresent int64 `json:"bytes_present"`
}

// CountMembers tallies members by state.
func CountMembers(members []Member) Counts {
	var c Counts
	for _, m := range members {
		switch m.State {
		case StatePresent:
			c.Present++
			c.BytesPresent += m.SizeBytes
		case StateReclaimed:
			c.Reclaimed++
		case StateUnrecorded:
			c.Unrecorded++
		case StateRejected:
			c.Rejected++
		}
	}
	return c
}

// NormalizeDigest renders a recorded file digest the way the manifest spells
// every file digest: lowercase hex without a "sha256:" prefix, as ReadArtifact
// normalises an ArtifactRef.sha256.
func NormalizeDigest(digest string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(digest), "sha256:"))
}

// manifestJSON is Manifest without its MarshalJSON method.
type manifestJSON Manifest

// MarshalJSON renders MANIFEST.json: two-space indented, snake_case keys, a
// trailing newline. The same Manifest always renders to the same bytes, so
// the document a manifest_only request streams is the bundle's first member.
func (m Manifest) MarshalJSON() ([]byte, error) {
	out, err := json.MarshalIndent(manifestJSON(m), "", "  ")
	if err != nil {
		return nil, fmt.Errorf("evidence: encode manifest: %w", err)
	}
	return append(out, '\n'), nil
}

// ParseManifest decodes a MANIFEST.json document. It reads schema_version 1
// and 2: version 2 requires a non-empty event_gaps, and version 1 forbids
// one. Any other version is refused: a newer bundle is not read by an older
// tool.
func ParseManifest(document []byte) (*Manifest, error) {
	var probe struct {
		SchemaVersion *int `json:"schema_version"`
	}
	if err := json.Unmarshal(document, &probe); err != nil {
		return nil, fmt.Errorf("evidence: MANIFEST.json is not JSON: %w", err)
	}
	if probe.SchemaVersion == nil {
		return nil, fmt.Errorf("evidence: MANIFEST.json has no schema_version")
	}
	if *probe.SchemaVersion != SchemaVersion && *probe.SchemaVersion != SchemaVersionEventGaps {
		return nil, fmt.Errorf("evidence: MANIFEST.json schema_version %d is not supported by this tool (it reads %d and %d)",
			*probe.SchemaVersion, SchemaVersion, SchemaVersionEventGaps)
	}
	var m manifestJSON
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("evidence: decode MANIFEST.json: %w", err)
	}
	manifest := Manifest(m)
	switch {
	case manifest.SchemaVersion == SchemaVersionEventGaps && len(manifest.EventGaps) == 0:
		return nil, fmt.Errorf("evidence: MANIFEST.json schema_version %d names no event_gaps", SchemaVersionEventGaps)
	case manifest.SchemaVersion == SchemaVersion && len(manifest.EventGaps) > 0:
		return nil, fmt.Errorf("evidence: MANIFEST.json schema_version %d carries event_gaps, which only schema_version %d may",
			SchemaVersion, SchemaVersionEventGaps)
	}
	for _, gap := range manifest.EventGaps {
		if gap.First < 1 || gap.Last < gap.First {
			return nil, fmt.Errorf("evidence: MANIFEST.json event gap %d–%d is not a range", gap.First, gap.Last)
		}
	}
	return &manifest, nil
}
