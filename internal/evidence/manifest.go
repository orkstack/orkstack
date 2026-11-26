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
	"encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// SchemaVersion is the manifest schema every bundle is written with, and the
// only one this build reads: a bundle of another version, earlier or later, is
// refused by name, for the orkctl that exported it to verify. event_gaps names
// every range of the run's event stream its archive does not hold — events
// lost from object storage before it was archived, or numbers it never
// published — and is absent when there is none.
const SchemaVersion = 3

// SchemaVersionError is a manifest of a version this build does not read.
type SchemaVersionError struct{ Version int }

func (e *SchemaVersionError) Error() string {
	return fmt.Sprintf("evidence: MANIFEST.json is schema_version %d, and this tool reads only schema_version %d: "+
		"verify it with the orkctl that exported it", e.Version, SchemaVersion)
}

// ManifestPath is the name of a bundle's first member.
const ManifestPath = "MANIFEST.json"

// Kind is what a member is.
type Kind string

// The member kinds, complete.
const (
	KindRun      Kind = "run"
	KindEnding   Kind = "ending"
	KindSource   Kind = "source"
	KindCommit   Kind = "commit"
	KindValues   Kind = "values"
	KindArtifact Kind = "artifact"
	KindAttempt  Kind = "attempt"
	KindStdout   Kind = "stdout"
	KindStderr   Kind = "stderr"
	// The records the engine makes of a run whose results it records
	// (docs/TASK_IDENTITY.md): an awaited task's resolution and the detail of
	// who gave it, a composed task's values, and the run's outputs. A bundle
	// holds one for every record found under the run's keys, applied or not.
	KindResolution       Kind = "resolution"
	KindResolutionDetail Kind = "resolution-detail"
	KindComposed         Kind = "composed"
	KindOutputs          Kind = "outputs"
)

// RecordState is what became of a record member, as ListRecordedResults says
// it: set on every present member of the four record kinds but a detail.
type RecordState string

// The record states, complete.
const (
	// RecordOnRecord: the run applied it.
	RecordOnRecord RecordState = "on_record"
	// RecordNotApplied: recorded in time, and the run did not take it.
	RecordNotApplied RecordState = "not_applied"
	// RecordNotTaken: recorded after its task stopped waiting or its run
	// ended, or under a scope a later resume opened again.
	RecordNotTaken RecordState = "not_taken"
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
	// StateWithheld: the caller who exported the bundle may not read the
	// object: another namespace's shared definition, or a record the engine
	// made, which needs run.artifacts.read or orker.read.
	StateWithheld State = "withheld"
	// StateMissing: a record the store said twice is not there, while the
	// run's state says its task succeeded and the run was not retired. The
	// gap: the verifier states it, and never passes it as present.
	StateMissing State = "missing"
)

// The reason tokens, complete. A client prints the token's sentence
// (ReasonText); an unknown token is printed as itself.
const (
	ReasonDefinitionDeleted        = "definition_deleted"
	ReasonDefinitionDigestMismatch = "definition_digest_mismatch"
	ReasonDefinitionNotReadable    = "definition_not_readable"
	ReasonObjectMissing            = "object_missing"
	ReasonCommitMissing            = "commit_missing"
	ReasonAttemptRecordMissing     = "attempt_record_missing"
	ReasonArtifactReclaimed        = "artifact_reclaimed"
	ReasonEndingMissing            = "ending_missing"
	ReasonEndingDamaged            = "ending_damaged"
	ReasonRecordMissing            = "record_missing"
	ReasonRecordReclaimed          = "record_reclaimed"
	ReasonRecordWithheld           = "record_withheld"
)

var reasonSentences = map[string]string{
	ReasonDefinitionDeleted:        "the pinned definition was deleted from the orkflow store; the run's definition digest still attests it",
	ReasonDefinitionDigestMismatch: "a document exists at the pinned coordinates but does not hash to the digest this run pinned",
	ReasonDefinitionNotReadable:    "the exporter may not read the shared Orkflow in its owner's namespace; the run's definition digest still attests it",
	ReasonCommitMissing:            "the commit record was reclaimed by cleanup; its recorded digest is preserved",
	ReasonObjectMissing:            "the object was removed from the store by a lifecycle rule or cleanup; its recorded digest is preserved",
	ReasonAttemptRecordMissing:     "no record: the orker died before filing one, or it was reclaimed",
	ReasonEndingMissing:            "the archive body of this earlier ending was removed from the store",
	ReasonEndingDamaged:            "an object is stored for this earlier ending, but it does not decode as an archive body of this run",
	ReasonArtifactReclaimed:        "the artifact GC deleted the object after its run was retired; its recorded digest is preserved",
	ReasonRecordMissing:            "the store holds no record the engine should have made: the run's state says its task succeeded, and the run was not retired",
	ReasonRecordReclaimed:          "the run was retired, and its records went with its commits at cleanup",
	ReasonRecordWithheld:           "the exporter may read neither the run's artifacts nor the Orkers, which a record names",
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
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	Namespace     string `json:"namespace"`
	// OrkflowNamespace is set only for a shared Orkflow's child run: the
	// namespace that owns the Orkflow. Namespace is the one the run ran in.
	OrkflowNamespace string      `json:"orkflow_namespace,omitempty"`
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
	// EventGaps is set only when the run's archive records a gap: every
	// range of the run's event stream its archive does not hold — lost from
	// object storage before the run was archived, or never published. The
	// archive body (run.jsonl) records the same ranges as gap lines.
	EventGaps []EventGap `json:"event_gaps,omitempty"`
	// Endings is set only for a run that was resumed: each of its endings, the
	// first first and the one this bundle is of last.
	Endings []Ending `json:"endings,omitempty"`
}

// EventGap is one range of a run's event sequence its archive does not hold,
// inclusive.
type EventGap struct {
	First  int64  `json:"first"`
	Last   int64  `json:"last"`
	Reason string `json:"reason"`
}

// UnpublishedEventsReason is the reason of a gap whose numbers the run never
// published: the engine's engine.UnpublishedEventsReason, word for word
// (internal/api checks they agree). Every other gap's reason says how its
// events were lost from object storage.
const UnpublishedEventsReason = "the run's history holds no event under these numbers"

// Sentence is the notice a reader prints for the gap, named for its cause.
func (g EventGap) Sentence() string {
	if g.Reason == UnpublishedEventsReason {
		return fmt.Sprintf("events %d–%d of this run were never published (%s)", g.First, g.Last, g.Reason)
	}
	return fmt.Sprintf("events %d–%d of this run were lost from object storage before it was archived (%s)",
		g.First, g.Last, g.Reason)
}

// Scope is what the request asked for.
type Scope struct {
	TaskID    string `json:"task_id"`
	Logs      bool   `json:"logs"`
	Artifacts bool   `json:"artifacts"`
	// Item is set when the request narrowed task_id to one item of a
	// fan-out.
	Item *int32 `json:"item,omitzero"`
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
	Item           *int32         `json:"item,omitzero"`
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
	CommitAdopted bool   `json:"commit_adopted,omitzero"`
}

// Member is one file of the bundle, or one file the bundle would have held.
type Member struct {
	Path   string `json:"path"`
	Kind   Kind   `json:"kind"`
	TaskID string `json:"task_id,omitempty"`
	// Item is set on a member one item of a fan-out recorded; such a member
	// lives under tasks/{task}/items/{item:05}/.
	Item    *int32 `json:"item,omitzero"`
	Attempt int32  `json:"attempt,omitzero"`
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
	// digest (reclaimed). Empty only for kind "source" when reclaimed,
	// rejected or withheld, and for "unrecorded".
	SHA256 string `json:"sha256"`
	State  State  `json:"state"`
	Reason string `json:"reason,omitempty"`
	// SourceRunID, SourceTaskID and SourceName name the run, task and
	// artifact that recorded an artifact passed between the runs of a tree:
	// one a child received (task_id "_inputs") or a composed task got back
	// from its child. Audit data from the recorded reference; empty for every
	// other member.
	SourceRunID  string `json:"source_run_id,omitempty"`
	SourceTaskID string `json:"source_task_id,omitempty"`
	SourceName   string `json:"source_name,omitempty"`
	// RecordState is what became of a present record member of kind
	// resolution, composed or outputs; empty for every other member.
	RecordState RecordState `json:"record_state,omitempty"`
	// ResumeScope is the resume a record member was made under; 0 for one
	// made before any resume, and for every other member.
	ResumeScope uint64 `json:"resume_scope,omitzero"`
}

// Counts tallies the members by state.
type Counts struct {
	Present      int   `json:"present"`
	Reclaimed    int   `json:"reclaimed"`
	Unrecorded   int   `json:"unrecorded"`
	Rejected     int   `json:"rejected"`
	Withheld     int   `json:"withheld,omitzero"`
	Missing      int   `json:"missing,omitzero"`
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
		case StateWithheld:
			c.Withheld++
		case StateMissing:
			c.Missing++
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
	out, err := stablejson.MarshalIndent(manifestJSON(m), "  ")
	if err != nil {
		return nil, fmt.Errorf("evidence: encode manifest: %w", err)
	}
	return append(out, '\n'), nil
}

// ParseManifest decodes a MANIFEST.json document. It reads SchemaVersion only
// and refuses any other, naming it, rather than guessing at what it means.
func ParseManifest(document []byte) (*Manifest, error) {
	var probe struct {
		SchemaVersion *int `json:"schema_version"`
	}
	if err := json.Unmarshal(document, &probe); err != nil {
		return nil, fmt.Errorf("evidence: MANIFEST.json is not JSON: %w", stablejson.Explained(err))
	}
	if probe.SchemaVersion == nil {
		return nil, fmt.Errorf("evidence: MANIFEST.json has no schema_version")
	}
	if *probe.SchemaVersion != SchemaVersion {
		return nil, &SchemaVersionError{Version: *probe.SchemaVersion}
	}
	var m manifestJSON
	if err := json.Unmarshal(document, &m); err != nil {
		return nil, fmt.Errorf("evidence: decode MANIFEST.json: %w", stablejson.Explained(err))
	}
	manifest := Manifest(m)
	if err := checkEndings(&manifest); err != nil {
		return nil, err
	}
	for _, gap := range manifest.EventGaps {
		if gap.First < 1 || gap.Last < gap.First {
			return nil, fmt.Errorf("evidence: MANIFEST.json event gap %d–%d is not a range", gap.First, gap.Last)
		}
	}
	return &manifest, nil
}
