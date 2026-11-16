package artifacts

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
)

// A resume's record (docs/TASK_IDENTITY.md): what one resume of a run
// changed, written under the scope it opened before anything of the run is
// reset, and the link that leads from the scope before it. Both are this
// design's own JSON objects, create-only.
const (
	resumeRecordFormat   = 1
	resumeLinkFormat     = 1
	maxResumeRecordBytes = 2 << 20
	maxResumeLinkBytes   = 1 << 10
)

// ResumeRecord is one resume's record: the run, the scope it opened and
// the plan number n it was written under, the scope before it, the tasks
// run again and the awaits reopened.
type ResumeRecord struct {
	Namespace      string   `json:"namespace"`
	RunID          string   `json:"run_id"`
	Scope          uint64   `json:"scope"`
	N              int      `json:"n"`
	PreviousScope  uint64   `json:"previous_scope"`
	Tasks          []string `json:"tasks"`
	ReopenedAwaits []string `json:"reopened_awaits"`
}

// resumeWire is the record as stored: every key always written.
type resumeWire struct {
	V int64 `json:"v"`
	ResumeRecord
}

var resumeKeys = []string{"v", "namespace", "run_id", "scope", "n", "previous_scope", "tasks", "reopened_awaits"}

// EncodeResumeRecord writes r as stored, after checking it is one the
// reader would find.
func EncodeResumeRecord(r ResumeRecord) ([]byte, error) {
	if reason := checkResume(r); reason != "" {
		return nil, fmt.Errorf("artifacts: resume record: %s: %w", reason, ErrInvalidRecordKey)
	}
	return encodeResume(r)
}

func encodeResume(r ResumeRecord) ([]byte, error) {
	if r.Tasks == nil {
		r.Tasks = []string{}
	}
	if r.ReopenedAwaits == nil {
		r.ReopenedAwaits = []string{}
	}
	raw, err := json.Marshal(resumeWire{V: resumeRecordFormat, ResumeRecord: r}, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	if len(raw)+1 > maxResumeRecordBytes {
		return nil, fmt.Errorf("artifacts: resume record: %d bytes, more than %d: %w", len(raw)+1, maxResumeRecordBytes, ErrRecordTooLarge)
	}
	return append(raw, '\n'), nil
}

// DecodeResumeRecord reads raw as the record of namespace's run at scope
// under plan number n. It is found only when every key is present and
// none is null, no other key is, and it names exactly what was asked.
func DecodeResumeRecord(raw []byte, namespace, runID string, scope uint64, n int) RecordRead[ResumeRecord] {
	const object = "resume record"
	if len(raw) > maxResumeRecordBytes {
		return invalidRead[ResumeRecord](object, "%d bytes, more than %d", len(raw), maxResumeRecordBytes)
	}
	version, bad := decodeVersioned(raw, resumeRecordFormat, resumeKeys)
	switch {
	case bad != "":
		return invalidRead[ResumeRecord](object, "%s", bad)
	case version != resumeRecordFormat:
		if version > resumeRecordFormat {
			return RecordRead[ResumeRecord]{Answer: RecordUnsupported, Object: object, Version: version}
		}
		return invalidRead[ResumeRecord](object, "format %d is no format", version)
	}
	var w resumeWire
	if err := json.Unmarshal(raw, &w, json.RejectUnknownMembers(true)); err != nil {
		return invalidRead[ResumeRecord](object, "does not decode: %v", err)
	}
	r := w.ResumeRecord
	if reason := checkResume(r); reason != "" {
		return invalidRead[ResumeRecord](object, "%s", reason)
	}
	if ResolveNamespace(namespace) != r.Namespace || r.RunID != runID || r.Scope != scope || r.N != n {
		return invalidRead[ResumeRecord](object, "names %s/%s scope %d plan %d, not %s/%s scope %d plan %d",
			r.Namespace, r.RunID, r.Scope, r.N, ResolveNamespace(namespace), runID, scope, n)
	}
	return RecordRead[ResumeRecord]{Answer: RecordFound, Value: r}
}

// checkResume says what makes r a record no reader would find, or "".
func checkResume(r ResumeRecord) string {
	if err := validNamespaceName(r.Namespace); err != nil {
		return "namespace: " + err.Error()
	}
	if r.RunID == "" {
		return "no run id"
	}
	if r.PreviousScope >= r.Scope {
		return fmt.Sprintf("previous scope %d is not below scope %d", r.PreviousScope, r.Scope)
	}
	if r.N < 0 || r.N > maxResumePlan {
		return fmt.Sprintf("plan number %d is not 0 to %d", r.N, maxResumePlan)
	}
	for _, list := range [][]string{r.Tasks, r.ReopenedAwaits} {
		seen := make(map[string]bool, len(list))
		for _, id := range list {
			if err := validCommitTaskID(id); err != nil || seen[id] {
				return fmt.Sprintf("task id %q is not a name, or is given twice", id)
			}
			seen[id] = true
		}
	}
	return ""
}

// decodeVersioned reads raw as one JSON object and answers its "v". When
// "v" is known, the object must hold exactly keys, none of them null; bad
// says why it does not. An unknown format's other keys are its own.
func decodeVersioned(raw []byte, known int64, keys []string) (int64, string) {
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members); err != nil {
		return 0, "does not decode: " + err.Error()
	}
	if members == nil {
		return 0, "is not an object"
	}
	var version int64
	if v, ok := members["v"]; !ok {
		return 0, `no "v"`
	} else if err := json.Unmarshal(v, &version); err != nil {
		return 0, `"v" is not a whole number`
	}
	if version != known {
		return version, ""
	}
	if len(members) != len(keys) {
		for k := range members {
			if !containsString(keys, k) {
				return 0, fmt.Sprintf("an unknown key %q", k)
			}
		}
	}
	for _, k := range keys {
		v, ok := members[k]
		if !ok {
			return 0, fmt.Sprintf("no %q", k)
		}
		if v.Kind() == 'n' {
			return 0, fmt.Sprintf("%q is null", k)
		}
	}
	return version, ""
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// resumeLink is the link from a scope to the resume that left it: the
// scope it opened, the plan number its record was written under and that
// record's SHA-256, so the scope's record is exactly the one the link
// committed, never another plan's written beside it.
type resumeLink struct {
	Scope        uint64 `json:"scope"`
	N            int    `json:"n"`
	RecordSHA256 string `json:"record_sha256"`
}

var resumeLinkKeys = []string{"v", "scope", "n", "record_sha256"}

// encodeResumeLink is the link's stored bytes.
func encodeResumeLink(l resumeLink) []byte {
	return fmt.Appendf(nil, "{\"v\":%d,\"scope\":%d,\"n\":%d,\"record_sha256\":%q}\n", resumeLinkFormat, l.Scope, l.N, l.RecordSHA256)
}

// decodeResumeLink reads a link.
func decodeResumeLink(raw []byte) (resumeLink, RecordAnswer, string) {
	if len(raw) > maxResumeLinkBytes {
		return resumeLink{}, RecordInvalid, "too large"
	}
	version, bad := decodeVersioned(raw, resumeLinkFormat, resumeLinkKeys)
	switch {
	case bad != "":
		return resumeLink{}, RecordInvalid, bad
	case version > resumeLinkFormat:
		return resumeLink{}, RecordUnsupported, ""
	case version != resumeLinkFormat:
		return resumeLink{}, RecordInvalid, fmt.Sprintf("format %d is no format", version)
	}
	var w struct {
		V int64 `json:"v"`
		resumeLink
	}
	if err := json.Unmarshal(raw, &w, json.RejectUnknownMembers(true)); err != nil {
		return resumeLink{}, RecordInvalid, "does not decode: " + err.Error()
	}
	if w.N < 0 || w.N > maxResumePlan || !lowerHex64(w.RecordSHA256) {
		return resumeLink{}, RecordInvalid, "names no plan or no record digest"
	}
	return w.resumeLink, RecordFound, ""
}
