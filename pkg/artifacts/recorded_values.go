package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// The identity records' bounds (docs/TASK_IDENTITY.md).
const (
	maxRecordedValuesBytes = 64 << 10
	maxResolutionDetail    = 16 << 10
	maxDetailReasonBytes   = 1024
	// maxInlineValues: a completion's values up to this size are read
	// with its record and handed over inline.
	maxInlineValues = 4096
	maxSourceRows   = 180
	inputBindingLen = sha256.Size
)

// ErrRecordTooLarge reports an identity record over its bound.
var ErrRecordTooLarge = errors.New("identity record too large")

// recordFormats are the record formats this build reads: 1, and 2 only
// where a test binary turns it on (record_format2_integration.go).
var recordFormats atomic.Int64

func init() { recordFormats.Store(1) }

func knownRecordFormat(v int64) bool { return v >= 1 && v <= recordFormats.Load() }

// RecordFormat is the format a run admitted now has its records written in.
const RecordFormat = 1

// WritesRecordFormat says this build reads records of format v, and so may
// write one a run pins.
func WritesRecordFormat(v int) bool { return knownRecordFormat(int64(v)) }

// MarshalRecordedValues writes a record as it is stored: protojson,
// indented by stablejson, with a final newline. A record over 64 KiB is
// refused.
func MarshalRecordedValues(r *orkstackv1.RecordedValues) ([]byte, error) {
	return marshalRecordProto(r, maxRecordedValuesBytes, "recorded values")
}

// MarshalResolutionDetail writes a resolution's detail as it is stored.
func MarshalResolutionDetail(d *orkstackv1.ResolutionDetail) ([]byte, error) {
	return marshalRecordProto(d, maxResolutionDetail, "resolution detail")
}

func marshalRecordProto(m proto.Message, max int, what string) ([]byte, error) {
	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode %s: %w", what, err)
	}
	var out bytes.Buffer
	if err := stablejson.Indent(&out, raw, "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode %s: %w", what, err)
	}
	out.WriteByte('\n')
	if out.Len() > max {
		return nil, fmt.Errorf("artifacts: encode %s: %d bytes, more than %d: %w", what, out.Len(), max, ErrRecordTooLarge)
	}
	return out.Bytes(), nil
}

// RecordWant is what a read of a RecordedValues expects it to name.
type RecordWant struct {
	Kind            orkstackv1.RecordedValuesKind
	Namespace       string
	RunID           string
	TaskID          string // empty for OUTPUTS
	Scope           uint64
	AdmissionSHA256 string
}

// RecordedFound is a record found and checked. Its values are adopted only
// through InTime: a record the store wrote after what it settles was due,
// or after its run ended, settles nothing.
type RecordedFound struct {
	record       *orkstackv1.RecordedValues
	recordSHA256 string
	storedAt     time.Time
	detail       *orkstackv1.ResolutionDetail
	inline       []byte
}

// RecordSHA256 is the SHA-256 of the record's stored bytes.
func (f *RecordedFound) RecordSHA256() string { return f.recordSHA256 }

// StoredAt is when the store wrote the record: its own Last-Modified.
func (f *RecordedFound) StoredAt() time.Time { return f.storedAt }

// Detail is a resolution's detail; nil for any other kind.
func (f *RecordedFound) Detail() *orkstackv1.ResolutionDetail { return f.detail }

// Standing is what a found record says of its task to a check that asks
// only whether the result is on record: whether it is a rejection, the
// digest of its values, and a copy of what it follows. It hands over no
// values: those are adopted through InTime.
func (f *RecordedFound) Standing() (rejected bool, valuesSHA256 string, follows []*orkstackv1.RecordedSource) {
	for _, row := range f.record.GetFollows() {
		follows = append(follows, proto.Clone(row).(*orkstackv1.RecordedSource))
	}
	return f.record.GetRejected(), f.record.GetValues().GetSha256(), follows
}

// Artifacts is a copy of the artifacts a found record of a composed task or of
// a run's outputs names, each under its output's name.
func (f *RecordedFound) Artifacts() []*orkstackv1.ArtifactRef {
	out := make([]*orkstackv1.ArtifactRef, 0, len(f.record.GetArtifacts()))
	for _, a := range f.record.GetArtifacts() {
		out = append(out, proto.Clone(a).(*orkstackv1.ArtifactRef))
	}
	return out
}

// ValuesRef is where a found record of a composed task or of a run's outputs
// keeps its values. A resolution's are taken only through InTime: nil.
func (f *RecordedFound) ValuesRef() *orkstackv1.ArtifactRef {
	if f.record.GetValues() == nil || f.record.GetKind() == orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION {
		return nil
	}
	return proto.Clone(f.record.GetValues()).(*orkstackv1.ArtifactRef)
}

// Child is the child run a composed task's record takes its values from, and
// the digests of that run's admission and of its outputs' record.
func (f *RecordedFound) Child() (runID, admissionSHA256, outputsSHA256 string) {
	return f.record.GetChildRunId(), f.record.GetChildAdmissionSha256(), f.record.GetChildOutputsSha256()
}

// RecordedAdoption is a found record adopted in time.
type RecordedAdoption struct{ f *RecordedFound }

// Record is a copy of the record adopted; nil for a zero adoption.
func (a RecordedAdoption) Record() *orkstackv1.RecordedValues {
	if a.f == nil {
		return nil
	}
	return proto.Clone(a.f.record).(*orkstackv1.RecordedValues)
}

// Inline is a copy of the values' bytes when they are at most 4 KiB; nil
// otherwise, and for a zero adoption.
func (a RecordedAdoption) Inline() []byte {
	if a.f == nil {
		return nil
	}
	return bytes.Clone(a.f.inline)
}

// InTime adopts the record when the store wrote it in time for deadline
// and, when the run has ended, for runEnd (RecordedInTime). It is the one
// way a found record's values are taken.
func (f *RecordedFound) InTime(deadline, runEnd time.Time) (RecordedAdoption, bool) {
	if !RecordedInTime(f.storedAt, deadline, runEnd) {
		return RecordedAdoption{}, false
	}
	return RecordedAdoption{f: f}, true
}

// RecordedInTime says a record the store wrote at storedAt settles what was
// due by deadline: written at or before it, in whole seconds, and, when
// the run has ended (runEnd set), at or before its end. Whole seconds
// because S3 dates an object to the second: an answer stored within the
// second of its deadline counts, by design (P13r D18). A zero storedAt or
// deadline is never in time; a zero runEnd means the run is still open.
func RecordedInTime(storedAt, deadline, runEnd time.Time) bool {
	if storedAt.IsZero() || deadline.IsZero() {
		return false
	}
	stored := storedAt.Truncate(time.Second)
	if stored.After(deadline) {
		return false
	}
	return runEnd.IsZero() || !stored.After(runEnd)
}

// ReadRecordedValues reads the record at key and every object it names,
// with the run's spacings (nil: the reader's own). It is found only when
// the record decodes, is of a format this build reads, names exactly what
// want says, its rows are well formed, a resolution's detail is present,
// hashes to its digest and names the same task, and values of at most 4 KiB
// are present and hash to theirs. A store that does not answer is
// *RecordStoreUnanswered.
func (s *RecordStore) ReadRecordedValues(ctx context.Context, class SlotClass, spacing *RecordSpacings, key string, want RecordWant) (RecordRead[*RecordedFound], error) {
	r := s.readerWith(spacing)
	body, storedAt, found, err := r.readObject(ctx, class, key, maxRecordedValuesBytes+1)
	if err != nil {
		return RecordRead[*RecordedFound]{}, err
	}
	if !found {
		return RecordRead[*RecordedFound]{Answer: RecordNone}, nil
	}
	rec, read := decodeRecordedValues(body, want)
	if read.Answer != RecordFound {
		return read, nil
	}
	f := &RecordedFound{record: rec, recordSHA256: hexDigest(body), storedAt: storedAt}
	if rec.GetKind() == orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION {
		read, err := r.readDetail(ctx, class, rec)
		if err != nil || read.Answer != RecordFound {
			return read, err
		}
		f.detail = read.Value.detail
	}
	if v := rec.GetValues(); v != nil && v.GetSizeBytes() <= maxInlineValues {
		inline, read, err := r.readValues(ctx, class, v)
		if err != nil || read.Answer != 0 {
			return read, err
		}
		f.inline = inline
	}
	return RecordRead[*RecordedFound]{Answer: RecordFound, Value: f}, nil
}

// decodeRecordedValues reads a record's stored bytes and holds them to want,
// without the objects it names: found means the record alone is good.
func decodeRecordedValues(body []byte, want RecordWant) (*orkstackv1.RecordedValues, RecordRead[*RecordedFound]) {
	if len(body) > maxRecordedValuesBytes {
		return nil, invalidRead[*RecordedFound]("record", "%d bytes, more than %d", len(body), maxRecordedValuesBytes)
	}
	rec := &orkstackv1.RecordedValues{}
	if read, ok := decodeRecordProto[*RecordedFound](body, "record", rec); !ok {
		return nil, read
	}
	if reason := checkRecorded(rec, want); reason != "" {
		return nil, invalidRead[*RecordedFound]("record", "%s", reason)
	}
	return rec, RecordRead[*RecordedFound]{Answer: RecordFound}
}

// decodeResolutionDetail reads a detail's stored bytes and holds them to
// the record that names it: its digest, task and scope.
func decodeResolutionDetail(body []byte, rec *orkstackv1.RecordedValues) (*orkstackv1.ResolutionDetail, RecordRead[*RecordedFound]) {
	if len(body) > maxResolutionDetail {
		return nil, invalidRead[*RecordedFound]("detail", "%d bytes, more than %d", len(body), maxResolutionDetail)
	}
	if hexDigest(body) != rec.GetDetailSha256() {
		return nil, invalidRead[*RecordedFound]("detail", "does not hash to the record's digest")
	}
	d := &orkstackv1.ResolutionDetail{}
	if read, ok := decodeRecordProto[*RecordedFound](body, "detail", d); !ok {
		return nil, read
	}
	if d.GetNamespace() != rec.GetNamespace() || d.GetRunId() != rec.GetRunId() || d.GetTaskId() != rec.GetTaskId() || d.GetResumeScope() != rec.GetResumeScope() {
		return nil, invalidRead[*RecordedFound]("detail", "names another task or scope")
	}
	if len(d.GetReason()) > maxDetailReasonBytes {
		return nil, invalidRead[*RecordedFound]("detail", "a reason over %d bytes", maxDetailReasonBytes)
	}
	return d, RecordRead[*RecordedFound]{Answer: RecordFound}
}

// decodeRecordProto reads body as m after checking its schemaVersion: an
// unknown one is unsupported, 0 (absent) and anything that does not decode
// strictly is invalid.
func decodeRecordProto[T any](body []byte, object string, m proto.Message) (RecordRead[T], bool) {
	var peek map[string]jsontext.Value
	if err := json.Unmarshal(body, &peek); err != nil || peek == nil {
		return invalidRead[T](object, "does not decode"), false
	}
	var tree any
	if err := json.Unmarshal(body, &tree); err != nil || holdsNull(tree) {
		return invalidRead[T](object, "holds a null"), false
	}
	var version int64
	if v, ok := peek["schemaVersion"]; ok {
		if err := json.Unmarshal(v, &version); err != nil {
			return invalidRead[T](object, "schemaVersion is not a whole number"), false
		}
	}
	if version <= 0 {
		return invalidRead[T](object, "no format"), false
	}
	if !knownRecordFormat(version) {
		return RecordRead[T]{Answer: RecordUnsupported, Object: object, Version: version}, false
	}
	if err := protojson.Unmarshal(body, m); err != nil {
		return invalidRead[T](object, "does not decode: %v", err), false
	}
	return RecordRead[T]{}, true
}

// holdsNull says a decoded JSON value holds null anywhere.
func holdsNull(v any) bool {
	switch v := v.(type) {
	case nil:
		return true
	case map[string]any:
		for _, x := range v {
			if holdsNull(x) {
				return true
			}
		}
	case []any:
		for _, x := range v {
			if holdsNull(x) {
				return true
			}
		}
	}
	return false
}

// checkRecorded says how rec differs from what want expects, or "".
func checkRecorded(rec *orkstackv1.RecordedValues, want RecordWant) string {
	outputs := want.Kind == orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS
	switch {
	case want.RunID == "" || !lowerHex64(want.AdmissionSHA256) || (want.TaskID == "") != outputs:
		return "the read names no run, admission or task"
	case rec.GetKind() != want.Kind:
		return fmt.Sprintf("kind %s, not %s", rec.GetKind(), want.Kind)
	case rec.GetNamespace() != ResolveNamespace(want.Namespace) || rec.GetRunId() != want.RunID || rec.GetTaskId() != want.TaskID:
		return fmt.Sprintf("names %s/%s/%s, not %s/%s/%s", rec.GetNamespace(), rec.GetRunId(), rec.GetTaskId(),
			ResolveNamespace(want.Namespace), want.RunID, want.TaskID)
	case rec.GetResumeScope() != want.Scope:
		return fmt.Sprintf("resume scope %d, not %d", rec.GetResumeScope(), want.Scope)
	case rec.GetAdmissionSha256() != want.AdmissionSHA256:
		return "another admission"
	case len(rec.GetInputBinding()) != inputBindingLen:
		return "no input binding"
	case len(rec.GetSources()) > maxSourceRows || len(rec.GetFollows()) > maxSourceRows:
		return fmt.Sprintf("more than %d rows", maxSourceRows)
	}
	resolution := rec.GetKind() == orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION
	composed := rec.GetKind() == orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED
	switch {
	case !resolution && (rec.GetRejected() || rec.GetDetailSha256() != ""):
		return "a resolution's field on another kind"
	case !composed && (rec.GetChildRunId() != "" || rec.GetChildAdmissionSha256() != "" || rec.GetChildOutputsSha256() != ""):
		return "a composed task's field on another kind"
	case resolution && len(rec.GetArtifacts()) != 0:
		return "artifacts on a resolution"
	case !outputs && len(rec.GetSources()) != 0:
		return "sources on a record that is not a run's outputs"
	}
	switch rec.GetKind() {
	case orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION:
		if rec.GetRejected() == (rec.GetValues() != nil) {
			return "a resolution is either rejected or has values"
		}
		if !lowerHex64(rec.GetDetailSha256()) {
			return "no detail digest"
		}
	case orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED:
		if _, err := encodeArchiveSegment(rec.GetChildRunId()); err != nil || rec.GetValues() == nil ||
			!lowerHex64(rec.GetChildOutputsSha256()) || !lowerHex64(rec.GetChildAdmissionSha256()) {
			return "a composed task's record names no values, or no child run, admission or outputs"
		}
	case orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS:
		if rec.GetValues() == nil || len(rec.GetFollows()) != 0 {
			return "a run's outputs record has no values, or follows"
		}
	default:
		return "no kind"
	}
	if v := rec.GetValues(); v != nil && !lowerHex64(v.GetSha256()) {
		return "the values' digest is not a SHA-256"
	}
	names := make(map[string]bool, len(rec.GetArtifacts()))
	for _, a := range rec.GetArtifacts() {
		if a.GetName() == "" || names[a.GetName()] || a.GetUri() == "" || !lowerHex64(a.GetSha256()) {
			return fmt.Sprintf("artifact %q is not named once, with where it is and its SHA-256", a.GetName())
		}
		names[a.GetName()] = true
	}
	for _, rows := range [][]*orkstackv1.RecordedSource{rec.GetSources(), rec.GetFollows()} {
		seen := make(map[string]bool, len(rows))
		for _, row := range rows {
			if err := validCommitTaskID(row.GetTaskId()); err != nil || seen[row.GetTaskId()] {
				return fmt.Sprintf("row task id %q is not a name, or is given twice", row.GetTaskId())
			}
			seen[row.GetTaskId()] = true
			if row.GetAbsent() {
				if row.GetRecordSha256() != "" || row.GetResultSha256() != "" {
					return fmt.Sprintf("absent row %q names digests", row.GetTaskId())
				}
				continue
			}
			if !lowerHex64(row.GetRecordSha256()) || !lowerHex64(row.GetResultSha256()) {
				return fmt.Sprintf("row %q's digests are not SHA-256s", row.GetTaskId())
			}
		}
	}
	for _, row := range rec.GetFollows() {
		if row.GetAbsent() {
			return "a follows row is absent"
		}
	}
	return ""
}

func (r *recordReader) readDetail(ctx context.Context, class SlotClass, rec *orkstackv1.RecordedValues) (RecordRead[*RecordedFound], error) {
	key, err := ResolutionDetailKey(rec.GetNamespace(), rec.GetRunId(), rec.GetTaskId(), rec.GetDetailSha256())
	if err != nil {
		return invalidRead[*RecordedFound]("detail", "%v", err), nil
	}
	body, _, found, err := r.readObject(ctx, class, key, maxResolutionDetail+1)
	if err != nil {
		return RecordRead[*RecordedFound]{}, err
	}
	if !found {
		return invalidRead[*RecordedFound]("detail", "missing"), nil
	}
	d, read := decodeResolutionDetail(body, rec)
	if read.Answer != RecordFound {
		return read, nil
	}
	return RecordRead[*RecordedFound]{Answer: RecordFound, Value: &RecordedFound{detail: d}}, nil
}

// readValues reads small values inline; a zero Answer means they are good.
func (r *recordReader) readValues(ctx context.Context, class SlotClass, v *orkstackv1.ArtifactRef) ([]byte, RecordRead[*RecordedFound], error) {
	key, err := r.b.resolve(v.GetUri())
	if err != nil || key == "" || key == "." {
		return nil, invalidRead[*RecordedFound]("values", "%q names no object of this store", v.GetUri()), nil
	}
	body, _, found, err := r.readObject(ctx, class, key, maxInlineValues+1)
	if err != nil {
		return nil, RecordRead[*RecordedFound]{}, err
	}
	if !found {
		return nil, invalidRead[*RecordedFound]("values", "missing"), nil
	}
	if hexDigest(body) != v.GetSha256() || int64(len(body)) != v.GetSizeBytes() {
		return nil, invalidRead[*RecordedFound]("values", "do not hash to the record's digest"), nil
	}
	return body, RecordRead[*RecordedFound]{}, nil
}

func hexDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
