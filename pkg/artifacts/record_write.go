package artifacts

import (
	"context"
	"errors"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The engine's writes of what it records for an awaited or a composed task
// (docs/TASK_IDENTITY.md): create-only, and only from what the engine
// checked itself.

// ErrRecordExists is a record already at the key: the first stands.
var ErrRecordExists = errors.New("a record is already there")

// OutcomeUnknown says a write may have landed: neither its answer nor a read
// of its object could say.
func OutcomeUnknown(err error) bool { return errors.Is(err, errOutcomeUnknown) }

// WriteResolutionDetail creates a resolution's detail at the key its bytes
// name and answers their SHA-256, which the resolution's record carries.
// One already there has the same bytes. A detail the reader would not find
// is refused before anything is written.
func (s *RecordStore) WriteResolutionDetail(ctx context.Context, d *orkstackv1.ResolutionDetail) (string, error) {
	raw, err := MarshalResolutionDetail(d)
	if err != nil {
		return "", err
	}
	sha := hexDigest(raw)
	if _, read := decodeResolutionDetail(raw, &orkstackv1.RecordedValues{Namespace: d.GetNamespace(), RunId: d.GetRunId(), TaskId: d.GetTaskId(),
		ResumeScope: d.GetResumeScope(), DetailSha256: sha}); read.Answer != RecordFound {
		return "", fmt.Errorf("artifacts: write resolution detail: %s: %w", read.Reason, ErrInvalidRecordKey)
	}
	key, err := ResolutionDetailKey(d.GetNamespace(), d.GetRunId(), d.GetTaskId(), sha)
	if err != nil {
		return "", err
	}
	if err := s.createOrSame(ctx, key, raw, maxResolutionDetail); err != nil {
		if errors.Is(err, errBlobExists) {
			return "", fmt.Errorf("artifacts: write resolution detail %q: other bytes are there: %w", key, ErrRecordExists)
		}
		return "", err
	}
	return sha, nil
}

// WriteRecordedValues creates rec at its own key and answers the SHA-256 of
// its stored bytes. A record already there is ErrRecordExists, whatever it
// holds, and is never replaced: the caller reads it. A record the reader
// would not find is refused before anything is written. A store that does
// not answer is *RecordStoreUnanswered, and OutcomeUnknown says whether the
// record may be there.
func (s *RecordStore) WriteRecordedValues(ctx context.Context, rec *orkstackv1.RecordedValues) (string, error) {
	want := RecordWant{Kind: rec.GetKind(), Namespace: rec.GetNamespace(), RunID: rec.GetRunId(), TaskID: rec.GetTaskId(),
		Scope: rec.GetResumeScope(), AdmissionSHA256: rec.GetAdmissionSha256()}
	raw, err := MarshalRecordedValues(rec)
	if err != nil {
		return "", err
	}
	if _, read := decodeRecordedValues(raw, want); read.Answer != RecordFound {
		return "", fmt.Errorf("artifacts: write recorded values: %s %s: %w", read.Answer, read.Reason, ErrInvalidRecordKey)
	}
	key, err := recordedValuesKey(want)
	if err != nil {
		return "", err
	}
	if err := s.createIn(ctx, key, raw); err != nil {
		if errors.Is(err, errBlobExists) {
			return "", fmt.Errorf("artifacts: write recorded values %q: %w", key, ErrRecordExists)
		}
		return "", err
	}
	return hexDigest(raw), nil
}

func recordedValuesKey(w RecordWant) (string, error) {
	switch w.Kind {
	case orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION:
		return RecordKey(RecordResolution, w.Namespace, w.RunID, w.TaskID, w.Scope)
	case orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED:
		return RecordKey(RecordComposed, w.Namespace, w.RunID, w.TaskID, w.Scope)
	case orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS:
		if w.Scope != 0 {
			return "", fmt.Errorf("artifacts: record key: a run's outputs have no resume scope: %w", ErrInvalidRecordKey)
		}
		return OutputsRecordKey(w.Namespace, w.RunID)
	default:
		return "", fmt.Errorf("artifacts: record key: no kind: %w", ErrInvalidRecordKey)
	}
}
