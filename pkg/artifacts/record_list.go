package artifacts

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// RecordName is one record of a run by its key: a container task's or an
// item's commit (RecordCommitName), a resolution, a composed task's, or, with no task,
// the run's outputs (RecordOutputsName).
type RecordName struct {
	Kind   RecordKind
	TaskID string
	Item   Item
	Scope  uint64
}

// RecordCommitName and RecordOutputsName are the kinds RecordKey renders no
// key of: a commit, and a run's outputs.
const (
	RecordCommitName  RecordKind = 0
	RecordOutputsName RecordKind = -1
)

// Rel is the record's key under its run's attested prefix.
func (n RecordName) Rel() string {
	if n.Kind == RecordOutputsName {
		return outputsObjectName
	}
	rel := n.TaskID + "/"
	if n.Scope != 0 {
		rel += scopeSegment(n.Scope) + "/"
	}
	switch n.Kind {
	case RecordResolution:
		return rel + resolutionObjectName
	case RecordComposed:
		return rel + composedObjectName
	}
	if n.Item.Set {
		rel += fmt.Sprintf("%05d/", n.Item.Index)
	}
	return rel + CommitObjectName
}

// ParseRecordName reads what Rel rendered; false for anything else.
func ParseRecordName(rel string) (RecordName, bool) {
	if rel == outputsObjectName {
		return RecordName{Kind: RecordOutputsName}, true
	}
	parts := strings.Split(rel, "/")
	if len(parts) < 2 || validCommitTaskID(parts[0]) != nil {
		return RecordName{}, false
	}
	n := RecordName{TaskID: parts[0]}
	mid := parts[1 : len(parts)-1]
	if len(mid) > 0 {
		if scope, ok := parseScopeSegment(mid[0]); ok && scope != 0 {
			n.Scope, mid = scope, mid[1:]
		}
	}
	switch parts[len(parts)-1] {
	case CommitObjectName:
		if len(mid) == 1 {
			i, ok := parseItemSegment(mid[0])
			if !ok {
				return RecordName{}, false
			}
			n.Item, mid = ItemAt(i), nil
		}
	case resolutionObjectName:
		n.Kind = RecordResolution
	case composedObjectName:
		n.Kind = RecordComposed
	default:
		return RecordName{}, false
	}
	if len(mid) != 0 || n.Rel() != rel {
		return RecordName{}, false
	}
	return n, true
}

// RecordURI is where the record of a run lies.
func (s *RecordStore) RecordURI(namespace, runID string, n RecordName) (string, error) {
	prefix, err := AttestedRunPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return s.b.locate(prefix + n.Rel()), nil
}

// RecordKeyOf is the key of the record of a run.
func RecordKeyOf(namespace, runID string, n RecordName) (string, error) {
	prefix, err := AttestedRunPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return prefix + n.Rel(), nil
}

// ListRecords lists the records of a run, of task alone when one is named, in
// the order of their keys, from the first after the key after ("" is from the
// start): at most max objects of the store's, and the last key they reached,
// with whether more follow it. An object there that is no record is passed
// over. A store that does not answer is *RecordStoreUnanswered.
func (s *RecordStore) ListRecords(ctx context.Context, class SlotClass, namespace, runID, taskID, after string, max int) (names []RecordName, reached string, more bool, err error) {
	run, err := AttestedRunPrefix(namespace, runID)
	if err != nil {
		return nil, "", false, err
	}
	prefix := run
	if taskID != "" {
		if err := validCommitTaskID(taskID); err != nil {
			return nil, "", false, fmt.Errorf("artifacts: list records: %w", errors.Join(ErrInvalidRecordKey, err))
		}
		prefix += taskID + "/"
	}
	in := blobListInput{Prefix: prefix, MaxKeys: int32(max)}
	if after != "" {
		in.StartAfter = run + after
	}
	release, err := s.reader.slots.acquireWithin(ctx, class, s.reader.spacing.AttemptMargin)
	if err != nil {
		return nil, "", false, err
	}
	page, err := s.b.list(ctx, in)
	release()
	if err != nil {
		return nil, "", false, unansweredIn(ctx, err)
	}
	if page.Truncated && len(page.Objects) == 0 {
		return nil, "", false, unanswered(errNothingYet)
	}
	for _, o := range page.Objects {
		reached = strings.TrimPrefix(o.Key, run)
		if n, ok := ParseRecordName(reached); ok {
			names = append(names, n)
		}
	}
	return names, reached, page.Truncated, nil
}

var errNothingYet = errors.New("the store answered a page of nothing that is not the last")

// ErrManyAdmissions is a run with more than one admission record, which no
// reader can choose between.
var ErrManyAdmissions = errors.New("the run has more than one admission record")

// AdmissionOf is the digest that names run's one admission record; "" for a
// run with none.
func (s *RecordStore) AdmissionOf(ctx context.Context, namespace, runID string) (string, error) {
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	page, err := s.b.list(ctx, blobListInput{Prefix: prefix + "admission-", MaxKeys: 3})
	if err != nil {
		return "", unansweredIn(ctx, err)
	}
	if page.Truncated && len(page.Objects) == 0 {
		return "", unanswered(errNothingYet)
	}
	var found []string
	for _, o := range page.Objects {
		sha := strings.TrimSuffix(strings.TrimPrefix(o.Key, prefix+"admission-"), ".json")
		if lowerHex64(sha) {
			found = append(found, sha)
		}
	}
	switch len(found) {
	case 0:
		return "", nil
	case 1:
		return found[0], nil
	}
	return "", ErrManyAdmissions
}
