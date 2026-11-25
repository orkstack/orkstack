package artifacts

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// HighestSnapshot is the highest event sequence any stored snapshot of a run
// names, full or overlay, or 0 for a run with none. A resume takes a sequence
// above it, so no snapshot another execution wrote is ever overwritten.
func (s *BlobRunSnapshotStore) HighestSnapshot(ctx context.Context, namespace, runID string) (int64, error) {
	prefix, err := RunSnapshotPrefix(namespace, runID)
	if err != nil {
		return 0, err
	}
	var highest int64
	err = listObjects(ctx, s.b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
		highest = max(highest, snapshotSequence(strings.TrimPrefix(object.Key, prefix)))
		return false, nil
	})
	if err != nil {
		return 0, fmt.Errorf("artifacts: list the snapshots of run %q: %w", runID, err)
	}
	return highest, nil
}

// snapshotSequence is the sequence a leaf of a run's snapshot prefix names,
// 0 for anything that is no snapshot.
func snapshotSequence(leaf string) int64 {
	digits, ok := strings.CutSuffix(leaf, snapshotSuffix)
	if !ok {
		if digits, ok = strings.CutSuffix(leaf, snapshotOverlaySuffix); !ok {
			return 0
		}
	}
	sequence, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || sequence < 1 || strconv.FormatInt(sequence, 10) != digits {
		return 0
	}
	return sequence
}
