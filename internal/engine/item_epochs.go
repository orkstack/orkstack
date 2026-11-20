package engine

import orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

// MaxItemEpochRuns bounds the ranges one fan-out's items may need
// (itemEpochRuns): ResumeRun refuses, RESUME_TOO_LARGE, a plan that would
// give one fan-out more, so its direct dependent's input stays well within
// the 256 KiB payload bound (TestItemEpochRuns_AtTheirBoundFitTheInput).
const MaxItemEpochRuns = 1024

// itemEpochRuns is a fan-out's item scopes as a direct dependent needs them
// (EachUpstream.epochs, E85 R2-2): consecutive items committed under the
// same non-zero scope, as ranges in index order. An item at scope 0, which
// is every item of a run never resumed, is in no range, so such a run sends
// none. items is indexed by item.
func itemEpochRuns(items []*orkstackv1.TaskRun) []*orkstackv1.ItemEpochRun {
	var runs []*orkstackv1.ItemEpochRun
	for i, item := range items {
		epoch := item.GetResultEpoch()
		if epoch <= 0 {
			continue
		}
		if n := len(runs); n > 0 {
			last := runs[n-1]
			if last.GetEpoch() == uint64(epoch) && int(last.GetFirst()+last.GetCount()) == i {
				last.Count++
				continue
			}
		}
		runs = append(runs, &orkstackv1.ItemEpochRun{First: int32(i), Count: 1, Epoch: uint64(epoch)})
	}
	return runs
}
