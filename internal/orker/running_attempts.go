package orker

import (
	"cmp"
	"slices"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// maxRunningAttempts bounds the attempts DescribeOrker lists, so its answer
// stays a small payload whatever an Orker runs; past it the answer says it
// was cut.
const maxRunningAttempts = 256

// runningAttempts is the attempts this process has claimed and not ended, and
// those whose container is not known to be gone. It is process-local, as the
// ledgers it asks are; a container of this Orker's previous life is added by
// the boot reap that could not remove it. The zero value is ready to use.
type runningAttempts struct {
	mu       sync.Mutex
	attempts map[artifacts.AttemptRef]*runningAttempt
	// unnamed: a previous life's containers stayed and could not be named.
	unnamed bool
}

type runningAttempt struct {
	inFlight  bool
	container ContainerID // "" until its container is created
}

func (r *runningAttempts) entry(ref artifacts.AttemptRef) *runningAttempt {
	if r.attempts == nil {
		r.attempts = map[artifacts.AttemptRef]*runningAttempt{}
	}
	e := r.attempts[ref]
	if e == nil {
		e = &runningAttempt{}
		r.attempts[ref] = e
	}
	return e
}

// begin is an attempt claimed.
func (r *runningAttempts) begin(ref artifacts.AttemptRef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entry(ref).inFlight = true
}

// container is the container an attempt created, or may have; "" once it is
// removed.
func (r *runningAttempts) container(ref artifacts.AttemptRef, id ContainerID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.attempts[ref]; e != nil {
		e.container = id
	}
}

// end is an attempt over: from here it stands only while its container is
// owed a removal.
func (r *runningAttempts) end(ref artifacts.AttemptRef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.attempts[ref]; e != nil {
		if e.inFlight = false; e.container == "" {
			delete(r.attempts, ref)
		}
	}
}

// orphan is a container of this Orker's previous life that its boot could not
// remove.
func (r *runningAttempts) orphan(ref artifacts.AttemptRef, id ContainerID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.entry(ref); !e.inFlight {
		e.container = id
	}
}

// incomplete is a previous life's containers that stayed and cannot be named:
// no list of this process is whole.
func (r *runningAttempts) incomplete() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unnamed = true
}

// describe is the attempts that stand, in order, and whether the list may be
// short of some. An ended attempt whose container is no longer owed a removal
// is forgotten here.
func (r *runningAttempts) describe(owed func(ContainerID) bool) (rows []*orkstackv1.RunningAttempt, truncated bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var refs []artifacts.AttemptRef
	for ref, e := range r.attempts {
		if !e.inFlight && !owed(e.container) {
			delete(r.attempts, ref)
			continue
		}
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b artifacts.AttemptRef) int {
		return cmp.Or(cmp.Compare(a.RunID, b.RunID), cmp.Compare(a.TaskID, b.TaskID),
			cmp.Compare(a.Item.Index, b.Item.Index), cmp.Compare(a.Attempt, b.Attempt))
	})
	truncated = r.unnamed || len(refs) > maxRunningAttempts
	for _, ref := range refs[:min(len(refs), maxRunningAttempts)] {
		row := &orkstackv1.RunningAttempt{RunId: ref.RunID, TaskId: ref.TaskID, Ordinal: ref.Attempt}
		if ref.Item.Set {
			item := ref.Item.Index
			row.Item = &item
		}
		rows = append(rows, row)
	}
	return rows, truncated
}

// describeRunning is DescribeOrker's running list: what runningAttempts holds,
// asked of both ledgers of containers owed a removal.
func (d *OCIDispatcher) describeRunning(c *Capacity) ([]*orkstackv1.RunningAttempt, bool) {
	return d.attempts.describe(func(id ContainerID) bool {
		return d.unremoved.owes(id) || c.owes(id)
	})
}

// owes reports whether a removal of id stands owed.
func (u *unremovedContainers) owes(id ContainerID) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	_, owed := u.owed[id]
	return owed
}

// owes reports whether id still holds a reservation because it is not
// removed. No capacity owes nothing.
func (c *Capacity) owes(id ContainerID) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, owed := c.unremoved[id]
	return owed
}
