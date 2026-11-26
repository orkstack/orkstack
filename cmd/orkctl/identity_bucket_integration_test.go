//go:build integration

package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
)

// bucketFault takes the whole store away from every engine, from a moment a
// test names: when all of some runs' approvals are on record and before their
// doors hear, or when a record of a name is about to be written.
type bucketFault struct {
	mu      sync.Mutex
	away    bool
	refused int
	// await are the runs whose approvals' records the fault waits for; cut is
	// the record whose write it begins at.
	await map[string]bool
	cut   string
	gone  chan struct{}
}

func newBucketFault(store *s3scope.Proxy) *bucketFault {
	f := &bucketFault{await: map[string]bool{}}
	store.Script(func(r s3scope.Request) *s3scope.Answer {
		f.mu.Lock()
		defer f.mu.Unlock()
		_, record := recordOf(r.Key)
		if f.cut != "" && r.Verb == s3scope.Put && strings.HasSuffix(record, f.cut) {
			f.takeAway()
		}
		if !f.away {
			return nil
		}
		f.refused++
		return &s3scope.Answer{Status: http.StatusServiceUnavailable, Code: "SlowDown"}
	})
	store.After(func(r s3scope.Request) {
		f.mu.Lock()
		run, record := recordOf(r.Key)
		if r.Verb != s3scope.Put || r.Status >= 300 || record != "approval/resolution.json" || !f.await[run] {
			f.mu.Unlock()
			return
		}
		if delete(f.await, run); len(f.await) == 0 {
			f.takeAway()
		}
		gone := f.gone
		f.mu.Unlock()
		// No door hears, and tells its run, before the bucket is away.
		select {
		case <-gone:
		case <-time.After(time.Minute):
		}
	})
	return f
}

// takeAway is called under the lock.
func (f *bucketFault) takeAway() {
	f.cut = ""
	if !f.away {
		f.away = true
		close(f.gone)
	}
}

func (f *bucketFault) arm(cut string, runs ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gone, f.cut = make(chan struct{}), cut
	for _, run := range runs {
		f.await[run] = true
	}
}

// wait blocks until the bucket is away, then until it has been for d with
// requests refused, so that the engines were working while it was.
func (f *bucketFault) wait(s *identityStack, d time.Duration) {
	s.t.Helper()
	select {
	case <-f.gone:
	case <-time.After(2 * time.Minute):
		s.t.Fatal("the bucket never went away")
	}
	began := time.Now()
	s.await(fmt.Sprintf("%s of a store that answers nothing", d), func() (bool, string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return time.Since(began) > d && f.refused > 0, fmt.Sprintf("%d requests refused", f.refused)
	})
}

func (f *bucketFault) back() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.away, f.refused, f.cut = false, 0, ""
}

// TestIntegration_IdentityRecordsBucketAway is AGENTS.md §11's outage of the
// whole bucket in a run whose results the engine records (B10): an await told
// of an answer it cannot read stays open and takes it when told again; a run
// that must record an event ends failed, explicitly; a child's end and a
// composed task's collect end as "could not be stored"; and none says that a
// record is missing.
func TestIntegration_IdentityRecordsBucketAway(t *testing.T) {
	s := newIdentityStack(t)
	fault := newBucketFault(s.store)
	s.cli("namespace", "update", "default", "--audience", identityE2EAudience)
	approved := `printf '%s' {{ sh .tasks.approval.output.approver }} > /orkstack/output/artifacts/approver.txt`
	gate := "  - id: approval\n    await:\n      timeout_seconds: 900\n"
	s.create("default", "guarded", orkflowHeader("guarded", "")+gate+holdsToken("apply", "    needs:\n      - approval\n", approved))
	// Its sleep wakes while the bucket is away.
	s.create("default", "sleeping", orkflowHeader("sleeping", "")+gate+"  - id: nap\n    sleep:\n      seconds: 45\n"+
		holdsToken("apply", "    needs:\n      - approval\n      - nap\n", approved))
	s.create("default", "inner", orkflowHeader("inner", "outputs:\n  who: '{{ .tasks.approval.output.approver }}'\n")+gate)
	s.create("default", "outer", orkflowHeader("outer", "")+"  - id: ask\n    uses: orkflow:inner@1\n"+
		holdsToken("apply", "    needs:\n      - ask\n", `true`))
	// silent fails when a run's error or tasks say that a record is missing.
	silent := func(id string, run *orkstackv1.Run) {
		t.Helper()
		said := fmt.Sprint(run.GetError(), run.GetTaskRuns())
		if strings.Contains(said, "NOT_RECORDED") || strings.Contains(said, "not on record") || strings.Contains(said, "AWAIT_TIMED_OUT") {
			t.Fatalf("run %s says a record is missing: %s", id, said)
		}
	}

	t.Run("a run told while it is away, and one that must record an event", func(t *testing.T) {
		s.t = t
		control := s.awaiting("guarded")
		s.approve(control, "control")
		s.approvedBy(control, "control")

		told, event := s.awaiting("guarded"), s.awaiting("sleeping")
		fault.arm("", told, event)
		var asks sync.WaitGroup
		for _, id := range []string{told, event} {
			asks.Go(func() {
				out, code := s.orkctlOn(s.addr, "-n", "default", "run", "resolve", id, "--task", "approval", "--values", `{"approver":"dana"}`)
				if code != 0 {
					t.Errorf("an answer on record: exit %d: %s", code, out)
				}
			})
		}
		asks.Wait()
		// Past the read's own budget and the sleep's end.
		fault.wait(s, 100*time.Second)
		fault.back()

		run := s.get("default", told)
		if got := taskOf(t, run, "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
			t.Fatalf("a run told while the bucket was away has its approval %v", got)
		}
		silent(told, run)
		if out, code := s.resolve(told, "--redeliver"); code != 0 {
			t.Fatalf("redeliver: exit %d: %s", code, out)
		}
		s.approvedBy(told, "dana")

		run = s.ended(event)
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != "RUN_FAILED" || !strings.Contains(run.GetError().GetMessage(), "write snapshot for event") {
			t.Fatalf("a run that had an event to record ended %v: %v", run.GetStatus(), run.GetError())
		}
		if got := taskOf(t, run, "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
			t.Fatalf("the approval of a run that ended on another event reads %v", got)
		}
		silent(event, run)
		// Its answer is still on record.
		if got := s.cli("-n", "default", "run", "records", event); !strings.Contains(got, "approval") {
			t.Fatalf("the answer of a run that ended is not listed: %s", got)
		}
	})

	for _, c := range []struct{ name, record, ended, says, code string }{
		// Whichever step of it the store fails first, the reason says so.
		{"a child's end", "outputs.json", "child", "their record could not be made", "OUTPUTS_UNAVAILABLE"},
		// The parent fails by its task, RUN_FAILED: its rows may predate the
		// outage, which no state of it could be stored in.
		{"a composed task's collect", "composed.json", "parent", "could not be recorded", "RUN_FAILED"},
	} {
		t.Run(c.name+" while it is away", func(t *testing.T) {
			s.t = t
			var id, child string
			composed := func() {
				id = parseRunID(t, s.cli("-n", "default", "run", "submit", "outer"))
				s.await("the child's approval to be awaited", func() (bool, string) {
					if child = rowIn(s.get("default", id), "ask").GetChild().GetRunId(); child == "" {
						return false, "no child run"
					}
					got := rowIn(s.get("default", child), "approval").GetStatus()
					return got == orkstackv1.Status_STATUS_AWAITING, got.String()
				})
			}
			composed()
			s.approve(child, "dana")
			s.succeeded(s.ended(id))

			composed()
			fault.arm(c.record)
			s.approve(child, "dana")
			fault.wait(s, 100*time.Second)
			fault.back()
			run := s.ended(id)
			of, code := run, run.GetError().GetCode()
			if c.ended == "child" {
				of = s.get("default", child)
				code = of.GetError().GetCode()
			}
			if code != c.code {
				t.Fatalf("the %s's reason is %q, want %s: %v", c.ended, code, c.code, of.GetError())
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || of.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
				!strings.Contains(fmt.Sprint(of.GetError(), of.GetTaskRuns()), c.says) {
				t.Fatalf("the parent ended %v, the %s %v: %v", run.GetStatus(), c.ended, of.GetStatus(), of.GetError())
			}
			silent(id, run)
			silent(child, s.get("default", child))
			if len(s.issued("default", id)) != 0 {
				t.Fatal("a dependent of a child whose outputs were not stored was issued a token")
			}
		})
	}
	t.Logf("EVIDENCE the whole bucket away: an await told, an event to record, a child's end and a collect")
}

// TestIntegration_IdentityRecordsDeadlineBucketAway: the whole bucket away from
// before an await's deadline until past both its reads: the run closes failed,
// saying that the record could not be read, and what it could not write is not
// there; the answer is still on record. Its length is the five minutes between
// the two reads.
func TestIntegration_IdentityRecordsDeadlineBucketAway(t *testing.T) {
	s := newIdentityStack(t)
	fault := newBucketFault(s.store)
	s.cli("namespace", "update", "default", "--audience", identityE2EAudience)
	s.guarded("soon", 90)
	control := s.awaiting("soon")
	s.approve(control, "control")
	s.approvedBy(control, "control")

	id := s.awaiting("soon")
	deadline := taskOf(t, s.get("default", id), "approval").GetAwait().GetDeadline().AsTime()
	fault.arm("", id)
	s.approve(id, "dana")
	fault.wait(s, 0)
	closed := func() (enums.WorkflowExecutionStatus, time.Time) {
		d, err := describeExecution(s.temporal, s.ctx, id, "")
		must.OK(t, err)
		return d.GetWorkflowExecutionInfo().GetStatus(), d.GetWorkflowExecutionInfo().GetCloseTime().AsTime()
	}
	open := enums.WORKFLOW_EXECUTION_STATUS_RUNNING
	for range 3 { // each waits at most three minutes
		if status, _ := closed(); status != open || s.ctx.Err() != nil {
			break
		}
		began := time.Now()
		s.await("the run to close", func() (bool, string) {
			status, _ := closed()
			return status != open || time.Since(began) > 170*time.Second, status.String()
		})
	}
	status, at := closed()
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED || at.Before(deadline.Add(5*time.Minute)) || at.After(deadline.Add(9*time.Minute)) {
		t.Fatalf("the run closed %v at %s, %s past its deadline", status, at, at.Sub(deadline))
	}
	if err := s.temporal.GetWorkflow(s.ctx, id, "").Get(s.ctx, nil); err == nil || !strings.Contains(err.Error(), engine.ResolutionRecordUnavailableCode) {
		t.Fatalf("the close says %v, want %s", err, engine.ResolutionRecordUnavailableCode)
	}
	fault.back()
	run := s.ended(id)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != "RUN_FAILED" || run.GetError().GetMessage() == "" {
		t.Fatalf("the run reads %v: %v", run.GetStatus(), run.GetError())
	}
	if got := taskOf(t, run, "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
		t.Fatalf("what the run could not write reads %v", got)
	}
	if said := fmt.Sprint(run.GetError(), run.GetTaskRuns()); strings.Contains(said, "NOT_RECORDED") || strings.Contains(said, "AWAIT_TIMED_OUT") {
		t.Fatalf("the run says a record is missing, or that nobody answered: %s", said)
	}
	if got := s.cli("-n", "default", "run", "records", id); !strings.Contains(got, "approval") {
		t.Fatalf("the answer of a run that ended is not listed: %s", got)
	}
	if len(s.issued("default", id)) != 0 {
		t.Fatal("a token was issued behind an approval the run did not take")
	}
	t.Logf("EVIDENCE the whole bucket away at a deadline: closed %s past it", at.Sub(deadline))
}
