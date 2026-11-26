//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordFaults makes the engine's records unreadable, run by run, and watches
// for a record's write.
type recordFaults struct {
	mu sync.Mutex
	// armed runs' records are unreadable from the moment their approval's
	// record is written; down ones' are now. suffix is a record's name no
	// request reaches, whatever its run.
	armed, down map[string]bool
	suffix      string
	// bucket is every request of every engine.
	bucket  bool
	refused map[string]int
	last    map[string]time.Time
	// landed is called when a run's approval is on record, before whoever
	// wrote it hears.
	landed map[string]func()
}

func newRecordFaults(store *s3scope.Proxy) *recordFaults {
	f := &recordFaults{armed: map[string]bool{}, down: map[string]bool{}, refused: map[string]int{}, last: map[string]time.Time{}, landed: map[string]func(){}}
	store.Script(func(r s3scope.Request) *s3scope.Answer {
		f.mu.Lock()
		defer f.mu.Unlock()
		run, record := recordOf(r.Key)
		if f.bucket {
			return &s3scope.Answer{Status: http.StatusServiceUnavailable, Code: "SlowDown"}
		}
		if record == "" || !((f.down[run] && r.Verb != s3scope.Put) || (f.suffix != "" && strings.HasSuffix(record, f.suffix))) {
			return nil
		}
		f.refused[run]++
		f.last[run] = time.Now()
		return &s3scope.Answer{Status: http.StatusServiceUnavailable, Code: "SlowDown"}
	})
	store.After(func(r s3scope.Request) {
		run, record := recordOf(r.Key)
		if r.Verb != s3scope.Put || record != "approval/resolution.json" || r.Status >= 300 {
			return
		}
		f.mu.Lock()
		if f.armed[run] {
			f.down[run] = true
		}
		landed := f.landed[run]
		f.mu.Unlock()
		if landed != nil {
			landed()
		}
	})
	return f
}

// recordOf is the run and the record a key under the engine's records names.
func recordOf(key string) (run, record string) {
	rest, ok := strings.CutPrefix(key, artifacts.AttestedPrefix+"/default/")
	if !ok {
		return "", ""
	}
	run, record, _ = strings.Cut(rest, "/")
	return run, record
}

func (f *recordFaults) set(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *recordFaults) lift(run string) { f.set(func() { delete(f.armed, run); delete(f.down, run) }) }

// refusedReads is how many reads of run's records were refused.
func (f *recordFaults) refusedReads(run string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refused[run]
}

// quietPast says run's records were asked for more than n times, and not in the
// last twenty seconds.
func (f *recordFaults) quietPast(run string, n int) (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refused[run] > n && time.Since(f.last[run]) > 20*time.Second, fmt.Sprintf("%d reads refused, %d at first", f.refused[run], n)
}

// orkctlOn is orkctl against one replica, for commands that run at once: it
// changes nothing of this process.
func (s *identityStack) orkctlOn(addr string, args ...string) (string, int) {
	cmd := exec.CommandContext(s.ctx, s.orkctl, append(args, "--server", "http://"+addr)...)
	cmd.Dir, cmd.Env = s.root, append(testbin.OrkerEnv(testbin.StackEnv(os.Environ())), "ORK_TOKEN_FILE="+s.token)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out) + err.Error(), max(cmd.ProcessState.ExitCode(), 1)
	}
	return string(out), 0
}

// awaiting submits name and waits until its approval is awaited.
// rowIn is task id's row of run, or nil while the run's state has none yet,
// for a wait that reads a run that has just started.
func rowIn(run *orkstackv1.Run, id string) *orkstackv1.TaskRun {
	for _, task := range run.GetTaskRuns() {
		if task.GetTaskId() == id {
			return task
		}
	}
	return nil
}

func (s *identityStack) awaiting(name string) string {
	s.t.Helper()
	id := parseRunID(s.t, s.cli("-n", "default", "run", "submit", name))
	s.await(name+"'s approval to be awaited", func() (bool, string) {
		got := rowIn(s.get("default", id), "approval").GetStatus()
		return got == orkstackv1.Status_STATUS_AWAITING, got.String()
	})
	return id
}

func (s *identityStack) resolve(id string, more ...string) (string, int) {
	s.t.Helper()
	out, stderr, code := s.try(append([]string{"-n", "default", "run", "resolve", id, "--task", "approval"}, more...)...)
	return out + stderr, code
}

func (s *identityStack) approve(id, who string) {
	s.t.Helper()
	if out, code := s.resolve(id, "--values", `{"approver":"`+who+`"}`); code != 0 {
		s.t.Fatalf("resolve %s: exit %d: %s", id, code, out)
	}
}

// ended waits for id to end.
func (s *identityStack) ended(id string) *orkstackv1.Run {
	s.t.Helper()
	s.try("-n", "default", "run", "watch", id)
	return s.get("default", id)
}

// events is id's whole stream, once it ended.
func (s *identityStack) events(id string) string {
	s.t.Helper()
	out, stderr, _ := s.try("-n", "default", "run", "watch", id, "--json")
	if !strings.Contains(out, "RUN_STARTED") {
		s.t.Fatalf("run %s's events were not read: %s%s", id, tokenShape.ReplaceAllString(out, "<token>"), stderr)
	}
	return out
}

// approvedBy fails unless id succeeded and its task was given who.
func (s *identityStack) approvedBy(id, who string) {
	s.t.Helper()
	run := s.ended(id)
	s.succeeded(run)
	if got := s.artifact("default", id, "apply", "approver.txt"); got != who {
		s.t.Fatalf("run %s's task was given approver %q, want %q", id, got, who)
	}
}

// forge tells run of a resolution of task no door recorded, as whoever may
// write to the cluster's namespace may.
func (s *identityStack) forge(run, task string, reject bool, marker string) {
	s.t.Helper()
	sig := &orkstackv1.AwaitSignal{TaskId: task, Reject: reject, Principal: marker, Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API}
	if reject {
		sig.Reason = marker
	} else {
		sig.ValuesJson = []byte(`{"approver":"` + marker + `"}`)
		sig.Values = &orkstackv1.ArtifactRef{Name: "values.json", Uri: "file:///" + marker, Sha256: strings.Repeat("0", 64), SizeBytes: int64(len(sig.ValuesJson))}
	}
	must.OK(s.t, s.temporal.SignalWorkflow(s.ctx, run, "", engine.ResolveAwaitSignal, sig))
}

// guarded creates Orkflow name, whose task follows an approval that times out.
func (s *identityStack) guarded(name string, seconds int) {
	s.t.Helper()
	s.create("default", name, orkflowHeader(name, "")+fmt.Sprintf("  - id: approval\n    await:\n      timeout_seconds: %d\n", seconds)+
		holdsToken("apply", "    needs:\n      - approval\n", `printf '%s' {{ sh .tasks.approval.output.approver }} > /orkstack/output/artifacts/approver.txt`))
}

// TestIntegration_IdentityRecordsDeadlines is AGENTS.md §11's awaits of a run
// whose results the engine records, told of and timed out while the records are
// unreadable (B10), on real ork, orker and orkctl: the record is taken when the
// run is told again, at its deadline or at that deadline's second read, or the
// await ends as "could not be read". Every fault has a run beside it that meets
// none. Its length is the five minutes between a deadline's two reads, and its
// cases share them.
func TestIntegration_IdentityRecordsDeadlines(t *testing.T) {
	s := newIdentityStack(t)
	faults := newRecordFaults(s.store)
	s.cli("namespace", "update", "default", "--audience", identityE2EAudience)
	s.guarded("guarded", 900)
	s.guarded("soon", 150)

	// Deadlines run while the rest of the test does.
	unread, lapsed := s.awaiting("soon"), s.awaiting("soon")
	untold, late := s.awaiting("soon"), s.awaiting("soon")
	deadline := taskOf(t, s.get("default", late), "approval").GetAwait().GetDeadline().AsTime()
	faults.set(func() { faults.armed[unread], faults.armed[untold], faults.armed[late] = true, true, true })
	// Each run is told, makes its own read and gives it up, all before any
	// deadline.
	doors := map[string]int{}
	for id, who := range map[string]string{unread: "unread", untold: "untold", late: "late"} {
		s.approve(id, who)
		doors[id] = faults.refusedReads(id)
	}
	for id, n := range doors {
		s.await("a run told to give its own read up", func() (bool, string) { return faults.quietPast(id, n) })
	}
	if first := taskOf(t, s.get("default", unread), "approval").GetAwait().GetDeadline().AsTime(); !time.Now().Before(first.Add(-5 * time.Second)) {
		t.Fatalf("the deadlines began to pass at %s, before the runs told had given their reads up: this host is too slow for what the test proves", first)
	}
	faults.lift(untold)
	refusedLate := faults.refusedReads(late)

	t.Run("the records unreadable when the run is told", func(t *testing.T) {
		s.t = t
		id := s.awaiting("guarded")
		faults.set(func() { faults.armed[id] = true })
		s.approve(id, "dana")
		// The door's own read is behind it: what is refused from here is the
		// run's, which makes it and gives it up.
		doors := faults.refusedReads(id)
		s.await("the run told to give its own read up", func() (bool, string) { return faults.quietPast(id, doors) })
		if got := taskOf(t, s.get("default", id), "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
			t.Fatalf("a run whose read went unanswered has its approval %v", got)
		}
		control := s.awaiting("guarded")
		s.approve(control, "control")
		s.approvedBy(control, "control")

		faults.lift(id)
		if out, code := s.resolve(id, "--redeliver"); code != 0 {
			t.Fatalf("redeliver: exit %d: %s", code, out)
		}
		s.approvedBy(id, "dana")
		s.onlyTimers(id, 900*time.Second)
		if strings.Contains(s.events(id), "not recorded") || strings.Contains(s.events(id), "RESOLUTION_NOT_RECORDED") {
			t.Fatal("a record that could not be read was reported as not recorded")
		}
	})

	t.Run("nobody tells a run again that could not read: its deadline takes the record", func(t *testing.T) {
		s.t = t
		s.approvedBy(untold, "untold")
		// The control: the same deadline, and nothing on record.
		run := s.ended(lapsed)
		if got := taskOf(t, run, "approval").GetError().GetCode(); run.GetStatus() != orkstackv1.Status_STATUS_FAILED || got != "AWAIT_TIMED_OUT" {
			t.Fatalf("a run nobody answered ended %v, %q", run.GetStatus(), got)
		}
	})

	// The records of late are readable again between its deadline's two reads,
	// five minutes apart: the first is made, refused and given up.
	s.t = t
	s.await("its deadline's first read to go unanswered", func() (bool, string) {
		quiet, saw := faults.quietPast(late, refusedLate)
		return quiet && time.Now().After(deadline), saw
	})
	if got := taskOf(t, s.get("default", late), "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
		t.Fatalf("past a deadline whose record could not be read, the approval is %v", got)
	}
	faults.lift(late)

	t.Run("the records unreadable at the deadline, and readable at its second read", func(t *testing.T) {
		s.t = t
		s.approvedBy(late, "late")
		s.onlyTimers(late, 150*time.Second, 300*time.Second)
		s.onlyTimers(untold, 150*time.Second)
	})

	t.Run("the records unreadable past the deadline", func(t *testing.T) {
		s.t = t
		run := s.ended(unread)
		s.onlyTimers(unread, 150*time.Second, 300*time.Second)
		if got := taskOf(t, run, "approval").GetError().GetCode(); run.GetStatus() != orkstackv1.Status_STATUS_FAILED || got != engine.ResolutionRecordUnavailableCode {
			t.Fatalf("the run ended %v, its approval %q", run.GetStatus(), got)
		}
		if !strings.Contains(s.events(lapsed), "AWAIT_TIMED_OUT") {
			t.Fatal("a run that timed out does not say so in its events")
		}
		if events := s.events(unread); strings.Contains(events, "AWAIT_TIMED_OUT") || strings.Contains(events, "NOT_RECORDED") {
			t.Fatal("a record that could not be read was reported as a timeout or as missing")
		}
	})
}

// TestIntegration_IdentityRecordsChaos is AGENTS.md §11's awaited and composed
// tasks on record (B10), on real ork, orker and orkctl: an answer is taken
// from its record or not at all, and none is lost. Every fault has a run
// beside it that meets none.
func TestIntegration_IdentityRecordsChaos(t *testing.T) {
	// A generation is short enough that a fan-out of 60 items fills one.
	s := newIdentityStack(t, "ORK_CONTINUATION_EVENTS=300")
	faults := newRecordFaults(s.store)
	s.cli("namespace", "update", "default", "--audience", identityE2EAudience)
	s.guarded("guarded", 900)
	s.guarded("soon", 60)
	s.create("default", "plain", orkflowHeader("plain", "")+"  - id: approval\n    await:\n      timeout_seconds: 900\n")
	engineA := s.addr

	t.Run("the whole bucket away when an answer is given", func(t *testing.T) {
		s.t = t
		id := s.awaiting("guarded")
		faults.set(func() { faults.bucket = true })
		out, code := s.resolve(id, "--values", `{"approver":"dana"}`)
		faults.set(func() { faults.bucket = false })
		// The refusal is the store's: the door says what it could not read.
		if code == 0 || !strings.Contains(out, "unavailable") || !strings.Contains(out, "SlowDown") {
			t.Fatalf("an answer with no store: exit %d: %s", code, out)
		}
		if got := taskOf(t, s.get("default", id), "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
			t.Fatalf("an answer with no store left the approval %v", got)
		}
		if got := s.recorded(artifacts.AttestedPrefix, "default", id); len(got) != 0 {
			t.Fatalf("an answer with no store recorded %v", got)
		}
		s.approve(id, "dana")
		s.approvedBy(id, "dana")
	})

	t.Run("an await crosses a switch", func(t *testing.T) {
		s.t = t
		s.create("default", "crossing", orkflowHeader("crossing", "")+`  - id: approval
    await:
      timeout_seconds: 900
  - id: list
    uses: json
    network: none
    with:
      op: query
      nullInput: true
      filter: '[range(60) | tostring]'
  - id: spread
    uses: json
    network: none
    needs:
      - list
    each:
      over: '{{ .tasks.list.output.result }}'
      as: part
      max_items: 60
    with:
      op: query
      nullInput: true
      filter: '1'
`+holdsToken("apply", "    needs:\n      - approval\n      - spread\n",
			`printf '%s' {{ sh .tasks.approval.output.approver }} > /orkstack/output/artifacts/approver.txt`))
		id := s.awaiting("crossing")
		execution := func() string {
			d, err := describeExecution(s.temporal, s.ctx, id, "")
			must.OK(t, err)
			return d.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		}
		first, at := execution(), taskOf(t, s.get("default", id), "approval").GetAwait().GetDeadline().AsTime()
		s.await("the run to cross a switch with its fan-out done", func() (bool, string) {
			got := rowIn(s.get("default", id), "spread").GetStatus()
			return got == orkstackv1.Status_STATUS_SUCCEEDED && execution() != first, got.String()
		})
		if a := taskOf(t, s.get("default", id), "approval"); a.GetStatus() != orkstackv1.Status_STATUS_AWAITING || !a.GetAwait().GetDeadline().AsTime().Equal(at) {
			t.Fatalf("across a switch the approval is %v, its deadline %v, was %v", a.GetStatus(), a.GetAwait().GetDeadline().AsTime(), at)
		}
		s.approve(id, "dana")
		s.approvedBy(id, "dana")
		for i, seq := range jsonSequences(t, s.events(id)) {
			if seq != int64(i+1) {
				t.Fatalf("event %d of the run is numbered %d", i+1, seq)
			}
		}
	})

	t.Run("a resolution no door recorded", func(t *testing.T) {
		s.t = t
		completed, rejected, plain := s.awaiting("guarded"), s.awaiting("guarded"), s.awaiting("plain")
		s.forge(completed, "approval", false, "forged-marker")
		s.forge(rejected, "approval", true, "forged-marker")
		// The control, and what is stated of a run that holds no identity.
		s.forge(plain, "approval", false, "forged-marker")
		s.succeeded(s.ended(plain))
		for _, id := range []string{completed, rejected} {
			// The run read for a record, twice, before anybody answers it.
			s.await("the run told to look for a record", func() (bool, string) {
				n := 0
				for _, r := range s.store.Requests() {
					if run, record := recordOf(r.Key); run == id && record == "approval/resolution.json" && r.Verb == s3scope.Get {
						n++
					}
				}
				return n >= 2, fmt.Sprintf("%d reads", n)
			})
			if got := taskOf(t, s.get("default", id), "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
				t.Fatalf("a forged resolution left the approval %v", got)
			}
			if recs, got := s.issued("default", id), s.recorded(artifacts.AttestedPrefix, "default", id); len(recs) != 0 || len(got) != 0 {
				t.Fatalf("a forged resolution: issued %v, recorded %v", recs, got)
			}
			s.approve(id, "dana")
			s.approvedBy(id, "dana")
			if events := s.events(id); strings.Contains(events, "forged-marker") || strings.Count(events, "RESOLUTION_NOT_RECORDED") != 1 {
				t.Fatalf("the run's events: %s", tokenShape.ReplaceAllString(events, "<token>"))
			}
		}
	})

	s.t = t
	replicaB := s.replica("ork-B")
	engineB := s.addr
	t.Run("two resolutions at once, through two replicas", func(t *testing.T) {
		s.t = t
		id := s.awaiting("guarded")
		type answer struct {
			out  string
			code int
		}
		answers := make(chan answer, 2)
		for addr, args := range map[string][]string{engineA: {"--values", `{"approver":"dana"}`}, engineB: {"--reject", "--reason", "no"}} {
			go func() {
				out, code := s.orkctlOn(addr, append([]string{"-n", "default", "run", "resolve", id, "--task", "approval"}, args...)...)
				answers <- answer{args[0] + ": " + out, code}
			}()
		}
		one, two := <-answers, <-answers
		if one.code != 0 {
			one, two = two, one
		}
		if one.code != 0 || two.code == 0 || !strings.Contains(two.out, "AWAIT_ALREADY_RESOLVED") {
			t.Fatalf("%+v and %+v, want one taken and one refused", one, two)
		}
		run := s.ended(id)
		if want := strings.HasPrefix(one.out, "--values"); want != (run.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED) {
			t.Fatalf("%s was recorded and the run ended %v", one.out, run.GetStatus())
		}
		if got := strings.Count(s.events(id), "TASK_RESOLVED"); got != 1 {
			t.Fatalf("the run was resolved %d times", got)
		}
	})

	t.Run("a task dispatched ahead of its approval by whoever answers the run's workflow tasks", func(t *testing.T) {
		s.t, s.addr = t, engineA
		like := s.awaiting("guarded")
		s.approve(like, "dana")
		s.approvedBy(like, "dana")
		id := s.awaiting("guarded")
		engines := []launched{s.first, replicaB}
		// No engine answers the run's next workflow task, and each answers
		// its Orker again afterwards.
		signal := func(sig os.Signal) {
			for _, p := range engines {
				must.OK(t, p.process.Signal(sig))
			}
		}
		signal(syscall.SIGSTOP)
		t.Cleanup(func() { signal(syscall.SIGCONT) })
		s.forgeDispatch(id, like, "apply")
		signal(syscall.SIGCONT)
		s.await("the attempt to end", func() (bool, string) {
			failed := s.activityFailures(id)
			return len(failed) > 0, fmt.Sprint(failed)
		})
		if failed := s.activityFailures(id); !strings.Contains(failed[0], "UPSTREAM_RESULT_NOT_RECORDED") || !strings.Contains(failed[0], `"approval"`) {
			t.Fatalf("the attempt ended %v, want the approval named, whose record it stages before it asks for a token", failed)
		}
		if recs, got := s.issued("default", id), s.recorded(artifacts.AttestedPrefix, "default", id); len(recs) != 0 || len(got) != 0 {
			t.Fatalf("dispatched ahead: issued %v, recorded %v", recs, got)
		}
		must.OK(t, s.temporal.TerminateWorkflow(s.ctx, id, "", "its history was written by the test"))
	})

	s.t = t
	replicaC := s.replica("ork-C")
	engineC := s.addr
	t.Run("an engine killed between the record and telling the run", func(t *testing.T) {
		s.t, s.addr = t, engineA
		repeated, silent := s.awaiting("guarded"), s.awaiting("soon")
		// The engine is killed once both records are there and before either
		// request hears.
		landed := make(chan string, 8)
		lost := func(id string) func() {
			return func() {
				landed <- id
				select {
				case <-replicaC.exited:
				case <-s.ctx.Done():
				}
			}
		}
		faults.set(func() { faults.landed[repeated], faults.landed[silent] = lost(repeated), lost(silent) })
		bothLanded := make(chan bool, 1)
		go func() {
			seen, late := map[string]bool{}, time.After(time.Minute)
			for len(seen) < 2 && late != nil {
				select {
				case id := <-landed:
					seen[id] = true
				case <-late:
					late = nil
				case <-s.ctx.Done():
					late = nil
				}
			}
			_ = replicaC.process.Kill()
			bothLanded <- len(seen) == 2
		}()
		codes := make(chan int, 2)
		for _, id := range []string{repeated, silent} {
			go func() {
				_, code := s.orkctlOn(engineC, "-n", "default", "run", "resolve", id, "--task", "approval", "--values", `{"approver":"dana"}`)
				codes <- code
			}()
		}
		if a, b := <-codes, <-codes; a == 0 || b == 0 {
			t.Fatalf("the killed engine answered: exits %d and %d", a, b)
		}
		faults.set(func() { clear(faults.landed) })
		if !<-bothLanded {
			t.Fatal("the engine was killed with a record not made: the requests failed of something else")
		}
		if got := taskOf(t, s.get("default", repeated), "approval").GetStatus(); got != orkstackv1.Status_STATUS_AWAITING {
			t.Fatalf("a run nobody told has its approval %v", got)
		}
		if out, code := s.resolve(repeated, "--reject", "--reason", "another answer"); code == 0 || !strings.Contains(out, "AWAIT_ALREADY_RESOLVED") {
			t.Fatalf("another answer: exit %d: %s", code, out)
		}
		// The refusal delivers the answer on record (AGENTS.md section 11).
		s.approvedBy(repeated, "dana")
		s.approve(repeated, "dana")
		s.approvedBy(repeated, "dana")
		s.approvedBy(silent, "dana")
	})

	s.t = t
	s.create("default", "inner", orkflowHeader("inner", "outputs:\n  who: '{{ .tasks.approval.output.approver }}'\n")+
		"  - id: approval\n    await:\n      timeout_seconds: 900\n")
	s.create("default", "outer", orkflowHeader("outer", "")+"  - id: ask\n    uses: orkflow:inner@1\n"+
		holdsToken("apply", "    needs:\n      - ask\n", `printf '%s' {{ sh .tasks.ask.output.who }} > /orkstack/output/artifacts/approver.txt`))
	// composed submits outer and waits until its child's approval is awaited.
	composed := func() (id, child string) {
		id = parseRunID(s.t, s.cli("-n", "default", "run", "submit", "outer"))
		s.await("the child's approval to be awaited", func() (bool, string) {
			child = rowIn(s.get("default", id), "ask").GetChild().GetRunId()
			if child == "" {
				return false, "no child run"
			}
			got := rowIn(s.get("default", child), "approval").GetStatus()
			return got == orkstackv1.Status_STATUS_AWAITING, got.String()
		})
		return id, child
	}

	t.Run("a composed run's records unwritable", func(t *testing.T) {
		s.t, s.addr = t, engineA
		id, child := composed()
		s.approve(child, "dana")
		s.approvedBy(id, "dana")
		for _, c := range []struct{ record, run, code string }{
			{"outputs.json", "child", "OUTPUTS_UNAVAILABLE"},
			{"composed.json", "parent", "CHILD_VALUES_UNSTORED"},
		} {
			id, child := composed()
			faults.set(func() { faults.suffix = c.record })
			s.approve(child, "dana")
			run := s.ended(id)
			faults.set(func() { faults.suffix = "" })
			ask := taskOf(t, run, "ask")
			got := ask.GetError().GetCode()
			if c.run == "child" {
				got = s.get("default", child).GetError().GetCode()
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || got != c.code {
				t.Fatalf("%s unwritable: the run ended %v, %s's reason %q (%v)", c.record, run.GetStatus(), c.run, got, ask.GetError())
			}
			if len(s.issued("default", id)) != 0 || strings.Contains(s.events(id)+s.events(child), "NOT_RECORDED") {
				t.Fatalf("%s unwritable: a token was issued, or the record was reported as missing", c.record)
			}
		}
	})

	t.Run("a child ended with outputs of somebody's choosing", func(t *testing.T) {
		s.t, s.addr = t, engineA
		id, child := composed()
		// Every engine still up: none may answer the child's next task.
		for _, p := range []launched{s.first, replicaB, replicaC} {
			select {
			case <-p.exited:
				continue
			default:
			}
			must.OK(t, p.process.Kill())
			<-p.exited
		}
		s.forgeEnd(child, map[string]any{"who": "forged-marker"})
		s.replica("ork-D")
		run := s.ended(id)
		if got := taskOf(t, run, "ask").GetError().GetCode(); run.GetStatus() != orkstackv1.Status_STATUS_FAILED || got != "CHILD_VALUES_NOT_RECORDED" {
			t.Fatalf("the run ended %v, its composed task %q", run.GetStatus(), got)
		}
		if a := taskOf(t, run, "apply"); a.GetStartedAt() != nil || len(s.issued("default", id)) != 0 {
			t.Fatalf("a dependent of a forged child started: %v", a)
		}
		if strings.Contains(s.events(id), "forged-marker") {
			t.Fatal("a forged output reached the parent's events")
		}
		if got := s.recorded(artifacts.AttestedPrefix, "default", id); slices.Contains(got, "ask/composed.json") {
			t.Fatalf("the engine recorded %v", got)
		}
		if got := s.recorded(artifacts.AttestedPrefix, "default", child); slices.Contains(got, "outputs.json") {
			t.Fatalf("the engine recorded the forged child's outputs: %v", got)
		}
	})
}

// forgeTask takes the next workflow task of run id, as whoever may write to the
// cluster's namespace may, and answers it with commands.
func (s *identityStack) forgeTask(id string, commands ...*commandpb.Command) {
	s.t.Helper()
	must.OK(s.t, s.temporal.SignalWorkflow(s.ctx, id, "", "wake", nil))
	of, err := describeExecution(s.temporal, s.ctx, id, "")
	must.OK(s.t, err)
	svc, queue := s.temporal.WorkflowService(), of.GetExecutionConfig().GetTaskQueue().GetName()
	for s.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
		task, err := svc.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{Namespace: s.namespace,
			TaskQueue: &taskqueue.TaskQueue{Name: queue, Kind: enums.TASK_QUEUE_KIND_NORMAL}, Identity: "cluster-writer"})
		cancel()
		if err != nil {
			select {
			case <-s.ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		if task.GetWorkflowExecution().GetWorkflowId() != id {
			continue
		}
		_, err = svc.RespondWorkflowTaskCompleted(s.ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{TaskToken: task.GetTaskToken(),
			Namespace: s.namespace, Identity: "cluster-writer", Commands: commands})
		must.OK(s.t, err)
		return
	}
	s.t.Fatal(s.ctx.Err())
}

// historyOf is the events of run id's current execution.
func (s *identityStack) historyOf(id string) []*historypb.HistoryEvent {
	s.t.Helper()
	var out []*historypb.HistoryEvent
	for it := s.temporal.GetWorkflowHistory(s.ctx, id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT); it.HasNext(); {
		e, err := it.Next()
		must.OK(s.t, err)
		out = append(out, e)
	}
	return out
}

// activityFailures is how run id's activities failed.
func (s *identityStack) activityFailures(id string) []string {
	s.t.Helper()
	var out []string
	for _, e := range s.historyOf(id) {
		if f := e.GetActivityTaskFailedEventAttributes().GetFailure(); f != nil {
			out = append(out, f.GetMessage())
		}
	}
	// One still pending is not in the history yet.
	d, err := describeExecution(s.temporal, s.ctx, id, "")
	must.OK(s.t, err)
	for _, p := range d.GetPendingActivities() {
		if f := p.GetLastFailure(); f != nil {
			out = append(out, f.GetMessage())
		}
	}
	return out
}

// forgeDispatch schedules task of run id with the input run like gave its own,
// made id's.
func (s *identityStack) forgeDispatch(id, like, task string) {
	s.t.Helper()
	plan := func(run string) string {
		start := s.historyOf(run)[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
		var p engine.RunPlan
		must.OK(s.t, engine.DataConverter().FromPayload(start[3], &p))
		return p.Identity.AdmissionSHA256
	}
	var like1 *historypb.ActivityTaskScheduledEventAttributes
	for _, e := range s.historyOf(like) {
		if a := e.GetActivityTaskScheduledEventAttributes(); a.GetActivityId() == "ork/"+task {
			like1 = a
		}
	}
	if like1 == nil {
		s.t.Fatalf("run %s scheduled no %s", like, task)
	}
	in := &orkstackv1.ExecuteTaskInput{}
	must.OK(s.t, engine.DataConverter().FromPayloads(like1.GetInput(), &in))
	raw, err := protojson.Marshal(in)
	must.OK(s.t, err)
	made := strings.NewReplacer(like, id, plan(like), plan(id)).Replace(string(raw))
	in = &orkstackv1.ExecuteTaskInput{}
	must.OK(s.t, protojson.Unmarshal([]byte(made), in))
	input, err := engine.DataConverter().ToPayloads(in)
	must.OK(s.t, err)
	s.forgeTask(id, &commandpb.Command{CommandType: enums.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId: like1.GetActivityId(), ActivityType: like1.GetActivityType(), TaskQueue: like1.GetTaskQueue(), Header: like1.GetHeader(), Input: input,
			ScheduleToCloseTimeout: like1.GetScheduleToCloseTimeout(), ScheduleToStartTimeout: like1.GetScheduleToStartTimeout(),
			StartToCloseTimeout: like1.GetStartToCloseTimeout(), HeartbeatTimeout: like1.GetHeartbeatTimeout(), RetryPolicy: like1.GetRetryPolicy()}}})
}

// onlyTimers fails unless run id's history holds no timer but of these lengths,
// one of them at least, and no failed workflow task: nothing of it waits for a
// record.
func (s *identityStack) onlyTimers(id string, lengths ...time.Duration) {
	s.t.Helper()
	found := 0
	for _, e := range s.historyOf(id) {
		if a := e.GetTimerStartedEventAttributes(); a != nil {
			if !slices.Contains(lengths, a.GetStartToFireTimeout().AsDuration()) {
				s.t.Errorf("run %s started a timer of %s, want only %v", id, a.GetStartToFireTimeout().AsDuration(), lengths)
			}
			found++
		}
		if f := e.GetWorkflowTaskFailedEventAttributes(); f != nil && f.GetCause() != enums.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND {
			s.t.Errorf("run %s failed a workflow task: %v %s", id, f.GetCause(), f.GetFailure().GetMessage())
		}
	}
	if found == 0 {
		s.t.Errorf("run %s started no timer, not even its approval's deadline", id)
	}
}

// forgeEnd ends the run id of a deployment with no engine, as whoever may
// write to the cluster's namespace may: it answers the run's next workflow
// task with a success that returns outputs.
func (s *identityStack) forgeEnd(id string, outputs map[string]any) {
	s.t.Helper()
	fields, err := structpb.NewStruct(outputs)
	must.OK(s.t, err)
	result, err := engine.DataConverter().ToPayloads(&orkstackv1.Run{Id: id, Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Output: &orkstackv1.Payload{Data: fields}})
	must.OK(s.t, err)
	s.forgeTask(id, &commandpb.Command{CommandType: enums.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{Result: result}}})
}
