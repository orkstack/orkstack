package dsl_test

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// chainKind is what each link of a test chain is.
type chainKind string

const (
	plainLink    chainKind = "plain"
	awaitLink    chainKind = "await"
	composedLink chainKind = "composed"
)

// writeLink writes one task of a chain document: id, needing need (none
// when empty), of kind, with an optional join.
func writeLink(b *strings.Builder, id, need string, kind chainKind, join string) {
	fmt.Fprintf(b, "  - id: %s\n", id)
	switch kind {
	case awaitLink:
		b.WriteString("    await: {timeout_seconds: 60}\n")
	case composedLink:
		b.WriteString("    uses: orkflow:child@1.0.0\n")
	default:
		b.WriteString("    uses: shell\n")
	}
	if need != "" {
		fmt.Fprintf(b, "    needs: [%s]\n", need)
	}
	if join != "" {
		fmt.Fprintf(b, "    join: %s\n", join)
	}
}

// chainDocument is a default-join chain of n tasks of kind, c0 … c(n−1),
// each needing the one before.
func chainDocument(n int, kind chainKind) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: chain\ntasks:\n")
	for i := range n {
		need := ""
		if i > 0 {
			need = fmt.Sprintf("c%d", i-1)
		}
		writeLink(&b, fmt.Sprintf("c%d", i), need, kind, "")
	}
	return b.String()
}

// explicitMinimalInput is the input rule 44 measures for id, built by hand
// from dsl.RequiredAncestors and dsl.MinimalUpstreamRef, so a test checks
// dsl.MinimalInputBytes against task.PayloadSize independently.
func explicitMinimalInput(t *testing.T, wf *orkstackv1.Orkflow, id string) int64 {
	t.Helper()
	byID := map[string]*orkstackv1.Task{}
	for _, task := range wf.GetTasks() {
		byID[task.GetId()] = task
	}
	direct := map[string]bool{}
	for _, need := range byID[id].GetNeeds() {
		direct[need] = true
	}
	in := &orkstackv1.ExecuteTaskInput{TaskId: id}
	for _, a := range dsl.RequiredAncestors(wf)[id] {
		kind := dsl.RefPlain
		switch {
		case byID[a].GetEach() != nil:
			if !direct[a] {
				continue
			}
			kind = dsl.RefFanOut
		case byID[a].GetAwait() != nil, byID[a].GetOrkflow() != nil:
			kind = dsl.RefNoCommit
		}
		in.Upstream = append(in.Upstream, dsl.MinimalUpstreamRef(kind, wf.GetNamespace(), a))
	}
	size, err := task.PayloadSize(in)
	if err != nil {
		t.Fatalf("measure %s's input: %v", id, err)
	}
	return size
}

// refusalDepth is the smallest default-join chain of kind whose last task
// rule 44 refuses: found by measuring chains, never by asking the rule.
func refusalDepth(t *testing.T, kind chainKind) int {
	t.Helper()
	low, high := 1, 4096
	for low < high {
		mid := (low + high) / 2
		wf := chainProto(mid, kind)
		size, _, err := dsl.MinimalInputBytes(wf, fmt.Sprintf("c%d", mid-1))
		if err != nil {
			t.Fatalf("measure: %v", err)
		}
		if size > task.MaxActivityInputBytes {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low
}

// chainProto is chainDocument's compiled shape, built by hand: rule 44 reads
// only ids, needs, joins and kinds, and a hand-built chain spares the
// search the compiler's own quadratic closures.
func chainProto(n int, kind chainKind) *orkstackv1.Orkflow {
	wf := &orkstackv1.Orkflow{Name: "chain"}
	for i := range n {
		task := &orkstackv1.Task{Id: fmt.Sprintf("c%d", i), Uses: "shell"}
		switch kind {
		case awaitLink:
			task.Uses, task.Await = "", &orkstackv1.AwaitSpec{TimeoutSeconds: 60}
		case composedLink:
			task.Orkflow = &orkstackv1.OrkflowRef{Name: "child", Version: "1.0.0"}
		}
		if i > 0 {
			task.Needs = []string{fmt.Sprintf("c%d", i-1)}
		}
		wf.Tasks = append(wf.Tasks, task)
	}
	return wf
}

// requireAncestryRefusal checks rule 44's refusal of task id at index with
// count required ancestors.
func requireAncestryRefusal(t *testing.T, err error, index int, id string, count int) {
	t.Helper()
	if !errors.Is(err, dsl.ErrAncestryTooLarge) {
		t.Fatalf("compile = %v, want ErrAncestryTooLarge", err)
	}
	var field *dsl.ValidationError
	if !errors.As(err, &field) || field.Field != fmt.Sprintf("tasks[%d].needs", index) {
		t.Fatalf("refusal %v anchored at %v, want tasks[%d].needs", err, field, index)
	}
	for _, want := range []string{
		fmt.Sprintf("task %q has %d required ancestors", id, count),
		fmt.Sprintf("over the %d-byte activity input bound", task.MaxActivityInputBytes),
		"Split the pipeline into composed Orkflows",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not say %q", err, want)
		}
	}
}

// TestValidate_MinimalInputBytesIsThePayloadSizeOfTheMinimalInput: the
// rule's arithmetic (one marginal per reference) is exactly what
// task.PayloadSize says of the input built by hand, for every task of a
// definition mixing every kind.
func TestValidate_MinimalInputBytesIsThePayloadSizeOfTheMinimalInput(t *testing.T) {
	wf := compile(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: mixed
namespace: payments
tasks:
  - id: a
    uses: shell
  - id: gate
    await: {timeout_seconds: 60}
    needs: [a]
  - id: child
    uses: orkflow:child@1.0.0
    needs: [gate]
  - id: list
    uses: shell
  - id: spread
    uses: shell
    needs: [list, child]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 5}
  - id: direct
    uses: shell
    needs: [spread]
  - id: indirect
    uses: shell
    needs: [direct]
  - id: soft
    uses: shell
    needs: [indirect]
    join: none_failed
  - id: after
    uses: shell
    needs: [soft, a]
`)
	for _, task := range wf.GetTasks() {
		got, _, err := dsl.MinimalInputBytes(wf, task.GetId())
		if err != nil {
			t.Fatalf("%s: %v", task.GetId(), err)
		}
		if want := explicitMinimalInput(t, wf, task.GetId()); got != want {
			t.Fatalf("%s: MinimalInputBytes = %d, PayloadSize of the minimal input = %d", task.GetId(), got, want)
		}
	}
}

// TestValidate_AncestryThatCannotFitIsRefused: the shortest default-join
// chain of plain tasks whose last task's minimal input crosses 256 KiB is
// refused at compile, naming that task and its count.
func TestValidate_AncestryThatCannotFitIsRefused(t *testing.T) {
	depth := refusalDepth(t, plainLink)
	_, err := dsl.Compile([]byte(chainDocument(depth, plainLink)))
	requireAncestryRefusal(t, err, depth-1, fmt.Sprintf("c%d", depth-1), depth-1)
}

// TestValidate_AncestryThatFitsIsAccepted: one task shorter, it compiles.
func TestValidate_AncestryThatFitsIsAccepted(t *testing.T) {
	depth := refusalDepth(t, plainLink)
	if _, err := dsl.Compile([]byte(chainDocument(depth-1, plainLink))); err != nil {
		t.Fatalf("a chain of %d plain tasks = %v, want it accepted", depth-1, err)
	}
}

// twoChainsDocument is two default-join chains a and b of n plain tasks each,
// then a task z needing both last links with join (default when empty).
func twoChainsDocument(n int, join string) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: two-chains\ntasks:\n")
	for _, lane := range []string{"a", "b"} {
		for i := range n {
			need := ""
			if i > 0 {
				need = fmt.Sprintf("%s%d", lane, i-1)
			}
			writeLink(&b, fmt.Sprintf("%s%d", lane, i), need, plainLink, "")
		}
	}
	writeLink(&b, "z", fmt.Sprintf("a%d, b%d", n-1, n-1), plainLink, join)
	return b.String()
}

// TestValidate_JoinEdgesStopTheCount: two chains each well inside the bound,
// together over it. A default-join task needing both is refused; the same
// task with any join policy counts none of its needs, and is accepted.
func TestValidate_JoinEdgesStopTheCount(t *testing.T) {
	depth := refusalDepth(t, plainLink)
	n := depth*2/3 + 1 // 2n > depth > n
	_, err := dsl.Compile([]byte(twoChainsDocument(n, "")))
	requireAncestryRefusal(t, err, 2*n, "z", 2*n)
	for _, join := range []string{"none_failed", "any_failed", "all_done"} {
		if _, err := dsl.Compile([]byte(twoChainsDocument(n, join))); err != nil {
			t.Fatalf("join %s = %v, want it accepted: a join policy counts none of its needs", join, err)
		}
	}
}

// TestValidate_SkippableAncestorsAreNotCounted: the ancestors behind a join
// policy add nothing to a task after it — the join task itself, a required
// need, is counted — and a failure handler's ancestors add nothing.
func TestValidate_SkippableAncestorsAreNotCounted(t *testing.T) {
	depth := refusalDepth(t, plainLink)
	n := depth*2/3 + 1
	// a0 … a(n−1), then j (none_failed) needing a(n−1), then b0 … b(n−1)
	// after j: the last b requires b0 … b(n−2) and j, never an a.
	behind := func(join string) string {
		var b strings.Builder
		b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: behind\ntasks:\n")
		for i := range n {
			need := ""
			if i > 0 {
				need = fmt.Sprintf("a%d", i-1)
			}
			writeLink(&b, fmt.Sprintf("a%d", i), need, plainLink, "")
		}
		writeLink(&b, "j", fmt.Sprintf("a%d", n-1), plainLink, join)
		for i := range n {
			need := "j"
			if i > 0 {
				need = fmt.Sprintf("b%d", i-1)
			}
			writeLink(&b, fmt.Sprintf("b%d", i), need, plainLink, "")
		}
		return b.String()
	}
	wf := compile(t, behind("none_failed"))
	last := fmt.Sprintf("b%d", n-1)
	if _, count, err := dsl.MinimalInputBytes(wf, last); err != nil || count != n {
		t.Fatalf("%s requires %d ancestors (%v), want %d: b0 … b%d and the join task j", last, count, err, n, n-2)
	}
	// With a default join at j, the a chain is required through it: the
	// first b to cross is refused, counting the a chain, j and the b's
	// before it.
	_, err := dsl.Compile([]byte(behind("")))
	match := regexp.MustCompile(`task "b(\d+)" has (\d+) required ancestors`).FindStringSubmatch(fmt.Sprint(err))
	if match == nil {
		t.Fatalf("the same chain with a default join at j = %v, want a b task refused", err)
	}
	k, _ := strconv.Atoi(match[1])
	requireAncestryRefusal(t, err, n+1+k, "b"+match[1], n+1+k)

	// A failure handler after a chain over the bound would be refused as a
	// default-join task; as a handler it counts nothing.
	handler := func(join string) string {
		var b strings.Builder
		b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: handler\ntasks:\n")
		for i := range depth {
			need := ""
			if i > 0 {
				need = fmt.Sprintf("c%d", i-1)
			}
			if i == depth-1 {
				writeLink(&b, fmt.Sprintf("c%d", i), need, plainLink, join)
				continue
			}
			writeLink(&b, fmt.Sprintf("c%d", i), need, plainLink, "")
		}
		return b.String()
	}
	for _, join := range []string{"any_failed", "all_done"} {
		wf := compile(t, handler(join))
		if _, count, err := dsl.MinimalInputBytes(wf, fmt.Sprintf("c%d", depth-1)); err != nil || count != 0 {
			t.Fatalf("a %s handler requires %d ancestors (%v), want none", join, count, err)
		}
	}
}

// TestValidate_IndirectFanOutAddsNoRef: a fan-out is one reference on a
// direct dependant's input and none further down, though it is required on
// both (its own needs stay required through it).
func TestValidate_IndirectFanOutAddsNoRef(t *testing.T) {
	wf := compile(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: fan
tasks:
  - id: list
    uses: shell
  - id: spread
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 5}
  - id: direct
    uses: shell
    needs: [spread]
  - id: indirect
    uses: shell
    needs: [direct]
`)
	required := dsl.RequiredAncestors(wf)
	if got := strings.Join(required["indirect"], ","); got != "list,spread,direct" {
		t.Fatalf("indirect requires %q, want list,spread,direct", got)
	}
	in := func(id string, refs ...*orkstackv1.UpstreamRef) int64 {
		size, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{TaskId: id, Upstream: refs})
		if err != nil {
			t.Fatal(err)
		}
		return size
	}
	plain := func(id string) *orkstackv1.UpstreamRef { return dsl.MinimalUpstreamRef(dsl.RefPlain, "", id) }
	fan := dsl.MinimalUpstreamRef(dsl.RefFanOut, "", "spread")
	for _, tc := range []struct {
		id   string
		want int64
	}{
		{"direct", in("direct", plain("list"), fan)},
		{"indirect", in("indirect", plain("list"), plain("direct"))},
	} {
		got, _, err := dsl.MinimalInputBytes(wf, tc.id)
		if err != nil || got != tc.want {
			t.Fatalf("%s: %d (%v), want %d", tc.id, got, err, tc.want)
		}
	}
}

// TestValidate_MinimalRefSize computes, and quotes, each kind's minimal
// reference, the default-join chain depth at which rule 44 refuses, a
// real-shaped reference's size and the depth at which a real deployment's
// dispatch fails INPUT_TOO_LARGE (spec 7.1; E7 quotes these figures). The
// minimal commit uri is exactly the one the engine composes, with an empty
// store root and the shortest run id; a real reference of each field is at
// least as long.
func TestValidate_MinimalRefSize(t *testing.T) {
	const ns = "default"
	key, err := artifacts.CommitKey(ns, "ork-0-0", "c1")
	if err != nil {
		t.Fatal(err)
	}
	minimal := dsl.MinimalUpstreamRef(dsl.RefPlain, ns, "c1")
	if minimal.GetCommitUri() != "/"+key {
		t.Fatalf("minimal commit uri %q, want the engine's %q with an empty store root", minimal.GetCommitUri(), "/"+key)
	}
	marginal := func(ref *orkstackv1.UpstreamRef) int64 {
		one, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{Upstream: []*orkstackv1.UpstreamRef{ref}})
		if err != nil {
			t.Fatal(err)
		}
		two, err := task.PayloadSize(&orkstackv1.ExecuteTaskInput{Upstream: []*orkstackv1.UpstreamRef{ref, ref}})
		if err != nil {
			t.Fatal(err)
		}
		return two - one
	}
	plain := marginal(minimal)
	noCommit := marginal(dsl.MinimalUpstreamRef(dsl.RefNoCommit, ns, "c1"))
	fanOut := marginal(dsl.MinimalUpstreamRef(dsl.RefFanOut, ns, "c1"))

	// A real reference of a Bash-fixture task: a store root, a real run id,
	// a content-addressed values.json and one artifact, as the Orker
	// records them.
	runID := "ork-1760550000123456789-8589934593"
	realKey, err := artifacts.CommitKey(ns, runID, "c1")
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("7f", 32)
	object := func(name string) string { // the S3 store's provenance key
		return "s3://orkstack-artifacts/orkflows/chain/" + runID + "/c1/" + name + "/" + sha
	}
	realRef := &orkstackv1.UpstreamRef{
		TaskId:    "c1",
		CommitUri: "s3://orkstack-artifacts/" + realKey,
		Values: &orkstackv1.ArtifactRef{Name: "values.json", Uri: object("values.json"),
			ContentType: "application/json", SizeBytes: 42, Sha256: sha},
		Artifacts: []*orkstackv1.ArtifactRef{{Name: "report.bin", Uri: object("report.bin"),
			ContentType: "application/octet-stream", SizeBytes: 6, Sha256: sha}},
	}
	real := marginal(realRef)
	if real < plain {
		t.Fatalf("a real reference (%d B) is smaller than the minimal one (%d B)", real, plain)
	}
	depth := refusalDepth(t, plainLink)
	runtimeDepth := int((task.MaxActivityInputBytes - 64) / real)
	t.Logf("minimal upstream reference: plain task %d B, await or composed task %d B, fan-out %d B", plain, noCommit, fanOut)
	t.Logf("rule 44 refuses a default-join chain of plain tasks at %d tasks (%d required ancestors)", depth, depth-1)
	t.Logf("a real-shaped reference is %d B, so dispatch fails INPUT_TOO_LARGE at about %d required ancestors", real, runtimeDepth)
	// Bands around the spec's estimates (7.1: about 200 B, 1,200, 664 B and
	// 385); docs/DSL.md rule 44 quotes the measured figures.
	for _, check := range []struct {
		name          string
		got, low, max int64
	}{
		{"minimal plain reference", plain, 150, 250},
		{"refusal depth", int64(depth), 1000, 1700},
		{"real reference", real, 500, 900},
		{"runtime depth", int64(runtimeDepth), 280, 500},
	} {
		if check.got < check.low || check.got > check.max {
			t.Fatalf("%s = %d, want about the spec's figure (%d–%d)", check.name, check.got, check.low, check.max)
		}
	}
}

// TestValidate_AwaitAndComposedAncestorsCountNoCommitURI: a default-join
// chain of awaits, and one of composed tasks, long enough that a commit uri
// per ancestor would cross the bound and their real references do not, are
// accepted.
func TestValidate_AwaitAndComposedAncestorsCountNoCommitURI(t *testing.T) {
	plainDepth := refusalDepth(t, plainLink)
	for _, kind := range []chainKind{awaitLink, composedLink} {
		depth := refusalDepth(t, kind)
		if depth <= plainDepth+1 {
			t.Fatalf("a %s chain is refused at %d, a plain one at %d: want the shorter references to admit more", kind, depth, plainDepth)
		}
		// Past the plain chain's refusal depth, inside its own.
		n := plainDepth + 1
		if _, err := dsl.Compile([]byte(chainDocument(n, kind))); err != nil {
			t.Fatalf("a chain of %d %s tasks = %v, want it accepted (refused only at %d)", n, kind, err, depth)
		}
		t.Logf("a default-join chain of %s tasks is refused at %d tasks, plain at %d", kind, depth, plainDepth)
	}
}
