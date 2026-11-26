package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// verifyFixture is a run with two kept tasks and a failed one over real
// in-memory commits and a local artifact store.
type verifyFixture struct {
	in      ResumePlanInput
	commits *artifacts.MemoryCommitStore
	store   *artifacts.LocalStore
}

func newVerifyFixture(t *testing.T) *verifyFixture {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newPlan([]*orkstackv1.Task{ptask("a"), eachTask("fan", "a", "a"), ptask("x", "fan")},
		prow("a", sOK), fanRow("fan", sFail, pitem(0, sOK), pitem(1, sFail)), prow("x", sCanc))
	f.in.Definition.DefinitionDigest = "sha256:def"
	f.in.Evidence = ResumeEvidence{}
	return &verifyFixture{in: f.in, commits: artifacts.NewMemoryCommitStore(), store: store}
}

const verifyRoot = "mem://root"

// commit writes unit u's commit, as change leaves it, and answers its uri.
func (f *verifyFixture) commit(t *testing.T, u ResumeUnit, change func(*orkstackv1.ActivityCommit)) string {
	t.Helper()
	var def *orkstackv1.Task
	for _, d := range f.in.Definition.GetTasks() {
		if d.GetId() == u.Task {
			def = d
		}
	}
	id, err := ExpectedCommitIdentity(f.in.Definition, f.in.Base.GetId(), def, int32(u.Item), 0)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := f.store.Put(context.Background(), "values.json", strings.NewReader(`{"n":1}`), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c := &orkstackv1.ActivityCommit{
		SchemaVersion: artifacts.CommitSchemaVersion, Namespace: id.Namespace, RunId: id.RunID, TaskId: id.TaskID,
		OrkflowDefinitionDigest: id.DefinitionDigest, IdempotencyKey: id.IdempotencyKey, Image: &orkstackv1.ImageRef{IndexDigest: id.ImageIndexDigest},
		Values:    &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes},
		Artifacts: []*orkstackv1.ArtifactRef{{Name: "out", Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}},
		StartedAt: timestamppb.Now(), FinishedAt: timestamppb.Now(),
	}
	if change != nil {
		change(c)
	}
	uri := verifyRoot + "/" + id.Key
	if _, err := f.commits.Create(context.Background(), uri, c); err != nil {
		t.Fatal(err)
	}
	return uri
}

func (f *verifyFixture) verify(t *testing.T) ResumeVerification {
	t.Helper()
	got, err := NewResumeVerifier(f.commits, f.store).Verify(context.Background(), f.in, verifyRoot)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestResumeVerifier_Answers: each kept task and item is asked about, and
// nothing else; what is there is this unit's and whole, or the unit reruns.
func TestResumeVerifier_Answers(t *testing.T) {
	a, item := nodeUnit("a"), ResumeUnit{Task: "fan", Item: 0}
	for name, tc := range map[string]struct {
		set  func(t *testing.T, f *verifyFixture)
		want KeptAnswer // of a; the item is always valid
		why  string
	}{
		"valid":     {func(t *testing.T, f *verifyFixture) { f.commit(t, a, nil) }, KeptValid, ""},
		"no commit": {func(*testing.T, *verifyFixture) {}, KeptGone, "gone"},
		"another unit's": {func(t *testing.T, f *verifyFixture) {
			f.commit(t, a, func(c *orkstackv1.ActivityCommit) { c.TaskId = "z" })
		}, KeptGone, "different activity"},
		"undecodable": {func(t *testing.T, f *verifyFixture) {
			f.commit(t, a, func(c *orkstackv1.ActivityCommit) { c.SchemaVersion = 99 })
		}, KeptGone, "can't be decoded"},
		"an artifact gone": {func(t *testing.T, f *verifyFixture) {
			f.commit(t, a, func(c *orkstackv1.ActivityCommit) {
				c.Artifacts[0].Uri = strings.Replace(c.Artifacts[0].Uri, c.Artifacts[0].Sha256[:8], "00000000", 1)
			})
		}, KeptGone, "out"},
		"an artifact cut short": {func(t *testing.T, f *verifyFixture) {
			f.commit(t, a, func(c *orkstackv1.ActivityCommit) { c.Artifacts[0].SizeBytes++ })
		}, KeptGone, "out"},
		"a result too large": {func(t *testing.T, f *verifyFixture) {
			f.commit(t, a, func(c *orkstackv1.ActivityCommit) {
				for range 900 {
					c.Artifacts = append(c.Artifacts, &orkstackv1.ArtifactRef{Name: strings.Repeat("n", 100), Uri: c.Artifacts[0].Uri, Sha256: c.Artifacts[0].Sha256})
				}
			})
		}, KeptValid, ""},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVerifyFixture(t)
			tc.set(t, f)
			f.commit(t, item, nil)
			got := f.verify(t)
			if len(got.Evidence.Kept) != 2 {
				t.Fatalf("asked about %v, want a and fan[0]", got.Evidence.Kept)
			}
			if got.Evidence.Kept[a] != tc.want || got.Evidence.Kept[item] != KeptValid {
				t.Fatalf("answers = %v (%v), want a %v", got.Evidence.Kept, got.Evidence.Why, tc.want)
			}
			if !strings.Contains(got.Evidence.Why[a], tc.why) {
				t.Fatalf("why = %q, want %q in it", got.Evidence.Why[a], tc.why)
			}
			if w := got.Evidence.Why[a]; strings.Contains(w, "://") || strings.Contains(w, "commit") || strings.Contains(w, "schema_version") {
				t.Fatalf("why = %q, which a caller is shown, names the store's own terms", w)
			}
			if (tc.want != KeptValid) != (got.Causes[a] != nil) {
				t.Fatalf("cause = %v, want one for the log of what is not valid", got.Causes[a])
			}
			if got.Sizes[item] <= 0 || (tc.want == KeptValid) != (got.Sizes[a] > 0) {
				t.Fatalf("sizes = %v", got.Sizes)
			}
			if large := name == "a result too large"; large != (got.Sizes[a] > MaxReadbackEntryBytes()) {
				t.Fatalf("size %d against the largest entry %d", got.Sizes[a], MaxReadbackEntryBytes())
			}
			// What verification measures is what the read-back encodes.
			if _, err := PlanResume(withEvidence(f.in, got.Evidence)); err != nil {
				t.Fatalf("the evidence does not plan: %v", err)
			}
		})
	}
}

func withEvidence(in ResumePlanInput, e ResumeEvidence) ResumePlanInput {
	in.Evidence = e
	return in
}

// unansweringStats is an artifact store that answers nothing: the outage no
// real store produces on demand.
type unansweringStats struct{ *artifacts.LocalStore }

func (unansweringStats) Stat(context.Context, string) (artifacts.ArtifactRef, error) {
	return artifacts.ArtifactRef{}, errors.New("connection refused")
}

// TestResumeVerifier_Unanswered: a store that does not answer leaves a unit
// unavailable, never gone: nothing reruns for an outage.
func TestResumeVerifier_Unanswered(t *testing.T) {
	f := newVerifyFixture(t)
	f.commit(t, nodeUnit("a"), nil)
	f.commit(t, ResumeUnit{Task: "fan", Item: 0}, nil)
	for name, v := range map[string]*ResumeVerifier{
		"commits":   NewResumeVerifier(unansweringCommits{}, f.store),
		"artifacts": NewResumeVerifier(f.commits, unansweringStats{f.store}),
	} {
		got, err := v.withTries(1).Verify(context.Background(), f.in, verifyRoot)
		if err != nil || got.Evidence.Kept[nodeUnit("a")] != KeptUnavailable || got.Evidence.Why[nodeUnit("a")] == "" {
			t.Errorf("%s: answers = %v, %v; want unavailable, with why", name, got.Evidence, err)
		}
		if w := got.Evidence.Why[nodeUnit("a")]; strings.Contains(w, "refused") || strings.Contains(w, "://") || got.Causes[nodeUnit("a")] == nil {
			t.Errorf("%s: why = %q, cause = %v; want the store's error in the cause alone", name, w, got.Causes[nodeUnit("a")])
		}
	}
	if _, err := NewResumeVerifier(f.commits, f.store).Verify(context.Background(), f.in, ""); err == nil {
		t.Error("a run with no commit root verified")
	}
}

// TestResumeVerifier_ValuesOfAwaitsAndComposedTasks: a kept await's or
// composed task's values object is asked for; one with no values has
// nothing to lose.
func TestResumeVerifier_ValuesOfAwaitsAndComposedTasks(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{awaitTask("w"), composedTask("c"), composedTask("none"), ptask("x")},
		prow("w", sOK), prow("c", sOK), prow("none", sOK), prow("x", sFail))
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), "values.json", strings.NewReader(`{}`), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f.in.Base.TaskRuns[0].ValuesRef = &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256}
	f.in.Base.TaskRuns[1].ValuesRef = &orkstackv1.ArtifactRef{Uri: strings.Replace(ref.URI, ref.SHA256[:8], "00000000", 1)}
	f.in.Evidence = ResumeEvidence{}
	got, err := NewResumeVerifier(artifacts.NewMemoryCommitStore(), store).Verify(context.Background(), f.in, verifyRoot)
	if err != nil {
		t.Fatal(err)
	}
	if k := got.Evidence.Kept; k[nodeUnit("w")] != KeptValid || k[nodeUnit("c")] != KeptGone || k[nodeUnit("none")] != KeptGone {
		t.Fatalf("answers = %v (%v)", k, got.Evidence.Why)
	}
	if got.Sizes[nodeUnit("w")] < 2 || got.Sizes[nodeUnit("c")] != 0 {
		t.Fatalf("sizes = %v, want the await's entry measured", got.Sizes)
	}
}

// A composed task's values are read back whole, and its artifacts are asked
// for: one over a call's budget cannot be kept, and an await's that large is
// kept by reference.
func TestResumeVerifier_MeasuresSettledValues(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	put := func(body string) *orkstackv1.ArtifactRef {
		ref, err := store.Put(context.Background(), "values.json", strings.NewReader(body), artifacts.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return &orkstackv1.ArtifactRef{Name: "values.json", Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}
	}
	large := put(`{"a":"` + strings.Repeat("x", 120<<10) + `"}`)
	f := newPlan([]*orkstackv1.Task{awaitTask("w"), composedTask("small"), composedTask("big"), composedTask("lost"), ptask("x")},
		prow("w", sOK), prow("small", sOK), prow("big", sOK), prow("lost", sOK), prow("x", sFail))
	for i, ref := range []*orkstackv1.ArtifactRef{large, put(`{"ok":true}`), large, put(`{}`)} {
		f.in.Base.TaskRuns[i].ValuesRef = ref
	}
	f.in.Base.TaskRuns[3].Artifacts = []*orkstackv1.ArtifactRef{{Name: "report", Uri: strings.Replace(large.Uri, large.Sha256[:8], "00000000", 1)}}
	f.in.Evidence = ResumeEvidence{}
	got, err := NewResumeVerifier(artifacts.NewMemoryCommitStore(), store).Verify(context.Background(), f.in, verifyRoot)
	if err != nil {
		t.Fatal(err)
	}
	size := func(task string) int64 { return got.Sizes[nodeUnit(task)] }
	if size("w") <= 0 || size("w") > 4096 || size("small") <= 0 || size("big") <= MaxReadbackEntryBytes() {
		t.Fatalf("sizes = %v, want the await kept by reference and the large composed task over a call", got.Sizes)
	}
	if got.Evidence.Kept[nodeUnit("lost")] != KeptGone || got.Evidence.Why[nodeUnit("lost")] != "its report is gone" {
		t.Fatalf("lost = %v (%s), want its report gone", got.Evidence.Kept[nodeUnit("lost")], got.Evidence.Why[nodeUnit("lost")])
	}
}
