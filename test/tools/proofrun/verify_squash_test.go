package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// squashRepo is a repository whose main has a tip, and the branches a change
// lands from, as AGENTS.md §11.3(b) finds them.
type squashRepo struct {
	t   *testing.T
	dir string
}

func newSquashRepo(t *testing.T) *squashRepo {
	t.Helper()
	r := &squashRepo{t: t, dir: t.TempDir()}
	r.git("init", "-q", "-b", "main")
	r.commit("a", "1", "chore: the first commit")
	r.commit("b", "1", "chore: the tip of main")
	return r
}

func (r *squashRepo) git(args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = r.dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (r *squashRepo) commit(name, text, message string) {
	r.t.Helper()
	must.OK(r.t, os.WriteFile(filepath.Join(r.dir, name), []byte(text), 0o644))
	r.git("add", ".")
	r.git("commit", "-q", "-m", message)
}

// branch makes a feature branch from main with two commits on it, the second
// of which fixes the first: the history a squash folds away.
func (r *squashRepo) branch(name string) {
	r.t.Helper()
	r.git("checkout", "-q", "-b", name, "main")
	r.commit(name, "draft", "wip")
	r.commit(name, "done", "fixup the draft")
	r.git("checkout", "-q", "main")
}

// squashOne builds the candidate for one branch: git merge --squash, then one
// Conventional Commit, on a branch from main's tip.
func (r *squashRepo) squashOne(name, subject string) {
	r.t.Helper()
	r.git("checkout", "-q", "-b", "candidate", "main")
	r.git("merge", "-q", "--squash", name)
	r.git("commit", "-q", "-m", subject)
}

// proof is a merged CI full proof of tree over main's tip, in a directory
// outside the worktree (proof/ is gitignored in the real one).
func (r *squashRepo) proof(tree string) string {
	r.t.Helper()
	return ciMerged(r.t, tierFull, tree, r.git("rev-parse", "main"))
}

func TestVerify_ASquashCandidateIsMergeEvidence(t *testing.T) {
	r := newSquashRepo(t)
	r.branch("feature")
	mainTip, featureTree := r.git("rev-parse", "main"), r.git("rev-parse", "feature^{tree}")
	r.squashOne("feature", "feat(engine): add a thing")

	if got := r.git("rev-parse", "HEAD^{tree}"); got != featureTree {
		t.Fatalf("the squash commit's tree is %s, and the branch's is %s: a squash on main's tip must keep the tree", got, featureTree)
	}
	if got := strings.Fields(r.git("show", "-s", "--format=%P", "HEAD")); len(got) != 1 || got[0] != mainTip {
		t.Fatalf("the squash commit's parents are %v, want main's tip %s alone", got, mainTip)
	}
	proof := r.proof(featureTree)
	t.Chdir(r.dir)
	quiet(t)
	if code := cmdVerify([]string{"-proof", proof}); code != 0 {
		t.Fatalf("verify refused a squash commit on main's tip with the proven tree: exit %d", code)
	}

	// Its body quotes the proof, as §11.3(b) says: the amend moves the commit
	// and never the tree, which is what the proof is bound to.
	before := r.git("rev-parse", "HEAD")
	r.git("commit", "-q", "--amend", "-m", "feat(engine): add a thing\n\nProof: proof/ci-1 tree "+featureTree)
	if r.git("rev-parse", "HEAD") == before {
		t.Fatal("the amend did not move the commit")
	}
	if code := cmdVerify([]string{"-proof", proof}); code != 0 {
		t.Errorf("verify refused the same tree once its body quoted the proof: exit %d", code)
	}

	// What main's tip then does: a fast-forward to exactly that commit.
	head := r.git("rev-parse", "HEAD")
	r.git("checkout", "-q", "main")
	r.git("merge", "-q", "--ff-only", "candidate")
	if got := r.git("rev-parse", "main"); got != head {
		t.Errorf("main is %s, want the squash commit %s", got, head)
	}
}

// Several branches proven together land as one commit: the proof taken on
// their merge is bound to its tree, and the squash of that merge keeps it.
func TestVerify_SeveralBranchesProvenTogetherLandAsOneCommit(t *testing.T) {
	r := newSquashRepo(t)
	r.branch("one")
	r.branch("two")
	mainTip := r.git("rev-parse", "main")
	r.git("checkout", "-q", "-b", "candidate", "main")
	r.git("merge", "-q", "--no-ff", "-m", "chore(merge): one", "one")
	r.git("merge", "-q", "--no-ff", "-m", "chore(merge): two", "two")
	mergedTree := r.git("rev-parse", "HEAD^{tree}")
	proof := r.proof(mergedTree)

	r.git("reset", "-q", "--soft", "main")
	r.git("commit", "-q", "-m", "feat: one and two")

	if got := r.git("rev-parse", "HEAD^{tree}"); got != mergedTree {
		t.Fatalf("the squash of the merge has tree %s, and the proof measured %s", got, mergedTree)
	}
	if got := strings.Fields(r.git("show", "-s", "--format=%P", "HEAD")); len(got) != 1 || got[0] != mainTip {
		t.Fatalf("the landed commit's parents are %v, want main's tip %s alone", got, mainTip)
	}
	t.Chdir(r.dir)
	quiet(t)
	if code := cmdVerify([]string{"-proof", proof}); code != 0 {
		t.Errorf("verify refused the squash of a proven merge: exit %d", code)
	}
}

func TestVerify_ASquashCandidateIsRefusedWhenItIsNotWhatWasProven(t *testing.T) {
	setup := func(t *testing.T) (*squashRepo, string) {
		t.Helper()
		r := newSquashRepo(t)
		r.branch("feature")
		r.squashOne("feature", "feat(engine): add a thing")
		return r, r.proof(r.git("rev-parse", "HEAD^{tree}"))
	}
	for _, c := range []struct {
		name   string
		mutate func(r *squashRepo)
	}{
		{"its tree changed after the proof", func(r *squashRepo) {
			r.commit("late", "x", "chore: a late change")
			// Back to one commit on main's tip, so only the tree is wrong.
			r.git("reset", "-q", "--soft", "main")
			r.git("commit", "-q", "-m", "feat(engine): add a thing")
		}},
		{"main moved under it", func(r *squashRepo) {
			r.git("checkout", "-q", "main")
			r.commit("other", "x", "chore: main moved")
			r.git("checkout", "-q", "candidate")
		}},
		{"its parent is not main's tip", func(r *squashRepo) {
			// The proven tree on the wrong parent: the branch's tip.
			tree := r.git("rev-parse", "HEAD^{tree}")
			c := r.git("commit-tree", tree, "-p", "feature", "-m", "feat(engine): add a thing")
			r.git("reset", "-q", "--hard", c)
		}},
		{"it is a merge commit of the proven tree", func(r *squashRepo) {
			tree := r.git("rev-parse", "HEAD^{tree}")
			c := r.git("commit-tree", tree, "-p", "main", "-p", "feature", "-m", "feat(engine): add a thing")
			r.git("reset", "-q", "--hard", c)
		}},
		{"its subject is no Conventional Commit", func(r *squashRepo) {
			r.git("commit", "-q", "--amend", "-m", "Squashed the feature")
		}},
		{"the worktree is dirty", func(r *squashRepo) {
			must.OK(r.t, os.WriteFile(filepath.Join(r.dir, "untracked"), []byte("x"), 0o644))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, proof := setup(t)
			t.Chdir(r.dir)
			quiet(t)
			if code := cmdVerify([]string{"-proof", proof}); code != 0 {
				t.Fatalf("the unmutated candidate was refused: exit %d", code)
			}
			c.mutate(r)
			if code := cmdVerify([]string{"-proof", proof}); code != 1 {
				t.Errorf("verify exited %d, want 1", code)
			}
		})
	}
}

func TestCandidateCommitProblems(t *testing.T) {
	for _, c := range []struct {
		name    string
		parents []string
		subject string
		want    []string
	}{
		{"one commit with a good subject", []string{"m1"}, "feat(engine): add a thing", nil},
		{"a subject GitHub suffixed", []string{"m1"}, "fix: repair a thing (#12)", nil},
		{"a merge commit", []string{"m1", "b1"}, "chore(merge): a train", []string{"HEAD has 2 parents", "git reset --soft main"}},
		{"a bad subject", []string{"m1"}, "Squashed", []string{"HEAD's subject", "must read type(scope)!: description", "git commit --amend"}},
		{"both", []string{"m1", "b1"}, "Merge branch 'x'", []string{"HEAD has 2 parents", "HEAD's subject"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := strings.Join(candidateCommitProblems(c.parents, c.subject, "main"), "\n")
			if len(c.want) == 0 && p != "" {
				t.Fatalf("refused: %s", p)
			}
			for _, w := range c.want {
				if !strings.Contains(p, w) {
					t.Errorf("problems %q lack %q", p, w)
				}
			}
		})
	}
}
