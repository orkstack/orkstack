package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// integration-plan.json, format 2 (plan §2.2): one plan, sharded once. Every
// run writes one — a local proof is a one-shard plan — so the same file tells
// a shard what to run and the merger what every shard had to report.

const planFormat = 2

// Tiers a plan or a proof can carry.
const (
	tierFull    = "full"
	tierFeature = "feature"
	tierNightly = "nightly"
	tierRelease = "release"
)

// Nightly modes: include runs every listed test (the default, so
// scripts/integration.sh keeps running everything); defer leaves out the
// entries both the tree's and the base's lists carry; only runs the list.
const (
	nightlyInclude = "include"
	nightlyDefer   = "defer"
	nightlyOnly    = "only"
)

type fileRef struct {
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
	Source string `json:"source,omitempty"`
}

type nightlyRef struct {
	File        string   `json:"file"`
	SHA256      string   `json:"sha256"`
	Mode        string   `json:"mode"`
	Entries     []string `json:"entries"`
	BaseCommit  string   `json:"base_commit,omitempty"`
	BaseSHA256  string   `json:"base_sha256"`
	BaseEntries []string `json:"base_entries"`
}

// quarantineRef records the quarantine list's hashes. It is empty until a
// quarantine list exists; the merger must still see that it read the plan.
type quarantineRef struct {
	File       string `json:"file"`
	SHA256     string `json:"sha256"`
	BaseSHA256 string `json:"base_sha256"`
}

// listChange is one gate file's added and removed rows, carried into
// proof.json when a change edits a list that decides what runs (plan §6.4).
type listChange struct {
	File    string   `json:"file"`
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
}
type prRecord struct {
	Number       int    `json:"number"`
	HeadSHA      string `json:"head_sha"`
	BaseSHA      string `json:"base_sha"`
	ContainsBase bool   `json:"contains_base"`
}

type planPackage struct {
	ImportPath    string   `json:"import_path"`
	Dir           string   `json:"dir"`
	Binary        string   `json:"binary"`
	BinarySHA256  string   `json:"binary_sha256"`
	ListingSHA256 string   `json:"listing_sha256,omitempty"`
	Listed        []string `json:"listed"`
}

type testRef struct {
	Package string `json:"package"`
	Test    string `json:"test"`
}

type deferredRef struct {
	Package string `json:"package"`
	Test    string `json:"test"`
	Entry   string `json:"entry"`
}

type unmeasuredRef struct {
	Package string `json:"package"`
	Test    string `json:"test"`
	Kind    string `json:"kind"`
}

type planShard struct {
	Index             int      `json:"index"`
	Jobs              []string `json:"jobs"`
	EstimatedSeconds  float64  `json:"estimated_seconds"`
	EstimatedMakespan float64  `json:"estimated_makespan_seconds"`
}

type integrationPlan struct {
	Format                int             `json:"format"`
	Tree                  string          `json:"tree"`
	Commit                string          `json:"commit"`
	Tier                  string          `json:"tier"`
	ProofID               string          `json:"proof_id"`
	BaseRef               string          `json:"base_ref"`
	BaseCommit            string          `json:"base_commit"`
	MergeBase             string          `json:"merge_base"`
	ObservabilityRequired bool            `json:"observability_required"`
	PR                    *prRecord       `json:"pr,omitempty"`
	Costs                 fileRef         `json:"costs"`
	Nightly               nightlyRef      `json:"nightly"`
	AllowedSkips          fileRef         `json:"allowed_skips"`
	Quarantine            quarantineRef   `json:"quarantine"`
	PartsExpected         []string        `json:"parts_expected"`
	Packages              []planPackage   `json:"packages"`
	Expected              []testRef       `json:"expected"`
	Deferred              []deferredRef   `json:"deferred"`
	OutOfTier             int             `json:"out_of_tier"`
	Unmeasured            []unmeasuredRef `json:"unmeasured"`
	Jobs                  []job           `json:"jobs"`
	Shards                []planShard     `json:"shards"`
	RequestedShards       int             `json:"requested_shards"`
	SlotsPerShard         int             `json:"slots_per_shard"`
	Problems              []string        `json:"problems"`
	ListsChanged          []listChange    `json:"lists_changed"`
}

// readPlan reads a format-2 plan. A format-1 plan (a bare array of jobs,
// every proof before this one) is refused: it names no listing to check a
// shard against.
func readPlan(file string) (*integrationPlan, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	if t := bytes.TrimSpace(b); len(t) > 0 && t[0] == '[' {
		return nil, fmt.Errorf("%s is a format-1 plan (a bare list of jobs): it cannot drive a shard", file)
	}
	var p integrationPlan
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	if p.Format != planFormat {
		return nil, fmt.Errorf("%s: plan format %d, want %d", file, p.Format, planFormat)
	}
	for i := range p.Jobs {
		p.Jobs[i].Timeout = time.Duration(p.Jobs[i].TimeoutSeconds * float64(time.Second))
		p.Jobs[i].Attempt = 1
	}
	return &p, nil
}

func (p *integrationPlan) pkg(importPath string) *planPackage {
	for i := range p.Packages {
		if p.Packages[i].ImportPath == importPath {
			return &p.Packages[i]
		}
	}
	return nil
}

func (p *integrationPlan) listedSets() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, pk := range p.Packages {
		out[pk.ImportPath] = map[string]bool{}
		for _, t := range pk.Listed {
			out[pk.ImportPath][t] = true
		}
	}
	return out
}

// shardJobs is shard k's jobs, in plan order (longest first).
func (p *integrationPlan) shardJobs(k int) []job {
	var out []job
	for _, j := range p.Jobs {
		if j.Shard == k {
			out = append(out, j)
		}
	}
	return out
}

// partsExpected is every part a proof of this shape must leave a result for.
// A local proof is one part; a CI proof is the job graph of plan §3.2.
func partsExpected(tier string, local bool, shards, uiShards int, observability bool) []string {
	if local {
		return []string{"local"}
	}
	parts := []string{"plan", "build"}
	if tier != tierNightly {
		parts = append(parts, "fast")
	}
	for k := 1; k <= shards; k++ {
		parts = append(parts, fmt.Sprintf("integration:%d", k))
	}
	if tier == tierNightly {
		return parts
	}
	for k := 1; k <= uiShards; k++ {
		parts = append(parts, fmt.Sprintf("ui-e2e:%d", k))
	}
	for _, a := range fixtureActivities {
		parts = append(parts, "activity-fixture:"+a)
	}
	if observability || tier == tierRelease {
		parts = append(parts, "observability")
	}
	return parts
}

// fixtureActivities are the activities the contract fixture runs on, one CI
// part each (images/Makefile's contract target).
var fixtureActivities = []string{"shell", "json", "git", "template"}

// testKinds tells, for every test a package's _test.go files declare, whether
// it is declared in a file the unit build compiles ("unit") or only in an
// integration-tagged one ("integration"). An unmeasured integration test is
// costed conservatively; an unmeasured unit test (the integration binary
// lists those too) keeps the old default.
func testKinds(dir string) map[string]string {
	out := map[string]string{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return out
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		f := srcFile{Name: e.Name(), Content: b}
		kind := "integration"
		if fileMatches(cfgUnit, dir, f) {
			kind = "unit"
		}
		for _, t := range parseGo(f).Tests {
			if out[t] != "unit" {
				out[t] = kind
			}
		}
	}
	return out
}

// workingTreeHash is the tree of the working tree — staged and untracked
// files included, ignored ones excluded — through a temporary index, exactly
// as scripts/proof.sh's tree_hash computes it.
func workingTreeHash(repo string) (string, error) {
	idx, err := os.CreateTemp("", "proofrun-index-*")
	if err != nil {
		return "", err
	}
	name := idx.Name()
	_ = idx.Close()
	_ = os.Remove(name)
	defer os.Remove(name)
	run := func(args ...string) (string, error) {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(), "GIT_INDEX_FILE="+name)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
		}
		return strings.TrimSpace(string(out)), nil
	}
	if _, err := run("read-tree", "HEAD"); err != nil {
		return "", err
	}
	if _, err := run("add", "-A", "."); err != nil {
		return "", err
	}
	return run("write-tree")
}

// repoRoot is the top of the working tree the process runs in.
func repoRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("not in a git working tree: %v", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func sortedTestRefs(keys []testKey) []testRef {
	out := make([]testRef, 0, len(keys))
	for _, k := range keys {
		out = append(out, testRef{k.Package, k.Test})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Package != out[j].Package {
			return out[i].Package < out[j].Package
		}
		return out[i].Test < out[j].Test
	})
	return out
}
