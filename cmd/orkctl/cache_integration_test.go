//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The task-caching CLI scenarios (docs/DSL.md "Caching"): real
// engines, a real Orker, real Temporal and MinIO, and the BUILT orkctl, over
// examples/cached-pipeline.ork.yaml applied under a stamped name.

func cachedCLIDoc(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "examples", "cached-pipeline.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Replace(string(raw), "name: cached-pipeline", "name: "+name, 1)
}

func cachedCLIName() string {
	return fmt.Sprintf("cached-cli-%d-%d", os.Getpid(), time.Now().UnixNano())
}

// cachedRun submits name through the CLI and waits for it to succeed.
func cachedRun(ctx context.Context, t *testing.T, s *timingStack, name string, extra ...string) *orkstackv1.Run {
	t.Helper()
	runID := s.submit(name, extra...)
	run := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s: %v", runID, run.GetStatus(), run.GetError())
	}
	return run
}

func cacheOutcomeOf(run *orkstackv1.Run, task string) orkstackv1.CacheOutcome {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == task {
			return tr.GetCache().GetOutcome()
		}
	}
	return orkstackv1.CacheOutcome_CACHE_OUTCOME_UNSPECIFIED
}

// cacheStatsDoc is the subset of `cache stats --json` these tests read.
type cacheStatsDoc struct {
	Usage []struct {
		OrkflowName string `json:"orkflowName"`
		TaskID      string `json:"taskId"`
		Hits        string `json:"hits"`
		Misses      string `json:"misses"`
	} `json:"usage"`
	Inventory []struct {
		OrkflowName string `json:"orkflowName"`
		TaskID      string `json:"taskId"`
		Memos       string `json:"memos"`
	} `json:"inventory"`
	RunsInWindow string `json:"runsInWindow"`
}

// awaitCacheStats polls `cache stats --json --orkflow name` on one replica
// until the archive holds at least runs finished runs, by deadline.
func awaitCacheStats(ctx context.Context, t *testing.T, s *timingStack, replica int, name string, runs int) cacheStatsDoc {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var doc cacheStatsDoc
	for time.Now().Before(deadline) {
		stdout, _, code := s.cli(replica, "--json", "cache", "stats", "--orkflow", name, "--since", "1h")
		if code == 0 && json.Unmarshal([]byte(stdout), &doc) == nil && doc.RunsInWindow == fmt.Sprint(runs) {
			return doc
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	t.Fatalf("cache stats never counted %d runs of %s: %+v", runs, name, doc)
	return doc
}

func usageOf(doc cacheStatsDoc, task string) (hits, misses string) {
	for _, row := range doc.Usage {
		if row.TaskID == task {
			return row.Hits, row.Misses
		}
	}
	return "", ""
}

func TestIntegration_CLICacheStatsAndPurge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	s.startOrker()
	name := cachedCLIName()
	s.apply(cachedCLIDoc(t, name))
	first := cachedRun(ctx, t, s, name)
	second := cachedRun(ctx, t, s, name)
	if cacheOutcomeOf(second, "featurize") != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		t.Fatalf("run 2's featurize = %v", cacheOutcomeOf(second, "featurize"))
	}
	got := s.mustCLI(0, "run", "get", second.GetId())
	for _, want := range []string{"SUCCEEDED (cached)", "featurize: cache hit — reused run " + first.GetId(), "train: cache miss"} {
		if !strings.Contains(got, want) {
			t.Fatalf("run get lacks %q:\n%s", want, got)
		}
	}
	doc := awaitCacheStats(ctx, t, s, 0, name, 2)
	if hits, misses := usageOf(doc, "featurize"); hits != "1" || misses != "1" {
		t.Fatalf("featurize usage = %s hits, %s misses", hits, misses)
	}
	if hits, misses := usageOf(doc, "train"); hits != "" && hits != "0" || misses != "2" {
		t.Fatalf("train usage = %s hits, %s misses", hits, misses)
	}
	human := s.mustCLI(0, "cache", "stats", "--orkflow", name, "--since", "1h")
	if !strings.Contains(human, "featurize") || !strings.Contains(human, "MEMOS") {
		t.Fatalf("cache stats:\n%s", human)
	}
	purged := s.mustCLI(0, "cache", "purge", "--orkflow", name, "--task", "featurize", "--yes")
	if !strings.Contains(purged, "Purged 1 memo") {
		t.Fatalf("cache purge:\n%s", purged)
	}
	third := cachedRun(ctx, t, s, name)
	if cacheOutcomeOf(third, "featurize") != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS {
		t.Fatalf("after a purge, featurize = %v", cacheOutcomeOf(third, "featurize"))
	}
	s.writeTranscript("cache-C1.txt")
}

// Replica B answers for runs replica A started.
func TestIntegration_CacheReadsOnASecondReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 2)
	s.startOrker()
	name := cachedCLIName()
	s.apply(cachedCLIDoc(t, name))
	awaitOrkflowVisible(ctx, t, s.replicas[1].flows, name)
	cachedRun(ctx, t, s, name)
	second := cachedRun(ctx, t, s, name)
	awaitTerminalOnB(ctx, t, s.replicas[1], second.GetId())
	got := s.mustCLI(1, "run", "get", second.GetId())
	if !strings.Contains(got, "featurize: cache hit") {
		t.Fatalf("replica B's run get lacks the cache line:\n%s", got)
	}
	doc := awaitCacheStats(ctx, t, s, 1, name, 2)
	if hits, _ := usageOf(doc, "featurize"); hits != "1" {
		t.Fatalf("replica B's featurize hits = %q", hits)
	}
	// featurize's one memo and train's two (stamp moves train's key every run).
	purged := s.mustCLI(1, "cache", "purge", "--orkflow", name, "--yes")
	if !strings.Contains(purged, "Purged 3 memos") {
		t.Fatalf("replica B's purge:\n%s", purged)
	}
	s.writeTranscript("cache-C2.txt")
}

func TestIntegration_CLIRunSubmitAndAgainNoCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTimingStack(ctx, t, 1)
	s.startOrker()
	name := cachedCLIName()
	s.apply(cachedCLIDoc(t, name))
	first := cachedRun(ctx, t, s, name)
	bypassed := cachedRun(ctx, t, s, name, "--no-cache")
	if cacheOutcomeOf(bypassed, "featurize") != orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED {
		t.Fatalf("run submit --no-cache: featurize = %v", cacheOutcomeOf(bypassed, "featurize"))
	}
	stdout := s.mustCLI(0, "run", "again", first.GetId(), "--no-cache")
	again := awaitTerminalOnB(ctx, t, s.replicas[0], parseRunID(t, stdout))
	if cacheOutcomeOf(again, "featurize") != orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED {
		t.Fatalf("run again --no-cache: featurize = %v", cacheOutcomeOf(again, "featurize"))
	}
	if got := s.mustCLI(0, "run", "get", again.GetId()); !strings.Contains(got, "featurize: cache bypassed (submitted with --no-cache)") {
		t.Fatalf("run get:\n%s", got)
	}
	s.writeTranscript("cache-C3.txt")
}
