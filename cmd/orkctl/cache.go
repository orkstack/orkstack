package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/dsl"
)

// Task caching on the command line (docs/DSL.md "Caching"): `run get`'s cache
// lines, `run attempts`' CACHE column, `orkflow get`'s CACHED table, and the
// `cache stats` / `cache purge` pair.

func (g *globalFlags) cacheClient() orkstackv1connect.CacheServiceClient {
	return orkstackv1connect.NewCacheServiceClient(g.httpClient(), g.baseURL(), g.clientOptions()...)
}

// missReasonSentences is one short clause per miss token, for `run get`'s
// cache line. A token this build has never heard of prints the Orker's own
// sentence instead.
var missReasonSentences = map[string]string{
	"MEMO_ABSENT":               "no memo for this key",
	"MEMO_UNREADABLE":           "the memo could not be read",
	"MEMO_MISMATCH":             "the memo did not describe this task",
	"MEMO_EXPIRED":              "the memoized result is older than the task's TTL",
	"SOURCE_COMMIT_GONE":        "the run that produced the memoized result was retired",
	"SOURCE_COMMIT_CHANGED":     "the memoized commit changed after it was memoized",
	"SOURCE_COMMIT_UNREADABLE":  "the memoized commit could not be read",
	"SOURCE_COMMIT_MISMATCH":    "the memoized commit did not match this task",
	"SOURCE_OVER_LIMITS":        "the memoized result does not fit this run's limits",
	"SOURCE_OBJECT_UNAVAILABLE": "an object of the memoized result is gone",
	"INPUT_UNHASHED":            "an input carries no digest, so no key could be computed",
	"MEMO_STORE_ABSENT":         "the Orker has no memo store",
}

// missSentence is the clause for a miss: the token's sentence, else the
// Orker's detail.
func missSentence(c *orkstackv1.CacheResult) string {
	if sentence, ok := missReasonSentences[c.GetMissReason()]; ok {
		return sentence
	}
	return c.GetMissDetail()
}

// clockDuration spells a span as 8m02s / 1h04m16s / 42s.
func clockDuration(d time.Duration) string {
	d = d.Round(time.Second)
	hours := d / time.Hour
	minutes := (d % time.Hour) / time.Minute
	seconds := (d % time.Minute) / time.Second
	switch {
	case hours > 0:
		return fmt.Sprintf("%dh%02dm%02ds", hours, minutes, seconds)
	case minutes > 0:
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}

// memoWriteClause is how a fresh run's line ends: whether its memo was
// written, could not be, or — for an adopted result — is not known.
func memoWriteClause(c *orkstackv1.CacheResult, written string) string {
	switch {
	case c.GetMemoWritten():
		return "the container ran and " + written
	case c.GetMemoWriteError() != "":
		return "the container ran but its memo could not be written: " + c.GetMemoWriteError()
	default:
		return "the container ran"
	}
}

// cacheLine is `run get`'s line for one task whose cache is set.
func cacheLine(namespace string, task *orkstackv1.TaskRun) string {
	c := task.GetCache()
	switch c.GetOutcome() {
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT:
		source := c.GetSource()
		took := ""
		if s, f := source.GetStartedAt(), source.GetFinishedAt(); s != nil && f != nil && !f.AsTime().Before(s.AsTime()) {
			took = fmt.Sprintf(" (its attempt took %s)", clockDuration(f.AsTime().Sub(s.AsTime())))
		}
		return fmt.Sprintf("%s: cache hit — reused run %s's result, committed %s%s; no container ran. Open it with: %s",
			task.GetTaskId(), source.GetRunId(), formatTime(source.GetFinishedAt()), took,
			orkctlCommand(namespace, "run", "get", source.GetRunId()))
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS:
		return fmt.Sprintf("%s: cache miss (%s: %s); %s", task.GetTaskId(), c.GetMissReason(), missSentence(c),
			memoWriteClause(c, "its memo was written"))
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED:
		return fmt.Sprintf("%s: cache bypassed (submitted with --no-cache); %s", task.GetTaskId(),
			memoWriteClause(c, "its memo was refreshed"))
	}
	return ""
}

// renderCacheLines prints one line per task whose cache is set, in
// declaration order, after a blank line; nothing when no task carries one.
func renderCacheLines(out io.Writer, run *orkstackv1.Run) {
	first := true
	for _, task := range run.GetTaskRuns() {
		line := cacheLine(run.GetNamespace(), task)
		if line == "" {
			continue
		}
		if first {
			fmt.Fprintln(out)
			first = false
		}
		fmt.Fprintln(out, line)
	}
}

// attemptCacheCell is `run attempts`' CACHE cell.
func attemptCacheCell(c *orkstackv1.CacheResult) string {
	switch c.GetOutcome() {
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT:
		return "hit " + c.GetSource().GetRunId()
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS:
		return "miss " + c.GetMissReason()
	case orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED:
		return "bypassed"
	}
	return dash
}

// cacheTTLLabel is a CacheSpec's TTL for a human: "24h", or "deployment cap"
// when the task set none.
func cacheTTLLabel(spec *orkstackv1.CacheSpec) string {
	if spec.GetTtlSeconds() <= 0 {
		return "deployment cap"
	}
	return shortDuration(time.Duration(spec.GetTtlSeconds()) * time.Second)
}

// renderCachedTable is `orkflow get`'s CACHED table, when any task caches.
func renderCachedTable(out io.Writer, tasks []*orkstackv1.Task) error {
	any := false
	for _, t := range tasks {
		if t.GetCache() != nil {
			any = true
		}
	}
	if !any {
		return nil
	}
	fmt.Fprintln(out)
	table := newTable(out)
	fmt.Fprintln(table, "CACHED\tTTL\tSCOPE")
	for _, t := range tasks {
		if spec := t.GetCache(); spec != nil {
			fmt.Fprintf(table, "%s\t%s\t%s\n", t.GetId(), cacheTTLLabel(spec), orDash(dsl.CacheScopeName(spec.GetScope())))
		}
	}
	if err := table.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// cachePlanDetail is `run plan`'s DETAIL for a cached task.
func cachePlanDetail(spec *orkstackv1.CacheSpec) string {
	ttl := "ttl " + cacheTTLLabel(spec)
	if spec.GetTtlSeconds() <= 0 {
		ttl = "ttl: the deployment's cap"
	}
	return fmt.Sprintf("cached across runs (%s, %s scope); whether it reuses a result is decided by the Orker when the run reaches it",
		ttl, orDash(dsl.CacheScopeName(spec.GetScope())))
}

func newCacheCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Read and trim the memos cached tasks reuse",
		Long: "A cached task (cache: in an Orkflow) reuses the result of an identical earlier\n" +
			"run through a memo. Memos are advisory: purging them changes no result, only\n" +
			"whether the next run of a cached task runs its container.",
	}
	cmd.AddCommand(newCacheStatsCommand(g), newCachePurgeCommand(g))
	return cmd
}

func newCacheStatsCommand(g *globalFlags) *cobra.Command {
	var since, until, orkflow string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Hit rate per cached task over a window, and the memos stored now",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req := &orkstackv1.GetCacheStatsRequest{Namespace: g.namespace, OrkflowName: orkflow}
			if since != "" {
				window, at, err := parseSince(since)
				if err != nil {
					return err
				}
				if window > 0 {
					req.Lookback = durationpb.New(window)
				} else {
					req.Since = timestamppb.New(at)
				}
			}
			if until != "" {
				at, err := parseUntil(until)
				if err != nil {
					return err
				}
				req.Until = timestamppb.New(at)
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			resp, err := g.cacheClient().GetCacheStats(ctx, connect.NewRequest(req))
			if err != nil {
				return rpcError("get cache stats", err)
			}
			if g.json {
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			return renderCacheStats(cmd.OutOrStdout(), resp.Msg)
		},
	}
	cmd.Flags().StringVar(&orkflow, "orkflow", "", "only this orkflow's runs and orkflow-scoped memos")
	cmd.Flags().StringVar(&since, "since", "",
		"how far back to look: a duration such as 24h, or an RFC3339 instant (default: the last 24 hours)")
	cmd.Flags().StringVar(&until, "until", "", "the window's exclusive end, an RFC3339 instant (default: now)")
	return cmd
}

// renderCacheStats writes the human report.
func renderCacheStats(out io.Writer, resp *orkstackv1.GetCacheStatsResponse) error {
	header := newTable(out)
	fmt.Fprintf(header, "Namespace:\t%s\n", orDash(resp.GetNamespace()))
	fmt.Fprintf(header, "Window:\t%s to %s\n", formatTime(resp.GetWindowSince()), formatTime(resp.GetWindowUntil()))
	fmt.Fprintf(header, "Computed:\t%s\n", formatTime(resp.GetComputedAt()))
	if err := header.Flush(); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	fmt.Fprintln(out)
	if len(resp.GetUsage()) == 0 {
		fmt.Fprintln(out, "No cached task finished in this window.")
	} else {
		table := newTable(out)
		fmt.Fprintln(table, "ORKFLOW\tTASK\tHITS\tMISSES\tBYPASSED\tHIT RATE\tREUSED")
		for _, row := range resp.GetUsage() {
			rate := "unavailable"
			if row.GetHitRateDefined() {
				rate = fmt.Sprintf("%.1f%%", row.GetHitRate()*100)
			}
			reused := dash
			if d := row.GetReused().AsDuration(); d > 0 {
				reused = clockDuration(d)
			}
			fmt.Fprintf(table, "%s\t%s\t%d\t%d\t%d\t%s\t%s\n", row.GetOrkflowName(), row.GetTaskId(),
				row.GetHits(), row.GetMisses(), row.GetBypassed(), rate, reused)
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	fmt.Fprintln(out)
	if len(resp.GetInventory()) == 0 {
		fmt.Fprintln(out, "No memos stored.")
	} else {
		fmt.Fprintln(out, "MEMOS")
		table := newTable(out)
		fmt.Fprintln(table, "SCOPE\tORKFLOW\tTASK\tMEMOS\tOLDEST\tNEWEST")
		for _, row := range resp.GetInventory() {
			fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\t%s\n", orDash(dsl.CacheScopeName(row.GetScope())),
				orDash(row.GetOrkflowName()), row.GetTaskId(), row.GetMemos(),
				formatTime(row.GetOldest()), formatTime(row.GetNewest()))
		}
		if err := table.Flush(); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	fmt.Fprintln(out)
	fmt.Fprintf(out, "scanned %d of %d finished runs in the window\n", resp.GetRunsScanned(), resp.GetRunsInWindow())
	fmt.Fprintln(out, "REUSED is the producing attempts' duration the hits did not spend again.")
	return nil
}

// errCachePurgeSelector is the client-side refusal of a purge without exactly
// one selector.
var errCachePurgeSelector = errors.New(
	"name exactly one of --orkflow, --namespace-scoped or --all — a purge never has a default scope")

func newCachePurgeCommand(g *globalFlags) *cobra.Command {
	var (
		orkflow, task, olderThan  string
		namespaceScoped, all, yes bool
	)
	cmd := &cobra.Command{
		Use:   "purge (--orkflow NAME | --namespace-scoped | --all) [--task ID] [--older-than DURATION]",
		Short: "Delete memos, so the next run of those cached tasks runs its container",
		Long: "purge deletes memos and nothing else: no run, commit or artifact is touched.\n" +
			"It changes no result — only whether the next run of a cached task runs its\n" +
			"container and writes a fresh memo. It never has a default scope.\n\n" +
			"It names the memos it will delete and asks for confirmation first, and\n" +
			"refuses when standard input is not a terminal unless --yes is given.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			selectors := 0
			for _, set := range []bool{orkflow != "", namespaceScoped, all} {
				if set {
					selectors++
				}
			}
			if selectors != 1 {
				return errCachePurgeSelector
			}
			if all && task != "" {
				return fmt.Errorf("--task narrows --orkflow or --namespace-scoped; it does not apply to --all")
			}
			req := &orkstackv1.PurgeCacheRequest{
				Namespace: g.namespace, OrkflowName: orkflow, NamespaceScoped: namespaceScoped, All: all, TaskId: task,
			}
			if olderThan != "" {
				d, err := time.ParseDuration(olderThan)
				if err != nil || d < 0 {
					return fmt.Errorf("invalid --older-than %q: want a Go duration such as 72h", olderThan)
				}
				req.OlderThan = durationpb.New(d)
			}
			if err := confirmDestructive(cmd, g, "purge cache memos", purgePrompt(req), yes); err != nil {
				return err
			}
			ctx, cancel := g.unaryContext(cmd)
			defer cancel()
			client := g.cacheClient()
			// --json is always exactly one response envelope: one call, and a
			// script loops on `more`.
			if g.json {
				resp, err := client.PurgeCache(ctx, connect.NewRequest(req))
				if err != nil {
					return rpcError("purge cache", err)
				}
				return writeJSON(cmd.OutOrStdout(), resp.Msg)
			}
			var deleted, kept int64
			namespace := g.namespace
			for {
				resp, err := client.PurgeCache(ctx, connect.NewRequest(req))
				if err != nil {
					if deleted > 0 {
						return rpcError(fmt.Sprintf("purge cache (after deleting %d memos)", deleted), err)
					}
					return rpcError("purge cache", err)
				}
				deleted += resp.Msg.GetDeleted()
				kept = resp.Msg.GetKept()
				namespace = resp.Msg.GetNamespace()
				if !resp.Msg.GetMore() {
					break
				}
			}
			return renderPurged(cmd.OutOrStdout(), req, namespace, deleted, kept)
		},
	}
	cmd.Flags().StringVar(&orkflow, "orkflow", "", "purge this orkflow's orkflow-scoped memos")
	cmd.Flags().BoolVar(&namespaceScoped, "namespace-scoped", false, "purge the namespace-scoped memos")
	cmd.Flags().BoolVar(&all, "all", false, "purge every memo of the namespace")
	cmd.Flags().StringVar(&task, "task", "", "narrow --orkflow or --namespace-scoped to one task")
	cmd.Flags().StringVar(&olderThan, "older-than", "", "keep memos written within this duration, e.g. 72h")
	addYesFlag(cmd, &yes)
	return cmd
}

// purgePrompt states which memos a purge deletes, before it is sent.
// PurgeCache has no count-only mode, so the scope is named rather than
// counted.
func purgePrompt(req *orkstackv1.PurgeCacheRequest) string {
	var scope string
	switch {
	case req.GetOrkflowName() != "":
		scope = fmt.Sprintf("the orkflow-scoped memos of orkflow %q", req.GetOrkflowName())
	case req.GetNamespaceScoped():
		scope = "the namespace-scoped memos"
	default:
		scope = "every memo"
	}
	if req.GetTaskId() != "" {
		scope += fmt.Sprintf(", task %q,", req.GetTaskId())
	}
	scope += fmt.Sprintf(" in namespace %q", displayNamespace(req.GetNamespace()))
	if d := req.GetOlderThan(); d != nil && d.AsDuration() > 0 {
		scope += ", except those written in the last " + shortDuration(d.AsDuration())
	}
	return "Purge " + scope + "?\n" +
		"No run, commit or artifact is touched; the next run of each of those cached tasks runs its container."
}

// renderPurged prints what a purge did.
func renderPurged(out io.Writer, req *orkstackv1.PurgeCacheRequest, namespace string, deleted, kept int64) error {
	var scope []string
	switch {
	case req.GetOrkflowName() != "":
		scope = append(scope, "orkflow "+req.GetOrkflowName())
	case req.GetNamespaceScoped():
		scope = append(scope, "namespace-scoped")
	case req.GetAll():
		scope = append(scope, "all")
	}
	if req.GetTaskId() != "" {
		scope = append(scope, "task "+req.GetTaskId())
	}
	noun := "memos"
	if deleted == 1 {
		noun = "memo"
	}
	age := ""
	if d := req.GetOlderThan(); d != nil && d.AsDuration() > 0 {
		age = " older than " + shortDuration(d.AsDuration())
	}
	line := fmt.Sprintf("Purged %d %s%s (%s) in namespace %s", deleted, noun, age, strings.Join(scope, ", "), orDash(namespace))
	if age != "" {
		line += fmt.Sprintf("; kept %d newer", kept)
	}
	line += "."
	fmt.Fprintln(out, line)
	fmt.Fprintln(out, "No run, commit or artifact was touched; the next run of these tasks runs its container and writes a fresh memo.")
	return nil
}
