package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/evidence"
)

// errEvidenceIncomplete is a stream that ended without `last`: not a bundle,
// and not a manifest.
var errEvidenceIncomplete = errors.New("evidence stream ended without completion")

func newRunEvidenceCommand(g *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "evidence",
		Short: "Export and verify a finished run's evidence bundle",
		Long: "A run's evidence bundle is one tar.gz holding everything the run recorded:\n" +
			"the archived run and events, the pinned definition's YAML, and per task\n" +
			"commit.json, values.json, every output artifact, every attempt record and\n" +
			"every attempt's stdout.log and stderr.log. Its first member, MANIFEST.json,\n" +
			"names each member's origin, size, sha256 and state. See docs/RUN_EVIDENCE.md.",
	}
	cmd.AddCommand(newRunEvidenceExportCommand(g), newRunEvidenceVerifyCommand(g))
	return cmd
}

func newRunEvidenceExportCommand(g *globalFlags) *cobra.Command {
	var task, output string
	var noLogs, noArtifacts, overwrite bool
	cmd := &cobra.Command{
		Use:   "export <run-id>",
		Short: "Write a finished run's evidence bundle to a file",
		Long: "Export assembles a finished run's evidence bundle on the engine and writes it\n" +
			"to --output. The file is committed only once the engine confirms a complete\n" +
			"bundle; an interrupted export leaves nothing behind.\n\n" +
			"--json writes no file: it prints MANIFEST.json itself — the exact document the\n" +
			"bundle would carry as its first member, not a proto envelope.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &orkstackv1.ExportRunEvidenceRequest{
				RunId: args[0], Namespace: g.namespace, TaskId: task,
				ExcludeLogs: noLogs, ExcludeArtifacts: noArtifacts,
			}
			if g.json {
				if output != "" {
					return fmt.Errorf("--json prints the manifest and writes no file")
				}
				document, err := fetchManifest(cmd, g, req)
				if err != nil {
					return err
				}
				_, err = cmd.OutOrStdout().Write(document)
				return err
			}
			if output == "" {
				return fmt.Errorf("--output is required (or pass --json to print the manifest)")
			}
			if err := safeOutputPath(output, overwrite); err != nil {
				return err
			}
			manifest, size, err := exportEvidenceFile(cmd, g, req, output, overwrite)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote %s: %s (%s)\n", output, evidenceCountsLine(manifest.Counts), formatBytes(size))
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "destination file (required unless --json)")
	cmd.Flags().StringVar(&task, "task", "", "narrow the per-task members to one task")
	cmd.Flags().BoolVar(&noLogs, "no-logs", false, "leave out every attempt's stdout.log and stderr.log")
	cmd.Flags().BoolVar(&noArtifacts, "no-artifacts", false, "leave out the tasks' output artifacts")
	cmd.Flags().BoolVar(&overwrite, "overwrite", false, "replace an existing destination file")
	return cmd
}

// fetchEvidence drains one ExportRunEvidence stream into sink. It succeeds
// only when `last` arrived: anything else is not a bundle and not a manifest.
func fetchEvidence(cmd *cobra.Command, g *globalFlags, req *orkstackv1.ExportRunEvidenceRequest, sink io.Writer) error {
	ctx, cancel := g.streamContext(cmd)
	defer cancel()
	stream, err := g.runClient().ExportRunEvidence(ctx, connect.NewRequest(req))
	if err != nil {
		return evidenceError(err)
	}
	defer stream.Close()
	last := false
	for stream.Receive() {
		chunk := stream.Msg()
		if data := chunk.GetData(); len(data) > 0 {
			if _, err := sink.Write(data); err != nil {
				return fmt.Errorf("write evidence: %w", err)
			}
		}
		if chunk.GetLast() {
			last = true
		}
	}
	if err := stream.Err(); err != nil {
		return evidenceError(err)
	}
	if !last {
		return errEvidenceIncomplete
	}
	return nil
}

// fetchManifest is a manifest_only export, collected whole.
func fetchManifest(cmd *cobra.Command, g *globalFlags, req *orkstackv1.ExportRunEvidenceRequest) ([]byte, error) {
	req.ManifestOnly = true
	var document bytes.Buffer
	if err := fetchEvidence(cmd, g, req, &document); err != nil {
		return nil, err
	}
	return document.Bytes(), nil
}

// exportEvidenceFile is `artifact download`'s commit loop: a sibling
// temporary, every chunk written, `last` required, a no-clobber link (or a
// rename under --overwrite), and the temporary removed on any failure.
func exportEvidenceFile(cmd *cobra.Command, g *globalFlags, req *orkstackv1.ExportRunEvidenceRequest, output string, overwrite bool) (*evidence.Manifest, int64, error) {
	tmp, err := os.CreateTemp(filepath.Dir(output), ".orkctl-evidence-*")
	if err != nil {
		return nil, 0, fmt.Errorf("create temporary bundle: %w", err)
	}
	tmpName := tmp.Name()
	complete := false
	defer func() {
		if !complete {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err := fetchEvidence(cmd, g, req, tmp); err != nil {
		return nil, 0, err
	}
	if err := tmp.Close(); err != nil {
		return nil, 0, fmt.Errorf("close bundle: %w", err)
	}
	// The summary describes the file on disk, read back from it — not a
	// second request.
	file, err := os.Open(tmpName)
	if err != nil {
		return nil, 0, fmt.Errorf("read bundle back: %w", err)
	}
	manifest, err := evidence.ReadManifest(file)
	info, statErr := file.Stat()
	_ = file.Close()
	if err != nil {
		return nil, 0, fmt.Errorf("read bundle back: %w", err)
	}
	if statErr != nil {
		return nil, 0, fmt.Errorf("read bundle back: %w", statErr)
	}
	if overwrite {
		if err := os.Rename(tmpName, output); err != nil {
			return nil, 0, fmt.Errorf("commit evidence bundle: %w", err)
		}
	} else {
		if err := os.Link(tmpName, output); err != nil {
			if os.IsExist(err) {
				return nil, 0, fmt.Errorf("output %q already exists (pass --overwrite to replace it)", output)
			}
			return nil, 0, fmt.Errorf("commit evidence bundle: %w", err)
		}
		if err := os.Remove(tmpName); err != nil {
			return nil, 0, fmt.Errorf("remove temporary bundle: %w", err)
		}
	}
	complete = true
	return manifest, info.Size(), nil
}

// evidenceError renders an export refusal. FailedPrecondition carries the
// server's own sentence (it names the missing store or the live run);
// everything else is rpcError's rendering.
func evidenceError(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeFailedPrecondition {
		return errors.New(connectErr.Message())
	}
	return rpcError("export run evidence", err)
}

// evidenceCountsLine is "41 present, 2 reclaimed, 1 unrecorded": present
// always, the others when non-zero.
func evidenceCountsLine(c evidence.Counts) string {
	parts := []string{fmt.Sprintf("%d present", c.Present)}
	for _, part := range []struct {
		n    int
		word string
	}{{c.Reclaimed, "reclaimed"}, {c.Unrecorded, "unrecorded"}, {c.Rejected, "rejected"}} {
		if part.n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", part.n, part.word))
		}
	}
	return strings.Join(parts, ", ")
}

func newRunEvidenceVerifyCommand(g *globalFlags) *cobra.Command {
	var againstStore bool
	cmd := &cobra.Command{
		Use:   "verify <bundle.tar.gz>",
		Short: "Check an evidence bundle's members against its MANIFEST.json",
		Long: "Verify hashes every member of a bundle and compares it with MANIFEST.json. It\n" +
			"needs no server, no credentials and writes nothing to disk. Reclaimed and\n" +
			"unrecorded members are findings, not failures; any member whose bytes do not\n" +
			"match is named and the command exits 1.\n\n" +
			"--against-store additionally asks the engine for the live manifest of the same\n" +
			"run and scope and compares the two member by member.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			file, err := os.Open(args[0])
			if err != nil {
				return fmt.Errorf("open bundle: %w", err)
			}
			report, err := evidence.Verify(file)
			_ = file.Close()
			if err != nil {
				return err
			}
			var store *storeComparison
			if againstStore && report.Manifest != nil {
				if cmd.Flags().Changed("namespace") && resolvedNamespace(g.namespace) != report.Manifest.Namespace {
					return fmt.Errorf("bundle is of namespace %q; -n names %q", report.Manifest.Namespace, g.namespace)
				}
				live, err := liveManifest(cmd, g, report.Manifest)
				if err != nil {
					return err
				}
				store = compareWithStore(report.Manifest, live)
			}
			out := cmd.OutOrStdout()
			if g.json {
				if err := writeVerifyJSON(out, report, store); err != nil {
					return err
				}
			} else {
				fmt.Fprint(out, renderVerifyReport(report))
				if store != nil {
					fmt.Fprint(out, renderStoreComparison(store))
				}
			}
			if !report.OK() || (store != nil && len(store.Failures) > 0) {
				return &exitCodeError{code: exitError, err: fmt.Errorf("evidence bundle %s does not match", args[0])}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&againstStore, "against-store", false, "also compare the bundle with the engine's live manifest of the run")
	return cmd
}

// resolvedNamespace is the wire spelling of the default namespace.
func resolvedNamespace(name string) string {
	if name == "" {
		return "default"
	}
	return name
}

// liveManifest asks the engine for the manifest of the bundle's run, in the
// bundle's namespace and scope.
func liveManifest(cmd *cobra.Command, g *globalFlags, m *evidence.Manifest) (*evidence.Manifest, error) {
	document, err := fetchManifest(cmd, g, &orkstackv1.ExportRunEvidenceRequest{
		RunId: m.RunID, Namespace: m.Namespace, TaskId: m.Scope.TaskID,
		ExcludeLogs: !m.Scope.Logs, ExcludeArtifacts: !m.Scope.Artifacts,
	})
	if err != nil {
		return nil, err
	}
	return evidence.ParseManifest(document)
}

// storeComparison is what --against-store found.
type storeComparison struct {
	Findings []evidence.Failure `json:"findings"`
	Failures []evidence.Failure `json:"failures"`
}

// compareWithStore compares the bundle's manifest with the live one. A run's
// recorded references are immutable, so only states may move.
func compareWithStore(file, live *evidence.Manifest) *storeComparison {
	c := &storeComparison{}
	if file.DefinitionDigest != live.DefinitionDigest {
		c.Failures = append(c.Failures, evidence.Failure{Path: "definition_digest", Reason: "store digest differs",
			Want: file.DefinitionDigest, Got: live.DefinitionDigest})
	}
	liveByPath := map[string]evidence.Member{}
	for _, m := range live.Members {
		liveByPath[m.Path] = m
	}
	fileByPath := map[string]bool{}
	for _, m := range file.Members {
		fileByPath[m.Path] = true
		now, ok := liveByPath[m.Path]
		if !ok {
			c.Failures = append(c.Failures, evidence.Failure{Path: m.Path, Reason: "member set differs", Want: "in bundle", Got: "not in store"})
			continue
		}
		if m.SHA256 != "" && now.SHA256 != "" && m.SHA256 != now.SHA256 {
			c.Failures = append(c.Failures, evidence.Failure{Path: m.Path, Reason: "store digest differs", Want: m.SHA256, Got: now.SHA256})
			continue
		}
		switch {
		case m.State == evidence.StatePresent && now.State == evidence.StateReclaimed:
			c.Findings = append(c.Findings, evidence.Failure{Path: m.Path, Reason: "reclaimed since export", Got: now.Reason})
		case m.State != evidence.StatePresent && now.State == evidence.StatePresent:
			c.Findings = append(c.Findings, evidence.Failure{Path: m.Path, Reason: "restored since export"})
		case m.State != now.State:
			c.Findings = append(c.Findings, evidence.Failure{Path: m.Path, Reason: fmt.Sprintf("%s since export", now.State), Got: now.Reason})
		}
	}
	for _, m := range live.Members {
		if !fileByPath[m.Path] {
			c.Failures = append(c.Failures, evidence.Failure{Path: m.Path, Reason: "member set differs", Want: "not in bundle", Got: "in store"})
		}
	}
	return c
}

func renderStoreComparison(c *storeComparison) string {
	var b strings.Builder
	b.WriteString("store:\n")
	if len(c.Findings) == 0 && len(c.Failures) == 0 {
		b.WriteString("  every member agrees with the store\n")
		return b.String()
	}
	for _, f := range c.Findings {
		line := fmt.Sprintf("  %s   %s", f.Path, f.Reason)
		if f.Got != "" {
			line += " (" + f.Got + ")"
		}
		b.WriteString(line + "\n")
	}
	if len(c.Failures) > 0 {
		fmt.Fprintf(&b, "FAILED: %s the store\n", pluralForms(len(c.Failures), "member disagrees with", "members disagree with"))
		writeFailures(&b, c.Failures)
	}
	return b.String()
}

func writeVerifyJSON(out io.Writer, report *evidence.Report, store *storeComparison) error {
	document := struct {
		*evidence.Report
		Store *storeComparison `json:"store,omitempty"`
	}{Report: report, Store: store}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	_, err = out.Write(append(encoded, '\n'))
	return err
}

// renderVerifyReport is the human verify report (docs/RUN_EVIDENCE.md,
// "verify").
func renderVerifyReport(r *evidence.Report) string {
	var b strings.Builder
	m := r.Manifest
	if m == nil {
		fmt.Fprintf(&b, "FAILED: the bundle cannot be checked\n")
		writeFailures(&b, r.Failures)
		return b.String()
	}
	fmt.Fprintf(&b, "verified %s against MANIFEST.json\n", pluralForms(len(r.Verified), "present member", "present members"))
	row := func(label, value, rest string) {
		line := fmt.Sprintf("  %-15s %s", label, value)
		if rest != "" {
			line = fmt.Sprintf("  %-15s %-23s %s", label, value, rest)
		}
		b.WriteString(strings.TrimRight(line, " ") + "\n")
	}
	row("run", m.RunID, "namespace "+m.Namespace)
	row("definition", m.DefinitionDigest, fmt.Sprintf("%s@%s (%s)", m.OrkflowName, m.OrkflowVersion, definitionState(m.Definition)))
	exported := fmt.Sprintf("exported %s by engine %s", m.ExportedAt.UTC().Format("2006-01-02T15:04:05Z07:00"), m.EngineVersion)
	if m.BuildID != "" {
		exported += " (build " + m.BuildID + ")"
	}
	row("status", m.RunStatus, exported)
	row("tasks", taskSummary(m.Tasks), "")
	if len(r.Reclaimed) > 0 {
		row("reclaimed", pluralForms(len(r.Reclaimed), "member", "members")+", recorded sha256 preserved:", "")
		width := 0
		for _, member := range r.Reclaimed {
			width = max(width, len(member.Path))
		}
		for _, member := range r.Reclaimed {
			fmt.Fprintf(&b, "                    %-*s  (%s)\n", width, member.Path, member.Reason)
		}
	}
	if len(r.Unrecorded) > 0 {
		names := make([]string, 0, len(r.Unrecorded))
		for _, member := range r.Unrecorded {
			names = append(names, fmt.Sprintf("%s attempt %d", member.TaskID, member.Attempt))
		}
		row("unrecorded", fmt.Sprintf("%s: %s (%s)", pluralForms(len(r.Unrecorded), "attempt", "attempts"),
			strings.Join(names, ", "), evidence.ReasonAttemptRecordMissing), "")
	}
	if len(r.Rejected) > 0 {
		paths := make([]string, 0, len(r.Rejected))
		for _, member := range r.Rejected {
			paths = append(paths, member.Path)
		}
		row("rejected", fmt.Sprintf("%s: %s", pluralForms(len(r.Rejected), "member", "members"), strings.Join(paths, ", ")), "")
	}
	tokens := reasonTokens(r)
	for i, token := range tokens {
		label := ""
		if i == 0 {
			label = "reasons"
		}
		row(label, token+": "+evidence.ReasonText(token), "")
	}
	if len(r.Failures) > 0 {
		fmt.Fprintf(&b, "FAILED: %s not match MANIFEST.json\n", pluralForms(len(r.Failures), "member does", "members do"))
		writeFailures(&b, r.Failures)
	}
	return b.String()
}

func writeFailures(b *strings.Builder, failures []evidence.Failure) {
	width, reasonWidth := 0, 0
	for _, f := range failures {
		width = max(width, len(f.Path))
		reasonWidth = max(reasonWidth, len(f.Reason))
	}
	for _, f := range failures {
		line := fmt.Sprintf("  %-*s   %-*s", width, f.Path, reasonWidth, f.Reason)
		if f.Want != "" || f.Got != "" {
			line += fmt.Sprintf("   want %s   got %s", f.Want, f.Got)
		}
		b.WriteString(strings.TrimRight(line, " ") + "\n")
	}
}

// reasonTokens is every distinct reason token among the absent members, in
// first-seen order.
func reasonTokens(r *evidence.Report) []string {
	var tokens []string
	seen := map[string]bool{}
	for _, list := range [][]evidence.Member{r.Reclaimed, r.Unrecorded, r.Rejected} {
		for _, member := range list {
			if member.Reason != "" && !seen[member.Reason] {
				seen[member.Reason] = true
				tokens = append(tokens, member.Reason)
			}
		}
	}
	return tokens
}

func definitionState(d evidence.Definition) string {
	switch d.State {
	case evidence.StatePresent:
		return "source.yaml present"
	case evidence.StateReclaimed:
		return "source.yaml reclaimed: definition deleted"
	case evidence.StateRejected:
		return fmt.Sprintf("source.yaml rejected: stored document hashes to %s, not the pinned digest", d.StoredDefinitionDigest)
	default:
		return "source.yaml " + string(d.State)
	}
}

// taskSummary is "6: 5 succeeded, 1 succeeded after 2 failed attempts".
func taskSummary(tasks []evidence.TaskEntry) string {
	succeeded := 0
	after := map[int32]int{}
	others := map[string]int{}
	for _, task := range tasks {
		switch {
		case task.Status == "SUCCEEDED" && task.OutcomeAttempt > 1:
			after[task.OutcomeAttempt-1]++
		case task.Status == "SUCCEEDED":
			succeeded++
		default:
			others[strings.ToLower(task.Status)]++
		}
	}
	var parts []string
	if succeeded > 0 {
		parts = append(parts, fmt.Sprintf("%d succeeded", succeeded))
	}
	failedCounts := make([]int32, 0, len(after))
	for n := range after {
		failedCounts = append(failedCounts, n)
	}
	sort.Slice(failedCounts, func(i, j int) bool { return failedCounts[i] < failedCounts[j] })
	for _, n := range failedCounts {
		parts = append(parts, fmt.Sprintf("%d succeeded after %s", after[n], pluralForms(int(n), "failed attempt", "failed attempts")))
	}
	for _, status := range []string{"failed", "canceled", "skipped"} {
		if others[status] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", others[status], status))
			delete(others, status)
		}
	}
	rest := make([]string, 0, len(others))
	for status := range others {
		rest = append(rest, status)
	}
	sort.Strings(rest)
	for _, status := range rest {
		parts = append(parts, fmt.Sprintf("%d %s", others[status], status))
	}
	if len(parts) == 0 {
		return fmt.Sprintf("%d", len(tasks))
	}
	return fmt.Sprintf("%d: %s", len(tasks), strings.Join(parts, ", "))
}

// pluralForms is "1 member" / "2 members", for nouns plural's "s" does not
// fit ("member does" / "members do").
func pluralForms(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}
