package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The §11 coverage map (AGENTS.md §11, docs/PROOFS.md §6): every claim of the
// required-coverage list is mapped to the evidence that proves it, and this
// file makes that map a checked contract instead of prose.
const (
	section11Map    = "../../../scripts/proof-section11-coverage.tsv"
	section11Agents = "../../../AGENTS.md"
	section11Doc    = "../../../docs/PROOFS.md"
)

// section11Item is one evidence item of a map row.
type section11Item struct {
	Kind    string // unit, integration, chaos or fixture
	Package string // for a test item
	Test    string // the test, /subtest allowed
	Script  string // for a fixture item
	Check   string // the fixture's check text
}

func (i section11Item) topTest() string {
	if cut := strings.Index(i.Test, "/"); cut >= 0 {
		return i.Test[:cut]
	}
	return i.Test
}

// section11Row is one parsed map row.
type section11Row struct {
	Bullet   int
	Claim    string
	Evidence []section11Item
	Gap      string
	Note     string
	Line     int
}

func (r section11Row) key() string {
	if fields := strings.Fields(r.Claim); len(fields) > 0 {
		return fields[0]
	}
	return ""
}

// parseSection11Map reads the TSV. Comments and blank lines are skipped.
func parseSection11Map(text string) ([]section11Row, error) {
	var rows []section11Row
	for n, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 5 {
			return nil, fmt.Errorf("map:%d: want \"bullet<TAB>claim<TAB>evidence<TAB>gap<TAB>note\", got %d columns", n+1, len(f))
		}
		bullet, err := strconv.Atoi(strings.TrimSpace(f[0]))
		if err != nil || bullet < 1 {
			return nil, fmt.Errorf("map:%d: bullet %q is not a positive integer", n+1, f[0])
		}
		evidence, err := parseSection11Evidence(f[2])
		if err != nil {
			return nil, fmt.Errorf("map:%d: %w", n+1, err)
		}
		row := section11Row{Bullet: bullet, Claim: strings.TrimSpace(f[1]), Evidence: evidence,
			Gap: strings.TrimSpace(f[3]), Note: strings.TrimSpace(f[4]), Line: n + 1}
		if row.Claim == "" {
			return nil, fmt.Errorf("map:%d: the claim is empty", n+1)
		}
		if len(evidence) == 0 && row.Gap == "" {
			return nil, fmt.Errorf("map:%d: %s names no evidence and no known gap", n+1, row.key())
		}
		if row.Gap != "" && !strings.HasPrefix(row.Gap, "known-gap: ") {
			return nil, fmt.Errorf("map:%d: gap %q must begin \"known-gap: \"", n+1, row.Gap)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseSection11Evidence(raw string) ([]section11Item, error) {
	var items []section11Item
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kind, rest, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("evidence item %q has no kind", part)
		}
		switch kind {
		case "unit", "integration", "chaos":
			dot := strings.LastIndex(rest, ".")
			if dot < 0 || dot == len(rest)-1 {
				return nil, fmt.Errorf("evidence item %q is not <package>.<Test>", part)
			}
			item := section11Item{Kind: kind, Package: rest[:dot], Test: rest[dot+1:]}
			if !strings.HasPrefix(item.Package, modulePath) {
				return nil, fmt.Errorf("evidence item %q is outside %s", part, modulePath)
			}
			items = append(items, item)
		case "fixture":
			script, check, ok := strings.Cut(rest, ":")
			if !ok || script == "" || check == "" {
				return nil, fmt.Errorf("evidence item %q is not fixture:<script>:<check text>", part)
			}
			items = append(items, section11Item{Kind: kind, Script: script, Check: check})
		default:
			return nil, fmt.Errorf("evidence item %q has unknown kind %q", part, kind)
		}
	}
	return items, nil
}

// section11Bullets counts AGENTS.md §11's required-coverage list.
func section11Bullets(agents string) int {
	const marker = "Required integration and chaos coverage"
	i := strings.Index(agents, marker)
	if i < 0 {
		return 0
	}
	count := 0
	for _, line := range strings.Split(agents[i+len(marker):], "\n") {
		if strings.HasPrefix(line, "- ") {
			count++
			continue
		}
		if count > 0 && strings.TrimSpace(line) == "" {
			break
		}
	}
	return count
}

var section11GapLine = regexp.MustCompile(`(?m)^- \*\*§11 ([A-Za-z0-9-]+)\*\*`)

// section11DocGaps is the set of known gaps PROOFS.md names.
func section11DocGaps(doc string) map[string]bool {
	out := map[string]bool{}
	for _, m := range section11GapLine.FindAllStringSubmatch(doc, -1) {
		out[m[1]] = true
	}
	return out
}

// section11KillTests are the three §11 kill tests (1b, 1c and 2b-orker) of
// internal/engine/orker_kill_integration_test.go. They inject their fault
// through hooked stores rather than living in a file named "chaos", so they
// are named here: the map's note calls them kill tests (plan §7.2), and the
// chaos kind is about what the test does, not what its file is called.
var section11KillTests = map[string]bool{
	"TestIntegration_AnOrkerKilledAfterItsContainerExitsRetriesElsewhere":    true,
	"TestIntegration_AnOrkerKilledDuringArtifactUploadRetriesElsewhere":      true,
	"TestIntegration_AnOrkerCommitDuringAStorageOutageNeverSucceedsSilently": true,
}

// section11Lookup answers whether a test is declared in its package and in
// which file.
type section11Lookup func(pkg, test string) (unit, integration bool, file string, err error)

// section11Fixture reads one fixture script.
type section11Fixture func(script string) (string, error)

// checkSection11 validates the map. It is pure over its inputs, so the
// fixture tests below can drive every refusal with a small map.
func checkSection11(mapText, agentsText, docText string, nightly, quarantine map[string]bool,
	lookup section11Lookup, fixture section11Fixture) []string {
	var problems []string
	rows, err := parseSection11Map(mapText)
	if err != nil {
		return []string{err.Error()}
	}
	bullets := section11Bullets(agentsText)
	if bullets == 0 {
		problems = append(problems, "AGENTS.md §11's required-coverage list was not found")
	}
	seenBullets := map[int]bool{}
	gapRows := map[string]bool{}
	for _, row := range rows {
		seenBullets[row.Bullet] = true
		if bullets > 0 && row.Bullet > bullets {
			problems = append(problems, fmt.Sprintf("%s: bullet %d is past AGENTS.md's %d", row.key(), row.Bullet, bullets))
		}
		mergeEvidence := false
		badEvidence := false
		for _, item := range row.Evidence {
			switch item.Kind {
			case "fixture":
				body, err := fixture(item.Script)
				if err != nil {
					problems = append(problems, fmt.Sprintf("%s: fixture %s: %v", row.key(), item.Script, err))
					badEvidence = true
					continue
				}
				if !strings.Contains(body, item.Check) {
					problems = append(problems, fmt.Sprintf("%s: fixture %s does not carry the check %q", row.key(), item.Script, item.Check))
					badEvidence = true
					continue
				}
				mergeEvidence = true
			default:
				unit, integration, file, err := lookup(item.Package, item.topTest())
				if err != nil {
					problems = append(problems, fmt.Sprintf("%s: %s: %v", row.key(), item.Test, err))
					badEvidence = true
					continue
				}
				if item.Kind == "unit" {
					if !unit {
						problems = append(problems, fmt.Sprintf("%s: unit:%s.%s is not declared in a file without the integration tag (file %s)",
							row.key(), item.Package, item.Test, file))
						badEvidence = true
					}
					continue
				}
				if !integration {
					problems = append(problems, fmt.Sprintf("%s: %s:%s.%s is not declared in an integration-tagged file (file %s)",
						row.key(), item.Kind, item.Package, item.Test, file))
					badEvidence = true
					continue
				}
				if item.Kind == "chaos" && !strings.Contains(strings.ToLower(file), "chaos") &&
					!strings.Contains(item.topTest(), "Chaos") && !section11KillTests[item.topTest()] {
					problems = append(problems, fmt.Sprintf("%s: chaos:%s.%s is not a chaos test: %s injects no fault by its name",
						row.key(), item.Package, item.Test, file))
					badEvidence = true
					continue
				}
				key := item.Package + "\t" + item.Test
				if nightly[key] || nightly[item.Package+"\t"+item.topTest()] ||
					quarantine[key] || quarantine[item.Package+"\t"+item.topTest()] {
					continue
				}
				mergeEvidence = true
			}
		}
		if row.Gap != "" {
			gapRows[row.key()] = true
			if mergeEvidence {
				problems = append(problems, fmt.Sprintf("%s has non-unit merge-gate evidence now: delete its known-gap row here and in docs/PROOFS.md",
					row.key()))
			}
		} else if !mergeEvidence && !badEvidence {
			problems = append(problems, fmt.Sprintf("%s has unit-only evidence and no known gap: add a merge-gate test or a known-gap row",
				row.key()))
		}
	}
	for n := 1; n <= bullets; n++ {
		if !seenBullets[n] {
			problems = append(problems, fmt.Sprintf("AGENTS.md §11 bullet %d has no row in the map", n))
		}
	}
	docGaps := section11DocGaps(docText)
	for key := range gapRows {
		if !docGaps[key] {
			problems = append(problems, fmt.Sprintf("%s: the known gap is not named in docs/PROOFS.md §12 as \"§11 %s\"", key, key))
		}
	}
	for key := range docGaps {
		if !gapRows[key] {
			problems = append(problems, fmt.Sprintf("docs/PROOFS.md names §11 %s as a gap, but no map row carries that known gap", key))
		}
	}
	return problems
}

// TestSection11Coverage holds AGENTS.md §11 to the map: every claim has
// evidence that runs in the merge gate, every named test is declared with the
// kind the map claims, and every known gap is named in PROOFS.md.
func TestSection11Coverage(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(data)
	}
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	nightly := map[string]bool{}
	if entries, err := readNightlyList(filepath.Join(root, "scripts/proof-nightly-tests.txt")); err != nil {
		t.Fatal(err)
	} else {
		for _, e := range entries {
			nightly[e.Package+"\t"+e.Test] = true
		}
	}
	quarantine := map[string]bool{}
	if entries, err := readQuarantine(filepath.Join(root, "scripts/proof-quarantine.txt")); err != nil {
		t.Fatal(err)
	} else {
		for _, e := range entries {
			quarantine[e.Package+"\t"+e.Test] = true
		}
	}
	declared := map[string]map[string]declaredTest{}
	lookup := func(pkg, test string) (bool, bool, string, error) {
		if !strings.HasPrefix(pkg, modulePath) {
			return false, false, "", fmt.Errorf("package %q is outside %s", pkg, modulePath)
		}
		dir := filepath.Join(root, strings.TrimPrefix(pkg, modulePath+"/"))
		if _, ok := declared[pkg]; !ok {
			files, err := readDirGo(dir)
			if err != nil {
				return false, false, "", err
			}
			unit, integration := testsDeclared(dir, files)
			m := map[string]declaredTest{}
			for _, f := range files {
				if !strings.HasSuffix(f.Name, "_test.go") {
					continue
				}
				pf := parseGo(f)
				for _, name := range pf.Tests {
					isIntegration := fileMatches(cfgIntegration, dir, f) && !fileMatches(cfgUnit, dir, f)
					if _, exists := m[name]; !exists || isIntegration {
						m[name] = declaredTest{unit: unit[name], integration: integration[name], file: f.Name}
					}
				}
			}
			declared[pkg] = m
		}
		d, ok := declared[pkg][test]
		if !ok {
			return false, false, "", fmt.Errorf("%s declares no test %s", pkg, test)
		}
		return d.unit, d.integration, d.file, nil
	}
	fixture := func(script string) (string, error) {
		data, err := os.ReadFile(filepath.Join(root, script))
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	problems := checkSection11(read(section11Map), read(section11Agents), read(section11Doc), nightly, quarantine, lookup, fixture)
	for _, problem := range problems {
		t.Error(problem)
	}
}

type declaredTest struct {
	unit, integration bool
	file              string
}

const section11FixtureAgents = `Required integration and chaos coverage includes:
- a claim
`

func section11FixtureLookup(kind string) section11Lookup {
	return func(pkg, test string) (bool, bool, string, error) {
		switch kind {
		case "unit":
			return true, false, "claim_test.go", nil
		case "integration":
			return false, true, "claim_integration_test.go", nil
		case "mismatch":
			return true, false, "claim_test.go", nil
		}
		return false, false, "", fmt.Errorf("no test %s", test)
	}
}

func section11FixtureMap(evidence, gap string) string {
	return "1\t1a a claim\t" + evidence + "\t" + gap + "\t\n"
}

func TestSection11Coverage_AUnitOnlyClaimNeedsAKnownGap(t *testing.T) {
	problems := checkSection11(
		section11FixtureMap("unit:github.com/orkstack/orkstack/pkg.TestClaim", ""),
		section11FixtureAgents, "", nil, nil, section11FixtureLookup("unit"),
		func(string) (string, error) { return "", nil })
	if len(problems) != 1 || !strings.Contains(problems[0], "unit-only evidence and no known gap") {
		t.Fatalf("problems = %v, want the unit-only refusal", problems)
	}
}

func TestSection11Coverage_AKindMustMatchItsFilesBuildTag(t *testing.T) {
	problems := checkSection11(
		section11FixtureMap("integration:github.com/orkstack/orkstack/pkg.TestClaim", ""),
		section11FixtureAgents, "", nil, nil, section11FixtureLookup("mismatch"),
		func(string) (string, error) { return "", nil })
	if len(problems) != 1 || !strings.Contains(problems[0], "is not declared in an integration-tagged file") {
		t.Fatalf("problems = %v, want the build-tag refusal", problems)
	}
}

func TestSection11Coverage_AKnownGapIsInProofsDoc(t *testing.T) {
	problems := checkSection11(
		section11FixtureMap("unit:github.com/orkstack/orkstack/pkg.TestClaim", "known-gap: nothing end to end"),
		section11FixtureAgents, "", nil, nil, section11FixtureLookup("unit"),
		func(string) (string, error) { return "", nil })
	if len(problems) != 1 || !strings.Contains(problems[0], "not named in docs/PROOFS.md") {
		t.Fatalf("problems = %v, want the missing §11 line", problems)
	}
}

func TestSection11Coverage_C6KillTestsAreChaosWithoutAChaosFileName(t *testing.T) {
	lookup := func(pkg, test string) (bool, bool, string, error) {
		return false, true, "orker_kill_integration_test.go", nil
	}
	problems := checkSection11(
		section11FixtureMap("chaos:github.com/orkstack/orkstack/pkg.TestIntegration_AnOrkerKilledAfterItsContainerExitsRetriesElsewhere", ""),
		section11FixtureAgents, "", nil, nil, lookup,
		func(string) (string, error) { return "", nil })
	if len(problems) != 0 {
		t.Fatalf("problems = %v, want a kill test accepted as chaos evidence", problems)
	}
	// Only the named kill tests are exempt: another test in the same file is
	// still refused for claiming to be chaos without a chaos name.
	problems = checkSection11(
		section11FixtureMap("chaos:github.com/orkstack/orkstack/pkg.TestIntegration_AnOrkerIsBusy", ""),
		section11FixtureAgents, "", nil, nil, lookup,
		func(string) (string, error) { return "", nil })
	if len(problems) != 1 || !strings.Contains(problems[0], "is not a chaos test") {
		t.Fatalf("problems = %v, want the chaos-name refusal", problems)
	}
}

func TestSection11Coverage_AClosedGapMustBeDeleted(t *testing.T) {
	problems := checkSection11(
		section11FixtureMap("integration:github.com/orkstack/orkstack/pkg.TestClaim", "known-gap: nothing end to end"),
		section11FixtureAgents, "- **§11 1a**: the gap\n", nil, nil, section11FixtureLookup("integration"),
		func(string) (string, error) { return "", nil })
	if len(problems) != 1 || !strings.Contains(problems[0], "has non-unit merge-gate evidence now") {
		t.Fatalf("problems = %v, want the closed-gap refusal", problems)
	}
}
