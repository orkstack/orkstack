package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
)

// defaultsHeader is RenderDefaults' preamble: what the file is, how it was
// made, and that hand-editing it is pointless — the next `make
// config-defaults` overwrites it.
const defaultsHeader = `# OrkStack configuration file — generated defaults.
#
# Every key config.Schema knows about, written at its own built-in
# default, nested by section in Schema's declaration order. docs/DEPLOY.md
# §1 documents each key. This file changes nothing when used as-is: loading it
# with --config and applying it sets every value to what the binary
# already falls back to with no file at all.
#
# GENERATED — do not hand-edit. Regenerate with "make config-defaults" (or
# "ork --defaults" / "orker --defaults") whenever internal/config/config.go
# gains, loses, or changes a Schema entry; internal/config/defaults_file_test.go
# fails the build until this file matches again.
#
# A key written active below (uncommented) carries a real literal default —
# uncomment and edit only what you mean to change. A key left commented
# has no single fixed default to write: either its value is computed at
# runtime (see the note on that line), or it belongs to an SDK/library this
# package does not second-guess. See deploy/orkstack.example.yaml for a
# worked, hand-annotated example, and docs/DEPLOY.md §1 for the full
# mechanism (--config, ORK_CONFIG_FILE, {ork:,orker:}, {file:}, --check).
`

// rawMachineOverride carries the actual YAML scalar RenderDefaults writes
// for a Schema entry whose display Var.Default is formatted for humans
// (formatBytes' "256 KiB") rather than written the way config.Load's
// KindInt actually parses it (the plain decimal "262144" a real deployment
// would write). Every entry here is, like every Var.Default, bound to the
// same code constant the binary itself falls back to — see config.go.
var rawMachineOverride = map[string]string{
	"ORK_VALUES_MAX_BYTES":               strconv.FormatInt(engine.DefaultValuesMaxBytes, 10),
	"ORK_ARTIFACT_MAX_BYTES":             strconv.FormatInt(engine.DefaultArtifactMaxBytes, 10),
	"ORK_LOG_MAX_BYTES":                  strconv.FormatInt(engine.DefaultLogMaxBytes, 10),
	"ORK_COMMIT_MAX_BYTES":               strconv.FormatInt(engine.DefaultCommitMaxBytes, 10),
	"ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC": strconv.FormatInt(api.DefaultFloorThroughputBytesPerSecond, 10),
	"ORK_ORKER_LOG_FLUSH_BYTES":          strconv.Itoa(orker.DefaultLogFlushBytes),
	"ORK_ORKER_LOG_MAX_BYTES":            strconv.Itoa(orker.DefaultLogMaxBytes),
	"ORK_ORKER_TMPFS_BYTES":              strconv.FormatInt(orker.DefaultTmpfsBytes, 10),
}

// alwaysCommentedEnv marks a Schema entry that must be written commented
// out regardless of Default, because its documented Default text is
// descriptive prose rather than a value config.Load could actually be fed
// back (writing it verbatim would set the variable to that prose, not
// leave it unset): the OrkStack activity catalog, the OTel sinks document,
// and the three literal-but-dynamic Orker settings whose Default field is
// a sentence, not a placeholder (config.go's own comment on each explains
// why they are not one of the two standardized placeholders either).
var alwaysCommentedEnv = map[string]bool{
	"ORK_ACTIVITY_CATALOG": true,
	"ORK_TELEMETRY_SINKS":  true,
	"ORK_ORKER_ID":         true,
	"ORK_ORKER_WORKDIR":    true,
	"ORK_DOCKER_BIN":       true,
}

// perBinaryDefault carries an explicit {ork: X, orker: Y} literal for an
// entry whose real default differs by binary — OTEL_SERVICE_NAME's "ork" /
// "orker" service.name (telemetry.ResourceAttrs) — rather than the single
// scalar form every other active entry below writes.
var perBinaryDefault = map[string][2]string{
	"OTEL_SERVICE_NAME": {"ork", "orker"},
}

// yamlTree is an insertion-ordered tree built from Schema's dotted Path
// values, so RenderDefaults can nest keys the way YAML needs to
// (storage: {s3: {endpoint: ...}}) while preserving Schema's own
// declaration order rather than a map's unspecified iteration order.
type yamlTree struct {
	key      string
	v        *Var
	children []*yamlTree
	index    map[string]int
}

func newYamlTree(key string) *yamlTree {
	return &yamlTree{key: key, index: map[string]int{}}
}

func (n *yamlTree) child(key string) *yamlTree {
	if i, ok := n.index[key]; ok {
		return n.children[i]
	}
	c := newYamlTree(key)
	n.index[key] = len(n.children)
	n.children = append(n.children, c)
	return c
}

// buildYamlTree walks Schema in order, splitting each entry's dotted Path
// into the nested tree RenderDefaults prints.
func buildYamlTree() *yamlTree {
	root := newYamlTree("")
	for i := range Schema {
		v := &Schema[i]
		node := root
		for _, seg := range strings.Split(v.Path, ".") {
			node = node.child(seg)
		}
		node.v = v
	}
	return root
}

// RenderDefaults produces the complete YAML document
// deploy/orkstack.defaults.yaml carries: every Schema entry, nested per its
// dotted Path, in Schema's own declaration order, each line at its code
// default (see config.go and defaults.go). config.Load accepts the result
// unchanged for both binaries, and applying it changes nothing observable
// — see defaults_file_test.go and defaults_render_test.go.
func RenderDefaults() string {
	var b strings.Builder
	b.WriteString(defaultsHeader)
	tree := buildYamlTree()
	for _, c := range tree.children {
		b.WriteString("\n")
		renderNode(&b, c, 0)
	}
	return b.String()
}

// renderNode prints one tree node at the given indent depth: a leaf as one
// value line (renderLeafLine), an interior node as "key:" followed by its
// children indented two spaces further.
func renderNode(b *strings.Builder, n *yamlTree, depth int) {
	indent := strings.Repeat("  ", depth)
	if n.v != nil {
		fmt.Fprintln(b, indent+renderLeafLine(*n.v, lastSegment(n.v.Path)))
		return
	}
	// A section with no active leaf anywhere beneath it (telemetry.sampler,
	// telemetry.otlp and its per-signal children, orker.docker, ...) would
	// otherwise print as a bare "key:" with only commented-out lines under
	// it — valid YAML, but a null scalar, not the mapping Load requires an
	// interior Schema node's value to be. "{}" keeps it a real (empty)
	// mapping. Everything beneath it is then printed as flat "#"-commented
	// documentation (renderInactiveComments), NEVER as real nested "key:"
	// lines — a nested interior key such as otlp.traces would otherwise
	// read as a structural sibling written at the wrong indentation
	// relative to a value ("{}") that has already closed the mapping.
	if !hasActiveLeaf(n) {
		fmt.Fprintln(b, indent+n.key+": {}")
		for _, c := range n.children {
			renderInactiveComments(b, c, depth+1, n.key)
		}
		return
	}
	fmt.Fprintln(b, indent+n.key+":")
	for _, c := range n.children {
		renderNode(b, c, depth+1)
	}
}

// renderInactiveComments prints every leaf beneath n — itself known to
// have no active descendant, since only renderNode's !hasActiveLeaf branch
// calls it — as one flat "#"-commented documentation line per leaf, keyed
// by its path relative to prefix (e.g. "traces.endpoint") rather than a
// nested "key:" mapping, for the reason renderNode's own comment explains.
func renderInactiveComments(b *strings.Builder, n *yamlTree, depth int, prefix string) {
	indent := strings.Repeat("  ", depth)
	displayKey := prefix + "." + n.key
	if n.v != nil {
		fmt.Fprintln(b, indent+renderLeafLine(*n.v, displayKey))
		return
	}
	for _, c := range n.children {
		renderInactiveComments(b, c, depth, displayKey)
	}
}

// hasActiveLeaf reports whether n (an interior node) has any descendant
// leaf isActiveDefault reports true for.
func hasActiveLeaf(n *yamlTree) bool {
	if n.v != nil {
		return isActiveDefault(*n.v)
	}
	for _, c := range n.children {
		if hasActiveLeaf(c) {
			return true
		}
	}
	return false
}

// renderLeafLine renders one Schema entry's YAML line, without leading
// indentation: key as the key (the leaf's own last path segment when it is
// printed nested, or a fuller relative path when renderInactiveComments is
// flattening an entirely inactive subtree), its default as the value (or,
// when there is none to write, "#"-commented with a placeholder value),
// and a trailing "# ..." comment carrying the binary restriction and/or
// DerivedNote a reader would otherwise have no way to see in a generated
// file with no prose beside each key.
func renderLeafLine(v Var, key string) string {
	var notes []string
	if len(v.Binaries) == 1 {
		notes = append(notes, string(v.Binaries[0]))
	}

	if pb, ok := perBinaryDefault[v.Env]; ok {
		return fmt.Sprintf("%s: {ork: %s, orker: %s}%s", key, pb[0], pb[1], trailingComment(notes))
	}

	if !isActiveDefault(v) {
		if v.DerivedNote != "" {
			notes = append(notes, v.DerivedNote)
		} else if v.Default != DerivedDefault {
			// One of the literal-but-dynamic entries in
			// alwaysCommentedEnv: config.go's own Default text IS the
			// explanatory note (e.g. "hostname-<hash of the work dir>").
			notes = append(notes, v.Default)
		}
		return fmt.Sprintf("# %s: %s%s", key, placeholderLiteral(v.Kind), trailingComment(notes))
	}

	if v.Default == EmptyDefault {
		if v.DerivedNote != "" {
			notes = append(notes, v.DerivedNote)
		}
		return fmt.Sprintf("%s: %s%s", key, emptyLiteral(v.Kind), trailingComment(notes))
	}

	value := v.Default
	if raw, ok := rawMachineOverride[v.Env]; ok {
		notes = append([]string{v.Default}, notes...)
		value = raw
	}
	if v.Kind == KindList {
		// A List leaf's Default is the comma-joined string load.go itself
		// renders a YAML list DOWN to (walkNode's KindList case); write it
		// back UP as a flow list, since Load requires a real
		// yaml.SequenceNode for this Kind, never a bare scalar.
		return fmt.Sprintf("%s: [%s]%s", key, strings.Join(strings.Split(value, ","), ", "), trailingComment(notes))
	}
	return fmt.Sprintf("%s: %s%s", key, quoteScalar(value), trailingComment(notes))
}

// isActiveDefault reports whether renderLeafLine writes v's key active
// (uncommented) rather than commented out. Shared with
// defaults_render_test.go so the test's notion of "should be active"
// can never drift from the renderer's own.
func isActiveDefault(v Var) bool {
	if _, ok := perBinaryDefault[v.Env]; ok {
		return true
	}
	return !alwaysCommentedEnv[v.Env] && v.Default != DerivedDefault
}

// canonicalDefaultValue is the exact string RenderDefaults writes as an
// active leaf's YAML value (before quoting) for binary — the same value
// defaults_render_test.go expects Document.resolve to report back after
// Load parses the rendered file. Meaningless (and never called) for a
// leaf isActiveDefault reports false for.
func canonicalDefaultValue(v Var, binary Binary) string {
	if pb, ok := perBinaryDefault[v.Env]; ok {
		if binary == BinaryOrk {
			return pb[0]
		}
		return pb[1]
	}
	if raw, ok := rawMachineOverride[v.Env]; ok {
		return raw
	}
	if v.Default == EmptyDefault {
		return ""
	}
	return v.Default
}

// lastSegment returns the final "."-separated component of a dotted Path.
func lastSegment(path string) string {
	if i := strings.LastIndex(path, "."); i >= 0 {
		return path[i+1:]
	}
	return path
}

// placeholderLiteral is the inert YAML shape renderLeafLine writes for a
// commented-out key, matching the leaf's Kind so the line at least LOOKS
// like the key it names, even though the "#" makes it inert.
func placeholderLiteral(k Kind) string {
	switch k {
	case KindList:
		return "[]"
	case KindMap, KindDocument:
		return "{}"
	default:
		return `""`
	}
}

// emptyLiteral is the real, active YAML value for a Kind whose documented
// default is genuinely empty (EmptyDefault) — a plain "" for every scalar
// kind config.Load accepts one for, [] for a list, {} for a map.
func emptyLiteral(k Kind) string {
	switch k {
	case KindList:
		return "[]"
	case KindMap:
		return "{}"
	default:
		return `""`
	}
}

// trailingComment joins non-empty notes into one "  # a; b" suffix, or
// returns "" when there is nothing to say.
func trailingComment(notes []string) string {
	var kept []string
	for _, n := range notes {
		if n != "" {
			kept = append(kept, n)
		}
	}
	if len(kept) == 0 {
		return ""
	}
	return "  # " + strings.Join(kept, "; ")
}

// yamlBareSafe reports whether s can be written as a bare (unquoted) YAML
// scalar in the "key: value" lines this renderer writes: no character that
// would otherwise be read as starting a comment, a flow collection, or a
// second mapping entry.
func yamlBareSafe(s string) bool {
	if s == "" {
		return false
	}
	if strings.ContainsAny(s, "#{}[]&*!|>'\"%@`") {
		return false
	}
	if strings.Contains(s, ": ") || strings.HasSuffix(s, ":") {
		return false
	}
	if strings.TrimSpace(s) != s {
		return false
	}
	return true
}

// quoteScalar double-quotes value when yamlBareSafe says a bare scalar
// would not round-trip safely.
func quoteScalar(value string) string {
	if yamlBareSafe(value) {
		return value
	}
	return strconv.Quote(value)
}

// sortedEnvNames is a small debugging helper (unused by RenderDefaults
// itself) kept beside rawMachineOverride/alwaysCommentedEnv so a future
// entry added to either map without a matching Schema entry fails loudly
// rather than being silently ignored — see defaults_render_test.go.
func sortedEnvNames(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
