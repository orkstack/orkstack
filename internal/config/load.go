package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// origin names where a Document (or, after merging with the environment,
// Effective) got a value from.
type origin string

const (
	originFile    origin = "file"
	originEnv     origin = "env"
	originDefault origin = "default"
)

// resolvedValue is one Schema entry's value as this document set it, along
// with the origin Effective reports for it.
type resolvedValue struct {
	value  string
	origin origin
}

// schemaNode is one node of the tree Schema's dotted Path values describe.
// A leaf (var != nil) is one mapped setting; an interior node is a key that
// exists only to hold further keys (e.g. "storage", "storage.s3").
type schemaNode struct {
	v        *Var
	children map[string]*schemaNode
	// forOrk/forOrker report whether ANY leaf under this node (itself
	// included) applies to that binary. A node neither applies to is a
	// section belonging entirely to the other binary — Load accepts and
	// ignores it wholesale, per the specification's "one shared file" rule.
	forOrk, forOrker bool
}

func (n *schemaNode) appliesTo(binary Binary) bool {
	if binary == BinaryOrk {
		return n.forOrk
	}
	return n.forOrker
}

// schemaRoot is built once from Schema at package init and walked by Load.
var schemaRoot = buildSchemaTree()

func buildSchemaTree() *schemaNode {
	root := &schemaNode{children: map[string]*schemaNode{}}
	for i := range Schema {
		v := &Schema[i]
		segments := strings.Split(v.Path, ".")
		node := root
		for _, seg := range segments {
			child, ok := node.children[seg]
			if !ok {
				child = &schemaNode{children: map[string]*schemaNode{}}
				node.children[seg] = child
			}
			node = child
		}
		node.v = v
	}
	markBinaries(root)
	return root
}

// markBinaries fills forOrk/forOrker bottom-up: true wherever the node
// itself, or anything beneath it, applies to that binary.
func markBinaries(n *schemaNode) {
	if n.v != nil {
		for _, b := range n.v.Binaries {
			if b == BinaryOrk {
				n.forOrk = true
			}
			if b == BinaryOrker {
				n.forOrker = true
			}
		}
	}
	for _, c := range n.children {
		markBinaries(c)
		n.forOrk = n.forOrk || c.forOrk
		n.forOrker = n.forOrker || c.forOrker
	}
}

// Document is a loaded, validated configuration file: every mapped value it
// set, keyed by the environment variable it feeds.
type Document struct {
	path   string
	values map[string]resolvedValue
}

// Load reads path and strictly validates it for binary: an unknown key is
// refused naming its full dotted path, a scalar leaf of the wrong type is
// refused naming the key, per-binary leaves ({ork:, orker:}) select this
// binary's value, {file: /path} leaves are read (one trailing newline
// trimmed), lists become comma-separated strings, maps become "k=v,..."
// with percent-encoded values, and the two document subtrees (activities,
// telemetry.sinks) are re-serialized to YAML text under their wrapper key.
// A key that belongs only to the OTHER binary is accepted and ignored,
// never an error — the file is shared between both daemons.
func Load(path string, binary Binary) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("config: %s: parse yaml: %w", path, err)
	}

	values := map[string]resolvedValue{}
	if len(root.Content) == 0 {
		// An empty (or all-comments) file is valid and sets nothing.
		return &Document{path: path, values: values}, nil
	}
	mapping := root.Content[0]
	if mapping.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("config: %s: the document must be a top-level mapping", path)
	}
	configDir := filepath.Dir(path)
	if err := walkNode(mapping, schemaRoot, nil, binary, values, configDir); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	// Every value reaches its daemon as an environment variable, which cannot
	// hold a NUL byte, and a double-quoted YAML scalar can spell one ("\0").
	// Refuse it here, naming the key, not partway through applying the file.
	for _, v := range Schema {
		if rv, ok := values[v.Env]; ok && strings.IndexByte(rv.value, 0) >= 0 {
			return nil, fmt.Errorf("config: %s: %s: the value holds a NUL byte, which an environment variable cannot", path, v.Path)
		}
	}
	return &Document{path: path, values: values}, nil
}

// dotted joins a path prefix and a final segment with ".", the form every
// error message and Var.Path use.
func dotted(prefix []string, key string) string {
	if len(prefix) == 0 {
		return key
	}
	return strings.Join(prefix, ".") + "." + key
}

// walkNode validates one YAML mapping node against the schema subtree
// rooted at snode, recording every leaf it finds into values. configDir is
// the directory containing the config file being loaded, so a relative
// {file: path} can be resolved against it rather than the process's cwd.
func walkNode(node *yaml.Node, snode *schemaNode, prefix []string, binary Binary, values map[string]resolvedValue, configDir string) error {
	seen := make(map[string]bool, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode, valNode := node.Content[i], node.Content[i+1]
		key := keyNode.Value

		if seen[key] {
			return fmt.Errorf("%s: duplicate key %q", dotted(prefix, key), key)
		}
		seen[key] = true

		child, ok := snode.children[key]
		if !ok {
			return fmt.Errorf("unknown key %q", dotted(prefix, key))
		}
		fullPath := append(append([]string{}, prefix...), key)
		path := dotted(prefix, key)

		if !child.appliesTo(binary) {
			// Belongs entirely to the other binary: accepted, never parsed.
			continue
		}

		if child.v == nil {
			// Interior node: recurse.
			if valNode.Kind != yaml.MappingNode {
				return fmt.Errorf("%s: expected a mapping", path)
			}
			if err := walkNode(valNode, child, fullPath, binary, values, configDir); err != nil {
				return err
			}
			continue
		}

		v := *child.v
		if !v.usesBinary(binary) {
			continue
		}

		switch v.Kind {
		case KindDocument:
			text, err := renderDocument(v, valNode)
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			values[v.Env] = resolvedValue{value: text, origin: originFile}

		case KindList:
			if valNode.Kind != yaml.SequenceNode {
				return fmt.Errorf("%s: must be a YAML list", path)
			}
			items := make([]string, 0, len(valNode.Content))
			for _, item := range valNode.Content {
				if item.Kind != yaml.ScalarNode {
					return fmt.Errorf("%s: list items must be strings", path)
				}
				items = append(items, item.Value)
			}
			rendered := strings.Join(items, ",")
			if err := checkExplicitEmpty(v, rendered, path); err != nil {
				return err
			}
			values[v.Env] = resolvedValue{value: rendered, origin: originFile}

		case KindMap:
			if valNode.Kind != yaml.MappingNode {
				return fmt.Errorf("%s: must be a mapping", path)
			}
			rendered, err := renderMap(valNode, path, configDir)
			if err != nil {
				return err
			}
			if err := checkExplicitEmpty(v, rendered, path); err != nil {
				return err
			}
			values[v.Env] = resolvedValue{value: rendered, origin: originFile}

		default: // KindString, KindInt, KindBool, KindDuration, KindSecret
			raw, present, err := resolveScalar(valNode, v, binary, path, configDir)
			if err != nil {
				return err
			}
			if !present {
				continue
			}
			if err := checkScalarType(v.Kind, raw, path); err != nil {
				return err
			}
			if err := checkExplicitEmpty(v, raw, path); err != nil {
				return err
			}
			values[v.Env] = resolvedValue{value: raw, origin: originFile}
		}
	}
	return nil
}

// isPlaceholderDefault reports whether Var.Default is a descriptive note
// ("(empty)", "(derived)", "(SDK default)", ...) rather than a literal
// value a binary actually falls back to when its variable is unset. Only a
// placeholder default tolerates an explicitly empty file value, since only
// then is "empty" indistinguishable from "absent" in the first place.
func isPlaceholderDefault(d string) bool {
	return strings.HasPrefix(strings.TrimSpace(d), "(")
}

// checkExplicitEmpty refuses a leaf whose file value renders to the empty
// string when Var.Default documents a real, non-empty fallback. Every
// *FromEnv reader in this codebase treats an empty environment variable as
// unset (os.LookupEnv reports it present, but the reader's own `if v != ""`
// check discards it) — so writing `key: ""`, `key: []`, or `key: {}` for
// such a leaf would silently produce the DEFAULT, not the empty value the
// file asked for. Refusing at load time surfaces that mismatch instead of
// letting it masquerade as configuration that took effect.
func checkExplicitEmpty(v Var, rendered, path string) error {
	if rendered != "" {
		return nil
	}
	if v.Default == "" || isPlaceholderDefault(v.Default) {
		return nil
	}
	return fmt.Errorf("%s: explicit empty value is refused: %s reads an empty environment value as \"use the default %s\", so this file could never actually set an empty value here — remove the key to use the default, or the binaries would silently ignore it", path, v.Env, v.Default)
}

// isScalarKind reports whether Kind k is eligible for the {ork:, orker:}
// per-binary form, per the specification's "ANY scalar leaf".
func isScalarKind(k Kind) bool {
	switch k {
	case KindString, KindInt, KindBool, KindDuration, KindSecret:
		return true
	default:
		return false
	}
}

// resolveScalar resolves one scalar-kind leaf's YAML node into its string
// value, handling both the {ork:, orker:} and {file: path} forms. present
// is false when a per-binary form has nothing for binary (the leaf is
// simply absent for this binary, not an error), or when the node is a YAML
// null (`null`, `~`, `Null`, `NULL`): a null leaf means "not set" and falls
// through to the environment or default, exactly like a key that was never
// written, rather than becoming the literal text "null". configDir anchors
// a relative {file: path}.
func resolveScalar(node *yaml.Node, v Var, binary Binary, path, configDir string) (value string, present bool, err error) {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return "", false, nil
	}

	if node.Kind == yaml.MappingNode {
		keys := make([]string, 0, len(node.Content)/2)
		byKey := map[string]*yaml.Node{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			k := node.Content[i].Value
			keys = append(keys, k)
			byKey[k] = node.Content[i+1]
		}

		allBinaryKeys := len(keys) > 0
		for _, k := range keys {
			if k != string(BinaryOrk) && k != string(BinaryOrker) {
				allBinaryKeys = false
				break
			}
		}
		if isScalarKind(v.Kind) && allBinaryKeys {
			selected, ok := byKey[string(binary)]
			if !ok {
				return "", false, nil
			}
			return resolveScalar(selected, v, binary, path, configDir)
		}

		if len(keys) == 1 && keys[0] == "file" {
			return readSecretFile(byKey["file"], path, configDir)
		}

		return "", false, fmt.Errorf("%s: expected a value, {ork: ..., orker: ...}, or {file: PATH}", path)
	}

	if node.Kind != yaml.ScalarNode {
		return "", false, fmt.Errorf("%s: expected a scalar value", path)
	}
	return node.Value, true, nil
}

// resolveFilePath anchors a {file: path} reference: an absolute path is
// used as-is, a relative one resolves against the directory containing the
// config file itself (never the process's current working directory), so
// the same file works no matter where a binary is launched from.
func resolveFilePath(configDir, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(configDir, p)
}

// readSecretFile implements {file: /path}: the file's contents with one
// trailing line ending ("\r\n" or "\n") trimmed.
func readSecretFile(pathNode *yaml.Node, key, configDir string) (string, bool, error) {
	if pathNode == nil || pathNode.Kind != yaml.ScalarNode {
		return "", false, fmt.Errorf("%s: file: must be a string path", key)
	}
	resolved := resolveFilePath(configDir, pathNode.Value)
	data, err := os.ReadFile(resolved)
	if err != nil {
		return "", false, fmt.Errorf("%s: read secret file %s: %w", key, resolved, err)
	}
	return trimOneLineEnding(string(data)), true, nil
}

// trimOneLineEnding trims exactly one trailing line ending: "\r\n" as a
// unit when present, else a bare "\n", and nothing when the file has
// neither — so a file with two trailing newlines keeps one, matching a
// secret written by `echo` (adds one) rather than one hand-edited with an
// extra blank line left in.
func trimOneLineEnding(s string) string {
	if strings.HasSuffix(s, "\r\n") {
		return strings.TrimSuffix(s, "\r\n")
	}
	return strings.TrimSuffix(s, "\n")
}

// checkScalarType validates a resolved scalar against its Kind, so a typo
// (a bool where a duration belongs) is refused at load time naming the key,
// rather than surfacing later from whatever *FromEnv reader parses it. The
// value is parsed as it stands: the readers do not trim it, so a padded
// duration the file passed would silently become the reader's default.
func checkScalarType(kind Kind, raw, path string) error {
	switch kind {
	case KindInt:
		if _, err := strconv.Atoi(raw); err != nil {
			return fmt.Errorf("%s: %q is not an integer", path, raw)
		}
	case KindBool:
		if _, err := strconv.ParseBool(raw); err != nil {
			return fmt.Errorf("%s: %q is not a boolean", path, raw)
		}
	case KindDuration:
		if _, err := time.ParseDuration(raw); err != nil {
			return fmt.Errorf("%s: %q is not a duration", path, raw)
		}
	}
	return nil
}

// renderMap turns a YAML mapping node into "k=v,k2=v2", percent-encoding
// each value per the OTel baggage rules, so it can populate a variable such
// as OTEL_RESOURCE_ATTRIBUTES or OTEL_EXPORTER_OTLP_HEADERS. Each value may
// itself be {file: path}, since a header may carry a secret
// (telemetry.otlp.headers.*).
func renderMap(node *yaml.Node, path, configDir string) (string, error) {
	var parts []string
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		entryPath := path + "." + key
		val, err := mapEntryValue(node.Content[i+1], entryPath, configDir)
		if err != nil {
			return "", err
		}
		parts = append(parts, key+"="+percentEncode(val))
	}
	return strings.Join(parts, ","), nil
}

// mapEntryValue resolves one map entry's value: a plain scalar, or
// {file: path}.
func mapEntryValue(node *yaml.Node, path, configDir string) (string, error) {
	if node.Kind == yaml.ScalarNode {
		return node.Value, nil
	}
	if node.Kind == yaml.MappingNode && len(node.Content) == 2 && node.Content[0].Value == "file" {
		value, _, err := readSecretFile(node.Content[1], path, configDir)
		return value, err
	}
	return "", fmt.Errorf("%s: must be a string or {file: PATH}", path)
}

// percentEncode escapes the four characters the OTel baggage-list shape
// (OTEL_EXPORTER_OTLP_HEADERS and friends) needs escaped: '%' first, so
// encoding the other three never produces a new '%' this function did not
// intend. internal/telemetry.parseBaggageHeaders decodes with
// url.QueryUnescape, which this stays deliberately compatible with —
// including '+', which QueryUnescape (form-decoding semantics) turns into a
// literal space unless it was itself escaped to "%2B" here.
func percentEncode(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '%':
			b.WriteString("%25")
		case ',':
			b.WriteString("%2C")
		case '=':
			b.WriteString("%3D")
		case '+':
			b.WriteString("%2B")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// documentWrapKey is the top-level key ORK_ACTIVITY_CATALOG and
// ORK_TELEMETRY_SINKS are wrapped under in their own env var text, matching
// dsl.catalogDocument and telemetry.sinksDocument.
func documentWrapKey(v Var) string {
	if v.Env == "ORK_ACTIVITY_CATALOG" {
		return "activities"
	}
	return "sinks"
}

// renderDocument re-serializes a document-kind subtree (activities,
// telemetry.sinks) to the YAML text its variable carries: the subtree
// wrapped back under its own key, so ORK_ACTIVITY_CATALOG reads exactly
// like a standalone `activities: {...}` document and dsl.ParseCatalog (or
// telemetry.ParseSinks) can decode it unchanged.
func renderDocument(v Var, node *yaml.Node) (string, error) {
	wrapper := &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: documentWrapKey(v)},
			node,
		},
	}
	out, err := yaml.Marshal(wrapper)
	if err != nil {
		return "", fmt.Errorf("re-serialize: %w", err)
	}
	return string(out), nil
}
