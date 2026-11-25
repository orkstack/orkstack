package provider

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
)

// Setting names the providers document in errors, and Var is the variable
// it is carried in, set identically on ork and orker.
const (
	Setting = "providers"
	Var     = "ORK_PROVIDERS"
)

// FromEnv parses the providers document a binary was started with (lookup is
// os.LookupEnv) against types, which in a binary is types.All's. Its
// error names the setting and the variable, for a refusal at boot.
func FromEnv(lookup func(string) (string, bool), types map[string]Type) (Set, error) {
	raw, _ := lookup(Var)
	set, err := ParseWith([]byte(raw), types)
	if err != nil {
		return Set{}, fmt.Errorf("%s (%s): %w", Setting, Var, err)
	}
	return set, nil
}

// MaxDocumentBytes bounds the providers document.
const MaxDocumentBytes = 64 << 10

// secretWords are the words no configuration key may contain. The document
// holds no secret: a provider authenticates with the attempt's token alone
// (providers.md §4.1). A type cannot add a key that would hold one, because
// the parser refuses the key before the type sees it.
var secretWords = []string{"secret", "password", "passwd", "token", "private", "credential", "apikey", "api_key"}

// Set is the deployment's declared providers, by name. The zero value
// declares none.
type Set struct {
	byName map[string]Provider
}

// Names are the declared names, sorted.
func (s Set) Names() []string {
	names := make([]string, 0, len(s.byName))
	for name := range s.byName {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// Len is how many providers are declared.
func (s Set) Len() int { return len(s.byName) }

// ByName is the provider declared under name.
func (s Set) ByName(name string) (Provider, bool) {
	p, ok := s.byName[name]
	return p, ok
}

// ParseWith decodes and validates one providers document against types.
// Empty input declares none. Every refusal names the provider and the key. It does no I/O: a file a provider
// names (ca_file) is read by the Orker when it builds its client.
func ParseWith(raw []byte, types map[string]Type) (Set, error) {
	if len(raw) > MaxDocumentBytes {
		return Set{}, fmt.Errorf("%s: the document is %d bytes; at most %d", Setting, len(raw), MaxDocumentBytes)
	}
	if strings.TrimSpace(string(raw)) == "" {
		return Set{}, nil
	}
	if line, found := secondDocumentLine(raw); found {
		return Set{}, fmt.Errorf("%s: line %d: a second YAML document begins here; the setting holds one", Setting, line)
	}
	var doc struct {
		Providers map[string]yaml.Node `yaml:"providers"`
	}
	// Anchors, aliases and tags are refused anywhere in the document, keys
	// and the declarations themselves included, before anything is decoded.
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return Set{}, fmt.Errorf("%s: parse yaml: %w", Setting, err)
	}
	if err := refuseNodeFeatures(&root); err != nil {
		return Set{}, fmt.Errorf("%s: %w", Setting, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil && !errors.Is(err, io.EOF) {
		return Set{}, fmt.Errorf("%s: parse yaml: %w", Setting, err)
	}
	if len(doc.Providers) > MaxProviders {
		return Set{}, fmt.Errorf("%s: %d providers are declared; at most %d", Setting, len(doc.Providers), MaxProviders)
	}
	names := make([]string, 0, len(doc.Providers))
	for name := range doc.Providers {
		names = append(names, name)
	}
	slices.Sort(names)
	out := Set{byName: make(map[string]Provider, len(names))}
	var errs []error
	for _, name := range names {
		p, err := build(name, doc.Providers[name], types)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %s: %w", Setting, printable(name), err))
			continue
		}
		out.byName[name] = p
	}
	if err := errors.Join(errs...); err != nil {
		return Set{}, err
	}
	return out, nil
}

// secondDocumentLine is the line a second YAML document begins on, if the
// stream holds one. This package does not import internal/dsl, which reads
// its own locators through this package's rules.
func secondDocumentLine(raw []byte) (int, bool) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var first yaml.Node
	if err := decoder.Decode(&first); err != nil {
		return 0, false
	}
	var second yaml.Node
	if err := decoder.Decode(&second); err != nil {
		return 0, false
	}
	return second.Line, true
}

func build(name string, node yaml.Node, types map[string]Type) (Provider, error) {
	if err := ValidName(name); err != nil {
		return nil, err
	}
	config, err := newConfig(&node)
	if err != nil {
		return nil, err
	}
	typeName, ok := config.String("type")
	if !ok {
		return nil, errors.Join(append(config.errs, fmt.Errorf("type is required"))...)
	}
	t, known := types[typeName]
	if !known {
		return nil, fmt.Errorf("type %q is not one this build has (%s)", printable(typeName), strings.Join(typeNames(types), ", "))
	}
	p, err := t.New(name, config)
	if err != nil {
		return nil, err
	}
	if err := config.finish(); err != nil {
		return nil, err
	}
	if p == nil || p.Name() != name || p.Type() != typeName {
		return nil, fmt.Errorf("type %q built a provider that does not name itself %q", typeName, name)
	}
	if p.Audience() == "" {
		return nil, fmt.Errorf("type %q built a provider with no audience", typeName)
	}
	if !loginSchemeShape.MatchString(p.LoginScheme()) {
		return nil, fmt.Errorf("type %q's login scheme %q must match %s", typeName, printable(p.LoginScheme()), loginSchemeShape)
	}
	return p, nil
}

func typeNames(types map[string]Type) []string {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	slices.Sort(names)
	if len(names) == 0 {
		return []string{"none"}
	}
	return names
}

// Config is one provider's declaration, read by its Type. Each getter marks
// its key as read; once New returns, a key nothing read is refused, so a
// misspelt key is never silently ignored. A value of the wrong shape is
// recorded and refused at the same point, so a type need not check what a
// getter already did: it gets the zero value and ok false.
type Config struct {
	values map[string]*yaml.Node
	read   map[string]bool
	errs   []error
}

func newConfig(node *yaml.Node) (*Config, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("a declaration is a mapping of keys")
	}
	c := &Config{values: map[string]*yaml.Node{}, read: map[string]bool{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		k, v := node.Content[i], node.Content[i+1]
		if k.Kind != yaml.ScalarNode || k.Tag != "!!str" {
			return nil, fmt.Errorf("line %d: a key is a plain string", k.Line)
		}
		key := k.Value
		if _, dup := c.values[key]; dup {
			return nil, fmt.Errorf("line %d: key %q is declared twice", k.Line, printable(key))
		}
		if err := refuseNodeFeatures(v); err != nil {
			return nil, fmt.Errorf("%s: %w", printable(key), err)
		}
		lower := strings.ToLower(key)
		for _, word := range secretWords {
			if strings.Contains(lower, word) {
				return nil, fmt.Errorf("key %q: the providers document holds no secret; a provider authenticates with the attempt's token", printable(key))
			}
		}
		c.values[key] = v
	}
	return c, nil
}

func refuseNodeFeatures(n *yaml.Node) error {
	if n.Kind == yaml.DocumentNode {
		for _, child := range n.Content {
			if err := refuseNodeFeatures(child); err != nil {
				return err
			}
		}
		return nil
	}
	if n.Tag == "!!merge" || (n.Kind == yaml.ScalarNode && n.Value == "<<" && n.Style == 0) {
		return fmt.Errorf("line %d: merge keys are refused", n.Line)
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return fmt.Errorf("line %d: anchors and aliases are refused", n.Line)
	}
	if n.Tag != "" && !strings.HasPrefix(n.Tag, "!!") {
		return fmt.Errorf("line %d: tag %q is refused", n.Line, n.Tag)
	}
	if n.Style&(yaml.TaggedStyle) != 0 {
		return fmt.Errorf("line %d: an explicit tag is refused", n.Line)
	}
	for _, child := range n.Content {
		if err := refuseNodeFeatures(child); err != nil {
			return err
		}
	}
	return nil
}

// unprintableRune is a character a value shown to an operator may not hold:
// C0 and C1 controls, and the formatting and bidirectional controls that
// make a line read as something it is not.
func unprintableRune(r rune) bool {
	switch {
	case r < 0x20, r == 0x7f, r >= 0x80 && r < 0xa0:
		return true
	case r == 0x200e, r == 0x200f, r >= 0x202a && r <= 0x202e, r >= 0x2066 && r <= 0x2069, r == 0xfeff:
		return true
	}
	return false
}

func (c *Config) take(key string) (*yaml.Node, bool) {
	c.read[key] = true
	n, ok := c.values[key]
	return n, ok
}

func (c *Config) fail(key string, n *yaml.Node, want string) {
	c.errs = append(c.errs, fmt.Errorf("%s (line %d) must be %s", key, n.Line, want))
}

// Has reports whether key is declared, and marks it read.
func (c *Config) Has(key string) bool {
	_, ok := c.take(key)
	return ok
}

// String is a plain string. A value YAML resolves to anything else (a number,
// a boolean, null) is refused, never coerced.
func (c *Config) String(key string) (string, bool) {
	n, ok := c.take(key)
	if !ok {
		return "", false
	}
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		c.fail(key, n, "a string")
		return "", false
	}
	if strings.ContainsFunc(n.Value, unprintableRune) {
		c.fail(key, n, "a string with no control or formatting character")
		return "", false
	}
	return n.Value, true
}

// Bool is true or false.
func (c *Config) Bool(key string) (bool, bool) {
	n, ok := c.take(key)
	if !ok {
		return false, false
	}
	if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" || (n.Value != "true" && n.Value != "false") {
		c.fail(key, n, "true or false")
		return false, false
	}
	return n.Value == "true", true
}

// Int is a decimal integer within [min, max].
func (c *Config) Int(key string, min, max int64) (int64, bool) {
	n, ok := c.take(key)
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseInt(n.Value, 10, 64)
	// Only plain decimal digits: "0755", "+5" and "1_000" are refused, never
	// read as numbers they might or might not mean.
	plain := n.Value == "0" || (n.Value != "" && n.Value[0] >= '1' && n.Value[0] <= '9' && strings.Trim(n.Value, "0123456789") == "")
	if n.Kind != yaml.ScalarNode || n.Tag != "!!int" || err != nil || !plain || v < min || v > max {
		c.fail(key, n, fmt.Sprintf("an integer from %d to %d", min, max))
		return 0, false
	}
	return v, true
}

// Duration is a Go duration string ("90s", "1h") within [min, max].
func (c *Config) Duration(key string, min, max time.Duration) (time.Duration, bool) {
	s, ok := c.String(key)
	if !ok {
		return 0, false
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < min || d > max {
		c.fail(key, c.values[key], fmt.Sprintf("a duration from %s to %s", min, max))
		return 0, false
	}
	return d, true
}

// Strings is a list of plain strings.
func (c *Config) Strings(key string) ([]string, bool) {
	n, ok := c.take(key)
	if !ok {
		return nil, false
	}
	if n.Kind != yaml.SequenceNode {
		c.fail(key, n, "a list of strings")
		return nil, false
	}
	out := make([]string, 0, len(n.Content))
	for _, item := range n.Content {
		if item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
			c.fail(key, item, "a list of strings")
			return nil, false
		}
		out = append(out, item.Value)
	}
	return out, true
}

// StringMap is a mapping of plain strings to plain strings.
func (c *Config) StringMap(key string) (map[string]string, bool) {
	n, ok := c.take(key)
	if !ok {
		return nil, false
	}
	if n.Kind != yaml.MappingNode {
		c.fail(key, n, "a mapping of strings")
		return nil, false
	}
	out := make(map[string]string, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Kind != yaml.ScalarNode || k.Tag != "!!str" || v.Kind != yaml.ScalarNode || v.Tag != "!!str" {
			c.fail(key, k, "a mapping of strings")
			return nil, false
		}
		if _, dup := out[k.Value]; dup {
			c.fail(key, k, "a mapping with no key twice")
			return nil, false
		}
		out[k.Value] = v.Value
	}
	return out, true
}

// File is an absolute, clean path to a file the Orker reads (a CA bundle).
// It joins the Orker's own paths, which no secret floor admits.
func (c *Config) File(key string) (string, bool) {
	s, ok := c.String(key)
	if !ok {
		return "", false
	}
	if !filepath.IsAbs(s) || filepath.Clean(s) != s || len(s) > 4096 {
		c.fail(key, c.values[key], "an absolute, clean path of at most 4096 bytes")
		return "", false
	}
	return s, true
}

// Errorf records a refusal of key that the type decided. It is reported
// with the getters' own once New returns.
func (c *Config) Errorf(key, format string, args ...any) {
	c.read[key] = true
	c.errs = append(c.errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
}

// Keys are the declared keys, sorted. A type that reads keys by pattern (none
// does) would need them; the parser uses them to name what nothing read.
func (c *Config) Keys() []string {
	keys := make([]string, 0, len(c.values))
	for key := range c.values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (c *Config) finish() error {
	errs := slices.Clone(c.errs)
	for _, key := range c.Keys() {
		if !c.read[key] {
			errs = append(errs, fmt.Errorf("key %q is not one this provider's type reads", printable(key)))
		}
	}
	return errors.Join(errs...)
}
