package automation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/triggers"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The keys allowed at each level, in document order.
var (
	topLevelKeys = []string{"apiVersion", "kind", "namespace", "settings", "schedules", "triggers", "webhooks"}
	scheduleKeys = []string{
		"orkflow", "version", "description", "cron", "interval", "timezone", "jitter",
		"start_at", "end_at", "overlap", "catchup_window", "pause_on_failure", "input",
	}
	triggerKeys = []string{"orkflow", "version", "description"}
	webhookKeys = []string{"url", "events", "orkflow", "task", "secret_env"}
)

const (
	// cronMaxBytes, scheduleTextMaxBytes and minCatchupWindow are the rules
	// ValidateScheduleSpec states (internal/api/schedule.go); the engine
	// checks the carried request with that function again at plan time.
	cronMaxBytes         = 200
	scheduleTextMaxBytes = 256
	minCatchupWindow     = 10 * time.Second
	secretEnvMaxBytes    = 256
)

// secretEnvPattern is the shape of an environment-variable name secret_env
// may name.
var secretEnvPattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// parser collects issues for one Parse.
type parser struct {
	issues []*orkstackv1.ValidationIssue
}

// errorAt records an ERROR on field, prefixed with the node's line when known.
func (p *parser) errorAt(field string, node *yaml.Node, format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if node != nil && node.Line > 0 {
		message = fmt.Sprintf("line %d: %s", node.Line, message)
	}
	p.issues = append(p.issues, errorIssue(field, message))
}

// mapping walks a mapping node's pairs, refusing a duplicate key and the
// first unknown key (naming the allowed ones). visit sees each known pair.
func (p *parser) mapping(field string, node *yaml.Node, allowed []string, unknown func(key *yaml.Node) bool, visit func(key string, keyNode, value *yaml.Node)) {
	seen := map[string]bool{}
	allowedSet := map[string]bool{}
	for _, key := range allowed {
		allowedSet[key] = true
	}
	reportedUnknown := false
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode, value := node.Content[i], node.Content[i+1]
		key := keyNode.Value
		child := joinField(field, key)
		if seen[key] {
			p.errorAt(child, keyNode, "%s is written twice", key)
			continue
		}
		seen[key] = true
		if !allowedSet[key] {
			if unknown != nil && unknown(keyNode) {
				continue
			}
			if !reportedUnknown {
				where := "at the top level"
				if field != "" {
					where = "in " + field
				}
				p.errorAt(child, keyNode, "unknown key %q %s; allowed: %s", key, where, joinKeys(allowed))
				reportedUnknown = true
			}
			continue
		}
		visit(key, keyNode, value)
	}
}

func joinField(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

// Parse reads and validates a document without state: shape, slugs, URLs,
// events and scope, durations, timestamps, overlap slugs, cron text, JSON
// input, secret_env. Any ERROR in the result means doc must not be planned.
func Parse(source []byte) (doc *Document, issues []*orkstackv1.ValidationIssue) {
	p := &parser{}
	decoder := yaml.NewDecoder(bytes.NewReader(source))
	var root yaml.Node
	if err := decoder.Decode(&root); err != nil {
		if errors.Is(err, io.EOF) {
			p.errorAt("", nil, "the document is empty: want apiVersion: %s, kind: %s and a namespace", dsl.APIVersion, dsl.KindAutomation)
		} else {
			p.errorAt("", nil, "the document is not valid YAML: %v", err)
		}
		return nil, p.issues
	}
	var second yaml.Node
	switch err := decoder.Decode(&second); {
	case errors.Is(err, io.EOF):
	case err != nil:
		p.errorAt("", nil, "the document is not valid YAML: %v", err)
		return nil, p.issues
	default:
		p.errorAt("", &second, "one automation document per file: a second YAML document (---) is not allowed")
		return nil, p.issues
	}
	if len(root.Content) == 0 {
		p.errorAt("", nil, "the document is empty: want apiVersion: %s, kind: %s and a namespace", dsl.APIVersion, dsl.KindAutomation)
		return nil, p.issues
	}
	body := root.Content[0]
	if body.Kind != yaml.MappingNode {
		p.errorAt("", body, "an automation document is a mapping with apiVersion, kind and namespace, got %s", describeNode(body))
		return nil, p.issues
	}
	// An Orkflow reaches this parser only through PlanAutomation or Admin →
	// Automation; it is named for what it is, not refused key by key.
	for i := 0; i+1 < len(body.Content); i += 2 {
		if key, value := body.Content[i], body.Content[i+1]; key.Value == "kind" &&
			value.Kind == yaml.ScalarNode && value.Tag == "!!str" && value.Value == dsl.KindOrkflow {
			p.errorAt("kind", value, "kind: %s is an Orkflow, not an automation document: apply it with orkctl apply or the Apply page", dsl.KindOrkflow)
			return nil, p.issues
		}
	}
	if alias := findAlias(body); alias != nil {
		p.errorAt("", alias, "anchors, aliases and merge keys are not supported in an automation document")
		return nil, p.issues
	}

	values := map[string]*yaml.Node{}
	keyNodes := map[string]*yaml.Node{}
	p.mapping("", body, topLevelKeys, nil, func(key string, keyNode, value *yaml.Node) {
		values[key] = value
		keyNodes[key] = keyNode
	})

	// Header errors stop the parse: a document whose identity is unclear
	// cannot be planned into any namespace.
	headerIssues := len(p.issues)
	p.header(values, keyNodes)
	if len(p.issues) > headerIssues {
		return nil, p.issues
	}
	doc = &Document{Namespace: values["namespace"].Value}

	if node, ok := values["settings"]; ok {
		doc.Settings = p.settings(doc.Namespace, keyNodes["settings"], node)
	}
	if node, ok := values["schedules"]; ok {
		doc.Schedules = map[string]ScheduleEntry{}
		p.section("schedules", "schedule", keyNodes["schedules"], node, func(name string, entry *yaml.Node) {
			if parsed, ok := p.schedule(name, entry); ok {
				doc.Schedules[name] = parsed
			}
		})
	}
	if node, ok := values["triggers"]; ok {
		doc.Triggers = map[string]TriggerEntry{}
		p.section("triggers", "trigger", keyNodes["triggers"], node, func(name string, entry *yaml.Node) {
			if parsed, ok := p.trigger(name, entry); ok {
				doc.Triggers[name] = parsed
			}
		})
	}
	if node, ok := values["webhooks"]; ok {
		doc.Webhooks = map[string]WebhookEntry{}
		p.section("webhooks", "webhook", keyNodes["webhooks"], node, func(name string, entry *yaml.Node) {
			if parsed, ok := p.webhook(doc.Namespace, name, entry); ok {
				doc.Webhooks[name] = parsed
			}
		})
	}
	if HasErrors(p.issues) {
		return nil, p.issues
	}
	return doc, p.issues
}

// findAlias returns the first alias, anchor or merge key under node.
func findAlias(node *yaml.Node) *yaml.Node {
	if node.Kind == yaml.AliasNode || node.Anchor != "" || (node.Kind == yaml.ScalarNode && node.Tag == "!!merge") {
		return node
	}
	for _, child := range node.Content {
		if found := findAlias(child); found != nil {
			return found
		}
	}
	return nil
}

func (p *parser) header(values, keyNodes map[string]*yaml.Node) {
	apiVersion, ok := values["apiVersion"]
	switch {
	case !ok:
		p.errorAt("apiVersion", nil, "apiVersion is required: want %s", dsl.APIVersion)
	case apiVersion.Kind != yaml.ScalarNode || apiVersion.Value != dsl.APIVersion:
		p.errorAt("apiVersion", apiVersion, "apiVersion %s is not supported: want %s", describeNode(apiVersion), dsl.APIVersion)
	}
	kind, ok := values["kind"]
	switch {
	case !ok:
		p.errorAt("kind", nil, "kind is required: want %s", dsl.KindAutomation)
	case kind.Kind != yaml.ScalarNode || kind.Value != dsl.KindAutomation:
		p.errorAt("kind", kind, "kind %s is not an automation document: want %s", describeNode(kind), dsl.KindAutomation)
	}
	namespace, ok := values["namespace"]
	switch {
	case !ok:
		p.errorAt("namespace", nil, "namespace is required: the document names the one namespace it describes, "+
			"and it is never inferred from -n, ORK_NAMESPACE or a saved default")
	default:
		name, err := stringScalar(namespace)
		if err != nil {
			p.errorAt("namespace", namespace, "namespace %v", err)
			return
		}
		if err := dsl.ValidSlug("namespace", name); err != nil {
			p.errorAt("namespace", namespace, "%v", err)
		}
	}
}

// section reads one of schedules, triggers, webhooks: a mapping of slug names
// to entries.
func (p *parser) section(section, kind string, keyNode, node *yaml.Node, entry func(name string, node *yaml.Node)) {
	if isNull(node) {
		p.errorAt(section, keyNode, "%s is null: write `%s: {}` to manage no %s, or remove the key to leave %s unmanaged",
			section, section, section, section)
		return
	}
	if node.Kind != yaml.MappingNode {
		p.errorAt(section, node, "%s must be a mapping of name: entry, got %s", section, describeNode(node))
		return
	}
	seen := map[string]bool{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		nameNode, value := node.Content[i], node.Content[i+1]
		name := nameNode.Value
		field := section + "." + name
		if seen[name] {
			p.errorAt(field, nameNode, "%s %q is declared twice", kind, name)
			continue
		}
		seen[name] = true
		if err := dsl.ValidSlug(kind+" name", name); err != nil {
			p.errorAt(field, nameNode, "%v", err)
			continue
		}
		if value.Kind != yaml.MappingNode {
			p.errorAt(field, value, "%s %q must be a mapping, got %s", kind, name, describeNode(value))
			continue
		}
		entry(name, value)
	}
}

func (p *parser) settings(namespace string, keyNode, node *yaml.Node) *Settings {
	if isNull(node) {
		p.errorAt("settings", keyNode, "settings is null: write `settings: {}` to manage no setting, or remove the key to leave settings unmanaged")
		return nil
	}
	if node.Kind != yaml.MappingNode {
		p.errorAt("settings", node, "settings must be a mapping of setting: value, got %s", describeNode(node))
		return nil
	}
	settings := &Settings{Present: map[string]bool{}, Values: &orkstackv1.Namespace{Name: namespace}}
	refuseHold := func(key *yaml.Node) bool {
		if key.Value != "hold" {
			return false
		}
		p.errorAt("settings.hold", key, "%s", holdRefusal)
		return true
	}
	p.mapping("settings", node, settingKeys(), refuseHold, func(key string, keyNode, value *yaml.Node) {
		row, _ := settingRowFor(key)
		if err := row.parse(value, settings.Values); err != nil {
			p.errorAt("settings."+key, value, "%s %v", key, trimKeyPrefix(key, err))
			return
		}
		settings.Present[key] = true
	})
	return settings
}

// trimKeyPrefix keeps "settings.run_retention: line 3: run_retention must be
// …" from naming the key twice.
func trimKeyPrefix(key string, err error) string {
	return strings.TrimPrefix(strings.TrimPrefix(err.Error(), key+" "), key+": ")
}

func (p *parser) schedule(name string, node *yaml.Node) (ScheduleEntry, bool) {
	field := "schedules." + name
	entry := ScheduleEntry{Spec: &orkstackv1.ScheduleSpec{Overlap: orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP}}
	before := len(p.issues)
	present := map[string]bool{}
	p.mapping(field, node, scheduleKeys, nil, func(key string, keyNode, value *yaml.Node) {
		present[key] = true
		at := field + "." + key
		switch key {
		case "orkflow":
			entry.Orkflow = p.slug(at, "orkflow", value)
		case "version":
			version, err := versionScalar(value)
			if err != nil {
				p.errorAt(at, value, "version %v", err)
			}
			entry.Version = version
		case "description":
			text, err := stringScalar(value)
			if err != nil {
				p.errorAt(at, value, "description %v", err)
			} else if err := scheduleText(text); err != nil {
				p.errorAt(at, value, "description %v", err)
			}
			entry.Description = text
		case "cron":
			entry.Spec.Cron = p.crons(at, value)
		case "interval":
			if d, ok := p.duration(at, key, value); ok {
				if d <= 0 {
					p.errorAt(at, value, "interval must be positive, got %s", d)
				}
				entry.Spec.Interval = durationpb.New(d)
			}
		case "timezone":
			tz, err := stringScalar(value)
			if err != nil {
				p.errorAt(at, value, "timezone %v", err)
			} else if _, err := time.LoadLocation(tz); err != nil || tz == "" {
				p.errorAt(at, value, "timezone %q is not an IANA time zone", tz)
			}
			entry.Spec.Timezone = tz
		case "jitter":
			if d, ok := p.duration(at, key, value); ok {
				if d < 0 {
					p.errorAt(at, value, "jitter must not be negative, got %s", d)
				}
				entry.Spec.Jitter = durationpb.New(d)
			}
		case "start_at":
			entry.Spec.StartAt = p.timestamp(at, key, value)
		case "end_at":
			entry.Spec.EndAt = p.timestamp(at, key, value)
		case "overlap":
			slug, err := stringScalar(value)
			policy, ok := parseOverlap(slug)
			if err != nil || !ok {
				p.errorAt(at, value, "overlap %s is not one of %s", describeNode(value), joinKeys(overlapSlugs()))
			}
			entry.Spec.Overlap = policy
		case "catchup_window":
			if d, ok := p.duration(at, key, value); ok {
				if d < minCatchupWindow {
					p.errorAt(at, value, "catchup_window must be at least %s (the cluster's minimum), got %s", minCatchupWindow, d)
				}
				entry.Spec.CatchupWindow = durationpb.New(d)
			}
		case "pause_on_failure":
			flag, err := strconv.ParseBool(value.Value)
			if value.Kind != yaml.ScalarNode || value.Tag != "!!bool" || err != nil {
				p.errorAt(at, value, "pause_on_failure must be true or false, got %s", describeNode(value))
			}
			entry.Spec.PauseOnFailure = flag
		case "input":
			entry.Input = p.input(at, value)
		}
	})
	for _, key := range scheduleKeys {
		if present[key] {
			entry.Keys = append(entry.Keys, key)
		}
	}
	if !present["orkflow"] {
		p.errorAt(field+".orkflow", node, "schedule %q needs an orkflow: the orkflow every fire starts", name)
	}
	if !present["cron"] && !present["interval"] {
		p.errorAt(field, node, "schedule %q needs a cron or an interval: when it fires", name)
	}
	if start, end := entry.Spec.GetStartAt(), entry.Spec.GetEndAt(); start != nil && end != nil && start.AsTime().After(end.AsTime()) {
		p.errorAt(field+".start_at", node, "start_at %s is after end_at %s",
			renderTimestamp(start), renderTimestamp(end))
	}
	return entry, len(p.issues) == before
}

// crons reads `cron`: one string, or a list of strings.
func (p *parser) crons(field string, node *yaml.Node) []string {
	var items []*yaml.Node
	switch node.Kind {
	case yaml.ScalarNode:
		items = []*yaml.Node{node}
	case yaml.SequenceNode:
		items = node.Content
	default:
		p.errorAt(field, node, "cron must be a string or a list of strings, got %s", describeNode(node))
		return nil
	}
	out := make([]string, 0, len(items))
	for i, item := range items {
		at := field
		if node.Kind == yaml.SequenceNode {
			at = fmt.Sprintf("%s[%d]", field, i)
		}
		expr, err := stringScalar(item)
		if err != nil {
			p.errorAt(at, item, "cron %v", err)
			continue
		}
		if err := cronText(expr); err != nil {
			p.errorAt(at, item, "%v", err)
			continue
		}
		out = append(out, expr)
	}
	return out
}

// cronText is the cron rule checkable without the cluster: non-empty, at most
// 200 bytes, no control character, no CRON_TZ=/TZ= prefix.
func cronText(expr string) error {
	if strings.TrimSpace(expr) == "" {
		return errors.New("a cron expression is required")
	}
	if len(expr) > cronMaxBytes {
		return fmt.Errorf("cron expression is %d bytes; the limit is %d", len(expr), cronMaxBytes)
	}
	for i := 0; i < len(expr); i++ {
		if b := expr[i]; b < 0x20 || b == 0x7f {
			return fmt.Errorf("cron expression contains control character 0x%02x at byte %d", b, i)
		}
	}
	upper := strings.ToUpper(strings.TrimSpace(expr))
	if strings.HasPrefix(upper, "CRON_TZ=") || strings.HasPrefix(upper, "TZ=") {
		return errors.New("set timezone instead of a CRON_TZ= prefix")
	}
	return nil
}

// scheduleText is the rule for a schedule's description: at most 256 bytes,
// valid UTF-8, no control character.
func scheduleText(value string) error {
	if len(value) > scheduleTextMaxBytes {
		return fmt.Errorf("is %d bytes; the limit is %d", len(value), scheduleTextMaxBytes)
	}
	if !utf8.ValidString(value) {
		return errors.New("is not valid UTF-8")
	}
	for i := 0; i < len(value); i++ {
		if b := value[i]; b < 0x20 || b == 0x7f {
			return fmt.Errorf("contains control character 0x%02x at byte %d", b, i)
		}
	}
	return nil
}

func (p *parser) duration(field, key string, node *yaml.Node) (time.Duration, bool) {
	text, err := stringScalar(node)
	if err != nil {
		p.errorAt(field, node, "%s must be a duration such as 30s or 1h, got %s", key, describeNode(node))
		return 0, false
	}
	d, err := time.ParseDuration(text)
	if err != nil {
		p.errorAt(field, node, "%s %q is not a duration such as 30s or 1h", key, text)
		return 0, false
	}
	return d, true
}

func (p *parser) timestamp(field, key string, node *yaml.Node) *timestamppb.Timestamp {
	if node.Kind != yaml.ScalarNode || (node.Tag != "!!str" && node.Tag != "!!timestamp") {
		p.errorAt(field, node, "%s must be an RFC 3339 time such as 2026-01-02T03:04:05Z, got %s", key, describeNode(node))
		return nil
	}
	at, err := time.Parse(time.RFC3339, node.Value)
	if err != nil {
		p.errorAt(field, node, "%s %q is not an RFC 3339 time such as 2026-01-02T03:04:05Z", key, node.Value)
		return nil
	}
	return timestamppb.New(at)
}

// input reads a schedule's run input: a YAML mapping, converted to JSON by
// the strict mapping (dsl.AuthoredJSON; docs/JSON.md, "Data written in an
// authored document"). A string is refused — a JSON object written as a
// string was the shape before — and the message names the YAML form; {} is
// recorded as unset.
func (p *parser) input(field string, node *yaml.Node) *orkstackv1.Payload {
	switch {
	case isNull(node):
		p.errorAt(field, node, "input is null: write the run input as a mapping, or remove the key")
		return nil
	case node.Kind == yaml.ScalarNode && node.Tag == "!!str":
		var object map[string]any
		if json.Unmarshal([]byte(node.Value), &object) == nil && object != nil {
			example := strings.TrimSpace(node.Value)
			if len(example) > 60 {
				cut := 60
				for cut > 0 && !utf8.RuneStart(example[cut]) {
					cut--
				}
				example = example[:cut] + "…"
			}
			p.errorAt(field, node, "input is a string: write the object as YAML instead — without the quotes, %s is a YAML mapping (docs/AUTOMATION.md, \"schedules\")", example)
			return nil
		}
		p.errorAt(field, node, "input must be a YAML mapping of the run's input, e.g. input: {mode: full}, got %s", describeNode(node))
		return nil
	case node.Kind != yaml.MappingNode:
		p.errorAt(field, node, "input must be a YAML mapping of the run's input, got %s", describeNode(node))
		return nil
	}
	value, err := dsl.AuthoredJSON(node, field)
	if err != nil {
		var refused *dsl.AuthoredDataError
		if errors.As(err, &refused) {
			p.issues = append(p.issues, errorIssue(refused.Path, fmt.Sprintf("line %d: %s", refused.Line, refused.Reason)))
		} else {
			p.errorAt(field, node, "%v", err)
		}
		return nil
	}
	object := value.(map[string]any)
	if len(object) == 0 {
		return nil
	}
	data, err := structpb.NewStruct(object)
	if err != nil {
		p.errorAt(field, node, "input cannot be carried as a run input: %v", err)
		return nil
	}
	return &orkstackv1.Payload{Data: data}
}

// slug reads a scalar that must be a slug.
func (p *parser) slug(field, kind string, node *yaml.Node) string {
	value, err := stringScalar(node)
	if err != nil {
		p.errorAt(field, node, "%s %v", kind, err)
		return ""
	}
	if err := dsl.ValidSlug(kind, value); err != nil {
		p.errorAt(field, node, "%v", err)
	}
	return value
}

func (p *parser) trigger(name string, node *yaml.Node) (TriggerEntry, bool) {
	field := "triggers." + name
	before := len(p.issues)
	if err := triggers.ValidateName(name); err != nil {
		p.errorAt(field, node, "%v", err)
	}
	var entry TriggerEntry
	present := map[string]bool{}
	p.mapping(field, node, triggerKeys, nil, func(key string, keyNode, value *yaml.Node) {
		present[key] = true
		at := field + "." + key
		switch key {
		case "orkflow":
			entry.Orkflow = p.slug(at, "orkflow", value)
		case "version":
			version, err := versionScalar(value)
			if err != nil {
				p.errorAt(at, value, "version %v", err)
			}
			entry.Version = version
		case "description":
			text, err := stringScalar(value)
			if err != nil {
				p.errorAt(at, value, "description %v", err)
			}
			entry.Description = text
		}
	})
	for _, key := range triggerKeys {
		if present[key] {
			entry.Keys = append(entry.Keys, key)
		}
	}
	if !present["orkflow"] {
		p.errorAt(field+".orkflow", node, "trigger %q needs an orkflow: the orkflow every invoke starts", name)
	}
	return entry, len(p.issues) == before
}

func (p *parser) webhook(namespace, name string, node *yaml.Node) (WebhookEntry, bool) {
	field := "webhooks." + name
	before := len(p.issues)
	var entry WebhookEntry
	present := map[string]bool{}
	refuseNamespace := func(key *yaml.Node) bool {
		if key.Value != "namespace" {
			return false
		}
		p.errorAt(field+".namespace", key, "namespace is not a webhook key: a webhook in a namespace's document is "+
			"scoped to that namespace (%s), always", namespace)
		return true
	}
	var eventsNode *yaml.Node
	p.mapping(field, node, webhookKeys, refuseNamespace, func(key string, keyNode, value *yaml.Node) {
		present[key] = true
		at := field + "." + key
		switch key {
		case "url":
			raw, err := stringScalar(value)
			if err != nil {
				p.errorAt(at, value, "url %v", err)
				return
			}
			target, err := webhooks.ValidateURL(raw)
			if err != nil {
				p.errorAt(at, value, "%v", err)
				return
			}
			entry.URL = target
		case "events":
			eventsNode = value
			entry.Events = p.events(at, value)
		case "orkflow":
			entry.Orkflow = p.slug(at, "orkflow", value)
		case "task":
			entry.Task = p.slug(at, "task", value)
		case "secret_env":
			env, err := stringScalar(value)
			switch {
			case err != nil:
				p.errorAt(at, value, "secret_env %v", err)
			case len(env) > secretEnvMaxBytes:
				p.errorAt(at, value, "secret_env %q is longer than %d bytes", env, secretEnvMaxBytes)
			case !secretEnvPattern.MatchString(env):
				p.errorAt(at, value, "secret_env %q must name an environment variable matching %s", env, secretEnvPattern)
			}
			entry.SecretEnv = env
		}
	})
	for _, key := range webhookKeys {
		if present[key] {
			entry.Keys = append(entry.Keys, key)
		}
	}
	if !present["url"] {
		p.errorAt(field+".url", node, "webhook %q needs a url: where deliveries are POSTed", name)
	}
	if len(p.issues) == before {
		if err := webhooks.ValidateScope(namespace, entry.Orkflow, entry.Task, entry.Events); err != nil {
			anchor := node
			if eventsNode != nil {
				anchor = eventsNode
			}
			p.errorAt(field+".task", anchor, "%v", err)
		}
		if err := webhooks.ValidateName(name, namespace); err != nil {
			p.errorAt(field, node, "%v", err)
		}
	}
	return entry, len(p.issues) == before
}

// events reads a list of event names, normalised to lifecycle order with
// duplicates removed, exactly as a registration normalises them.
func (p *parser) events(field string, node *yaml.Node) []webhooks.Event {
	if node.Kind != yaml.SequenceNode {
		p.errorAt(field, node, "events must be a list of event names, got %s", describeNode(node))
		return nil
	}
	wanted := map[webhooks.Event]bool{}
	for i, item := range node.Content {
		at := fmt.Sprintf("%s[%d]", field, i)
		raw, err := stringScalar(item)
		if err != nil {
			p.errorAt(at, item, "event %v", err)
			continue
		}
		event, err := webhooks.ParseEvent(raw)
		if err != nil {
			p.errorAt(at, item, "%v", err)
			continue
		}
		wanted[event] = true
	}
	var out []webhooks.Event
	for _, known := range webhooks.KnownEvents() {
		if wanted[known] {
			out = append(out, known)
		}
	}
	return out
}
