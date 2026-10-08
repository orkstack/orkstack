package automation

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ExportError is an export refused rather than written without an object:
// an export missing an object would delete it when applied with --prune.
type ExportError struct{ Message string }

func (e *ExportError) Error() string { return e.Message }

// Export renders live as a document — the document whose plan against live
// has only UNCHANGED and UNMANAGED steps — and lists the unnamed objects. It
// is deterministic: the same live state exports the same bytes. It refuses,
// never omits, when a schedule row is unavailable or two live objects share a
// name.
func Export(namespace string, live Live) (string, []*orkstackv1.AutomationStep, error) {
	ns := artifacts.ResolveNamespace(namespace)
	settings := live.Namespace
	if settings == nil {
		if ns != artifacts.DefaultNamespace {
			return "", nil, &ExportError{Message: fmt.Sprintf("cannot export namespace %q: it does not exist", ns)}
		}
		settings = artifacts.DefaultNamespaceDocument()
	}

	schedules := append([]*orkstackv1.Schedule(nil), live.Schedules...)
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].GetName() < schedules[j].GetName() })
	for i, s := range schedules {
		if reason := s.GetUnavailableReason(); reason != "" {
			return "", nil, &ExportError{Message: fmt.Sprintf(
				"cannot export namespace %q: schedule %q could not be read (%s); an export that omitted it would "+
					"delete it when applied with --prune", ns, s.GetName(), reason)}
		}
		if i > 0 && schedules[i-1].GetName() == s.GetName() {
			return "", nil, duplicateExportError(ns, "schedule", s.GetName(),
				[]string{schedules[i-1].GetId(), s.GetId()}, fmt.Sprintf("orkctl schedule delete NAME -n %s", ns))
		}
	}

	var named []*orkstackv1.Trigger
	var unmanaged []*orkstackv1.AutomationStep
	byTriggerName := map[string][]string{}
	for _, t := range live.Triggers {
		if t.GetName() == "" {
			unmanaged = append(unmanaged, unmanagedTrigger(t, ns))
			continue
		}
		named = append(named, t)
		byTriggerName[t.GetName()] = append(byTriggerName[t.GetName()], t.GetId())
	}
	for _, name := range sortedKeys(byTriggerName) {
		if ids := byTriggerName[name]; len(ids) > 1 {
			return "", nil, duplicateExportError(ns, "trigger", name, ids, fmt.Sprintf("orkctl trigger delete ID -n %s", ns))
		}
	}
	sort.Slice(named, func(i, j int) bool { return named[i].GetName() < named[j].GetName() })

	var hooks []liveHook
	byHookName := map[string][]string{}
	for _, hook := range live.Webhooks {
		if hook.GetNamespace() == "" || artifacts.ResolveNamespace(hook.GetNamespace()) != ns {
			continue
		}
		h := toLiveHook(hook)
		if hook.GetName() == "" {
			unmanaged = append(unmanaged, unmanagedWebhook(h))
			continue
		}
		hooks = append(hooks, h)
		byHookName[hook.GetName()] = append(byHookName[hook.GetName()], hook.GetId())
	}
	for _, name := range sortedKeys(byHookName) {
		if ids := byHookName[name]; len(ids) > 1 {
			return "", nil, duplicateExportError(ns, "webhook", name, ids, "orkctl webhook delete ID")
		}
	}
	sort.Slice(hooks, func(i, j int) bool { return hooks[i].proto.GetName() < hooks[j].proto.GetName() })
	sortSteps(unmanaged)

	header := mapping(
		plainNode("apiVersion"), plainNode(APIVersion),
		plainNode("kind"), plainNode(Kind),
		plainNode("namespace"), plainNode(ns),
	)
	settingsNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, row := range namespaceSettings {
		settingsNode.Content = append(settingsNode.Content, plainNode(row.key), row.export(settings))
	}

	schedulesNode := emptyMapping()
	for _, s := range schedules {
		key := plainNode(s.GetName())
		if s.GetPaused() {
			if note := s.GetNote(); note != "" {
				key.LineComment = fmt.Sprintf("# paused: %q — operational state; not managed here", note)
			} else {
				key.LineComment = "# paused — operational state; not managed here"
			}
		}
		addPair(schedulesNode, key, exportSchedule(s))
	}
	triggersNode := emptyMapping()
	for _, t := range named {
		entry := mapping(plainNode("orkflow"), strNode(t.GetOrkflowName()))
		if v := t.GetOrkflowVersion(); v != "" {
			addPair(entry, plainNode("version"), strNode(v))
		}
		if d := t.GetDescription(); d != "" {
			addPair(entry, plainNode("description"), strNode(d))
		}
		addPair(triggersNode, plainNode(t.GetName()), entry)
	}
	webhooksNode := emptyMapping()
	for _, h := range hooks {
		entry := mapping(plainNode("url"), strNode(h.url))
		if len(h.events) > 0 {
			events := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Style: yaml.FlowStyle}
			for _, event := range h.events {
				events.Content = append(events.Content, plainNode(string(event)))
			}
			addPair(entry, plainNode("events"), events)
		}
		if o := h.proto.GetOrkflowName(); o != "" {
			addPair(entry, plainNode("orkflow"), strNode(o))
		}
		if task := h.proto.GetTaskId(); task != "" {
			addPair(entry, plainNode("task"), strNode(task))
		}
		addPair(webhooksNode, plainNode(h.proto.GetName()), entry)
	}

	var out strings.Builder
	for i, section := range []*yaml.Node{
		header,
		mapping(plainNode("settings"), settingsNode),
		mapping(plainNode("schedules"), schedulesNode),
		mapping(plainNode("triggers"), triggersNode),
		mapping(plainNode("webhooks"), webhooksNode),
	} {
		if i > 0 {
			out.WriteString("\n")
		}
		text, err := encodeNode(section)
		if err != nil {
			return "", nil, fmt.Errorf("render namespace %q: %w", ns, err)
		}
		out.WriteString(text)
	}
	if len(unmanaged) > 0 {
		out.WriteString("\n")
		out.WriteString("# Not managed by this document: these objects have no name, and an unnamed object is\n")
		out.WriteString("# outside every automation document — no apply touches it, --prune included. Name one\n")
		out.WriteString("# in place (its token or secret is kept), then export again to see it above:\n")
		for _, step := range unmanaged {
			out.WriteString("#   " + namingCommand(step, ns) + "   (" + step.GetSummary() + ")\n")
		}
	}
	return out.String(), unmanaged, nil
}

func duplicateExportError(ns, kind, name string, ids []string, fix string) error {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	return &ExportError{Message: fmt.Sprintf(
		"cannot export namespace %q: %s name %q is held by %s %ss (%s); delete one with `%s` and export again",
		ns, kind, name, countWord(len(sorted)), kind, strings.Join(sorted, ", "), fix)}
}

// namingCommand is the command that names an unnamed object in place.
func namingCommand(step *orkstackv1.AutomationStep, ns string) string {
	if step.GetKind() == kindTrigger {
		return fmt.Sprintf("orkctl trigger update %s -n %s --name NAME", step.GetId(), ns)
	}
	return fmt.Sprintf("orkctl webhook update %s --name NAME", step.GetId())
}

func unmanagedTrigger(t *orkstackv1.Trigger, ns string) *orkstackv1.AutomationStep {
	return &orkstackv1.AutomationStep{
		Action: actionUnmanaged, Kind: kindTrigger, Id: t.GetId(),
		Summary: triggerSummary(t.GetOrkflowName(), t.GetOrkflowVersion()),
		Notes: []string{fmt.Sprintf("has no name; name it with `orkctl trigger update %s -n %s --name NAME` to manage it here",
			t.GetId(), ns)},
	}
}

func unmanagedWebhook(h liveHook) *orkstackv1.AutomationStep {
	return &orkstackv1.AutomationStep{
		Action: actionUnmanaged, Kind: kindWebhook, Id: h.proto.GetId(),
		Summary: webhookSummary(h.url, h.proto.GetNamespace(), h.proto.GetOrkflowName(), h.proto.GetTaskId()),
		Notes: []string{fmt.Sprintf("has no name; name it with `orkctl webhook update %s --name NAME` to manage it here",
			h.proto.GetId())},
	}
}

// exportSchedule writes one schedule entry: keys in the `schedules` table's order,
// optional keys at their absent value omitted, except overlap, always written.
func exportSchedule(s *orkstackv1.Schedule) *yaml.Node {
	spec := s.GetSpec()
	entry := mapping(plainNode("orkflow"), strNode(s.GetOrkflowName()))
	if v := s.GetOrkflowVersion(); v != "" {
		addPair(entry, plainNode("version"), strNode(v))
	}
	if d := s.GetDescription(); d != "" {
		addPair(entry, plainNode("description"), strNode(d))
	}
	if crons := spec.GetCron(); len(crons) > 0 {
		// A cron expression is quoted: "*" opens an alias in plain YAML, and
		// a reader should not have to know when that bites.
		list := listNode(crons)
		for _, item := range list.Content {
			item.Style = yaml.DoubleQuotedStyle
		}
		addPair(entry, plainNode("cron"), list)
	}
	if iv := spec.GetInterval(); iv != nil {
		addPair(entry, plainNode("interval"), plainNode(shortDuration(iv.AsDuration())))
	}
	if tz := spec.GetTimezone(); tz != "" {
		addPair(entry, plainNode("timezone"), strNode(tz))
	}
	if j := spec.GetJitter(); j.AsDuration() != 0 {
		addPair(entry, plainNode("jitter"), plainNode(shortDuration(j.AsDuration())))
	}
	if at := spec.GetStartAt(); at != nil {
		addPair(entry, plainNode("start_at"), strNode(at.AsTime().UTC().Format(time.RFC3339)))
	}
	if at := spec.GetEndAt(); at != nil {
		addPair(entry, plainNode("end_at"), strNode(at.AsTime().UTC().Format(time.RFC3339)))
	}
	addPair(entry, plainNode("overlap"), plainNode(overlapSlug(spec.GetOverlap())))
	if cw := spec.GetCatchupWindow(); cw != nil {
		addPair(entry, plainNode("catchup_window"), plainNode(shortDuration(cw.AsDuration())))
	}
	if spec.GetPauseOnFailure() {
		addPair(entry, plainNode("pause_on_failure"), &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
	}
	if !inputEmpty(s.GetInput()) {
		addPair(entry, plainNode("input"), &yaml.Node{
			Kind: yaml.ScalarNode, Tag: "!!str", Value: compactInput(s.GetInput()), Style: yaml.SingleQuotedStyle,
		})
	}
	return entry
}

func mapping(pairs ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: pairs}
}

func emptyMapping() *yaml.Node {
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Style: yaml.FlowStyle}
}

// addPair appends a pair; a mapping that gains one is written in block style.
func addPair(m, key, value *yaml.Node) {
	m.Style = 0
	m.Content = append(m.Content, key, value)
}

func encodeNode(node *yaml.Node) (string, error) {
	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(node); err != nil {
		return "", err
	}
	if err := encoder.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}
