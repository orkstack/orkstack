package automation

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

const (
	kindNamespace = orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_NAMESPACE
	kindSchedule  = orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_SCHEDULE
	kindTrigger   = orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER
	kindWebhook   = orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_WEBHOOK

	actionCreate     = orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE
	actionUpdate     = orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE
	actionDelete     = orkstackv1.AutomationAction_AUTOMATION_ACTION_DELETE
	actionUnchanged  = orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED
	actionUndeclared = orkstackv1.AutomationAction_AUTOMATION_ACTION_UNDECLARED
	actionUnmanaged  = orkstackv1.AutomationAction_AUTOMATION_ACTION_UNMANAGED
)

// operationalNote is the tail of every note about state a document does not
// manage.
const operationalNote = " — operational state; this document does not manage it"

// secretEnvNote is what a secret_env on an existing subscription earns.
const secretEnvNote = "secret_env applies only when the subscription is created; this one keeps its secret"

// planner holds one Plan's inputs and what it has produced so far.
type planner struct {
	doc       *Document
	live      Live
	opts      Options
	namespace string
	steps     []*orkstackv1.AutomationStep
	issues    []*orkstackv1.ValidationIssue
}

// Plan compares doc with live. Pure: the same inputs give the same steps, in
// the same order, with the same digest. The planner never sees a parse ERROR;
// the caller stops first.
func Plan(doc *Document, live Live, opts Options) (steps []*orkstackv1.AutomationStep, issues []*orkstackv1.ValidationIssue) {
	p := &planner{doc: doc, live: live, opts: opts, namespace: artifacts.ResolveNamespace(doc.Namespace)}
	p.settings()
	if doc.Schedules != nil {
		p.schedules()
	}
	if doc.Triggers != nil {
		p.triggers()
	}
	if doc.Webhooks != nil {
		p.webhooks()
	}
	p.references()
	sortSteps(p.steps)
	return p.steps, p.issues
}

// ---------------------------------------------------------------------------
// settings
// ---------------------------------------------------------------------------

func (p *planner) settings() {
	live := p.live.Namespace
	if live == nil && p.namespace == artifacts.DefaultNamespace {
		// default always resolves and is never created.
		live = artifacts.DefaultNamespaceDocument()
	}
	if live == nil {
		desired := overlay(p.namespace, nil, p.doc.Settings)
		desired.Name = p.namespace
		step := &orkstackv1.AutomationStep{
			Action: actionCreate, Kind: kindNamespace, Name: p.namespace,
			Request: &orkstackv1.AutomationStep_CreateNamespace{
				CreateNamespace: &orkstackv1.CreateNamespaceRequest{Namespace: desired},
			},
		}
		if p.doc.Settings != nil {
			for _, row := range namespaceSettings {
				if p.doc.Settings.Present[row.key] {
					step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: row.key, After: row.render(desired)})
				}
			}
		}
		p.steps = append(p.steps, step)
		return
	}
	if p.doc.Settings == nil {
		return
	}
	desired := overlay(p.namespace, live, p.doc.Settings)
	changes := NamespaceFieldChanges(live, desired, p.doc.Settings.Present)
	step := &orkstackv1.AutomationStep{Action: actionUnchanged, Kind: kindNamespace, Name: p.namespace}
	if len(changes) > 0 {
		step.Action = actionUpdate
		step.Changes = changes
		// desired is the live document with the present keys overlaid: it
		// carries the live etag, so a namespace changed between plan and
		// apply fails its compare-and-swap instead of being overwritten.
		step.Request = &orkstackv1.AutomationStep_UpdateNamespace{
			UpdateNamespace: &orkstackv1.UpdateNamespaceRequest{Namespace: desired},
		}
	}
	if hold := live.GetHold(); hold != nil {
		since := "an unknown time"
		if hold.GetSince() != nil {
			since = renderTimestamp(hold.GetSince())
		}
		step.Notes = append(step.Notes, fmt.Sprintf("on hold since %s: %s%s", since, hold.GetReason(), operationalNote))
	}
	p.steps = append(p.steps, step)
}

// ---------------------------------------------------------------------------
// schedules
// ---------------------------------------------------------------------------

// scheduleValues is one schedule's comparable fields, from a document entry
// or a live row.
type scheduleValues struct {
	orkflow, version, description string
	spec                          *orkstackv1.ScheduleSpec
	input                         *orkstackv1.Payload
}

func entryValues(entry ScheduleEntry) scheduleValues {
	return scheduleValues{orkflow: entry.Orkflow, version: entry.Version, description: entry.Description, spec: entry.Spec, input: entry.Input}
}

func liveValues(s *orkstackv1.Schedule) scheduleValues {
	return scheduleValues{orkflow: s.GetOrkflowName(), version: s.GetOrkflowVersion(), description: s.GetDescription(), spec: s.GetSpec(), input: s.GetInput()}
}

// scheduleComparedKeys are the keys an UPDATE compares; orkflow is
// immutable and handled apart.
var scheduleComparedKeys = []string{
	"version", "description", "cron", "interval", "timezone", "jitter",
	"start_at", "end_at", "overlap", "catchup_window", "pause_on_failure", "input",
}

// renderScheduleKey renders one key of a schedule for a plan.
func renderScheduleKey(key string, v scheduleValues) string {
	spec := v.spec
	switch key {
	case "orkflow":
		return renderString(v.orkflow)
	case "version":
		return renderVersion(v.version)
	case "description":
		return renderString(v.description)
	case "cron":
		return renderStrings(spec.GetCron())
	case "interval":
		return renderDuration(spec.GetInterval())
	case "timezone":
		return renderString(spec.GetTimezone())
	case "jitter":
		if spec.GetJitter().AsDuration() == 0 {
			return unsetLabel
		}
		return renderDuration(spec.GetJitter())
	case "start_at":
		return renderTimestamp(spec.GetStartAt())
	case "end_at":
		return renderTimestamp(spec.GetEndAt())
	case "overlap":
		return overlapSlug(spec.GetOverlap())
	case "catchup_window":
		return renderDuration(spec.GetCatchupWindow())
	case "pause_on_failure":
		return strconv.FormatBool(spec.GetPauseOnFailure())
	case "input":
		return renderInput(v.input)
	}
	return ""
}

// scheduleKeyEqual compares one key of two schedules with the plan's rules:
// UNSPECIFIED overlap is SKIP, cron is an ordered list, a nil and a zero
// jitter are equal, input compares as a proto.
func scheduleKeyEqual(key string, a, b scheduleValues) bool {
	x, y := a.spec, b.spec
	switch key {
	case "version":
		return a.version == b.version
	case "description":
		return a.description == b.description
	case "cron":
		return orderedEqual(x.GetCron(), y.GetCron())
	case "interval":
		return optionalDurationEqual(x.GetInterval(), y.GetInterval())
	case "timezone":
		return x.GetTimezone() == y.GetTimezone()
	case "jitter":
		return x.GetJitter().AsDuration() == y.GetJitter().AsDuration()
	case "start_at":
		return timestampEqual(x.GetStartAt(), y.GetStartAt())
	case "end_at":
		return timestampEqual(x.GetEndAt(), y.GetEndAt())
	case "overlap":
		return overlapSlug(x.GetOverlap()) == overlapSlug(y.GetOverlap())
	case "catchup_window":
		return optionalDurationEqual(x.GetCatchupWindow(), y.GetCatchupWindow())
	case "pause_on_failure":
		return x.GetPauseOnFailure() == y.GetPauseOnFailure()
	case "input":
		if inputEmpty(a.input) || inputEmpty(b.input) {
			return inputEmpty(a.input) == inputEmpty(b.input)
		}
		return proto.Equal(a.input.GetData(), b.input.GetData())
	}
	return true
}

func orderedEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func timestampEqual(a, b *timestamppb.Timestamp) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.AsTime().Equal(b.AsTime())
}

func pausedNote(s *orkstackv1.Schedule) string {
	if s.GetNote() == "" {
		return "paused" + operationalNote
	}
	return fmt.Sprintf("paused: %q%s", s.GetNote(), operationalNote)
}

func (p *planner) schedules() {
	byName := map[string][]*orkstackv1.Schedule{}
	for _, s := range p.live.Schedules {
		byName[s.GetName()] = append(byName[s.GetName()], s)
	}
	blocked := p.duplicates("schedules", "schedule", func(name string) []string {
		var ids []string
		for _, s := range byName[name] {
			ids = append(ids, s.GetId())
		}
		return ids
	}, keysOf(byName), fmt.Sprintf("orkctl schedule delete NAME -n %s", p.namespace))

	for _, name := range sortedKeys(p.doc.Schedules) {
		if blocked[name] {
			continue
		}
		entry := p.doc.Schedules[name]
		want := entryValues(entry)
		field := "schedules." + name
		live := byName[name]
		if len(live) == 0 {
			step := &orkstackv1.AutomationStep{
				Action: actionCreate, Kind: kindSchedule, Name: name,
				Summary: scheduleSummary(entry.Orkflow, entry.Version, entry.Spec),
				Request: &orkstackv1.AutomationStep_CreateSchedule{CreateSchedule: &orkstackv1.CreateScheduleRequest{
					Namespace: p.namespace, Name: name, OrkflowName: entry.Orkflow, OrkflowVersion: entry.Version,
					Input: cloneInput(entry.Input), Description: entry.Description,
					Spec: proto.Clone(entry.Spec).(*orkstackv1.ScheduleSpec), Paused: false,
				}},
			}
			for _, key := range entry.Keys {
				step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: key, After: renderScheduleKey(key, want)})
			}
			p.steps = append(p.steps, step)
			continue
		}
		cur := live[0]
		if reason := cur.GetUnavailableReason(); reason != "" {
			p.issues = append(p.issues, errorIssue(field, fmt.Sprintf("schedule could not be read: %s; the plan cannot compare it", reason)))
			continue
		}
		if cur.GetOrkflowName() != entry.Orkflow {
			p.issues = append(p.issues, errorIssue(field+".orkflow", fmt.Sprintf(
				"schedule %s starts %s; a schedule's orkflow is fixed at creation — give the entry a new name (and --prune the old one), "+
					"or delete it first with `orkctl schedule delete %s -n %s`", name, cur.GetOrkflowName(), name, p.namespace)))
			continue
		}
		have := liveValues(cur)
		step := &orkstackv1.AutomationStep{
			Action: actionUnchanged, Kind: kindSchedule, Name: name, Id: cur.GetId(),
			Summary: scheduleSummary(entry.Orkflow, entry.Version, entry.Spec),
		}
		for _, key := range scheduleComparedKeys {
			if !scheduleKeyEqual(key, have, want) {
				step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{
					Field: key, Before: renderScheduleKey(key, have), After: renderScheduleKey(key, want),
				})
			}
		}
		if len(step.Changes) > 0 {
			step.Action = actionUpdate
			step.Request = &orkstackv1.AutomationStep_UpdateSchedule{UpdateSchedule: &orkstackv1.UpdateScheduleRequest{
				Name: name, Namespace: p.namespace, OrkflowVersion: entry.Version, Input: cloneInput(entry.Input),
				Description: entry.Description, Spec: proto.Clone(entry.Spec).(*orkstackv1.ScheduleSpec),
			}}
		}
		if cur.GetPaused() {
			step.Notes = append(step.Notes, pausedNote(cur))
		}
		p.steps = append(p.steps, step)
	}

	for _, name := range sortedKeys(byName) {
		if blocked[name] {
			continue
		}
		if _, declared := p.doc.Schedules[name]; declared {
			continue
		}
		cur := byName[name][0]
		step := &orkstackv1.AutomationStep{Action: actionUndeclared, Kind: kindSchedule, Name: name, Id: cur.GetId()}
		if reason := cur.GetUnavailableReason(); reason != "" {
			step.Summary = cur.GetOrkflowName()
			step.Notes = append(step.Notes, "could not be described: "+reason)
		} else {
			step.Summary = scheduleSummary(cur.GetOrkflowName(), cur.GetOrkflowVersion(), cur.GetSpec())
			if cur.GetPaused() {
				step.Notes = append(step.Notes, pausedNote(cur))
			}
		}
		if p.opts.Prune {
			step.Action = actionDelete
			step.Request = &orkstackv1.AutomationStep_DeleteSchedule{DeleteSchedule: &orkstackv1.DeleteScheduleRequest{
				Name: name, Namespace: p.namespace,
			}}
		}
		p.steps = append(p.steps, step)
	}
}

func cloneInput(in *orkstackv1.Payload) *orkstackv1.Payload {
	if inputEmpty(in) {
		return nil
	}
	return proto.Clone(in).(*orkstackv1.Payload)
}

// duplicates reports, as one ERROR on the section, every name two live
// objects share — the state a best-effort cross-replica uniqueness check can
// leave behind — and returns those names so no step is produced for them.
func (p *planner) duplicates(section, kind string, ids func(name string) []string, names []string, fix string) map[string]bool {
	blocked := map[string]bool{}
	sort.Strings(names)
	for _, name := range names {
		if name == "" {
			continue
		}
		held := ids(name)
		if len(held) < 2 {
			continue
		}
		sort.Strings(held)
		blocked[name] = true
		p.issues = append(p.issues, errorIssue(section, fmt.Sprintf(
			"%s name %q is held by %s %ss (%s); delete one with `%s` and plan again",
			kind, name, countWord(len(held)), kind, strings.Join(held, ", "), fix)))
	}
	return blocked
}

func countWord(n int) string {
	if n == 2 {
		return "two"
	}
	return strconv.Itoa(n)
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}

// ---------------------------------------------------------------------------
// triggers
// ---------------------------------------------------------------------------

func (p *planner) triggers() {
	named := map[string][]*orkstackv1.Trigger{}
	var unnamed []*orkstackv1.Trigger
	for _, t := range p.live.Triggers {
		if t.GetName() == "" {
			unnamed = append(unnamed, t)
			continue
		}
		named[t.GetName()] = append(named[t.GetName()], t)
	}
	blocked := p.duplicates("triggers", "trigger", func(name string) []string {
		var ids []string
		for _, t := range named[name] {
			ids = append(ids, t.GetId())
		}
		return ids
	}, keysOf(named), fmt.Sprintf("orkctl trigger delete ID -n %s", p.namespace))

	var creates []string
	for _, name := range sortedKeys(p.doc.Triggers) {
		if blocked[name] {
			continue
		}
		entry := p.doc.Triggers[name]
		field := "triggers." + name
		live := named[name]
		if len(live) == 0 {
			step := &orkstackv1.AutomationStep{
				Action: actionCreate, Kind: kindTrigger, Name: name, Summary: triggerSummary(entry.Orkflow, entry.Version),
				Request: &orkstackv1.AutomationStep_CreateTrigger{CreateTrigger: &orkstackv1.CreateTriggerRequest{
					Namespace: p.namespace, OrkflowName: entry.Orkflow, OrkflowVersion: entry.Version,
					Description: entry.Description, Name: name,
				}},
			}
			for _, key := range entry.Keys {
				step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: key, After: renderTriggerKey(key, entry.Orkflow, entry.Version, entry.Description)})
			}
			p.steps = append(p.steps, step)
			creates = append(creates, name)
			continue
		}
		cur := live[0]
		if cur.GetOrkflowName() != entry.Orkflow {
			p.issues = append(p.issues, errorIssue(field+".orkflow", fmt.Sprintf(
				"trigger %s starts %s; a trigger's orkflow is fixed at creation — give the entry a new name (and --prune the old one), "+
					"or delete it first with `orkctl trigger delete %s -n %s`", name, cur.GetOrkflowName(), cur.GetId(), p.namespace)))
			continue
		}
		step := &orkstackv1.AutomationStep{
			Action: actionUnchanged, Kind: kindTrigger, Name: name, Id: cur.GetId(), Summary: triggerSummary(entry.Orkflow, entry.Version),
		}
		if cur.GetOrkflowVersion() != entry.Version {
			step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{
				Field: "version", Before: renderVersion(cur.GetOrkflowVersion()), After: renderVersion(entry.Version),
			})
		}
		if cur.GetDescription() != entry.Description {
			step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{
				Field: "description", Before: renderString(cur.GetDescription()), After: renderString(entry.Description),
			})
		}
		if len(step.Changes) > 0 {
			step.Action = actionUpdate
			step.Request = &orkstackv1.AutomationStep_UpdateTrigger{UpdateTrigger: &orkstackv1.UpdateTriggerRequest{
				Id: cur.GetId(), Namespace: p.namespace, Name: name, OrkflowVersion: entry.Version, Description: entry.Description,
			}}
		}
		p.steps = append(p.steps, step)
	}

	var gone []*orkstackv1.Trigger
	for _, name := range sortedKeys(named) {
		if blocked[name] {
			continue
		}
		if _, declared := p.doc.Triggers[name]; declared {
			continue
		}
		cur := named[name][0]
		gone = append(gone, cur)
		step := &orkstackv1.AutomationStep{
			Action: actionUndeclared, Kind: kindTrigger, Name: name, Id: cur.GetId(),
			Summary: triggerSummary(cur.GetOrkflowName(), cur.GetOrkflowVersion()),
		}
		if p.opts.Prune {
			step.Action = actionDelete
			step.Request = &orkstackv1.AutomationStep_DeleteTrigger{DeleteTrigger: &orkstackv1.DeleteTriggerRequest{
				Id: cur.GetId(), Namespace: p.namespace,
			}}
		}
		p.steps = append(p.steps, step)
	}
	for _, cur := range unnamed {
		p.steps = append(p.steps, unmanagedTrigger(cur, p.namespace))
	}

	// A probable rename: a key is the object's identity, so renaming the
	// entry declares a new trigger — and a new trigger is a new token.
	for _, name := range creates {
		entry := p.doc.Triggers[name]
		for _, old := range gone {
			if old.GetOrkflowName() == entry.Orkflow && old.GetOrkflowVersion() == entry.Version && old.GetDescription() == entry.Description {
				p.issues = append(p.issues, warningIssue("triggers."+name, fmt.Sprintf(
					"trigger %s matches undeclared trigger %s (%s); if this is a rename, run "+
						"`orkctl trigger update %s -n %s --name %s` first to keep its URL and token",
					name, old.GetName(), old.GetId(), old.GetId(), p.namespace, name)))
			}
		}
	}
}

func renderTriggerKey(key, orkflow, version, description string) string {
	switch key {
	case "orkflow":
		return renderString(orkflow)
	case "version":
		return renderVersion(version)
	case "description":
		return renderString(description)
	}
	return ""
}

// ---------------------------------------------------------------------------
// webhooks
// ---------------------------------------------------------------------------

// liveHook is a live subscription in the plan's own terms.
type liveHook struct {
	proto  *orkstackv1.Webhook
	url    string
	events []webhooks.Event
}

func toLiveHook(hook *orkstackv1.Webhook) liveHook {
	url := strings.TrimSpace(hook.GetUrl())
	if normalized, err := webhooks.ValidateURL(hook.GetUrl()); err == nil {
		url = normalized
	}
	wanted := map[webhooks.Event]bool{}
	for _, event := range hook.GetEvents() {
		if parsed, err := webhooks.ParseEvent(event.String()); err == nil {
			wanted[parsed] = true
		}
	}
	var events []webhooks.Event
	for _, known := range webhooks.KnownEvents() {
		if wanted[known] {
			events = append(events, known)
		}
	}
	return liveHook{proto: hook, url: url, events: events}
}

func eventsEqual(a, b []webhooks.Event) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// EventTypes converts lifecycle events to the wire enum.
func EventTypes(events []webhooks.Event) []orkstackv1.EventType {
	out := make([]orkstackv1.EventType, 0, len(events))
	for _, event := range events {
		if value, ok := orkstackv1.EventType_value["EVENT_TYPE_"+string(event)]; ok {
			out = append(out, orkstackv1.EventType(value))
		}
	}
	return out
}

func renderWebhookKey(key string, entry WebhookEntry) string {
	switch key {
	case "url":
		return renderString(entry.URL)
	case "events":
		return renderEvents(entry.Events)
	case "orkflow":
		return renderString(entry.Orkflow)
	case "task":
		return renderString(entry.Task)
	case "secret_env":
		return renderString(entry.SecretEnv)
	}
	return ""
}

func (p *planner) webhooks() {
	named := map[string][]liveHook{}
	var unnamed []liveHook
	for _, hook := range p.live.Webhooks {
		// A deployment-wide subscription, or one scoped to another
		// namespace, belongs to no part of this document: it is never read
		// into a plan, so no apply can see, change or prune it.
		if hook.GetNamespace() == "" || artifacts.ResolveNamespace(hook.GetNamespace()) != p.namespace {
			continue
		}
		live := toLiveHook(hook)
		if hook.GetName() == "" {
			unnamed = append(unnamed, live)
			continue
		}
		named[hook.GetName()] = append(named[hook.GetName()], live)
	}
	blocked := p.duplicates("webhooks", "webhook", func(name string) []string {
		var ids []string
		for _, h := range named[name] {
			ids = append(ids, h.proto.GetId())
		}
		return ids
	}, keysOf(named), "orkctl webhook delete ID")

	var creates []string
	for _, name := range sortedKeys(p.doc.Webhooks) {
		if blocked[name] {
			continue
		}
		entry := p.doc.Webhooks[name]
		summary := webhookSummary(entry.URL, p.namespace, entry.Orkflow, entry.Task)
		live := named[name]
		if len(live) == 0 {
			step := &orkstackv1.AutomationStep{
				Action: actionCreate, Kind: kindWebhook, Name: name, Summary: summary, SecretEnv: entry.SecretEnv,
				Request: &orkstackv1.AutomationStep_RegisterWebhook{RegisterWebhook: &orkstackv1.RegisterWebhookRequest{
					Url: entry.URL, Events: EventTypes(entry.Events), Namespace: p.namespace,
					OrkflowName: entry.Orkflow, TaskId: entry.Task, Name: name,
				}},
			}
			for _, key := range entry.Keys {
				step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: key, After: renderWebhookKey(key, entry)})
			}
			p.steps = append(p.steps, step)
			creates = append(creates, name)
			continue
		}
		cur := live[0]
		step := &orkstackv1.AutomationStep{
			Action: actionUnchanged, Kind: kindWebhook, Name: name, Id: cur.proto.GetId(), Summary: summary,
		}
		if cur.url != entry.URL {
			step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: "url", Before: renderString(cur.url), After: renderString(entry.URL)})
		}
		if !eventsEqual(cur.events, entry.Events) {
			step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: "events", Before: renderEvents(cur.events), After: renderEvents(entry.Events)})
		}
		if cur.proto.GetOrkflowName() != entry.Orkflow {
			step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: "orkflow", Before: renderString(cur.proto.GetOrkflowName()), After: renderString(entry.Orkflow)})
		}
		if cur.proto.GetTaskId() != entry.Task {
			step.Changes = append(step.Changes, &orkstackv1.AutomationFieldChange{Field: "task", Before: renderString(cur.proto.GetTaskId()), After: renderString(entry.Task)})
		}
		if len(step.Changes) > 0 {
			step.Action = actionUpdate
			step.Request = &orkstackv1.AutomationStep_UpdateWebhook{UpdateWebhook: &orkstackv1.UpdateWebhookRequest{
				Id: cur.proto.GetId(), Name: name, Url: entry.URL, Events: EventTypes(entry.Events),
				Namespace: cur.proto.GetNamespace(), OrkflowName: entry.Orkflow, TaskId: entry.Task,
			}}
		}
		if entry.SecretEnv != "" {
			step.Notes = append(step.Notes, secretEnvNote)
		}
		p.steps = append(p.steps, step)
	}

	var gone []liveHook
	for _, name := range sortedKeys(named) {
		if blocked[name] {
			continue
		}
		if _, declared := p.doc.Webhooks[name]; declared {
			continue
		}
		cur := named[name][0]
		gone = append(gone, cur)
		step := &orkstackv1.AutomationStep{
			Action: actionUndeclared, Kind: kindWebhook, Name: name, Id: cur.proto.GetId(),
			Summary: webhookSummary(cur.url, cur.proto.GetNamespace(), cur.proto.GetOrkflowName(), cur.proto.GetTaskId()),
		}
		if p.opts.Prune {
			step.Action = actionDelete
			step.Request = &orkstackv1.AutomationStep_DeleteWebhook{DeleteWebhook: &orkstackv1.DeleteWebhookRequest{Id: cur.proto.GetId()}}
		}
		p.steps = append(p.steps, step)
	}
	for _, cur := range unnamed {
		p.steps = append(p.steps, unmanagedWebhook(cur))
	}

	for _, name := range creates {
		entry := p.doc.Webhooks[name]
		for _, old := range gone {
			if old.url == entry.URL && eventsEqual(old.events, entry.Events) &&
				old.proto.GetOrkflowName() == entry.Orkflow && old.proto.GetTaskId() == entry.Task {
				p.issues = append(p.issues, warningIssue("webhooks."+name, fmt.Sprintf(
					"webhook %s matches undeclared webhook %s (%s); if this is a rename, run "+
						"`orkctl webhook update %s --name %s` first to keep its id and signing secret",
					name, old.proto.GetName(), old.proto.GetId(), old.proto.GetId(), name)))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// across sections
// ---------------------------------------------------------------------------

// references checks that every orkflow a schedule or trigger entry names is
// stored (or pending in this invocation).
func (p *planner) references() {
	check := func(section, name, orkflow, version string) {
		if orkflow == "" || p.live.Orkflows[OrkflowRef{Name: orkflow, Version: version}] {
			return
		}
		field := section + "." + name
		if version == "" {
			p.issues = append(p.issues, errorIssue(field+".orkflow", fmt.Sprintf(
				"orkflow %q has no stored version in namespace %q", orkflow, p.namespace)))
			return
		}
		message := fmt.Sprintf("orkflow %q version %q is not stored in namespace %q", orkflow, version, p.namespace)
		if version == currentLabel {
			message += " (omit `version` to follow the current pointer)"
		}
		p.issues = append(p.issues, errorIssue(field+".version", message))
	}
	for _, name := range sortedKeys(p.doc.Schedules) {
		entry := p.doc.Schedules[name]
		check("schedules", name, entry.Orkflow, entry.Version)
	}
	for _, name := range sortedKeys(p.doc.Triggers) {
		entry := p.doc.Triggers[name]
		check("triggers", name, entry.Orkflow, entry.Version)
	}
}

// stepGroup is a step's place in execution order: the namespace's own write
// first, then creates, updates, deletes — additive before destructive — and
// the steps that write nothing last.
func stepGroup(step *orkstackv1.AutomationStep) int {
	switch step.GetAction() {
	case actionCreate, actionUpdate:
		if step.GetKind() == kindNamespace {
			return 0
		}
		if step.GetAction() == actionCreate {
			return 1
		}
		return 2
	case actionDelete:
		return 3
	case actionUnchanged:
		return 4
	case actionUndeclared:
		return 5
	case actionUnmanaged:
		return 6
	}
	return 7
}

// sortSteps orders steps by (group, kind, name, id), so actionable steps are
// a prefix of the list and are executed in list order.
func sortSteps(steps []*orkstackv1.AutomationStep) {
	sort.SliceStable(steps, func(i, j int) bool {
		a, b := steps[i], steps[j]
		if ga, gb := stepGroup(a), stepGroup(b); ga != gb {
			return ga < gb
		}
		if a.GetKind() != b.GetKind() {
			return a.GetKind() < b.GetKind()
		}
		if a.GetName() != b.GetName() {
			return a.GetName() < b.GetName()
		}
		return a.GetId() < b.GetId()
	})
}
