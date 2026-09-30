package webhooks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Store is the durable subscription registry. Production implementations read
// on every operation so registrations made through another engine replica are
// visible both to management and delivery. Registry remains the lightweight
// in-memory implementation used by focused unit tests.
type Store interface {
	Register(context.Context, Registration) (Webhook, error)
	// List returns every subscription without its secret. It is the API's
	// read: what ListWebhooks and the JSON adapter answer with.
	List(context.Context) ([]Webhook, error)
	// ListForDelivery returns every subscription WITH its signing secret, in
	// the same order List uses. It is the dispatcher's read and nobody
	// else's: no handler, adapter, CLI or client calls it, and its result is
	// never serialized. Registry.MatchingContext is its only caller.
	ListForDelivery(context.Context) ([]Webhook, error)
	Get(context.Context, string) (Webhook, bool, error)
	Delete(context.Context, string) (bool, error)
}

// Event names a transition a subscriber can ask for: one of the run's own
// transitions, or a task outcome. The values are the lifecycle event types
// without their proto prefix, which is how every other OrkStack JSON surface
// spells an enum (see internal/api/json.go). TASK_LOG is deliberately not one:
// a log line is not a state transition.
type Event string

const (
	// EventRunStarted is PENDING → RUNNING.
	EventRunStarted Event = "RUN_STARTED"
	// EventTaskScheduled is a task handed to the interpreter's scheduler.
	EventTaskScheduled Event = "TASK_SCHEDULED"
	// EventTaskStarted is a task dispatched to an Orker. It means the activity
	// was scheduled, not that a container is running.
	EventTaskStarted Event = "TASK_STARTED"
	// EventTaskSucceeded is a task's committed success.
	EventTaskSucceeded Event = "TASK_SUCCEEDED"
	// EventTaskFailed is a task's final failure, after Temporal stopped
	// retrying it. A task canceled because its run ended is never one.
	EventTaskFailed Event = "TASK_FAILED"
	// EventRunSucceeded is RUNNING → SUCCEEDED.
	EventRunSucceeded Event = "RUN_SUCCEEDED"
	// EventRunFailed is RUNNING → FAILED.
	EventRunFailed Event = "RUN_FAILED"
	// EventRunCanceled is RUNNING → CANCELED.
	EventRunCanceled Event = "RUN_CANCELED"
)

// KnownEvents lists every subscribable event, in lifecycle order. The four
// task outcomes sit between RUN_STARTED and RUN_SUCCEEDED, which leaves the
// relative order of the run transitions what it always was.
func KnownEvents() []Event {
	return []Event{
		EventRunStarted,
		EventTaskScheduled, EventTaskStarted, EventTaskSucceeded, EventTaskFailed,
		EventRunSucceeded, EventRunFailed, EventRunCanceled,
	}
}

// RunEvents lists the run's own transitions: what an empty subscription means.
func RunEvents() []Event {
	return []Event{EventRunStarted, EventRunSucceeded, EventRunFailed, EventRunCanceled}
}

func isRunEvent(event Event) bool {
	for _, run := range RunEvents() {
		if run == event {
			return true
		}
	}
	return false
}

func isTaskEvent(event Event) bool {
	switch event {
	case EventTaskScheduled, EventTaskStarted, EventTaskSucceeded, EventTaskFailed:
		return true
	}
	return false
}

// ParseEvent reads an event name. It accepts the short form a subscriber
// writes ("RUN_SUCCEEDED"), the proto spelling the engine carries
// ("EVENT_TYPE_RUN_SUCCEEDED"), and either in lower case, so a caller never
// has to know which layer produced the string it is holding.
func ParseEvent(raw string) (Event, error) {
	normalized := Event(strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(raw)), "EVENT_TYPE_"))
	for _, known := range KnownEvents() {
		if normalized == known {
			return known, nil
		}
	}
	names := make([]string, 0, len(KnownEvents()))
	for _, known := range KnownEvents() {
		names = append(names, string(known))
	}
	return "", fmt.Errorf("webhooks: unknown event %q, want one of %s", raw, strings.Join(names, ", "))
}

// Webhook is one registered subscription.
type Webhook struct {
	// ID addresses the subscription. It is assigned at registration.
	ID string `json:"id"`
	// URL receives the POST.
	URL string `json:"url"`
	// Events selects transitions. Empty means every run transition; task
	// outcome events are delivered only to a subscription that names them.
	Events []Event `json:"events,omitempty"`
	// Secret signs the body. It is returned once, by Register, and never
	// again: List and Get answer with Public copies.
	Secret string `json:"secret,omitempty"`
	// CreatedAt is when the subscription was registered, in UTC.
	CreatedAt time.Time `json:"createdAt"`
	// Namespace, OrkflowName and TaskID are the subscription's scope. Each is
	// an exact match and empty means every; all three empty is a
	// deployment-wide subscription. TaskID is consulted only for task
	// outcome events.
	Namespace   string `json:"namespace,omitempty"`
	OrkflowName string `json:"orkflowName,omitempty"`
	TaskID      string `json:"taskId,omitempty"`
}

// Subscribes reports whether w wants event. A subscription that named no
// events wants every run transition — the useful default for an audit sink,
// and the meaning it had when run transitions were the only events. Task
// outcome events are opt-in by name, always.
func (w Webhook) Subscribes(event Event) bool {
	if len(w.Events) == 0 {
		return isRunEvent(event)
	}
	for _, want := range w.Events {
		if want == event {
			return true
		}
	}
	return false
}

// Match is what one event looks like to the registry: which transition, of
// which run, of which task. Namespace is always the resolved name ("default",
// never ""), because the adapter resolves it before asking; TaskID is empty
// for a run transition.
type Match struct {
	Event       Event
	Namespace   string
	OrkflowName string
	TaskID      string
}

// Matches reports whether w wants this event. Each scope field is an equality
// on one coordinate, and an empty field matches every value. TaskID is
// consulted only when the event is a task outcome event, so one subscription
// can want TASK_SUCCEEDED of one task and RUN_FAILED of the whole run.
func (w Webhook) Matches(m Match) bool {
	if !w.Subscribes(m.Event) {
		return false
	}
	if w.Namespace != "" && w.Namespace != m.Namespace {
		return false
	}
	if w.OrkflowName != "" && w.OrkflowName != m.OrkflowName {
		return false
	}
	if w.TaskID != "" && isTaskEvent(m.Event) && w.TaskID != m.TaskID {
		return false
	}
	return true
}

// Public returns a copy safe to hand back to a caller: everything but the
// secret.
func (w Webhook) Public() Webhook {
	w.Secret = ""
	return w
}

// Registration is the caller-supplied half of a Webhook.
type Registration struct {
	// URL must be an absolute http or https URL.
	URL string `json:"url"`
	// Events selects transitions. Empty means every run transition.
	Events []Event `json:"events,omitempty"`
	// Secret signs deliveries. Empty means "generate one for me", which is
	// the path most callers take.
	Secret string `json:"secret,omitempty"`
	// Namespace, OrkflowName and TaskID are the scope, on the terms Webhook
	// states them. ValidateScope decides whether they can be stored.
	Namespace   string `json:"namespace,omitempty"`
	OrkflowName string `json:"orkflowName,omitempty"`
	TaskID      string `json:"taskId,omitempty"`
}

// maxScopeBytes and scopePattern are the slug rule AGENTS.md §7 states for a
// namespace, an orkflow name and a task id. It is declared here rather than
// imported because pkg/webhooks stays ignorant of proto and pkg/artifacts,
// exactly as pkg/triggers declares its own.
const maxScopeBytes = 200

var scopePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// ValidateScope reports why a scope cannot be stored. Each non-empty field
// must be a slug (^[a-z][a-z0-9-]*$, at most 200 bytes), validated and never
// sanitized. A task id requires at least one task outcome event in events: an
// empty events means run transitions only, so the task filter could never be
// consulted.
func ValidateScope(namespace, orkflowName, taskID string, events []Event) error {
	for _, field := range []struct{ name, value string }{
		{"namespace", namespace}, {"orkflow", orkflowName}, {"task", taskID},
	} {
		if field.value == "" {
			continue
		}
		if len(field.value) > maxScopeBytes {
			return fmt.Errorf("webhooks: invalid %s scope %q: longer than %d bytes", field.name, field.value, maxScopeBytes)
		}
		if !scopePattern.MatchString(field.value) {
			return fmt.Errorf("webhooks: invalid %s scope %q: must match %s", field.name, field.value, scopePattern)
		}
	}
	if taskID == "" {
		return nil
	}
	for _, raw := range events {
		if event, err := ParseEvent(string(raw)); err == nil && isTaskEvent(event) {
			return nil
		}
	}
	return fmt.Errorf("webhooks: a task scope %q needs at least one task event (TASK_SCHEDULED, TASK_STARTED, TASK_SUCCEEDED, TASK_FAILED); "+
		"without one the subscription receives run transitions only and the task filter is never consulted", taskID)
}

// Registry holds subscriptions in process. It is the smallest thing that
// satisfies the contract: registrations live as long as the engine does.
// Persisting them is a storage decision, and the seam for it is this type's
// method set, not its callers.
type Registry struct {
	mu    sync.RWMutex
	byID  map[string]Webhook
	order []string

	newID     func() (string, error)
	newSecret func() (string, error)
	now       func() time.Time
	store     Store
}

// NewDurableRegistry adapts a shared store to the existing registry API. It
// deliberately retains no subscription cache.
func NewDurableRegistry(store Store) *Registry {
	return &Registry{store: store}
}

// Durable reports whether reads leave this process. It lets the notifier keep
// the historical synchronous queued-count contract for unit-test registries
// while ensuring object-store lookup never runs on a workflow transition.
func (r *Registry) Durable() bool { return r != nil && r.store != nil }

// RegistryOption tunes a Registry. The generators and clock are injectable so
// tests can pin ids, secrets and timestamps.
type RegistryOption func(*Registry)

// WithIDGenerator replaces the subscription id source.
func WithIDGenerator(gen func() (string, error)) RegistryOption {
	return func(r *Registry) { r.newID = gen }
}

// WithSecretGenerator replaces the signing-secret source.
func WithSecretGenerator(gen func() (string, error)) RegistryOption {
	return func(r *Registry) { r.newSecret = gen }
}

// WithClock replaces the registration clock.
func WithClock(now func() time.Time) RegistryOption {
	return func(r *Registry) { r.now = now }
}

// NewRegistry returns an empty registry.
func NewRegistry(options ...RegistryOption) *Registry {
	r := &Registry{
		byID:      map[string]Webhook{},
		newID:     func() (string, error) { return randomToken("wh-", 8) },
		newSecret: func() (string, error) { return randomToken("", 32) },
		now:       time.Now,
	}
	for _, option := range options {
		option(r)
	}
	return r
}

// Register validates and stores a subscription, returning it with its assigned
// id and its secret. This is the only time the secret is disclosed.
func (r *Registry) Register(reg Registration) (Webhook, error) {
	return r.RegisterContext(context.Background(), reg)
}

// RegisterContext persists one subscription when Registry is backed by a
// Store. The in-memory fallback is retained for unit tests.
func (r *Registry) RegisterContext(ctx context.Context, reg Registration) (Webhook, error) {
	if r.store != nil {
		return r.store.Register(ctx, reg)
	}
	target, err := ValidateURL(reg.URL)
	if err != nil {
		return Webhook{}, err
	}
	events, err := normalizeEvents(reg.Events)
	if err != nil {
		return Webhook{}, err
	}
	if err := ValidateScope(reg.Namespace, reg.OrkflowName, reg.TaskID, events); err != nil {
		return Webhook{}, err
	}

	secret := reg.Secret
	if secret == "" {
		secret, err = r.newSecret()
		if err != nil {
			return Webhook{}, fmt.Errorf("webhooks: generate secret: %w", err)
		}
	}
	id, err := r.newID()
	if err != nil {
		return Webhook{}, fmt.Errorf("webhooks: generate id: %w", err)
	}

	hook := Webhook{
		ID:          id,
		URL:         target,
		Events:      events,
		Secret:      secret,
		CreatedAt:   r.now().UTC(),
		Namespace:   reg.Namespace,
		OrkflowName: reg.OrkflowName,
		TaskID:      reg.TaskID,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, clash := r.byID[id]; clash {
		return Webhook{}, fmt.Errorf("webhooks: id %q is already registered", id)
	}
	r.byID[id] = hook
	r.order = append(r.order, id)
	return hook, nil
}

// List returns every subscription in registration order, without secrets.
func (r *Registry) List() []Webhook {
	hooks, _ := r.ListContext(context.Background())
	return hooks
}

// ListContext returns public subscriptions. It returns storage failures so an
// API never turns an unavailable shared registry into an empty deployment.
func (r *Registry) ListContext(ctx context.Context) ([]Webhook, error) {
	if r.store != nil {
		return r.store.List(ctx)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Webhook, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, r.byID[id].Public())
	}
	return out, nil
}

// Get returns one subscription without its secret.
func (r *Registry) Get(id string) (Webhook, bool) {
	hook, ok, _ := r.GetContext(context.Background(), id)
	return hook, ok
}

func (r *Registry) GetContext(ctx context.Context, id string) (Webhook, bool, error) {
	if r.store != nil {
		return r.store.Get(ctx, id)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	hook, ok := r.byID[id]
	return hook.Public(), ok, nil
}

// Delete removes a subscription, reporting whether it existed.
func (r *Registry) Delete(id string) bool {
	ok, _ := r.DeleteContext(context.Background(), id)
	return ok
}

func (r *Registry) DeleteContext(ctx context.Context, id string) (bool, error) {
	if r.store != nil {
		return r.store.Delete(ctx, id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[id]; !ok {
		return false, nil
	}
	delete(r.byID, id)
	for i, existing := range r.order {
		if existing == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	return true, nil
}

// Matching returns the subscriptions that want this event, secrets included:
// this is the dispatcher's read, not an API response.
func (r *Registry) Matching(m Match) []Webhook {
	hooks, _ := r.MatchingContext(context.Background(), m)
	return hooks
}

// MatchingContext performs the lookup against the shared registry. Callers
// that dispatch run notifications should bound this context and treat errors
// as best-effort delivery failures.
func (r *Registry) MatchingContext(ctx context.Context, m Match) ([]Webhook, error) {
	if r.store != nil {
		hooks, err := r.store.ListForDelivery(ctx)
		if err != nil {
			return nil, err
		}
		out := hooks[:0]
		for _, hook := range hooks {
			if hook.Matches(m) {
				out = append(out, hook)
			}
		}
		return out, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Webhook, 0, len(r.order))
	for _, id := range r.order {
		if hook := r.byID[id]; hook.Matches(m) {
			out = append(out, hook)
		}
	}
	return out, nil
}

// LookupForDelivery returns the subscription with the given id WITH its
// signing secret, or ok=false when no such subscription is registered. It is
// the redelivery read and nobody else's: RedeliverWebhook is its only
// caller, and its result is never serialized. The durable branch reads
// through Store.ListForDelivery and filters in front of the caller — the
// same "list everything, filter here" shape ListTriggers uses — so a Store
// needs no new method; the in-memory branch reads byID under the lock.
func (r *Registry) LookupForDelivery(ctx context.Context, id string) (Webhook, bool, error) {
	if r.store != nil {
		hooks, err := r.store.ListForDelivery(ctx)
		if err != nil {
			return Webhook{}, false, fmt.Errorf("lookup webhook %s for delivery: %w", id, err)
		}
		for _, hook := range hooks {
			if hook.ID == id {
				return hook, true, nil
			}
		}
		return Webhook{}, false, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	hook, ok := r.byID[id]
	if !ok {
		return Webhook{}, false, nil
	}
	return hook, true, nil
}

// ValidateURL accepts the absolute http(s) URLs a delivery can actually reach
// and returns the URL as stored.
func ValidateURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("webhooks: url is required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("webhooks: invalid url %q: %w", raw, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("webhooks: invalid url %q: scheme must be http or https", raw)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("webhooks: invalid url %q: missing host", raw)
	}
	return trimmed, nil
}

// normalizeEvents validates a subscription's events and removes duplicates,
// keeping lifecycle order so two equivalent registrations read identically.
func normalizeEvents(events []Event) ([]Event, error) {
	if len(events) == 0 {
		return nil, nil
	}
	wanted := map[Event]struct{}{}
	for _, raw := range events {
		event, err := ParseEvent(string(raw))
		if err != nil {
			return nil, err
		}
		wanted[event] = struct{}{}
	}
	out := make([]Event, 0, len(wanted))
	for _, known := range KnownEvents() {
		if _, ok := wanted[known]; ok {
			out = append(out, known)
		}
	}
	return out, nil
}

// SortEvents orders events the way KnownEvents does, for stable output.
func SortEvents(events []Event) {
	rank := map[Event]int{}
	for i, known := range KnownEvents() {
		rank[known] = i
	}
	sort.SliceStable(events, func(i, j int) bool { return rank[events[i]] < rank[events[j]] })
}

// randomToken returns prefix followed by size bytes of crypto-random hex.
func randomToken(prefix string, size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(buf), nil
}
