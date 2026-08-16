package webhooks

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Event names a run state transition a subscriber can ask for. The values are
// the run lifecycle event types without their proto prefix, which is how every
// other OrkStack JSON surface spells an enum (see internal/api/json.go).
type Event string

const (
	// EventRunStarted is PENDING → RUNNING.
	EventRunStarted Event = "RUN_STARTED"
	// EventRunSucceeded is RUNNING → SUCCEEDED.
	EventRunSucceeded Event = "RUN_SUCCEEDED"
	// EventRunFailed is RUNNING → FAILED.
	EventRunFailed Event = "RUN_FAILED"
	// EventRunCanceled is RUNNING → CANCELED.
	EventRunCanceled Event = "RUN_CANCELED"
)

// KnownEvents lists every transition a subscription may name, in lifecycle
// order.
func KnownEvents() []Event {
	return []Event{EventRunStarted, EventRunSucceeded, EventRunFailed, EventRunCanceled}
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
	// Events selects transitions. Empty means every transition.
	Events []Event `json:"events,omitempty"`
	// Secret signs the body. It is returned once, by Register, and never
	// again: List and Get answer with Public copies.
	Secret string `json:"secret,omitempty"`
	// CreatedAt is when the subscription was registered, in UTC.
	CreatedAt time.Time `json:"createdAt"`
}

// Subscribes reports whether w wants event. A subscription that named no
// events wants all of them, which is the useful default for an audit sink.
func (w Webhook) Subscribes(event Event) bool {
	if len(w.Events) == 0 {
		return true
	}
	for _, want := range w.Events {
		if want == event {
			return true
		}
	}
	return false
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
	// Events selects transitions. Empty means every transition.
	Events []Event `json:"events,omitempty"`
	// Secret signs deliveries. Empty means "generate one for me", which is
	// the path most callers take.
	Secret string `json:"secret,omitempty"`
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
}

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
	target, err := ValidateURL(reg.URL)
	if err != nil {
		return Webhook{}, err
	}
	events, err := normalizeEvents(reg.Events)
	if err != nil {
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
		ID:        id,
		URL:       target,
		Events:    events,
		Secret:    secret,
		CreatedAt: r.now().UTC(),
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
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Webhook, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, r.byID[id].Public())
	}
	return out
}

// Get returns one subscription without its secret.
func (r *Registry) Get(id string) (Webhook, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	hook, ok := r.byID[id]
	return hook.Public(), ok
}

// Delete removes a subscription, reporting whether it existed.
func (r *Registry) Delete(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[id]; !ok {
		return false
	}
	delete(r.byID, id)
	for i, existing := range r.order {
		if existing == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	return true
}

// Matching returns the subscriptions that want event, secrets included: this
// is the dispatcher's read, not an API response.
func (r *Registry) Matching(event Event) []Webhook {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Webhook, 0, len(r.order))
	for _, id := range r.order {
		if hook := r.byID[id]; hook.Subscribes(event) {
			out = append(out, hook)
		}
	}
	return out
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
