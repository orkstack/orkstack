// Package triggers holds the inbound run-starter registry: the domain type an
// engine stores, the caller-supplied half of a creation, and the in-memory
// implementation focused unit tests use.
//
// A trigger is the mirror image of a webhook (pkg/webhooks). A webhook is
// OUTBOUND and deployment-wide unless it names a scope: OrkStack calls the
// subscriber when a run it covers changes state. A trigger is INBOUND and orkflow-scoped: an external caller
// presents the trigger's bearer token and one run of exactly one orkflow
// starts. The orkflow coordinates live on the stored record and never on the
// request, so a token can only ever start the orkflow its trigger was created
// for.
//
// The package split mirrors pkg/webhooks: the domain types, the id and secret
// generation and the verification rule live here; the durable Local and S3
// backends live in pkg/artifacts (trigger_store.go), beside the artifact,
// archive and webhook stores that share their configuration.
package triggers

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Store is the durable trigger registry. Production implementations read the
// shared backend on every operation, so a trigger created through one engine
// replica is listable, invocable and deletable on every other one — there is
// no startup snapshot to go stale.
type Store interface {
	// Create generates a trigger's id and secret, stores the record with the
	// secret's salted digest, and returns the record together with the ONE
	// copy of the plaintext secret that will ever exist. Nothing reads that
	// plaintext back afterwards: it is not in the record.
	Create(ctx context.Context, reg Registration) (Trigger, string, error)
	// List returns the triggers of one namespace, oldest first, without their
	// verification material. An empty orkflowName means every trigger in the
	// namespace.
	List(ctx context.Context, namespace, orkflowName string) ([]Trigger, error)
	// Get resolves one trigger from its bare id, WITH its verification
	// material: the invoke path has only the id from the URL, and the salt and
	// digest it must compare against live on the record.
	Get(ctx context.Context, id string) (Trigger, bool, error)
	// Delete removes one trigger addressed by id and namespace, reporting
	// whether it existed. A trigger of another namespace is reported absent —
	// not forbidden, and never deleted.
	Delete(ctx context.Context, id, namespace string) (bool, error)
}

// Trigger is one stored inbound run starter.
//
// SecretSalt and SecretHash are the only trace the secret leaves at rest. A
// webhook secret must sit in plaintext because delivery replays it into an
// HMAC signature; a trigger secret is only ever compared, so the record holds
// sha256(salt || secret) and the salt, and the plaintext exists only in the
// creation response.
type Trigger struct {
	// ID addresses the trigger. It is opaque, shaped trg-{16 hex}, and nothing
	// derives a namespace or orkflow from it.
	ID string
	// Namespace and OrkflowName are the orkflow this trigger starts. They are
	// a coordinate, never a filter, and never come from an invoke request.
	Namespace   string
	OrkflowName string
	// OrkflowVersion pins a version. Empty means the current pointer, resolved
	// at invoke time, exactly as SubmitRunRequest.orkflow_version defaults.
	OrkflowVersion string
	// Description is an optional operator label, returned as given.
	Description string
	// CreatedAt is when the trigger was created, in UTC.
	CreatedAt time.Time

	// SecretSalt is per-trigger random bytes. It is not secret.
	SecretSalt []byte
	// SecretHash is sha256(SecretSalt || secret).
	SecretHash []byte
}

// Public returns a copy safe to hand to a caller: the record without the
// material an offline attacker would need. Every read path except the invoke
// lookup answers with one of these.
func (t Trigger) Public() Trigger {
	t.SecretSalt = nil
	t.SecretHash = nil
	return t
}

// Matches reports whether t belongs to one namespace, and to orkflowName when
// one is named. It is the filter List applies over the flat collection.
func (t Trigger) Matches(namespace, orkflowName string) bool {
	if t.Namespace != namespace {
		return false
	}
	return orkflowName == "" || t.OrkflowName == orkflowName
}

// Registration is the caller-supplied half of a Trigger: which orkflow it
// starts, and the operator's label for it. The id, the secret and the creation
// time are the store's to assign.
type Registration struct {
	Namespace      string
	OrkflowName    string
	OrkflowVersion string
	Description    string
}

// slugPattern is the name rule AGENTS.md §7 states for a namespace and an
// orkflow name. Names are validated, never sanitized, at each boundary.
var slugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// versionPattern is the rule for a stored version string, which is a version
// and not a slug: "0.2.0" and "2026-09-08.1" are both versions.
var versionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// MaxComponent bounds a namespace, orkflow name or version, matching the
// 200-byte limit every other OrkStack name boundary applies.
const MaxComponent = 200

// Validate reports why a registration cannot be stored. The engine has already
// resolved the namespace and confirmed the orkflow exists by the time it calls
// Create; this is the store's own boundary, so a record can never be written
// with coordinates no submission could use.
func (r Registration) Validate() error {
	if err := validComponent("namespace", r.Namespace, slugPattern); err != nil {
		return err
	}
	if err := validComponent("orkflow_name", r.OrkflowName, slugPattern); err != nil {
		return err
	}
	if r.OrkflowVersion == "" {
		return nil
	}
	return validComponent("orkflow_version", r.OrkflowVersion, versionPattern)
}

func validComponent(field, value string, pattern *regexp.Regexp) error {
	switch {
	case value == "":
		return fmt.Errorf("triggers: %s is required", field)
	case len(value) > MaxComponent:
		return fmt.Errorf("triggers: %s %q exceeds %d bytes", field, value, MaxComponent)
	case !pattern.MatchString(value):
		return fmt.Errorf("triggers: %s %q must match %s", field, value, pattern)
	}
	return nil
}

// Prepare turns a validated registration into the record to store and the
// plaintext secret to disclose once. Both durable backends call it, so one
// place decides a trigger's id, salt, digest and creation time.
func Prepare(reg Registration) (Trigger, string, error) {
	if err := reg.Validate(); err != nil {
		return Trigger{}, "", err
	}
	id, err := NewID()
	if err != nil {
		return Trigger{}, "", err
	}
	secret, err := NewSecret()
	if err != nil {
		return Trigger{}, "", err
	}
	salt, err := NewSalt()
	if err != nil {
		return Trigger{}, "", err
	}
	return Trigger{
		ID:             id,
		Namespace:      reg.Namespace,
		OrkflowName:    reg.OrkflowName,
		OrkflowVersion: reg.OrkflowVersion,
		Description:    reg.Description,
		CreatedAt:      time.Now().UTC(),
		SecretSalt:     salt,
		SecretHash:     HashSecret(salt, secret),
	}, secret, nil
}

// SortByCreation orders triggers oldest first, breaking a tie on id so two
// records written in the same instant still read in a stable order.
func SortByCreation(list []Trigger) {
	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt.Equal(list[j].CreatedAt) {
			return list[i].ID < list[j].ID
		}
		return list[i].CreatedAt.Before(list[j].CreatedAt)
	})
}

// MemoryStore holds triggers in process. It is what an unconfigured Service
// gets, on the same terms pkg/webhooks' Registry is: correct only because a
// process with no shared backend is the only process there is, and therefore
// for unit tests. A configured engine always wires the durable store.
type MemoryStore struct {
	mu    sync.RWMutex
	byID  map[string]Trigger
	order []string
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty in-process store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{byID: map[string]Trigger{}}
}

func (s *MemoryStore) Create(ctx context.Context, reg Registration) (Trigger, string, error) {
	if err := ctx.Err(); err != nil {
		return Trigger{}, "", err
	}
	for attempt := 0; attempt < CreateAttempts; attempt++ {
		trigger, secret, err := Prepare(reg)
		if err != nil {
			return Trigger{}, "", err
		}
		s.mu.Lock()
		if _, clash := s.byID[trigger.ID]; clash {
			s.mu.Unlock()
			continue
		}
		s.byID[trigger.ID] = trigger
		s.order = append(s.order, trigger.ID)
		s.mu.Unlock()
		return trigger, secret, nil
	}
	return Trigger{}, "", ErrIDCollision
}

func (s *MemoryStore) List(ctx context.Context, namespace, orkflowName string) ([]Trigger, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validComponent("namespace", namespace, slugPattern); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Trigger, 0, len(s.order))
	for _, id := range s.order {
		if trigger := s.byID[id]; trigger.Matches(namespace, orkflowName) {
			out = append(out, trigger.Public())
		}
	}
	SortByCreation(out)
	return out, nil
}

func (s *MemoryStore) Get(ctx context.Context, id string) (Trigger, bool, error) {
	if err := ctx.Err(); err != nil {
		return Trigger{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	trigger, ok := s.byID[id]
	return trigger, ok, nil
}

func (s *MemoryStore) Delete(ctx context.Context, id, namespace string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	trigger, ok := s.byID[id]
	if !ok || trigger.Namespace != namespace {
		return false, nil
	}
	delete(s.byID, id)
	for i, existing := range s.order {
		if existing == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return true, nil
}
