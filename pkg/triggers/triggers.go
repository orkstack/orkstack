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
	"errors"
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
	// Update applies change to the trigger with this id in this namespace and
	// stores the result, reporting whether it existed. change may alter Name,
	// OrkflowVersion and Description only; any other difference is refused
	// before anything is written. The verification material is carried over
	// untouched, so the token the trigger was created with keeps verifying.
	// A trigger of another namespace is reported absent.
	Update(ctx context.Context, id, namespace string, change func(*Trigger) error) (Trigger, bool, error)
}

// ErrNameTaken reports a create or a rename onto a name another trigger of the
// same namespace already holds. The API answers it with AlreadyExists.
var ErrNameTaken = errors.New("triggers: name already in use in this namespace")

// ErrUpdateContention reports a conditional update that lost its race on
// every round it was allowed. The API answers it with Unavailable: nothing was
// written, and trying again is the remedy.
var ErrUpdateContention = errors.New("triggers: the trigger kept changing while it was being updated; try again")

// UpdateAttempts bounds how many read-modify-conditional-write rounds a
// durable Update tries before it reports ErrUpdateContention. It is its own
// constant rather than CreateAttempts because it bounds a different thing —
// concurrent writers to one record, not random id collisions — and eight
// rounds of a handful of milliseconds each is far more than a hand-run
// management write meets in practice.
const UpdateAttempts = 8

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
	// Name is the operator's handle: a slug unique among the named triggers
	// of the namespace, or empty for an unnamed trigger. It is how an
	// automation document addresses the trigger and never part of the invoke
	// URL.
	Name string
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
	// Name is optional; see Trigger.Name and ValidateName.
	Name string
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
	if err := ValidateName(r.Name); err != nil {
		return err
	}
	if r.OrkflowVersion == "" {
		return nil
	}
	return validComponent("orkflow_version", r.OrkflowVersion, versionPattern)
}

// ValidateName reports why name cannot be a trigger's name. Empty is valid —
// it is an unnamed trigger. Otherwise it is the slug rule AGENTS.md §7 states
// for every name, at most MaxComponent bytes, validated and never sanitized.
func ValidateName(name string) error {
	if name == "" {
		return nil
	}
	if len(name) > MaxComponent {
		return fmt.Errorf("triggers: name %q exceeds %d bytes", name, MaxComponent)
	}
	if !slugPattern.MatchString(name) {
		return fmt.Errorf("triggers: name %q must match %s", name, slugPattern)
	}
	return nil
}

// ApplyChange runs change against a copy of current and returns the result,
// refusing any difference outside the mutable fields (Name, OrkflowVersion,
// Description) and validating the ones that did change. Every backend's Update
// calls it, so one place decides what an update may touch: the id, the
// coordinates, the creation time and the verification material are carried
// over, and a change that tried to alter one of them writes nothing.
func ApplyChange(current Trigger, change func(*Trigger) error) (Trigger, error) {
	next := current
	next.SecretSalt = append([]byte(nil), current.SecretSalt...)
	next.SecretHash = append([]byte(nil), current.SecretHash...)
	if err := change(&next); err != nil {
		return Trigger{}, err
	}
	switch {
	case next.ID != current.ID:
		return Trigger{}, fmt.Errorf("triggers: an update cannot change a trigger's id")
	case next.Namespace != current.Namespace:
		return Trigger{}, fmt.Errorf("triggers: an update cannot change a trigger's namespace")
	case next.OrkflowName != current.OrkflowName:
		return Trigger{}, fmt.Errorf("triggers: an update cannot change a trigger's orkflow; delete it and create another")
	case !next.CreatedAt.Equal(current.CreatedAt):
		return Trigger{}, fmt.Errorf("triggers: an update cannot change a trigger's creation time")
	case string(next.SecretSalt) != string(current.SecretSalt) || string(next.SecretHash) != string(current.SecretHash):
		return Trigger{}, fmt.Errorf("triggers: an update cannot change a trigger's token")
	}
	reg := Registration{
		Namespace:      next.Namespace,
		OrkflowName:    next.OrkflowName,
		OrkflowVersion: next.OrkflowVersion,
		Description:    next.Description,
		Name:           next.Name,
	}
	if err := reg.Validate(); err != nil {
		return Trigger{}, err
	}
	return next, nil
}

// NameHeld reports whether a trigger other than self already holds name among
// list. An empty name is never held: unnamed triggers never collide.
func NameHeld(list []Trigger, name, self string) bool {
	if name == "" {
		return false
	}
	for _, trigger := range list {
		if trigger.ID != self && trigger.Name == name {
			return true
		}
	}
	return false
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
		Name:           reg.Name,
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
		if s.nameHeldLocked(trigger.Namespace, trigger.Name, "") {
			s.mu.Unlock()
			return Trigger{}, "", ErrNameTaken
		}
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

// Update applies change under the store's mutex, so within this process an
// update can never interleave with a delete or another rename.
func (s *MemoryStore) Update(ctx context.Context, id, namespace string, change func(*Trigger) error) (Trigger, bool, error) {
	if err := ctx.Err(); err != nil {
		return Trigger{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.byID[id]
	if !ok || current.Namespace != namespace {
		return Trigger{}, false, nil
	}
	next, err := ApplyChange(current, change)
	if err != nil {
		return Trigger{}, true, err
	}
	if next.Name != current.Name && s.nameHeldLocked(namespace, next.Name, id) {
		return Trigger{}, true, ErrNameTaken
	}
	s.byID[id] = next
	return next.Public(), true, nil
}

// nameHeldLocked reports whether a trigger of namespace other than self holds
// name. The caller holds s.mu.
func (s *MemoryStore) nameHeldLocked(namespace, name, self string) bool {
	if name == "" {
		return false
	}
	for _, trigger := range s.byID {
		if trigger.Namespace == namespace && trigger.ID != self && trigger.Name == name {
			return true
		}
	}
	return false
}
