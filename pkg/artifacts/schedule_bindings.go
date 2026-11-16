package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/schedules"
)

// ScheduleBindingPrefix holds the object the API keeps for each schedule:
// who bound it and what they consented to, beside what the cluster's
// schedule submits (docs/ORKFLOW_SCHEDULES.md, "Who a schedule runs as").
// It is configuration the API maintains, never the schedule's state: the
// cluster alone holds the schedule, decides its fires and says what it
// submits, and nothing reads this object to decide a fire.
//
// metadata/schedules/{namespace}/{schedule-id}.json
const ScheduleBindingPrefix = "metadata/schedules"

const (
	// MaxScheduleBindings is the most entries one object holds: the entry of
	// the save in progress, the one equal to what the cluster holds, and room
	// for saves whose trim did not happen.
	MaxScheduleBindings = 4
	// scheduleBindingTries is how many conditional writes an update makes
	// before it gives up as contended.
	scheduleBindingTries = 5
	// scheduleBindingMaxBytes bounds one object read back.
	scheduleBindingMaxBytes = 64 << 10
)

// ErrScheduleBindingContended is an update that lost its conditional write
// every time: someone else is changing the schedule.
var ErrScheduleBindingContended = errors.New("this schedule is being changed by someone else; try again")

// ScheduleBinding is one save the API accepted: what it submits, who bound
// it and how that principal was known, and the consent recorded with it.
type ScheduleBinding struct {
	OrkflowName    string `json:"orkflowName"`
	OrkflowVersion string `json:"orkflowVersion,omitempty"` // as written; empty is the current pointer
	InputSHA256    string `json:"inputSha256"`              // of the input's bytes as the API stores them
	// Binder is the principal who last bound the schedule: created it,
	// changed what it submits, or rebound it. BinderVerified says OrkStack
	// verified it; an asserted principal is kept marked as such, and binds
	// nothing a later release admits.
	Binder         string    `json:"binder,omitempty"`
	BinderVerified bool      `json:"binderVerified,omitempty"`
	BinderMethod   string    `json:"binderMethod,omitempty"`
	BoundAt        time.Time `json:"boundAt"`
	// Consent is the consent digest recorded with the binder (internal/consent).
	Consent   string    `json:"consent"`
	ConsentAt time.Time `json:"consentAt"`
	// SavedAt is when this entry was written.
	SavedAt time.Time `json:"savedAt"`
}

// Submits reports whether b and o describe the same submission: the same
// Orkflow, version as written and input.
func (b ScheduleBinding) Submits(o ScheduleBinding) bool {
	return b.OrkflowName == o.OrkflowName && b.OrkflowVersion == o.OrkflowVersion && b.InputSHA256 == o.InputSHA256
}

// ScheduleBindings is one schedule's object.
type ScheduleBindings struct {
	V          int               `json:"v"`
	ScheduleID string            `json:"scheduleId"`
	Namespace  string            `json:"namespace"`
	Entries    []ScheduleBinding `json:"entries"`
}

// Find is the newest entry that submits what want submits, if any.
func (s ScheduleBindings) Find(want ScheduleBinding) (ScheduleBinding, bool) {
	for i := len(s.Entries) - 1; i >= 0; i-- {
		if s.Entries[i].Submits(want) {
			return s.Entries[i], true
		}
	}
	return ScheduleBinding{}, false
}

// Add appends e, dropping any older entry that submits the same, and keeps
// the newest MaxScheduleBindings.
func (s *ScheduleBindings) Add(e ScheduleBinding) {
	s.Entries = slices.DeleteFunc(s.Entries, e.Submits)
	s.Entries = append(s.Entries, e)
	if n := len(s.Entries); n > MaxScheduleBindings {
		s.Entries = s.Entries[n-MaxScheduleBindings:]
	}
}

// Keep trims the set to the entries that submit what one of keep submits,
// newest of each.
func (s *ScheduleBindings) Keep(keep ...ScheduleBinding) {
	var out []ScheduleBinding
	for _, k := range keep {
		if e, ok := s.Find(k); ok && !slices.ContainsFunc(out, e.Submits) {
			out = append(out, e)
		}
	}
	s.Entries = out
}

// ScheduleBindingStore keeps each schedule's object.
type ScheduleBindingStore struct{ blobBase }

// NewScheduleBindingStore opens the store on the deployment's backend.
func NewScheduleBindingStore(ctx context.Context, cfg Config) (*ScheduleBindingStore, error) {
	b, err := openBlobs(ctx, cfg, "schedule binding store", ScheduleBindingPrefix)
	if err != nil {
		return nil, err
	}
	return &ScheduleBindingStore{blobBase: blobBase{b}}, nil
}

func scheduleBindingKey(namespace, id string) (string, error) {
	if err := validNamespaceName(namespace); err != nil {
		return "", fmt.Errorf("artifacts: schedule binding: %w", err)
	}
	if !schedules.ValidID(id) {
		return "", fmt.Errorf("artifacts: schedule binding: invalid schedule id %q", id)
	}
	return ScheduleBindingPrefix + "/" + namespace + "/" + id + ".json", nil
}

// Get reads one schedule's object; a schedule with none answers an empty set.
func (s *ScheduleBindingStore) Get(ctx context.Context, namespace, id string) (ScheduleBindings, error) {
	key, err := scheduleBindingKey(namespace, id)
	if err != nil {
		return ScheduleBindings{}, err
	}
	doc, _, err := s.get(ctx, key, namespace, id)
	return doc, err
}

func (s *ScheduleBindingStore) get(ctx context.Context, key, namespace, id string) (ScheduleBindings, string, error) {
	raw, version, err := s.b.get(ctx, key, scheduleBindingMaxBytes)
	if errors.Is(err, errBlobNotFound) {
		return ScheduleBindings{V: 1, ScheduleID: id, Namespace: namespace}, "", nil
	}
	if err != nil {
		return ScheduleBindings{}, "", fmt.Errorf("artifacts: read %s: %w", s.b.uri(key), err)
	}
	if len(raw) > scheduleBindingMaxBytes {
		return ScheduleBindings{}, "", fmt.Errorf("artifacts: %s is larger than %d bytes", s.b.uri(key), scheduleBindingMaxBytes)
	}
	var doc ScheduleBindings
	if err := json.Unmarshal(raw, &doc); err != nil {
		return ScheduleBindings{}, "", fmt.Errorf("artifacts: decode %s: %w", s.b.uri(key), err)
	}
	if doc.V != 1 || doc.ScheduleID != id || doc.Namespace != namespace {
		return ScheduleBindings{}, "", fmt.Errorf("artifacts: %s names schedule %q of namespace %q (format %d), not %q of %q",
			s.b.uri(key), doc.ScheduleID, doc.Namespace, doc.V, id, namespace)
	}
	return doc, version, nil
}

// Update applies change to the object under a conditional write, retried
// while another writer wins, and returns what was written.
func (s *ScheduleBindingStore) Update(ctx context.Context, namespace, id string, change func(*ScheduleBindings) error) (ScheduleBindings, error) {
	key, err := scheduleBindingKey(namespace, id)
	if err != nil {
		return ScheduleBindings{}, err
	}
	for range scheduleBindingTries {
		doc, version, err := s.get(ctx, key, namespace, id)
		if err != nil {
			return ScheduleBindings{}, err
		}
		if err := change(&doc); err != nil {
			return ScheduleBindings{}, err
		}
		body, err := stablejson.Marshal(doc)
		if err != nil {
			return ScheduleBindings{}, fmt.Errorf("artifacts: encode %s: %w", s.b.uri(key), err)
		}
		if version == "" {
			err = s.b.create(ctx, key, "application/json", body)
		} else {
			err = s.b.replace(ctx, key, "application/json", body, version)
		}
		if errors.Is(err, errBlobExists) || errors.Is(err, errBlobChanged) {
			continue
		}
		if err != nil {
			return ScheduleBindings{}, fmt.Errorf("artifacts: write %s: %w", s.b.uri(key), err)
		}
		return doc, nil
	}
	return ScheduleBindings{}, ErrScheduleBindingContended
}

// Delete removes one schedule's object; a missing one is not an error.
func (s *ScheduleBindingStore) Delete(ctx context.Context, namespace, id string) error {
	key, err := scheduleBindingKey(namespace, id)
	if err != nil {
		return err
	}
	if err := s.b.remove(ctx, key); err != nil {
		return fmt.Errorf("artifacts: delete %s: %w", s.b.uri(key), err)
	}
	return nil
}

// DeleteNamespace removes every object of one namespace.
func (s *ScheduleBindingStore) DeleteNamespace(ctx context.Context, namespace string) error {
	if err := validNamespaceName(namespace); err != nil {
		return fmt.Errorf("artifacts: schedule bindings: %w", err)
	}
	prefix := ScheduleBindingPrefix + "/" + namespace + "/"
	in := blobListInput{Prefix: prefix}
	for {
		page, err := s.b.list(ctx, in)
		if err != nil {
			return fmt.Errorf("artifacts: list %s: %w", s.b.uri(prefix), err)
		}
		keys := make([]string, 0, len(page.Objects))
		for _, o := range page.Objects {
			keys = append(keys, o.Key)
		}
		if len(keys) > 0 {
			failed, err := s.b.removeKeys(ctx, keys)
			if err != nil {
				return fmt.Errorf("artifacts: delete under %s: %w", s.b.uri(prefix), err)
			}
			for key, kerr := range failed {
				return fmt.Errorf("artifacts: delete %s: %w", s.b.uri(key), kerr)
			}
		}
		if !page.Truncated {
			return nil
		}
		in.Token = page.Next
	}
}
