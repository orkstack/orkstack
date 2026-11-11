package artifacts

import (
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// TriggerPrefix holds the inbound run starters. The layout is FLAT —
// metadata/triggers/{id}.json — exactly as the webhook registry is, and
// deliberately not scoped by namespace and orkflow.
//
// The invoke route is POST /v1/triggers/{id}: a trigger URL is one opaque
// thing to paste into a caller, so it carries no namespace, so Get has nothing
// but the bare id to resolve. A scoped key would need the namespace to build
// the key, which is precisely the question the lookup is answering. The
// namespace and orkflow name are therefore fields INSIDE the record, and
// List filters the collection in application code — the same shape ListWebhooks
// already uses deployment-wide, and right at this scale: an operator creates a
// handful of triggers per orkflow, so no secondary index earns its keep.
const TriggerPrefix = "metadata/triggers"

// NewTriggerStore selects the same shared backend as artifacts, run history
// and webhooks. It is a registry, not an artifact: callers always read it
// directly and no process keeps a startup snapshot, so a trigger created on
// one replica is invocable on every other one.
func NewTriggerStore(ctx context.Context, cfg Config) (triggers.Store, error) {
	b, err := openBlobs(ctx, cfg, "trigger store", TriggerPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobTriggerStore{blobBase: blobBase{b}}, nil
}

func triggerKey(id string) (string, error) {
	if !triggers.ValidID(id) {
		return "", fmt.Errorf("artifacts: invalid trigger id %q", id)
	}
	return TriggerPrefix + "/" + id + ".json", nil
}

func parseTriggerKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, TriggerPrefix+"/")
	if !ok {
		return "", false
	}
	id, ok := strings.CutSuffix(rest, ".json")
	return id, ok && triggers.ValidID(id)
}

// triggerRecord is the durable wire form of one trigger.
//
// Unlike the webhook store, this is NOT a generated response message reused as
// a record: orkstack.v1.Trigger deliberately has no secret field, because a
// trigger's plaintext secret must never be marshaled anywhere. What has to
// survive a round trip is the verification material — the salt and the digest
// — which is exactly what no wire type carries, so the record is its own small
// shape and the response messages are built from it.
type triggerRecord struct {
	ID             string `json:"id"`
	Namespace      string `json:"namespace"`
	OrkflowName    string `json:"orkflowName"`
	OrkflowVersion string `json:"orkflowVersion,omitempty"`
	Description    string `json:"description,omitempty"`
	// Name is the operator's handle (triggers.ValidateName). A record written
	// before names existed has none and decodes unnamed.
	Name      string    `json:"name,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	// SecretSalt and SecretHash are hex. SecretHash is
	// sha256(SecretSalt || secret); the secret itself is not here and is not
	// anywhere else either.
	SecretSalt string `json:"secretSalt"`
	SecretHash string `json:"secretHash"`
}

func marshalTriggerRecord(trigger triggers.Trigger) ([]byte, error) {
	record := triggerRecord{
		ID:             trigger.ID,
		Namespace:      trigger.Namespace,
		OrkflowName:    trigger.OrkflowName,
		OrkflowVersion: trigger.OrkflowVersion,
		Description:    trigger.Description,
		Name:           trigger.Name,
		CreatedAt:      trigger.CreatedAt.UTC(),
		SecretSalt:     hex.EncodeToString(trigger.SecretSalt),
		SecretHash:     hex.EncodeToString(trigger.SecretHash),
	}
	raw, err := stablejson.MarshalIndent(record, "  ")
	if err != nil {
		return nil, fmt.Errorf("encode trigger %q: %w", trigger.ID, err)
	}
	return append(raw, '\n'), nil
}

func unmarshalTriggerRecord(raw []byte) (triggers.Trigger, error) {
	var record triggerRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return triggers.Trigger{}, fmt.Errorf("decode trigger document: %w", err)
	}
	if _, err := triggerKey(record.ID); err != nil {
		return triggers.Trigger{}, err
	}
	reg := triggers.Registration{
		Namespace:      record.Namespace,
		OrkflowName:    record.OrkflowName,
		OrkflowVersion: record.OrkflowVersion,
		Description:    record.Description,
		// A hand-edited invalid name is refused on read, exactly as an
		// invalid namespace already is: Validate runs ValidateName.
		Name: record.Name,
	}
	if err := reg.Validate(); err != nil {
		return triggers.Trigger{}, fmt.Errorf("decode trigger %q: %w", record.ID, err)
	}
	if record.CreatedAt.IsZero() {
		return triggers.Trigger{}, fmt.Errorf("decode trigger %q: invalid created_at", record.ID)
	}
	salt, err := hex.DecodeString(record.SecretSalt)
	if err != nil || len(salt) == 0 {
		return triggers.Trigger{}, fmt.Errorf("decode trigger %q: invalid secret salt", record.ID)
	}
	digest, err := hex.DecodeString(record.SecretHash)
	if err != nil || len(digest) == 0 {
		return triggers.Trigger{}, fmt.Errorf("decode trigger %q: invalid secret hash", record.ID)
	}
	return triggers.Trigger{
		ID:             record.ID,
		Namespace:      record.Namespace,
		OrkflowName:    record.OrkflowName,
		OrkflowVersion: record.OrkflowVersion,
		Description:    record.Description,
		Name:           record.Name,
		CreatedAt:      record.CreatedAt.UTC(),
		SecretSalt:     salt,
		SecretHash:     digest,
	}, nil
}

// BlobTriggerStore is the shared trigger registry, one object per trigger in
// the object store every replica reads.
//
// nameMu serialises, within one process, a named create's uniqueness check
// with its publish, and an update's check with its write, so two creates in
// one process cannot both take one name. Across replicas the check is
// best-effort (Create says why); a conditional write, never this lock, is
// what keeps an update from recreating a deleted trigger.
type BlobTriggerStore struct {
	blobBase
	nameMu sync.Mutex
	// beforeConditionalPut, when set, runs between Update's read and its
	// conditional write. Tests use it to delete the record in that window.
	beforeConditionalPut func()
}

// LocalTriggerStore and S3TriggerStore are the trigger registry over a local
// directory and over a bucket: one implementation, two backends.
type (
	LocalTriggerStore = BlobTriggerStore
	S3TriggerStore    = BlobTriggerStore
)

var _ triggers.Store = (*BlobTriggerStore)(nil)

// NewLocalTriggerStore opens (creating if needed) the registry under dir.
func NewLocalTriggerStore(dir string) (*LocalTriggerStore, error) {
	b, err := openLocalBlobs("trigger store", dir, TriggerPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobTriggerStore{blobBase: blobBase{b}}, nil
}

// NewS3TriggerStore builds the registry from the shared ORK_S3_* config.
func NewS3TriggerStore(ctx context.Context, cfg Config) (*S3TriggerStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobTriggerStore{blobBase: blobBase{b}}, nil
}

// Create writes one record create-only, so two replicas drawing the same id
// cannot both believe they created it: the loser retries with a fresh one
// rather than overwriting a trigger somebody is already holding a token for.
//
// A named create first refuses a name another trigger of the namespace holds.
// That check is best-effort across replicas: two creates of one name racing
// through two replicas' list-then-write windows can both succeed. The state is
// never silent — every automation plan and export refuses a namespace in which
// two triggers share a name, naming both — and a lock across replicas would be
// the distributed lock AGENTS.md §2.1 forbids.
func (s *BlobTriggerStore) Create(ctx context.Context, reg triggers.Registration) (triggers.Trigger, string, error) {
	if err := reg.Validate(); err != nil {
		return triggers.Trigger{}, "", err
	}
	if reg.Name != "" {
		s.nameMu.Lock()
		defer s.nameMu.Unlock()
		if err := s.refuseTakenName(ctx, reg.Namespace, reg.Name, ""); err != nil {
			return triggers.Trigger{}, "", err
		}
	}
	for attempt := 0; attempt < triggers.CreateAttempts; attempt++ {
		trigger, secret, err := triggers.Prepare(reg)
		if err != nil {
			return triggers.Trigger{}, "", err
		}
		key, _ := triggerKey(trigger.ID)
		body, err := marshalTriggerRecord(trigger)
		if err != nil {
			return triggers.Trigger{}, "", err
		}
		err = s.b.create(ctx, key, "application/json", body)
		if err == nil {
			return trigger, secret, nil
		}
		if errors.Is(err, errBlobExists) {
			continue
		}
		return triggers.Trigger{}, "", fmt.Errorf("artifacts: create trigger %q: %w", trigger.ID, err)
	}
	return triggers.Trigger{}, "", triggers.ErrIDCollision
}

// List returns the triggers of namespace (and of orkflowName, when set),
// oldest first, without their verification material.
func (s *BlobTriggerStore) List(ctx context.Context, namespace, orkflowName string) ([]triggers.Trigger, error) {
	var out []triggers.Trigger
	err := listObjects(ctx, s.b, blobListInput{Prefix: TriggerPrefix + "/"}, func(object blobObject) (bool, error) {
		if _, ok := parseTriggerKey(object.Key); !ok {
			return false, nil
		}
		trigger, _, found, err := s.get(ctx, object.Key)
		if err != nil {
			return true, err
		}
		if found && trigger.Matches(namespace, orkflowName) {
			out = append(out, trigger.Public())
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: list triggers: %w", err)
	}
	triggers.SortByCreation(out)
	return out, nil
}

// Get reads one trigger with its verification material.
func (s *BlobTriggerStore) Get(ctx context.Context, id string) (triggers.Trigger, bool, error) {
	key, err := triggerKey(id)
	if err != nil {
		return triggers.Trigger{}, false, err
	}
	trigger, _, found, err := s.get(ctx, key)
	return trigger, found, err
}

// get reads one record and the version a conditional write names.
func (s *BlobTriggerStore) get(ctx context.Context, key string) (triggers.Trigger, string, bool, error) {
	raw, version, err := s.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return triggers.Trigger{}, "", false, nil
		}
		return triggers.Trigger{}, "", false, fmt.Errorf("artifacts: get trigger %q: %w", key, err)
	}
	trigger, err := unmarshalTriggerRecord(raw)
	if err != nil {
		return triggers.Trigger{}, "", false, fmt.Errorf("artifacts: get trigger %q: %w", key, err)
	}
	return trigger, version, true, nil
}

// Delete removes one trigger of namespace.
func (s *BlobTriggerStore) Delete(ctx context.Context, id, namespace string) (bool, error) {
	key, err := triggerKey(id)
	if err != nil {
		return false, err
	}
	trigger, _, found, err := s.get(ctx, key)
	if err != nil {
		return false, err
	}
	// A trigger of another namespace is reported absent rather than refused:
	// an id is not confidential, and "not yours" would confirm one exists.
	if !found || trigger.Namespace != namespace {
		return false, nil
	}
	if err := s.b.remove(ctx, key); err != nil {
		return false, fmt.Errorf("artifacts: delete trigger %q: %w", id, err)
	}
	return true, nil
}

// refuseTakenName answers ErrNameTaken when a trigger of namespace other than
// self already holds name. An empty name never collides.
func (s *BlobTriggerStore) refuseTakenName(ctx context.Context, namespace, name, self string) error {
	if name == "" {
		return nil
	}
	list, err := s.List(ctx, namespace, "")
	if err != nil {
		return err
	}
	if triggers.NameHeld(list, name, self) {
		return triggers.ErrNameTaken
	}
	return nil
}

// Update rewrites one record in place with a conditional write: it reads the
// record and its version, applies change, and writes only if the object still
// carries that version. A changed or vanished object re-reads — so a trigger
// deleted meanwhile is then absent and the update reports false. It never
// writes unconditionally, so it can never recreate a deleted record.
func (s *BlobTriggerStore) Update(ctx context.Context, id, namespace string, change func(*triggers.Trigger) error) (triggers.Trigger, bool, error) {
	key, err := triggerKey(id)
	if err != nil {
		return triggers.Trigger{}, false, err
	}
	for attempt := 0; attempt < triggers.UpdateAttempts; attempt++ {
		next, found, err := s.updateOnce(ctx, key, id, namespace, change)
		if !errors.Is(err, errBlobChanged) {
			return next, found, err
		}
	}
	return triggers.Trigger{}, true, triggers.ErrUpdateContention
}

// updateOnce is one read-modify-conditional-write round; errBlobChanged asks
// for another.
func (s *BlobTriggerStore) updateOnce(ctx context.Context, key, id, namespace string, change func(*triggers.Trigger) error) (triggers.Trigger, bool, error) {
	current, version, found, err := s.get(ctx, key)
	if err != nil {
		return triggers.Trigger{}, false, err
	}
	if !found || current.Namespace != namespace {
		return triggers.Trigger{}, false, nil
	}
	next, err := triggers.ApplyChange(current, change)
	if err != nil {
		return triggers.Trigger{}, true, err
	}
	if next.Name != current.Name && next.Name != "" {
		s.nameMu.Lock()
		defer s.nameMu.Unlock()
		if err := s.refuseTakenName(ctx, namespace, next.Name, id); err != nil {
			return triggers.Trigger{}, true, err
		}
	}
	if s.beforeConditionalPut != nil {
		s.beforeConditionalPut()
	}
	body, err := marshalTriggerRecord(next)
	if err != nil {
		return triggers.Trigger{}, true, err
	}
	if err := s.b.replace(ctx, key, "application/json", body, version); err != nil {
		if errors.Is(err, errBlobChanged) {
			return triggers.Trigger{}, true, err
		}
		return triggers.Trigger{}, true, fmt.Errorf("artifacts: update trigger %q: %w", id, err)
	}
	return next.Public(), true, nil
}
