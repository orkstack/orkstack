package artifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// WebhookPrefix is a deployment-wide registry, deliberately outside a
// namespace: a subscription's scope lives inside its record, so the key
// layout stays flat. Each subscription has its own object so registration
// can use object-store conditional create rather than a process-local lock.
//
// A record is rewritable by exactly one operation, UpdateWebhook, and only in
// its mutable fields (name, url, events, scope): the id and the signing
// secret — what a receiver and a delivery record hold — never change. On S3
// the rewrite is a conditional put naming the ETag it read, so it can never
// recreate a record deleted meanwhile; a local directory compares and renames
// under the lock its removals take.
const WebhookPrefix = "metadata/webhooks"

var webhookIDPattern = regexp.MustCompile(`^wh-[0-9a-f]{16,}$`)

// NewWebhookStore selects the same shared backend as artifacts and run
// history. It is a registry, not an artifact: callers always read it directly
// and no process keeps a startup snapshot.
func NewWebhookStore(ctx context.Context, cfg Config) (webhooks.Store, error) {
	b, err := openBlobs(ctx, cfg, "webhook store", WebhookPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobWebhookStore{blobBase: blobBase{b}}, nil
}

// ErrInvalidWebhookID reports an id no stored subscription can have. A caller
// answers it as "not found" rather than as a storage fault.
var ErrInvalidWebhookID = errors.New("invalid webhook id")

func webhookKey(id string) (string, error) {
	if !webhookIDPattern.MatchString(id) {
		return "", fmt.Errorf("artifacts: %w %q", ErrInvalidWebhookID, id)
	}
	return WebhookPrefix + "/" + id + ".json", nil
}

func parseWebhookKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, WebhookPrefix+"/")
	if !ok {
		return "", false
	}
	id, ok := strings.CutSuffix(rest, ".json")
	return id, ok && webhookIDPattern.MatchString(id)
}

// webhookRecord deliberately reuses the generated RegisterWebhookResponse as
// the durable wire record: public Webhook cannot contain the signing secret.
// It is never returned from List or Get.
func marshalWebhookRecord(hook webhooks.Webhook) ([]byte, error) {
	message := &orkstackv1.RegisterWebhookResponse{
		Webhook: &orkstackv1.Webhook{
			Id: hook.ID, Url: hook.URL, Events: eventsToProto(hook.Events), CreatedAt: timestamppb.New(hook.CreatedAt),
			Namespace: hook.Namespace, OrkflowName: hook.OrkflowName, TaskId: hook.TaskID,
			Name: hook.Name,
		},
		Secret: hook.Secret,
	}
	raw, err := protojson.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode webhook %q: %w", hook.ID, err)
	}
	var out bytes.Buffer
	if err := stablejson.Indent(&out, raw, "  "); err != nil {
		return nil, fmt.Errorf("encode webhook %q: %w", hook.ID, err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

func unmarshalWebhookRecord(raw []byte) (webhooks.Webhook, error) {
	message := &orkstackv1.RegisterWebhookResponse{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, message); err != nil {
		return webhooks.Webhook{}, fmt.Errorf("decode webhook document: %w", err)
	}
	w := message.GetWebhook()
	if w == nil {
		return webhooks.Webhook{}, errors.New("decode webhook document: missing webhook")
	}
	id := w.GetId()
	if _, err := webhookKey(id); err != nil {
		return webhooks.Webhook{}, err
	}
	events, err := eventsFromProto(w.GetEvents())
	if err != nil {
		return webhooks.Webhook{}, err
	}
	if _, err := webhooks.ValidateURL(w.GetUrl()); err != nil {
		return webhooks.Webhook{}, err
	}
	if !w.GetCreatedAt().IsValid() {
		return webhooks.Webhook{}, fmt.Errorf("decode webhook %q: invalid created_at", id)
	}
	// A record written before scope existed has none, which is
	// deployment-wide: what it meant when it was written. A record edited by
	// hand into an invalid scope is refused, never silently widened.
	if err := webhooks.ValidateScope(w.GetNamespace(), w.GetOrkflowName(), w.GetTaskId(), events); err != nil {
		return webhooks.Webhook{}, fmt.Errorf("decode webhook %q: %w", id, err)
	}
	// A record written before names existed has none and decodes unnamed; a
	// hand-edited invalid name is refused, as an invalid scope is.
	if err := webhooks.ValidateName(w.GetName(), w.GetNamespace()); err != nil {
		return webhooks.Webhook{}, fmt.Errorf("decode webhook %q: %w", id, err)
	}
	return webhooks.Webhook{
		ID: id, URL: w.GetUrl(), Events: events, Secret: message.GetSecret(), CreatedAt: w.GetCreatedAt().AsTime().UTC(),
		Namespace: w.GetNamespace(), OrkflowName: w.GetOrkflowName(), TaskID: w.GetTaskId(), Name: w.GetName(),
	}, nil
}

func eventsToProto(events []webhooks.Event) []orkstackv1.EventType {
	out := make([]orkstackv1.EventType, 0, len(events))
	for _, event := range events {
		if value, ok := orkstackv1.EventType_value["EVENT_TYPE_"+string(event)]; ok {
			out = append(out, orkstackv1.EventType(value))
		}
	}
	return out
}

func eventsFromProto(events []orkstackv1.EventType) ([]webhooks.Event, error) {
	out := make([]webhooks.Event, 0, len(events))
	for _, event := range events {
		parsed, err := webhooks.ParseEvent(event.String())
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	// Registration validates and normalizes duplicates/order. Reuse that rule
	// through the public parser without inventing a second event schema.
	seen := make(map[webhooks.Event]bool, len(out))
	normalized := out[:0]
	for _, want := range webhooks.KnownEvents() {
		for _, event := range out {
			if event == want {
				seen[event] = true
			}
		}
		if seen[want] {
			normalized = append(normalized, want)
		}
	}
	return normalized, nil
}

func preparedWebhook(reg webhooks.Registration) (webhooks.Webhook, error) {
	url, err := webhooks.ValidateURL(reg.URL)
	if err != nil {
		return webhooks.Webhook{}, err
	}
	// Validate via ParseEvent and normalize lifecycle order.
	seen := map[webhooks.Event]bool{}
	for _, raw := range reg.Events {
		event, err := webhooks.ParseEvent(string(raw))
		if err != nil {
			return webhooks.Webhook{}, err
		}
		seen[event] = true
	}
	events := make([]webhooks.Event, 0, len(seen))
	for _, event := range webhooks.KnownEvents() {
		if seen[event] {
			events = append(events, event)
		}
	}
	if err := webhooks.ValidateScope(reg.Namespace, reg.OrkflowName, reg.TaskID, events); err != nil {
		return webhooks.Webhook{}, err
	}
	if err := webhooks.ValidateName(reg.Name, reg.Namespace); err != nil {
		return webhooks.Webhook{}, err
	}
	secret := reg.Secret
	if secret == "" {
		secret, err = webhookToken("", 32)
		if err != nil {
			return webhooks.Webhook{}, err
		}
	}
	id, err := webhookToken("wh-", 8)
	if err != nil {
		return webhooks.Webhook{}, err
	}
	return webhooks.Webhook{
		ID: id, URL: url, Events: events, Secret: secret, CreatedAt: time.Now().UTC(),
		Namespace: reg.Namespace, OrkflowName: reg.OrkflowName, TaskID: reg.TaskID, Name: reg.Name,
	}, nil
}

func webhookToken(prefix string, bytes int) (string, error) {
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate webhook token: %w", err)
	}
	return prefix + hex.EncodeToString(b), nil
}

// BlobWebhookStore is the shared webhook registry, one object per
// subscription in the object store every replica reads.
//
// nameMu serialises, within one process, a named registration's uniqueness
// check with its publish, and an update's check with its write. Across
// replicas the check is best-effort (Register says why); a conditional
// write, never this lock, is what keeps an update from recreating a deleted
// subscription.
type BlobWebhookStore struct {
	blobBase
	nameMu sync.Mutex
	// beforeConditionalPut, when set, runs between Update's read and its
	// conditional write. Tests use it to delete the record in that window.
	beforeConditionalPut func()
}

// LocalWebhookStore and S3WebhookStore are the webhook registry over a local
// directory and over a bucket: one implementation, two backends.
type (
	LocalWebhookStore = BlobWebhookStore
	S3WebhookStore    = BlobWebhookStore
)

var _ webhooks.Store = (*BlobWebhookStore)(nil)

// NewLocalWebhookStore opens (creating if needed) the registry under dir.
func NewLocalWebhookStore(dir string) (*LocalWebhookStore, error) {
	b, err := openLocalBlobs("webhook store", dir, WebhookPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobWebhookStore{blobBase: blobBase{b}}, nil
}

// NewS3WebhookStore builds the registry from the shared ORK_S3_* config.
func NewS3WebhookStore(ctx context.Context, cfg Config) (*S3WebhookStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobWebhookStore{blobBase: blobBase{b}}, nil
}

// Register writes one record create-only. A named registration first refuses
// a name another subscription of the same namespace scope holds; that check
// is best-effort across replicas on the terms BlobTriggerStore.Create states,
// and every automation plan and export refuses the state it can leave behind.
func (s *BlobWebhookStore) Register(ctx context.Context, reg webhooks.Registration) (webhooks.Webhook, error) {
	if err := ctx.Err(); err != nil {
		return webhooks.Webhook{}, err
	}
	if reg.Name != "" {
		if err := webhooks.ValidateName(reg.Name, reg.Namespace); err != nil {
			return webhooks.Webhook{}, err
		}
		s.nameMu.Lock()
		defer s.nameMu.Unlock()
		list, err := s.List(ctx)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		if webhooks.NameHeld(list, reg.Name, reg.Namespace, "") {
			return webhooks.Webhook{}, webhooks.ErrNameTaken
		}
	}
	for attempt := 0; attempt < webhooks.CreateAttempts; attempt++ {
		hook, err := preparedWebhook(reg)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		key, _ := webhookKey(hook.ID)
		body, err := marshalWebhookRecord(hook)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		err = s.b.create(ctx, key, "application/json", body)
		if err == nil {
			return hook, nil
		}
		if errors.Is(err, errBlobExists) {
			continue
		}
		return webhooks.Webhook{}, fmt.Errorf("artifacts: register webhook %q: %w", hook.ID, err)
	}
	return webhooks.Webhook{}, errors.New("artifacts: register webhook: repeated id collision")
}

// List returns every subscription without its secret.
func (s *BlobWebhookStore) List(ctx context.Context) ([]webhooks.Webhook, error) {
	return s.list(ctx, true)
}

// ListForDelivery returns every subscription with its signing secret, for the
// dispatcher only.
func (s *BlobWebhookStore) ListForDelivery(ctx context.Context) ([]webhooks.Webhook, error) {
	return s.list(ctx, false)
}

func (s *BlobWebhookStore) list(ctx context.Context, public bool) ([]webhooks.Webhook, error) {
	var out []webhooks.Webhook
	err := listObjects(ctx, s.b, blobListInput{Prefix: WebhookPrefix + "/"}, func(object blobObject) (bool, error) {
		if _, ok := parseWebhookKey(object.Key); !ok {
			return false, nil
		}
		hook, _, found, err := s.get(ctx, object.Key)
		if err != nil {
			return true, err
		}
		if found {
			if public {
				hook = hook.Public()
			}
			out = append(out, hook)
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: list webhooks: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

// Get reads one subscription without its secret.
func (s *BlobWebhookStore) Get(ctx context.Context, id string) (webhooks.Webhook, bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	hook, _, ok, err := s.get(ctx, key)
	return hook.Public(), ok, err
}

// get reads one record and the version a conditional write names.
func (s *BlobWebhookStore) get(ctx context.Context, key string) (webhooks.Webhook, string, bool, error) {
	raw, version, err := s.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return webhooks.Webhook{}, "", false, nil
		}
		return webhooks.Webhook{}, "", false, fmt.Errorf("artifacts: get webhook %q: %w", key, err)
	}
	hook, err := unmarshalWebhookRecord(raw)
	if err != nil {
		return webhooks.Webhook{}, "", false, fmt.Errorf("artifacts: get webhook %q: %w", key, err)
	}
	return hook, version, true, nil
}

// Delete removes one subscription.
func (s *BlobWebhookStore) Delete(ctx context.Context, id string) (bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return false, err
	}
	_, _, found, err := s.get(ctx, key)
	if err != nil || !found {
		return found, err
	}
	if err := s.b.remove(ctx, key); err != nil {
		return false, fmt.Errorf("artifacts: delete webhook %q: %w", id, err)
	}
	return true, nil
}

// Update rewrites one record in place with a conditional write on the version
// it read; a changed or vanished object re-reads, so a subscription deleted
// meanwhile is reported absent and never recreated. The secret is decoded
// from the record, carried into the rewrite, and stripped from the return.
func (s *BlobWebhookStore) Update(ctx context.Context, id string, change func(*webhooks.Webhook) error) (webhooks.Webhook, bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	for attempt := 0; attempt < webhooks.UpdateAttempts; attempt++ {
		next, found, err := s.updateOnce(ctx, key, id, change)
		if !errors.Is(err, errBlobChanged) {
			return next, found, err
		}
	}
	return webhooks.Webhook{}, true, webhooks.ErrUpdateContention
}

// updateOnce is one read-modify-conditional-write round; errBlobChanged asks
// for another.
func (s *BlobWebhookStore) updateOnce(ctx context.Context, key, id string, change func(*webhooks.Webhook) error) (webhooks.Webhook, bool, error) {
	current, version, found, err := s.get(ctx, key)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	if !found {
		return webhooks.Webhook{}, false, nil
	}
	next, err := webhooks.ApplyChange(current, change)
	if err != nil {
		return webhooks.Webhook{}, true, err
	}
	if next.Name != "" && (next.Name != current.Name || next.Namespace != current.Namespace) {
		s.nameMu.Lock()
		defer s.nameMu.Unlock()
		list, err := s.List(ctx)
		if err != nil {
			return webhooks.Webhook{}, true, err
		}
		if webhooks.NameHeld(list, next.Name, next.Namespace, id) {
			return webhooks.Webhook{}, true, webhooks.ErrNameTaken
		}
	}
	if s.beforeConditionalPut != nil {
		s.beforeConditionalPut()
	}
	body, err := marshalWebhookRecord(next)
	if err != nil {
		return webhooks.Webhook{}, true, err
	}
	if err := s.b.replace(ctx, key, "application/json", body, version); err != nil {
		if errors.Is(err, errBlobChanged) {
			return webhooks.Webhook{}, true, err
		}
		return webhooks.Webhook{}, true, fmt.Errorf("artifacts: update webhook %q: %w", id, err)
	}
	return next.Public(), true, nil
}
