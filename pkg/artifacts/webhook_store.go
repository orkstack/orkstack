package artifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
// recreate a record deleted meanwhile; on the local backend it is serialised
// with deletes by the store's mutex.
const WebhookPrefix = "metadata/webhooks"

var webhookIDPattern = regexp.MustCompile(`^wh-[0-9a-f]{16,}$`)

// NewWebhookStore selects the same shared backend as artifacts and run
// history. It is a registry, not an artifact: callers always read it directly
// and no process keeps a startup snapshot.
func NewWebhookStore(ctx context.Context, cfg Config) (webhooks.Store, error) {
	switch cfg.Backend {
	case "", BackendLocal:
		return NewLocalWebhookStore(localDir(cfg))
	case BackendS3:
		return NewS3WebhookStore(ctx, cfg)
	default:
		return nil, fmt.Errorf("artifacts: webhook store backend %q: %w", cfg.Backend, ErrUnknownBackend)
	}
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
	if err := json.Indent(&out, raw, "", "  "); err != nil {
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

// S3WebhookStore is an S3-compatible shared webhook registry.
type S3WebhookStore struct {
	client *s3.Client
	bucket string
	// beforeConditionalPut, when set, runs between Update's read and its
	// conditional write. Tests use it to delete the record in that window.
	beforeConditionalPut func()
}

var _ webhooks.Store = (*S3WebhookStore)(nil)

func NewS3WebhookStore(ctx context.Context, cfg Config) (*S3WebhookStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3WebhookStore{client: client, bucket: bucket}, nil
}

// Register writes one record with a conditional create. A named registration
// first refuses a name another subscription of the same namespace scope holds;
// that check is best-effort across replicas on the terms
// S3TriggerStore.Create states, and every automation plan and export refuses
// the state it can leave behind.
func (s *S3WebhookStore) Register(ctx context.Context, reg webhooks.Registration) (webhooks.Webhook, error) {
	if reg.Name != "" {
		if err := webhooks.ValidateName(reg.Name, reg.Namespace); err != nil {
			return webhooks.Webhook{}, err
		}
		list, err := s.List(ctx)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		if webhooks.NameHeld(list, reg.Name, reg.Namespace, "") {
			return webhooks.Webhook{}, webhooks.ErrNameTaken
		}
	}
	for attempt := 0; attempt < 8; attempt++ {
		hook, err := preparedWebhook(reg)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		key, _ := webhookKey(hook.ID)
		body, err := marshalWebhookRecord(hook)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*")})
		if err == nil {
			return hook, nil
		}
		if isPreconditionFailed(err) {
			continue
		}
		return webhooks.Webhook{}, fmt.Errorf("artifacts: register webhook %q: %w", hook.ID, err)
	}
	return webhooks.Webhook{}, errors.New("artifacts: register webhook: repeated id collision")
}

// List returns every subscription without its secret.
func (s *S3WebhookStore) List(ctx context.Context) ([]webhooks.Webhook, error) {
	return s.list(ctx, true)
}

// ListForDelivery returns every subscription with its signing secret, for the
// dispatcher only.
func (s *S3WebhookStore) ListForDelivery(ctx context.Context) ([]webhooks.Webhook, error) {
	return s.list(ctx, false)
}

func (s *S3WebhookStore) list(ctx context.Context, public bool) ([]webhooks.Webhook, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(WebhookPrefix + "/")})
	var out []webhooks.Webhook
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list webhooks: %w", err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if _, ok := parseWebhookKey(key); !ok {
				continue
			}
			hook, found, err := s.get(ctx, key)
			if err != nil {
				return nil, err
			}
			if found {
				if public {
					hook = hook.Public()
				}
				out = append(out, hook)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}
func (s *S3WebhookStore) Get(ctx context.Context, id string) (webhooks.Webhook, bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	hook, ok, err := s.get(ctx, key)
	return hook.Public(), ok, err
}
func (s *S3WebhookStore) get(ctx context.Context, key string) (webhooks.Webhook, bool, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return webhooks.Webhook{}, false, nil
		}
		return webhooks.Webhook{}, false, fmt.Errorf("artifacts: get webhook %q: %w", key, err)
	}
	defer object.Body.Close()
	raw, err := io.ReadAll(object.Body)
	if err != nil {
		return webhooks.Webhook{}, false, fmt.Errorf("artifacts: read webhook %q: %w", key, err)
	}
	hook, err := unmarshalWebhookRecord(raw)
	if err != nil {
		return webhooks.Webhook{}, false, fmt.Errorf("artifacts: get webhook %q: %w", key, err)
	}
	return hook, true, nil
}
func (s *S3WebhookStore) Delete(ctx context.Context, id string) (bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return false, err
	}
	_, found, err := s.get(ctx, key)
	if err != nil || !found {
		return found, err
	}
	if _, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}); err != nil {
		return false, fmt.Errorf("artifacts: delete webhook %q: %w", id, err)
	}
	return true, nil
}

// Update rewrites one record in place with a conditional PUT on the ETag it
// read; a precondition failure or the object vanishing re-reads, so a
// subscription deleted meanwhile is reported absent and never recreated. The
// secret is decoded from the record, carried into the rewrite, and stripped
// from the return.
func (s *S3WebhookStore) Update(ctx context.Context, id string, change func(*webhooks.Webhook) error) (webhooks.Webhook, bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	for attempt := 0; attempt < webhooks.UpdateAttempts; attempt++ {
		current, etag, found, err := s.getWithETag(ctx, key)
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
		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String("application/json"),
			IfMatch: aws.String(etag),
		})
		if err == nil {
			return next.Public(), true, nil
		}
		if isPreconditionFailed(err) || isNotFound(err) {
			continue
		}
		return webhooks.Webhook{}, true, fmt.Errorf("artifacts: update webhook %q: %w", id, err)
	}
	return webhooks.Webhook{}, true, webhooks.ErrUpdateContention
}

// getWithETag is get plus the object's ETag, the token Update's conditional
// write names.
func (s *S3WebhookStore) getWithETag(ctx context.Context, key string) (webhooks.Webhook, string, bool, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return webhooks.Webhook{}, "", false, nil
		}
		return webhooks.Webhook{}, "", false, fmt.Errorf("artifacts: get webhook %q: %w", key, err)
	}
	defer object.Body.Close()
	raw, err := io.ReadAll(object.Body)
	if err != nil {
		return webhooks.Webhook{}, "", false, fmt.Errorf("artifacts: read webhook %q: %w", key, err)
	}
	hook, err := unmarshalWebhookRecord(raw)
	if err != nil {
		return webhooks.Webhook{}, "", false, fmt.Errorf("artifacts: get webhook %q: %w", key, err)
	}
	return hook, aws.ToString(object.ETag), true, nil
}

// LocalWebhookStore confines registry files below an os.Root. IDs are
// validated before every path operation; no client value can traverse this
// directory or follow a symlink outside it.
//
// mu serialises a named registration's uniqueness check and publish, every
// update and every delete, so within one process an update can never
// resurrect a delete. Several processes writing one local directory
// concurrently are not a supported topology (AGENTS.md §2.2).
type LocalWebhookStore struct {
	root *os.Root
	mu   sync.Mutex
}

var _ webhooks.Store = (*LocalWebhookStore)(nil)

func NewLocalWebhookStore(dir string) (*LocalWebhookStore, error) {
	if dir == "" {
		return nil, errors.New("artifacts: local webhook store: empty directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	if err := root.MkdirAll(WebhookPrefix, 0o755); err != nil {
		_ = root.Close()
		return nil, err
	}
	if err := root.MkdirAll(localTempDir, 0o755); err != nil {
		_ = root.Close()
		return nil, err
	}
	return &LocalWebhookStore{root: root}, nil
}
func (s *LocalWebhookStore) Register(ctx context.Context, reg webhooks.Registration) (webhooks.Webhook, error) {
	if err := ctx.Err(); err != nil {
		return webhooks.Webhook{}, err
	}
	if reg.Name != "" {
		if err := webhooks.ValidateName(reg.Name, reg.Namespace); err != nil {
			return webhooks.Webhook{}, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		list, err := s.List(ctx)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		if webhooks.NameHeld(list, reg.Name, reg.Namespace, "") {
			return webhooks.Webhook{}, webhooks.ErrNameTaken
		}
	}
	for attempt := 0; attempt < 8; attempt++ {
		hook, err := preparedWebhook(reg)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		key, _ := webhookKey(hook.ID)
		if err := s.root.MkdirAll(path.Dir(key), 0o755); err != nil {
			return webhooks.Webhook{}, fmt.Errorf("artifacts: create webhook directory: %w", err)
		}
		body, err := marshalWebhookRecord(hook)
		if err != nil {
			return webhooks.Webhook{}, err
		}
		tmpKey := path.Join(localTempDir, "webhook-"+randomToken())
		f, err := s.root.OpenFile(tmpKey, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return webhooks.Webhook{}, fmt.Errorf("artifacts: register webhook %q: %w", hook.ID, err)
		}
		if _, err = f.Write(body); err == nil {
			err = f.Sync()
		}
		cerr := f.Close()
		if err == nil {
			err = cerr
		}
		if err != nil {
			_ = s.root.Remove(tmpKey)
			return webhooks.Webhook{}, fmt.Errorf("artifacts: register webhook %q: %w", hook.ID, err)
		}
		// Link publishes the fully written inode without replacing another
		// replica's winner. A crash before this point leaves only an ignored
		// temp file; readers never see a partial final record.
		err = s.root.Link(tmpKey, key)
		_ = s.root.Remove(tmpKey)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return webhooks.Webhook{}, fmt.Errorf("artifacts: publish webhook %q: %w", hook.ID, err)
		}
		if err := s.syncDir(path.Dir(key)); err != nil {
			return webhooks.Webhook{}, fmt.Errorf("artifacts: sync webhook %q directory: %w", hook.ID, err)
		}
		return hook, nil
	}
	return webhooks.Webhook{}, errors.New("artifacts: register webhook: repeated id collision")
}

func (s *LocalWebhookStore) syncDir(key string) error {
	dir, err := s.root.Open(key)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// List returns every subscription without its secret.
func (s *LocalWebhookStore) List(ctx context.Context) ([]webhooks.Webhook, error) {
	return s.list(ctx, true)
}

// ListForDelivery returns every subscription with its signing secret, for the
// dispatcher only.
func (s *LocalWebhookStore) ListForDelivery(ctx context.Context) ([]webhooks.Webhook, error) {
	return s.list(ctx, false)
}

func (s *LocalWebhookStore) list(ctx context.Context, public bool) ([]webhooks.Webhook, error) {
	var out []webhooks.Webhook
	err := fs.WalkDir(s.root.FS(), WebhookPrefix, func(key string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("artifacts: webhook registry contains symlink %q", key)
		}
		if _, ok := parseWebhookKey(key); !ok {
			return nil
		}
		hook, ok, err := s.get(ctx, key)
		if err != nil {
			return err
		}
		if ok {
			if public {
				hook = hook.Public()
			}
			out = append(out, hook)
		}
		return nil
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
func (s *LocalWebhookStore) Get(ctx context.Context, id string) (webhooks.Webhook, bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	hook, ok, err := s.get(ctx, key)
	return hook.Public(), ok, err
}
func (s *LocalWebhookStore) get(ctx context.Context, key string) (webhooks.Webhook, bool, error) {
	if err := ctx.Err(); err != nil {
		return webhooks.Webhook{}, false, err
	}
	f, err := s.root.Open(key)
	if errors.Is(err, fs.ErrNotExist) {
		return webhooks.Webhook{}, false, nil
	}
	if err != nil {
		return webhooks.Webhook{}, false, fmt.Errorf("artifacts: get webhook %q: %w", key, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	hook, err := unmarshalWebhookRecord(raw)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	return hook, true, nil
}
func (s *LocalWebhookStore) Delete(ctx context.Context, id string) (bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found, err := s.get(ctx, key)
	if err != nil || !found {
		return found, err
	}
	if err := s.root.Remove(key); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("artifacts: delete webhook %q: %w", id, err)
	}
	return true, nil
}

// Update reads, applies change, and renames a rewritten record over the key,
// all under the mutex Delete holds, so a deleted subscription stays deleted.
// The secret is carried over and stripped from the return.
func (s *LocalWebhookStore) Update(ctx context.Context, id string, change func(*webhooks.Webhook) error) (webhooks.Webhook, bool, error) {
	key, err := webhookKey(id)
	if err != nil {
		return webhooks.Webhook{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found, err := s.get(ctx, key)
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
		list, err := s.List(ctx)
		if err != nil {
			return webhooks.Webhook{}, true, err
		}
		if webhooks.NameHeld(list, next.Name, next.Namespace, id) {
			return webhooks.Webhook{}, true, webhooks.ErrNameTaken
		}
	}
	body, err := marshalWebhookRecord(next)
	if err != nil {
		return webhooks.Webhook{}, true, err
	}
	if err := replaceLocalRecord(s.root, key, "webhook-", body); err != nil {
		return webhooks.Webhook{}, true, fmt.Errorf("artifacts: update webhook %q: %w", id, err)
	}
	return next.Public(), true, nil
}
