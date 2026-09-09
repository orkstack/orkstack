package artifacts

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

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
	switch cfg.Backend {
	case "", BackendLocal:
		return NewLocalTriggerStore(localDir(cfg))
	case BackendS3:
		return NewS3TriggerStore(ctx, cfg)
	default:
		return nil, fmt.Errorf("artifacts: trigger store backend %q: %w", cfg.Backend, ErrUnknownBackend)
	}
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
	ID             string    `json:"id"`
	Namespace      string    `json:"namespace"`
	OrkflowName    string    `json:"orkflowName"`
	OrkflowVersion string    `json:"orkflowVersion,omitempty"`
	Description    string    `json:"description,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
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
		CreatedAt:      trigger.CreatedAt.UTC(),
		SecretSalt:     hex.EncodeToString(trigger.SecretSalt),
		SecretHash:     hex.EncodeToString(trigger.SecretHash),
	}
	raw, err := json.MarshalIndent(record, "", "  ")
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
		CreatedAt:      record.CreatedAt.UTC(),
		SecretSalt:     salt,
		SecretHash:     digest,
	}, nil
}

// S3TriggerStore is an S3-compatible shared trigger registry.
type S3TriggerStore struct {
	client *s3.Client
	bucket string
}

var _ triggers.Store = (*S3TriggerStore)(nil)

func NewS3TriggerStore(ctx context.Context, cfg Config) (*S3TriggerStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3TriggerStore{client: client, bucket: bucket}, nil
}

// Create writes one record with a conditional PUT, so two replicas drawing the
// same id cannot both believe they created it: the loser retries with a fresh
// one rather than overwriting a trigger somebody is already holding a token
// for.
func (s *S3TriggerStore) Create(ctx context.Context, reg triggers.Registration) (triggers.Trigger, string, error) {
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
		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
			ContentType:   aws.String("application/json"),
			IfNoneMatch:   aws.String("*"),
		})
		if err == nil {
			return trigger, secret, nil
		}
		if isPreconditionFailed(err) {
			continue
		}
		return triggers.Trigger{}, "", fmt.Errorf("artifacts: create trigger %q: %w", trigger.ID, err)
	}
	return triggers.Trigger{}, "", triggers.ErrIDCollision
}

func (s *S3TriggerStore) List(ctx context.Context, namespace, orkflowName string) ([]triggers.Trigger, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(TriggerPrefix + "/"),
	})
	var out []triggers.Trigger
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list triggers: %w", err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if _, ok := parseTriggerKey(key); !ok {
				continue
			}
			trigger, found, err := s.get(ctx, key)
			if err != nil {
				return nil, err
			}
			if found && trigger.Matches(namespace, orkflowName) {
				out = append(out, trigger.Public())
			}
		}
	}
	triggers.SortByCreation(out)
	return out, nil
}

func (s *S3TriggerStore) Get(ctx context.Context, id string) (triggers.Trigger, bool, error) {
	key, err := triggerKey(id)
	if err != nil {
		return triggers.Trigger{}, false, err
	}
	return s.get(ctx, key)
}

func (s *S3TriggerStore) get(ctx context.Context, key string) (triggers.Trigger, bool, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return triggers.Trigger{}, false, nil
		}
		return triggers.Trigger{}, false, fmt.Errorf("artifacts: get trigger %q: %w", key, err)
	}
	defer object.Body.Close()
	raw, err := io.ReadAll(object.Body)
	if err != nil {
		return triggers.Trigger{}, false, fmt.Errorf("artifacts: read trigger %q: %w", key, err)
	}
	trigger, err := unmarshalTriggerRecord(raw)
	if err != nil {
		return triggers.Trigger{}, false, fmt.Errorf("artifacts: get trigger %q: %w", key, err)
	}
	return trigger, true, nil
}

func (s *S3TriggerStore) Delete(ctx context.Context, id, namespace string) (bool, error) {
	key, err := triggerKey(id)
	if err != nil {
		return false, err
	}
	trigger, found, err := s.get(ctx, key)
	if err != nil {
		return false, err
	}
	// A trigger of another namespace is reported absent rather than refused:
	// an id is not confidential, and "not yours" would confirm one exists.
	if !found || trigger.Namespace != namespace {
		return false, nil
	}
	if _, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}); err != nil {
		return false, fmt.Errorf("artifacts: delete trigger %q: %w", id, err)
	}
	return true, nil
}

// LocalTriggerStore confines registry files below an os.Root. IDs are
// validated before every path operation; no client value can traverse this
// directory or follow a symlink outside it.
type LocalTriggerStore struct{ root *os.Root }

var _ triggers.Store = (*LocalTriggerStore)(nil)

func NewLocalTriggerStore(dir string) (*LocalTriggerStore, error) {
	if dir == "" {
		return nil, errors.New("artifacts: local trigger store: empty directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	if err := root.MkdirAll(TriggerPrefix, 0o755); err != nil {
		_ = root.Close()
		return nil, err
	}
	if err := root.MkdirAll(localTempDir, 0o755); err != nil {
		_ = root.Close()
		return nil, err
	}
	return &LocalTriggerStore{root: root}, nil
}

func (s *LocalTriggerStore) Create(ctx context.Context, reg triggers.Registration) (triggers.Trigger, string, error) {
	if err := ctx.Err(); err != nil {
		return triggers.Trigger{}, "", err
	}
	for attempt := 0; attempt < triggers.CreateAttempts; attempt++ {
		trigger, secret, err := triggers.Prepare(reg)
		if err != nil {
			return triggers.Trigger{}, "", err
		}
		key, _ := triggerKey(trigger.ID)
		if err := s.root.MkdirAll(path.Dir(key), 0o755); err != nil {
			return triggers.Trigger{}, "", fmt.Errorf("artifacts: create trigger directory: %w", err)
		}
		body, err := marshalTriggerRecord(trigger)
		if err != nil {
			return triggers.Trigger{}, "", err
		}
		tmpKey := path.Join(localTempDir, "trigger-"+randomToken())
		f, err := s.root.OpenFile(tmpKey, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return triggers.Trigger{}, "", fmt.Errorf("artifacts: create trigger %q: %w", trigger.ID, err)
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
			return triggers.Trigger{}, "", fmt.Errorf("artifacts: create trigger %q: %w", trigger.ID, err)
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
			return triggers.Trigger{}, "", fmt.Errorf("artifacts: publish trigger %q: %w", trigger.ID, err)
		}
		if err := s.syncDir(path.Dir(key)); err != nil {
			return triggers.Trigger{}, "", fmt.Errorf("artifacts: sync trigger %q directory: %w", trigger.ID, err)
		}
		return trigger, secret, nil
	}
	return triggers.Trigger{}, "", triggers.ErrIDCollision
}

func (s *LocalTriggerStore) syncDir(key string) error {
	dir, err := s.root.Open(key)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *LocalTriggerStore) List(ctx context.Context, namespace, orkflowName string) ([]triggers.Trigger, error) {
	var out []triggers.Trigger
	err := fs.WalkDir(s.root.FS(), TriggerPrefix, func(key string, d fs.DirEntry, err error) error {
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
			return fmt.Errorf("artifacts: trigger registry contains symlink %q", key)
		}
		if _, ok := parseTriggerKey(key); !ok {
			return nil
		}
		trigger, ok, err := s.get(ctx, key)
		if err != nil {
			return err
		}
		if ok && trigger.Matches(namespace, orkflowName) {
			out = append(out, trigger.Public())
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: list triggers: %w", err)
	}
	triggers.SortByCreation(out)
	return out, nil
}

func (s *LocalTriggerStore) Get(ctx context.Context, id string) (triggers.Trigger, bool, error) {
	key, err := triggerKey(id)
	if err != nil {
		return triggers.Trigger{}, false, err
	}
	return s.get(ctx, key)
}

func (s *LocalTriggerStore) get(ctx context.Context, key string) (triggers.Trigger, bool, error) {
	if err := ctx.Err(); err != nil {
		return triggers.Trigger{}, false, err
	}
	f, err := s.root.Open(key)
	if errors.Is(err, fs.ErrNotExist) {
		return triggers.Trigger{}, false, nil
	}
	if err != nil {
		return triggers.Trigger{}, false, fmt.Errorf("artifacts: get trigger %q: %w", key, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		return triggers.Trigger{}, false, err
	}
	trigger, err := unmarshalTriggerRecord(raw)
	if err != nil {
		return triggers.Trigger{}, false, err
	}
	return trigger, true, nil
}

func (s *LocalTriggerStore) Delete(ctx context.Context, id, namespace string) (bool, error) {
	key, err := triggerKey(id)
	if err != nil {
		return false, err
	}
	trigger, found, err := s.get(ctx, key)
	if err != nil {
		return false, err
	}
	// Same rule as the S3 store: another namespace's trigger is absent here.
	if !found || trigger.Namespace != namespace {
		return false, nil
	}
	if err := s.root.Remove(key); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("artifacts: delete trigger %q: %w", id, err)
	}
	return true, nil
}
