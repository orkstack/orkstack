package aws

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/sts/types"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// namespaceContext is the KMS encryption context key that binds a ciphertext
// to the namespace it was encrypted for.
const namespaceContext = "ork:namespace"

// session is one attempt's session of one role.
type session struct {
	p         *Provider
	principal string
	who       provider.Attempt
	token     provider.TokenSource
	now       func() time.Time

	// exchange is held for the whole of an exchange, so that two credentials
	// of one grant asking at once make one.
	exchange sync.Mutex

	mu     sync.Mutex
	keys   keys
	closed bool
}

// keys are the role's session keys.
type keys struct {
	id, secret, token string
	// expiration is AWS's own instant, as it said it, for the SDKs that read
	// it.
	expiration time.Time
	// life is how long they last from when they were asked for.
	life  time.Duration
	asked time.Time
}

// login asks STS for new keys with a fresh identity token.
func (s *session) login(ctx context.Context) error {
	s.mu.Lock()
	mint, closed := s.token, s.closed
	s.mu.Unlock()
	if closed {
		return &provider.Error{Class: provider.Unavailable, Op: opLogin, Code: codeClosed}
	}
	token, _, err := mint(ctx)
	if err != nil {
		var refused *provider.TokenRefused
		if errors.As(err, &refused) {
			return err
		}
		return &provider.Error{Class: provider.Unavailable, Op: opLogin, Code: codeToken, Err: contextEnd(err)}
	}
	asked := s.now()
	duration := s.p.duration(s.who)
	out, err := s.p.sts().AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          awssdk.String(s.principal),
		RoleSessionName:  awssdk.String(sessionName(s.who)),
		WebIdentityToken: awssdk.String(token),
		DurationSeconds:  awssdk.Int32(int32(duration / time.Second)),
	})
	if err != nil {
		return classify(opLogin, err, loginClasses)
	}
	served, _ := awsmiddleware.GetServerTime(out.ResultMetadata)
	k, err := keysOf(out.Credentials, served, duration, asked)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &provider.Error{Class: provider.Unavailable, Op: opLogin, Code: codeClosed}
	}
	s.keys = k
	return nil
}

// keysOf are the keys of STS's answer, made at served by its own date, the
// zero time when it carries none. Their life is counted from that date, never
// the Orker's clock, and is never longer than was asked for.
func keysOf(c *types.Credentials, served time.Time, duration time.Duration, asked time.Time) (keys, error) {
	if c == nil || awssdk.ToString(c.AccessKeyId) == "" || awssdk.ToString(c.SecretAccessKey) == "" ||
		awssdk.ToString(c.SessionToken) == "" || c.Expiration == nil {
		return keys{}, &provider.Error{Class: provider.Unavailable, Op: opLogin, Code: codeIncomplete}
	}
	life := duration
	if !served.IsZero() {
		life = min(life, c.Expiration.Sub(served))
	}
	// Keys too short-lived to be put in place are asked for again rather than
	// handed out: a bundle that short is refused for good.
	if life < provider.MinBundleLifetime {
		return keys{}, &provider.Error{Class: provider.Unavailable, Op: opLogin, Code: codeExpired}
	}
	return keys{id: *c.AccessKeyId, secret: *c.SecretAccessKey, token: *c.SessionToken,
		expiration: *c.Expiration, life: life, asked: asked}, nil
}

// age is how long ago the keys were asked for, by whichever of the monotonic
// and the wall clock says longer: the monotonic one does not run while the
// host sleeps.
func (k keys) age(now time.Time) time.Duration {
	return max(now.Sub(k.asked), now.Round(0).Sub(k.asked.Round(0)), 0)
}

// left is how long the keys still last, at least one nanosecond: zero would
// say they never end.
func (k keys) left(now time.Time) time.Duration { return max(k.life-k.age(now), 1) }

// current are the keys an op uses.
func (s *session) current(op string) (keys, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return keys{}, &provider.Error{Class: provider.Unavailable, Op: op, Code: codeClosed}
	}
	return s.keys, nil
}

// Lifetime is zero, a login that does not end: what ends is its keys, and
// Credential exchanges for new ones itself, so the refresh engine makes one
// exchange a cycle and replaces the login only at its own bound.
func (s *session) Lifetime() time.Duration { return 0 }

// Renew asks for a new credential: an AWS session is never extended, only
// exchanged for again.
func (s *session) Renew(context.Context, provider.Bundle) (provider.Bundle, error) {
	return provider.Bundle{}, provider.ErrReissue
}

// Close drops the keys. AWS has no way to end a session: its keys work until
// they expire, wherever the task copied them.
func (s *session) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed, s.keys, s.token = true, keys{}, nil
	return nil
}

// young are the session's keys while they are in the first quarter of their
// life, and after it new ones, exchanged for with a fresh identity token. The
// callers of one grant that ask at once make one exchange.
func (s *session) young(ctx context.Context, op string) (keys, error) {
	s.exchange.Lock()
	defer s.exchange.Unlock()
	k, err := s.current(op)
	if err != nil || k.age(s.now()) < k.life/4 {
		return k, err
	}
	if err := s.login(ctx); err != nil {
		return keys{}, err
	}
	return s.current(op)
}

// Credential is the role's session as files. A refresh comes no sooner than a
// third of a life, so its keys are always exchanged for anew: it starts from
// the attempt's identity, never from the keys it replaces.
func (s *session) Credential(ctx context.Context, ref provider.Parsed) (provider.Bundle, error) {
	if detail, ok := ref.Detail.(reference); !ok || detail != (reference{}) || ref.Ref.Kind != provider.KindCredential {
		return provider.Bundle{}, &provider.Error{Class: provider.Invalid, Op: opIssue, Code: codeIncomplete}
	}
	if err := provider.ValidCredentialName(ref.Ref.Name); err != nil {
		return provider.Bundle{}, &provider.Error{Class: provider.Invalid, Op: opIssue, Code: codeNoName}
	}
	k, err := s.young(ctx, opIssue)
	if err != nil {
		return provider.Bundle{}, err
	}
	return bundle(s.p.region, ref.Ref.Name, k, s.now())
}

// The files of a credential. Config is plain: it holds a region and a path.
const (
	fileConfig      = "config"
	fileProcess     = "process.json"
	fileCredentials = "credentials"
)

// process is what an AWS SDK reads from a credential_process.
type process struct {
	Version         int
	AccessKeyId     string
	SecretAccessKey string
	SessionToken    string
	Expiration      string
}

// bundle is keys as the files of the credential called name.
func bundle(region, name string, k keys, now time.Time) (provider.Bundle, error) {
	doc, err := json.Marshal(process{Version: 1, AccessKeyId: k.id, SecretAccessKey: k.secret, SessionToken: k.token,
		Expiration: k.expiration.UTC().Format(time.RFC3339)}, stablejson.Unescaped)
	if err != nil {
		return provider.Bundle{}, &provider.Error{Class: provider.Unavailable, Op: opIssue, Code: codeIncomplete}
	}
	return provider.Bundle{
		Files: map[string][]byte{
			fileConfig: fmt.Appendf(nil, "[default]\nregion = %s\ncredential_process = cat %s/%s/%s\n",
				region, provider.ContainerDir, name, fileProcess),
			fileProcess: append(doc, '\n'),
			fileCredentials: fmt.Appendf(nil, "[default]\naws_access_key_id = %s\naws_secret_access_key = %s\naws_session_token = %s\n",
				k.id, k.secret, k.token),
		},
		Plain: map[string]bool{fileConfig: true},
		// The access key id is an identifier, which a task may return.
		Secrets:  [][]byte{[]byte(k.secret), []byte(k.token)},
		Lifetime: k.left(now),
	}, nil
}

// Secret reads one value with the role's session.
func (s *session) Secret(ctx context.Context, ref provider.Parsed) (provider.Value, error) {
	detail, ok := ref.Detail.(reference)
	if !ok {
		return provider.Value{}, &provider.Error{Class: provider.Invalid, Op: opRead, Code: codeIncomplete}
	}
	k, err := s.young(ctx, opRead)
	if err != nil {
		return provider.Value{}, err
	}
	creds := k.credentials()
	var value []byte
	switch detail.service {
	case serviceSSM:
		out, err := ssm.New(ssm.Options{Region: s.p.region, BaseEndpoint: override(s.p.endpoint), HTTPClient: s.p.http, Retryer: awssdk.NopRetryer{}, Credentials: creds}).
			GetParameter(ctx, &ssm.GetParameterInput{Name: awssdk.String(detail.id), WithDecryption: awssdk.Bool(true)})
		if err != nil {
			return provider.Value{}, classify(opRead, err, readClasses)
		}
		if out.Parameter == nil || out.Parameter.Value == nil {
			return provider.Value{}, &provider.Error{Class: provider.Unavailable, Op: opRead, Code: codeIncomplete}
		}
		value = []byte(*out.Parameter.Value)
	case serviceSecrets:
		out, err := secretsmanager.New(secretsmanager.Options{Region: s.p.region, BaseEndpoint: override(s.p.endpoint), HTTPClient: s.p.http, Retryer: awssdk.NopRetryer{}, Credentials: creds}).
			GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: awssdk.String(detail.id)})
		if err != nil {
			return provider.Value{}, classify(opRead, err, readClasses)
		}
		switch {
		case out.SecretString != nil:
			value = []byte(*out.SecretString)
		case out.SecretBinary != nil:
			value = out.SecretBinary
		default:
			return provider.Value{}, &provider.Error{Class: provider.Unavailable, Op: opRead, Code: codeIncomplete}
		}
		if detail.field != "" {
			field, err := fieldOf(value, detail.field)
			if err != nil {
				return provider.Value{}, err
			}
			value = field
		}
	case serviceKMS:
		// The context binds a ciphertext to a namespace; an empty one would
		// bind it to none.
		if s.who.Namespace == "" {
			return provider.Value{}, &provider.Error{Class: provider.Invalid, Op: opRead, Code: codeNoNamespace}
		}
		out, err := kmssdk.New(kmssdk.Options{Region: s.p.region, BaseEndpoint: override(s.p.endpoint), HTTPClient: s.p.http, Retryer: awssdk.NopRetryer{}, Credentials: creds}).
			Decrypt(ctx, &kmssdk.DecryptInput{KeyId: awssdk.String(detail.id), CiphertextBlob: ref.Ref.Ciphertext,
				EncryptionContext: map[string]string{namespaceContext: s.who.Namespace}})
		if err != nil {
			return provider.Value{}, classify(opRead, err, readClasses)
		}
		if out.Plaintext == nil {
			return provider.Value{}, &provider.Error{Class: provider.Unavailable, Op: opRead, Code: codeIncomplete}
		}
		value = out.Plaintext
	default:
		return provider.Value{}, &provider.Error{Class: provider.Invalid, Op: opRead, Code: codeIncomplete}
	}
	return provider.Value{Bytes: value}, nil
}

// fieldOf is the value of key in secret, a JSON object: a string's own
// bytes, or a number's or boolean's as written. The object is read strictly
// (docs/JSON.md): a key written twice is refused.
func fieldOf(secret []byte, key string) ([]byte, error) {
	var object map[string]jsontext.Value
	if err := json.Unmarshal(secret, &object); err != nil || object == nil {
		return nil, &provider.Error{Class: provider.Invalid, Op: opRead, Code: codeNotJSON}
	}
	raw, ok := object[key]
	if !ok {
		return nil, &provider.Error{Class: provider.NotFound, Op: opRead, Code: codeField}
	}
	switch raw.Kind() {
	case '"':
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, &provider.Error{Class: provider.Invalid, Op: opRead, Code: codeNotJSON}
		}
		return []byte(text), nil
	case '0', 't', 'f':
		return bytes.Clone(raw), nil
	}
	return nil, &provider.Error{Class: provider.Invalid, Op: opRead, Code: codeNotJSON}
}

func (k keys) credentials() awssdk.CredentialsProvider {
	return awssdk.CredentialsProviderFunc(func(context.Context) (awssdk.Credentials, error) {
		return awssdk.Credentials{AccessKeyID: k.id, SecretAccessKey: k.secret, SessionToken: k.token, Source: "orkstack"}, nil
	})
}

// sts is the exchange's client: anonymous, since the token is the
// authentication.
func (p *Provider) sts() *sts.Client {
	return sts.New(sts.Options{Region: p.region, BaseEndpoint: override(p.stsEndpoint), HTTPClient: p.http,
		Retryer: awssdk.NopRetryer{}, Credentials: awssdk.AnonymousCredentials{}})
}

func override(endpoint string) *string {
	if endpoint == "" {
		return nil
	}
	return &endpoint
}
