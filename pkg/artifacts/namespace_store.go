package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
)

// NamespaceStore is the registry of namespaces: one JSON document per name,
// beside the orkflows they scope.
//
//	s3://{bucket}/metadata/namespaces/{name}.json
//
// It holds no cache, on the same terms as OrkflowStore: any replica answers
// for any namespace, including one it never created.
type NamespaceStore interface {
	// Put writes one namespace document and returns what it stored, including
	// the etag naming those bytes.
	//
	// cond decides whether the write is allowed to land: an unconditional Put
	// creates or replaces, IfAbsent must create, IfMatch must replace exactly
	// the version whose etag it names. A condition the store refuses returns
	// an error matching ErrPreconditionFailed — which is a fact about the
	// object, not about the request, so a caller re-reads rather than retrying
	// verbatim.
	Put(ctx context.Context, ns *orkstackv1.Namespace, cond PutConditions) (*orkstackv1.Namespace, error)
	// Get reads one namespace. DefaultNamespace always resolves, whether or
	// not a document was ever written for it. Any other missing name returns
	// an error matching ErrNotFound.
	Get(ctx context.Context, name string) (*orkstackv1.Namespace, error)
	// List returns every registered namespace ordered by name, with the
	// default namespace included whether or not it was ever written.
	List(ctx context.Context) ([]*orkstackv1.Namespace, error)
	// Delete removes a namespace that holds no orkflows. A namespace that
	// still holds any is refused, and the error says how many.
	Delete(ctx context.Context, name string) error
}

// PutConditions is the compare-and-swap a registry write may demand.
//
// It exists because two engine replicas share one bucket and neither
// coordinates with the other (AGENTS.md §7): an unconditional write is a
// last-writer-wins, and the loser never learns it lost. S3 and MinIO both
// answer conditional PUTs natively, so the arbitration is the store's and
// needs no lock, no lease and nothing in Temporal.
//
// The zero value is unconditional, which is what a caller that genuinely does
// not care must ask for by saying nothing.
type PutConditions struct {
	// IfAbsent requires the object not to exist. It is how a create stays a
	// create: two replicas both racing to register "payments" produce one
	// winner and one ErrPreconditionFailed, never two writes where the second
	// silently overwrote the first.
	IfAbsent bool
	// IfMatch requires the stored object to carry exactly this etag — the one
	// a read handed back. Empty means no such requirement.
	IfMatch string
}

// conditional reports whether these conditions ask the store for anything.
func (c PutConditions) conditional() bool { return c.IfAbsent || c.IfMatch != "" }

const (
	// NamespacePrefix roots the namespace registry in the bucket. It sits
	// beside OrkflowPrefix rather than inside it, so listing namespaces never
	// scans definitions and a definition can never be mistaken for a registry
	// document.
	NamespacePrefix = "metadata/namespaces"

	// namespaceSuffix ends every registry object.
	namespaceSuffix = ".json"

	// namespaceContentType is what a registry object is served as.
	namespaceContentType = "application/json"
)

// ErrInvalidNamespace reports a namespace name this store cannot address.
var ErrInvalidNamespace = errors.New("invalid namespace")

// ErrNamespaceNotEmpty reports a delete refused because the namespace still
// holds orkflows.
var ErrNamespaceNotEmpty = errors.New("namespace is not empty")

// ErrPreconditionFailed reports a conditional write the store refused: the
// object already existed under IfAbsent, or its etag had moved under IfMatch.
//
// It is never "the write failed": the write did not happen, and the reason is
// that somebody else got there first. A caller re-reads and decides again.
var ErrPreconditionFailed = errors.New("precondition failed")

// NamespaceKey renders the object key holding one namespace document:
// metadata/namespaces/{name}.json.
//
// The name is validated rather than sanitized, exactly as an orkflow's
// coordinates are: nothing can escape the prefix or address another
// namespace's document.
func NamespaceKey(name string) (string, error) {
	name = ResolveNamespace(name)
	if err := validNamespaceName(name); err != nil {
		return "", err
	}
	return NamespacePrefix + "/" + name + namespaceSuffix, nil
}

// ParseNamespaceKey reverses NamespaceKey. A key this package did not generate
// reports ok=false, so a scan skips foreign objects instead of decoding them.
func ParseNamespaceKey(key string) (name string, ok bool) {
	rest, found := strings.CutPrefix(key, NamespacePrefix+"/")
	if !found {
		return "", false
	}
	name, found = strings.CutSuffix(rest, namespaceSuffix)
	if !found || validNamespaceName(name) != nil {
		return "", false
	}
	return name, true
}

// namespaceNamePattern is the rule for a namespace name.
//
// It is the DSL's slug rule (internal/dsl.slugPattern), not the
// cleanComponentPattern that validates a stored VERSION string. A version is a
// semver or a content hash and may be neither lower-case nor slug-shaped; a
// namespace is a NAME, and the only way an orkflow enters one is a
// `namespace:` in a document. cleanComponentPattern accepted `Payments`,
// `pay.ments`, `1payments` and `a_b` — every one refused by the compiler — so
// each was a namespace that could be created, written to, and then never named
// in YAML. This package does not import the compiler — it is public and the
// compiler is internal, and a program that only stores artifacts should not
// have to build a YAML front end to do it — so the two rules are held
// together by measurement instead: TestNamespaceName_RegistryAndCompilerAgree
// runs one corpus through this rule and through the compiler and fails on any
// disagreement, in either direction. Being a test, it can import what this
// file will not, at no cost to any importer.
var namespaceNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

func validNamespaceName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("artifacts: namespace name is empty: %w", ErrInvalidNamespace)
	case len(name) > maxOrkflowComponent:
		return fmt.Errorf("artifacts: namespace %q exceeds %d bytes: %w",
			name, maxOrkflowComponent, ErrInvalidNamespace)
	case !namespaceNamePattern.MatchString(name):
		return fmt.Errorf("artifacts: namespace %q must match %s: %w",
			name, namespaceNamePattern, ErrInvalidNamespace)
	}
	return nil
}

// DefaultNamespaceDocument is what DefaultNamespace resolves to when nobody
// ever wrote one. It is synthesized, not stored: a fresh deployment has a
// working namespace before anyone creates anything, and no bootstrap step can
// be forgotten or half-run.
func DefaultNamespaceDocument() *orkstackv1.Namespace {
	return &orkstackv1.Namespace{
		Name:        DefaultNamespace,
		Description: "the namespace every orkflow that names none belongs to",
	}
}

// marshalNamespace renders a registry document with the same normalization the
// orkflow store uses, so byte comparison stays meaningful.
func marshalNamespace(ns *orkstackv1.Namespace) ([]byte, error) {
	if ns == nil {
		return nil, fmt.Errorf("artifacts: encode namespace: %w: nil namespace", ErrInvalidNamespace)
	}
	raw, err := protojson.Marshal(ns)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode namespace %q: %w", ns.GetName(), err)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return nil, fmt.Errorf("artifacts: encode namespace %q: %w", ns.GetName(), err)
	}
	out.WriteByte('\n')
	return out.Bytes(), nil
}

// unmarshalNamespace decodes a registry document. Unknown fields are
// discarded, so an engine can read what a newer engine wrote.
func unmarshalNamespace(document []byte) (*orkstackv1.Namespace, error) {
	ns := &orkstackv1.Namespace{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, ns); err != nil {
		return nil, fmt.Errorf("decode namespace document: %w", err)
	}
	return ns, nil
}

// S3NamespaceStore keeps the namespace registry in an S3-compatible bucket.
type S3NamespaceStore struct {
	client *s3.Client
	bucket string
	// now stamps created_at and updated_at. It is a field only so a test can
	// hold the clock still; every constructor sets it to the wall clock.
	now func() time.Time
}

// compile-time contract check.
var _ NamespaceStore = (*S3NamespaceStore)(nil)

// NewS3NamespaceStore builds the namespace registry from the same ORK_S3_*
// config the artifact and orkflow stores use.
func NewS3NamespaceStore(ctx context.Context, cfg Config) (*S3NamespaceStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3NamespaceStore{
		client: client,
		bucket: bucket,
		now:    func() time.Time { return time.Now().UTC() },
	}, nil
}

// Bucket returns the bucket holding the registry.
func (s *S3NamespaceStore) Bucket() string { return s.bucket }

// Location is the s3:// URI of the prefix this store owns.
func (s *S3NamespaceStore) Location() string { return s.uri(NamespacePrefix + "/") }

func (s *S3NamespaceStore) uri(key string) string {
	return "s3://" + s.bucket + "/" + key
}

// Put writes one namespace document and returns what it stored.
//
// The registry owns the document's times, not the caller: created_at is
// stamped when the name is first written and carried forward verbatim on every
// later write, updated_at is stamped on each one. Whatever a caller sent in
// either field is discarded — a client that could backdate created_at could
// hide when a tenant appeared, and a client that could freeze updated_at could
// hide that it changed one. The etag is discarded for the same reason and a
// stronger one: it names the bytes about to be written, so it cannot be inside
// them.
//
// cond is handed to the backend as a conditional PUT, so the arbitration
// between two replicas is the store's own and no lock is invented above it.
func (s *S3NamespaceStore) Put(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	cond PutConditions,
) (*orkstackv1.Namespace, error) {
	if ns == nil {
		return nil, fmt.Errorf("artifacts: encode namespace: %w: nil namespace", ErrInvalidNamespace)
	}
	name := ResolveNamespace(ns.GetName())
	key, err := NamespaceKey(name)
	if err != nil {
		return nil, err
	}
	if err := validNamespacePools(ns); err != nil {
		return nil, err
	}
	if err := validNamespaceRetention(ns); err != nil {
		return nil, err
	}
	if err := validNamespaceTelemetry(ns); err != nil {
		return nil, err
	}
	if err := ValidNamespaceSecrets(ns); err != nil {
		return nil, err
	}
	if err := ValidNamespaceSigner(ns); err != nil {
		return nil, err
	}
	if err := validNamespaceHold(ns); err != nil {
		return nil, err
	}
	if err := validNamespaceMaxRunning(ns); err != nil {
		return nil, err
	}
	if err := validNamespaceBudget(ns); err != nil {
		return nil, err
	}
	stored := proto.Clone(ns).(*orkstackv1.Namespace)
	stored.Name = name
	stored.Etag = ""
	stamp := timestamppb.New(s.clock())
	stored.CreatedAt, stored.UpdatedAt = stamp, stamp
	switch existing, err := s.read(ctx, key); {
	case err == nil:
		if created := existing.GetCreatedAt(); created != nil {
			stored.CreatedAt = created
		}
		StampNamespaceHold(stored, existing, stamp.AsTime())
	case errors.Is(err, ErrNotFound):
		// First write of this name: the stamp above is its birth, and a hold
		// it carries is set by this write.
		StampNamespaceHold(stored, nil, stamp.AsTime())
	default:
		return nil, err
	}
	document, err := marshalNamespace(stored)
	if err != nil {
		return nil, err
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(namespaceContentType),
	}
	if cond.IfAbsent {
		// The S3 conditional-create spelling: "*" means any etag, so
		// If-None-Match: * is "only if nothing is there".
		input.IfNoneMatch = aws.String("*")
	}
	if cond.IfMatch != "" {
		input.IfMatch = aws.String(cond.IfMatch)
	}
	out, err := s.client.PutObject(ctx, input)
	if err != nil {
		if cond.conditional() && isPreconditionFailed(err) {
			return nil, fmt.Errorf("artifacts: put namespace %q: %w",
				s.uri(key), ErrPreconditionFailed)
		}
		return nil, fmt.Errorf("artifacts: put namespace %q: %w", s.uri(key), err)
	}
	stored.Etag = aws.ToString(out.ETag)
	return stored, nil
}

// clock is the store's time source, defaulting to the wall clock so a
// zero-valued store still stamps a real time rather than the epoch.
func (s *S3NamespaceStore) clock() time.Time {
	if s.now == nil {
		return time.Now().UTC()
	}
	return s.now()
}

// DefaultPool is the pool a task runs on when neither the task nor its
// namespace names one. It is the same name internal/task.DefaultPool resolves
// to the deployment's default execution channel; the constant is repeated
// here because this package does not import internal/task, which would put
// the Temporal SDK into the build of every program that stores an artifact.
// The two are pinned together by
// TestNamespacePools_AreExactlyThePoolsDispatchCanReach, a test, which does
// import it — a test dependency reaches no importer.
const DefaultPool = "main"

// poolNamePattern is the rule for a pool name this registry will store.
//
// It is TIGHTER than cleanComponentPattern, which validated these two fields
// until review measured what that let through: "GPU", "Main", "gpu.1" and
// "1gpu" were all stored, and every one of them is refused by the only
// pool->queue translation in the tree (internal/task.QueueForPool) and by the
// DSL. A registry that stores a routing target no dispatch can honour is the
// same "stored field nothing can honour" defect as a namespace no read path
// reaches — and internal/engine.DefaultPoolFor is what reads default_pool, so
// it would be the one handed the unroutable name.
var poolNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// maxPoolName bounds a pool name. It is internal/task.MaxPoolName and the
// DSL's dsl.MaxSlug, the same 200: a pattern with no bound is half a rule, and
// the half that was missing is the one a corpus of short names cannot see.
const maxPoolName = 200

// validPoolName reports why a pool name cannot be stored. field names where
// the pool was written, so an allow-list entry and a default_pool report
// differently.
func validPoolName(field, pool string) error {
	switch {
	case len(pool) > maxPoolName:
		return fmt.Errorf("artifacts: %s of %d bytes exceeds the %d-byte limit (%.32q…): %w",
			field, len(pool), maxPoolName, pool, ErrInvalidNamespace)
	case !poolNamePattern.MatchString(pool):
		return fmt.Errorf("artifacts: %s %q must match %s: %w",
			field, pool, poolNamePattern, ErrInvalidNamespace)
	}
	return nil
}

// validNamespacePools enforces what the document can be held to on its own:
// every pool it names must be one dispatch can reach, and an allow-list that
// excluded the namespace's own default would refuse every task that names no
// pool.
func validNamespacePools(ns *orkstackv1.Namespace) error {
	defaultPool := ns.GetDefaultPool()
	if defaultPool == "" {
		defaultPool = DefaultPool
	}
	if err := validPoolName(
		fmt.Sprintf("namespace %q default pool", ns.GetName()), defaultPool); err != nil {
		return err
	}
	allowed := ns.GetAllowedPools()
	if len(allowed) == 0 {
		return nil
	}
	foundDefault := false
	for _, pool := range allowed {
		if err := validPoolName(
			fmt.Sprintf("namespace %q allowed pool", ns.GetName()), pool); err != nil {
			return err
		}
		if pool == defaultPool {
			foundDefault = true
		}
	}
	if foundDefault {
		return nil
	}
	return fmt.Errorf("artifacts: namespace %q allows pools %s but defaults to %q: %w",
		ns.GetName(), strings.Join(allowed, ", "), defaultPool, ErrInvalidNamespace)
}

// validNamespaceRetention enforces what the document can be held to on its
// own: an explicit run_retention must be a usable positive duration. The
// store cannot check it against the deployment maximum — that ceiling lives
// in RetentionPolicyStore, which this package does not depend on — so that
// half of the rule is the API's (internal/api.validNamespaceRetention).
func validNamespaceRetention(ns *orkstackv1.Namespace) error {
	d := ns.GetRunRetention()
	if d == nil {
		return nil
	}
	if err := d.CheckValid(); err != nil {
		return fmt.Errorf("artifacts: namespace %q run retention is not a valid duration: %w: %w",
			ns.GetName(), err, ErrInvalidNamespace)
	}
	if d.AsDuration() <= 0 {
		return fmt.Errorf("artifacts: namespace %q run retention must be a positive duration: %w",
			ns.GetName(), ErrInvalidNamespace)
	}
	return nil
}

// validNamespaceTelemetry enforces what the document can be held to on its
// own: attribute shape (internal/dsl.ValidateAttributeMap, the one rule
// every bounded string-map in the tree shares) and the reserved prefixes
// (internal/dsl.ReservedAttributePrefixes / ValidateNoReservedPrefix). It
// deliberately does NOT check that telemetry.sink names a sink that exists —
// this store has no ORK_TELEMETRY_SINKS table to check it against, only the
// engine that parsed one does — so that half of the rule is the API's
// (internal/api.validNamespaceTelemetry), the same retention/pools split
// validNamespaceRetention and validNamespacePools already draw.
func validNamespaceTelemetry(ns *orkstackv1.Namespace) error {
	attributes := ns.GetTelemetry().GetAttributes()
	if len(attributes) == 0 {
		return nil
	}
	if err := dsl.ValidateAttributeMap("telemetry attribute", attributes, 16); err != nil {
		return fmt.Errorf("artifacts: namespace %q %w: %w", ns.GetName(), err, ErrInvalidNamespace)
	}
	if err := dsl.ValidateNoReservedPrefix("telemetry attribute", attributes); err != nil {
		return fmt.Errorf("artifacts: namespace %q %w: %w", ns.GetName(), err, ErrInvalidNamespace)
	}
	return nil
}

// maxHoldReason bounds a hold's reason: a short human line, never a document.
const maxHoldReason = 512

// ValidNamespaceSecrets enforces what the document can be held to on its
// own: every allowed_secrets entry is a well-formed scheme:pattern the
// engine and every Orker read the same way (dsl.ValidSecretPatterns).
// Whether a scheme is one any Orker resolves is not the document's business.
// Exported so the registry doubles in internal/api and cmd/orkctl tests
// refuse exactly what the store refuses.
func ValidNamespaceSecrets(ns *orkstackv1.Namespace) error {
	if err := dsl.ValidSecretPatterns(ns.GetAllowedSecrets()); err != nil {
		return fmt.Errorf("artifacts: namespace %q allowed_secrets: %w: %w",
			ns.GetName(), err, ErrInvalidNamespace)
	}
	return nil
}

// ValidNamespaceSigner enforces what the document can be held to on its
// own: require_signed_by is empty or "cosign:<valid key name>". Whether
// that key EXISTS in the deployment's ImagePolicy is not the document's
// business — this package does not depend on ImagePolicyStore — so that
// half is the API's (internal/api.validNamespaceSigner), the same split
// validNamespaceRetention draws with the deployment maximum. Exported so the
// registry doubles in internal/api and cmd/orkctl tests refuse exactly what
// the store refuses.
func ValidNamespaceSigner(ns *orkstackv1.Namespace) error {
	if _, _, err := task.ParseSigner(ns.GetRequireSignedBy()); err != nil {
		return fmt.Errorf("artifacts: namespace %q %w: %w", ns.GetName(), err, ErrInvalidNamespace)
	}
	return nil
}

// validNamespaceHold enforces the hold rule on the document alone: a hold
// carries a reason the next operator can read — non-empty once trimmed, at
// most 512 bytes, and free of control characters, which `orkctl namespace get`
// and the UI render verbatim. Validated, never sanitized: a reason that fails
// is refused, not rewritten.
func validNamespaceHold(ns *orkstackv1.Namespace) error {
	hold := ns.GetHold()
	if hold == nil {
		return nil
	}
	reason := hold.GetReason()
	switch {
	case strings.TrimSpace(reason) == "":
		return fmt.Errorf("artifacts: namespace %q hold needs a reason: %w",
			ns.GetName(), ErrInvalidNamespace)
	case len(reason) > maxHoldReason:
		return fmt.Errorf("artifacts: namespace %q hold reason of %d bytes exceeds the %d-byte limit: %w",
			ns.GetName(), len(reason), maxHoldReason, ErrInvalidNamespace)
	case !utf8.ValidString(reason):
		return fmt.Errorf("artifacts: namespace %q hold reason is not valid UTF-8: %w",
			ns.GetName(), ErrInvalidNamespace)
	}
	for i := 0; i < len(reason); i++ {
		if b := reason[i]; b < 0x20 || b == 0x7f {
			return fmt.Errorf("artifacts: namespace %q hold reason contains control character %#02x: %w",
				ns.GetName(), b, ErrInvalidNamespace)
		}
	}
	return nil
}

// validNamespaceMaxRunning refuses a negative cap. 0 is unlimited.
func validNamespaceMaxRunning(ns *orkstackv1.Namespace) error {
	if ns.GetMaxRunning() < 0 {
		return fmt.Errorf("artifacts: namespace %q max_running %d must be 0 (unlimited) or positive: %w",
			ns.GetName(), ns.GetMaxRunning(), ErrInvalidNamespace)
	}
	return nil
}

// Budget period bounds. Exported so the API, the CLI help text and the UI
// prefilter (ui/src/lib/namespaceRules.ts) name the same numbers.
const (
	BudgetMinPeriod = time.Hour
	BudgetMaxPeriod = 744 * time.Hour // 31 days
)

// validNamespaceBudget enforces what a budget can be held to on its own: a
// valid period inside [BudgetMinPeriod, BudgetMaxPeriod], non-negative
// bounds, and at least one bounded dimension. Nil is always fine — it means
// unbounded.
func validNamespaceBudget(ns *orkstackv1.Namespace) error {
	budget := ns.GetBudget()
	if budget == nil {
		return nil
	}
	refuse := func(format string, args ...any) error {
		return fmt.Errorf("artifacts: namespace %q budget: %s: %w",
			ns.GetName(), fmt.Sprintf(format, args...), ErrInvalidNamespace)
	}
	period := budget.GetPeriod()
	if period == nil {
		return refuse("period is required, e.g. 720h")
	}
	if err := period.CheckValid(); err != nil {
		return refuse("period is not a valid duration: %v", err)
	}
	if d := period.AsDuration(); d < BudgetMinPeriod || d > BudgetMaxPeriod {
		return refuse("period %s is outside [%s, %s]", d, BudgetMinPeriod, BudgetMaxPeriod)
	}
	if budget.GetCpuSeconds() < 0 {
		return refuse("cpu_seconds %d is negative", budget.GetCpuSeconds())
	}
	if budget.GetGibSeconds() < 0 {
		return refuse("gib_seconds %d is negative", budget.GetGibSeconds())
	}
	if budget.GetCpuSeconds() == 0 && budget.GetGibSeconds() == 0 {
		return refuse("bounds nothing — set cpu_seconds or gib_seconds above 0, or clear the budget")
	}
	return nil
}

// StampNamespaceHold gives `next` the hold timestamps the registry owns:
// a hold that `existing` already carried keeps its since, a hold that is new
// is stamped now, and whatever since the client sent is discarded.
//
// It is exported so every registry double in the tree calls the same rule the
// store does, and for no other reason. It edits next in place and never
// touches existing.
func StampNamespaceHold(next, existing *orkstackv1.Namespace, now time.Time) {
	hold := next.GetHold()
	if hold == nil {
		return
	}
	if prior := existing.GetHold().GetSince(); prior != nil {
		hold.Since = proto.Clone(prior).(*timestamppb.Timestamp)
		return
	}
	hold.Since = timestamppb.New(now)
}

// Get reads one namespace. The default namespace always resolves.
func (s *S3NamespaceStore) Get(ctx context.Context, name string) (*orkstackv1.Namespace, error) {
	name = ResolveNamespace(name)
	key, err := NamespaceKey(name)
	if err != nil {
		return nil, err
	}
	ns, err := s.read(ctx, key)
	if errors.Is(err, ErrNotFound) && name == DefaultNamespace {
		return DefaultNamespaceDocument(), nil
	}
	return ns, err
}

// List returns every registered namespace, ordered by name. The default
// namespace is always present, synthesized when no document exists for it, so
// a caller listing namespaces sees the one every unnamespaced orkflow is in.
func (s *S3NamespaceStore) List(ctx context.Context) ([]*orkstackv1.Namespace, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(NamespacePrefix + "/"),
	})
	byName := map[string]*orkstackv1.Namespace{DefaultNamespace: DefaultNamespaceDocument()}
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list namespaces %q: %w",
				s.uri(NamespacePrefix+"/"), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			name, ok := ParseNamespaceKey(key)
			if !ok {
				continue
			}
			ns, err := s.read(ctx, key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue // deleted between the listing and the read
				}
				return nil, err
			}
			byName[name] = ns
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.Namespace, 0, len(names))
	for _, name := range names {
		out = append(out, byName[name])
	}
	return out, nil
}

// Delete removes a namespace document.
//
// It refuses while the namespace still holds orkflows, and says how many:
// deleting the registry entry would not delete the definitions, so the objects
// would survive under a namespace nothing lists — reachable by key, invisible
// to every reader. The default namespace is refused unconditionally, because
// every orkflow that names none is in it.
func (s *S3NamespaceStore) Delete(ctx context.Context, name string) error {
	name = ResolveNamespace(name)
	key, err := NamespaceKey(name)
	if err != nil {
		return err
	}
	if name == DefaultNamespace {
		return fmt.Errorf(
			"artifacts: namespace %q cannot be deleted: it holds every orkflow that names none: %w",
			name, ErrNamespaceNotEmpty)
	}
	held, err := s.countOrkflows(ctx, name)
	if err != nil {
		return err
	}
	if held > 0 {
		return fmt.Errorf("artifacts: namespace %q still holds %d orkflow(s); delete them first: %w",
			name, held, ErrNamespaceNotEmpty)
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("artifacts: delete namespace %q: %w", s.uri(key), err)
	}
	return nil
}

// countOrkflows counts the distinct orkflow names stored in a namespace. It is
// a keys-only scan: the definitions are never read, because the question is
// how many there are, not what they say.
func (s *S3NamespaceStore) countOrkflows(ctx context.Context, namespace string) (int, error) {
	prefix, err := OrkflowNamespacePrefix(namespace)
	if err != nil {
		return 0, err
	}
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	names := map[string]struct{}{}
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("artifacts: list orkflows %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			_, name, _, ok := parseOrkflowKey(aws.ToString(object.Key))
			if !ok {
				continue
			}
			names[name] = struct{}{}
		}
	}
	return len(names), nil
}

func (s *S3NamespaceStore) read(ctx context.Context, key string) (*orkstackv1.Namespace, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: get namespace %q: %w", s.uri(key), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get namespace %q: %w", s.uri(key), err)
	}
	defer object.Body.Close()
	document, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get namespace %q: read body: %w", s.uri(key), err)
	}
	ns, err := unmarshalNamespace(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get namespace %q: %w", s.uri(key), err)
	}
	if name, ok := ParseNamespaceKey(key); ok {
		ns.Name = name
	}
	// The etag names the bytes just read, so it comes from the response and
	// never from the document: a token stored inside the object it names could
	// only ever be the previous write's.
	ns.Etag = aws.ToString(object.ETag)
	return ns, nil
}

// isPreconditionFailed reports a conditional PUT the backend refused.
//
// Two codes, because the two backends this repo deploys use both: a plain
// refusal is 412 PreconditionFailed, and S3 answers 409
// ConditionalRequestConflict when a concurrent write raced the same key. Both
// mean the same thing to a caller — somebody else got there first — and the
// SDK surfaces the code without a typed error, so the string is what there is.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.HTTPStatusCode() {
		case http.StatusPreconditionFailed, http.StatusConflict:
			return true
		}
	}
	return false
}
