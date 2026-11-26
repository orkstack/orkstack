package identity

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The mint's reasons (workload-identity §3, §10). A step that could not ask
// is ReasonUnavailable; one that asked and was told no is the refusal named.
const (
	ReasonUnavailable       = "IDENTITY_UNAVAILABLE"
	ReasonEngineRefused     = "IDENTITY_ENGINE_REFUSED"
	ReasonNotAdmitted       = "IDENTITY_NOT_ADMITTED"
	ReasonAttemptNotHeld    = "ATTEMPT_NOT_HELD"
	ReasonTaskNotAsAdmitted = "TASK_NOT_AS_ADMITTED"
	ReasonAttemptsExhausted = "IDENTITY_ATTEMPTS_EXHAUSTED"
	ReasonAttemptExpired    = "IDENTITY_ATTEMPT_EXPIRED"
	ReasonRevoked           = "IDENTITY_REVOKED"
	// ReasonRequiredResultNotRecorded is the mint's and the commit's: a task
	// the attempt's own follows has no record of success.
	ReasonRequiredResultNotRecorded = "REQUIRED_RESULT_NOT_RECORDED"
)

// ErrNamespaceGone is the namespace a run executes in, read and found no
// longer there: its runs' identities are revoked, not unavailable.
var ErrNamespaceGone = errors.New("identity: the namespace is gone")

// MintObserver counts each mint's outcome: "granted", or the reason it
// issued nothing. Counting can fail nothing.
type MintObserver interface {
	Minted(reason string)
}

// The outcomes a mint is counted under beside its reasons: a closed set.
const (
	OutcomeGranted           = "granted"
	OutcomePermissionDenied  = "permission_denied"
	OutcomeInvalid           = "invalid"
	OutcomeResourceExhausted = "resource_exhausted"
)

// MaxMintTokens bounds the tokens of one mint request.
const MaxMintTokens = 16

// MintRequest is one MintAttemptTokens request, as the API decoded it.
type MintRequest struct {
	Namespace, RunID, TaskID string
	Item                     artifacts.Item
	// Scopes are the resume scopes the attempt says it was dispatched under.
	Scopes          Scopes
	Attempt         int32
	TaskSpecSHA256  string
	AdmissionSHA256 string
	Tokens          []TokenAsk
}

// TokenAsk is one token asked for: an identity's name and audience, or
// "grant:<name>".
type TokenAsk struct{ Name, Audience string }

// Caller is who asks, from its credential: never from the request.
type Caller struct{ Principal, OrkerID string }

// MintError is a mint that issued nothing: its reason, a message for the
// caller, whether the step could not ask (Unavailable) and whether it says
// something about the caller or the run, and so writes an audit record
// (Audit). EngineBuild is the admitting engine's, for TASK_NOT_AS_ADMITTED.
type MintError struct {
	Reason      string
	Message     string
	Unavailable bool
	Audit       bool
	EngineBuild string
	// Task is the task REQUIRED_RESULT_NOT_RECORDED is of.
	Task string

	cause error
	// invalid says the step was answered with a record that cannot be used.
	invalid bool
	// absent says the store answered that Task has no record.
	absent bool
}

// Unwrap is what a step that could not ask was answered, if anything.
func (e *MintError) Unwrap() error { return e.cause }

func (e *MintError) Error() string { return e.Reason + ": " + e.Message }

// Public is what the caller is told: the reason and a fixed sentence, so a
// store's key, a cluster's error or a run's admitted shape never reach an
// Orker (and through it a task). TASK_NOT_AS_ADMITTED alone carries its
// detail, which names the members that differ and both builds and nothing
// else, and REQUIRED_RESULT_NOT_RECORDED the id of the task that has none. The whole message is the engine's log line and audit record.
func (e *MintError) Public() string {
	if e.Reason == ReasonTaskNotAsAdmitted {
		return e.Error()
	}
	if e.Reason == ReasonRequiredResultNotRecorded && e.Task != "" {
		return fmt.Sprintf("%s: task %q, which this one follows, has no result on record", e.Reason, e.Task)
	}
	sentence, ok := publicSentences[e.Reason]
	if !ok {
		sentence = "the mint issued nothing"
	}
	return e.Reason + ": " + sentence
}

var publicSentences = map[string]string{
	ReasonUnavailable:       "a fact the mint needs could not be read; ask again",
	ReasonEngineRefused:     "this engine issues no token for this request",
	ReasonNotAdmitted:       "the request is not what the run was admitted with",
	ReasonAttemptNotHeld:    "the caller does not hold this attempt",
	ReasonAttemptsExhausted: "every attempt that may hold the task's identities is spent",
	ReasonAttemptExpired:    "the attempt outlived its deadline",
	ReasonRevoked:           "the namespace no longer allows what the run was admitted with",
	// What a task follows (follows_check.go).
	ReasonRequiredResultNotRecorded: "a task this one follows has no result on record",
	// The commit's own (commit_record.go).
	ReasonCommitIdentityMismatch: "the result is not of the attempt the request names",
}

func refuse(reason string, audit bool, format string, args ...any) *MintError {
	return &MintError{Reason: reason, Audit: audit, Message: fmt.Sprintf(format, args...)}
}

func unavailableMint(format string, args ...any) *MintError {
	return &MintError{Reason: ReasonUnavailable, Unavailable: true, Message: fmt.Sprintf(format, args...), cause: firstError(args)}
}

// firstError is the first error among a message's arguments: what it is about.
func firstError(args []any) error {
	for _, a := range args {
		if err, ok := a.(error); ok {
			return err
		}
	}
	return nil
}

// Admitted is a request that passed step 2: the run's admission, the
// task's row, and the identities the request asks for, as admitted.
type Admitted struct {
	Admission  artifacts.Admission
	Task       artifacts.AdmittedTask
	Identities []artifacts.AdmittedIdentity
	// Grants are the grants the request asks a login's token of, as admitted.
	Grants []AdmittedGrant
}

// AdmittedGrant is a grant of the run, as its admission selects it.
type AdmittedGrant struct {
	Name string
	artifacts.GrantSelection
}

// grantTokenPrefix begins the name of a token a grant's login is made with.
const grantTokenPrefix = "grant:"

// MintStores is what the mint reads and writes in object storage.
type MintStores interface {
	ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error)
	TakeAttemptSlot(ctx context.Context, req artifacts.AttemptSlotRequest) (artifacts.AttemptSlot, error)
	WriteIssued(ctx context.Context, r artifacts.IssuedRecord) (string, error)
}

// FollowsChecker is step 6a and the scope it begins at: the resume scope an
// attempt names is its root's newest, and each task the admission says this
// one follows has its record of success there. nil is a build without it,
// and refuses a scope and an admission that names any follow (fail closed).
type FollowsChecker interface {
	CheckScope(ctx context.Context, adm artifacts.Admission, taskID string, scopes Scopes) error
	CheckFollows(ctx context.Context, adm artifacts.Admission, admissionSHA256, taskID string, rootScope uint64) ([]FollowedRecord, error)
	CheckReceived(ctx context.Context, adm artifacts.Admission, admissionSHA256, taskID string, rootScope uint64) ([]*orkstackv1.ArtifactRef, error)
}

// Minter is the mint's steps 2 and 4 to 8; the API holds steps 1 and 3,
// the replica's guard and the caller's permissions at the admitted pool.
type Minter struct {
	Stores MintStores
	Starts StartReader
	// Describe is a describe of the run, made after the request arrived.
	Describe func(ctx context.Context, runID string) (ExecutionFacts, error)
	// Policy is the namespace's identity policy now (step 7), for the
	// incarnation whose id the run was admitted with: a namespace made again
	// under the name answers ErrNamespaceGone.
	Policy  func(ctx context.Context, namespace, namespaceID string) (artifacts.NamespacePolicy, error)
	Follows FollowsChecker
	Signer  func() (*jwt.Signer, error)
	Issuer  string
	// Deployment is ORK_DEPLOYMENT_NAME.
	Deployment string
	TTL        TTLBounds
	// Skew is auth.clock_skew, the slack past an attempt's deadline.
	Skew time.Duration
	// RecordIssuance is identity.issuance.records: required writes the
	// record of what was issued before any token is returned, and a mint
	// whose record cannot be written issues nothing; off writes none.
	RecordIssuance bool
	Now            func() time.Time
	NewJTI         func() string
}

// Admit is step 2: the admission record the request names, read by the
// run's id and digest; the request's namespace must be the record's, the
// task must be in its table and declare each token asked for with that
// audience. Absent is IDENTITY_NOT_ADMITTED; unreadable or not trusted is
// IDENTITY_UNAVAILABLE.
func (m *Minter) Admit(ctx context.Context, req MintRequest) (Admitted, error) {
	if len(req.Tokens) == 0 || len(req.Tokens) > MaxMintTokens {
		return Admitted{}, refuse(ReasonNotAdmitted, false, "a mint asks for 1 to %d tokens, not %d", MaxMintTokens, len(req.Tokens))
	}
	adm, row, err := admitTask(ctx, m.Stores, attemptOf(req.Namespace, req.RunID, req.TaskID, req.AdmissionSHA256, req.Item, req.Scopes))
	if err != nil {
		return Admitted{}, err
	}
	declared := adm.SetOf(req.TaskID)
	out := Admitted{Admission: adm, Task: row}
	asked := make(map[string]bool, len(req.Tokens))
	for _, ask := range req.Tokens {
		if asked[ask.Name] {
			return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q asks for %q twice", req.TaskID, ask.Name)
		}
		asked[ask.Name] = true
		if name, ok := strings.CutPrefix(ask.Name, grantTokenPrefix); ok {
			// A grant the run was admitted with and this task uses, for the
			// audience it was admitted for.
			g, admitted := adm.Grants[name]
			if !admitted || !slices.Contains(row.Grants, name) || g.Audience != ask.Audience {
				return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q was not admitted grant %q for audience %q", req.TaskID, name, ask.Audience)
			}
			out.Grants = append(out.Grants, AdmittedGrant{Name: name, GrantSelection: g})
			continue
		}
		found := false
		for _, id := range declared {
			if id.Name == ask.Name && id.Audience == ask.Audience {
				out.Identities, found = append(out.Identities, id), true
				break
			}
		}
		if !found {
			return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q was not admitted identity %q for audience %q", req.TaskID, ask.Name, ask.Audience)
		}
	}
	return out, nil
}

// Issued is what a mint answers: its tokens, and the artifacts of the run's
// start the task names, as the engine's records have them.
type Issued struct {
	Tokens   []IssuedToken
	Received []*orkstackv1.ArtifactRef
}

// Issue is steps 4 to 8 for a request Admit passed and the caller's
// permissions allowed: the attempt held by the caller's Orker, the run's
// start, the task's binding, what it follows, the attempt cap, the policy
// still allowing each audience, and the signing, with the record of what
// was issued written before any token is returned.
func (m *Minter) Issue(ctx context.Context, a Admitted, req MintRequest, caller Caller) (Issued, error) {
	adm := a.Admission
	// A run is issued tokens only by the issuer it was admitted under: one
	// whose keys its relying parties were told to trust.
	if adm.Issuer != m.Issuer {
		return Issued{}, refuse(ReasonEngineRefused, false, "run %q was admitted under issuer %q, and this engine issues as %q", req.RunID, adm.Issuer, m.Issuer)
	}
	// Steps 4 to 6a: the attempt, the start, the binding, what it follows.
	held, err := checkAttempt(ctx, attemptChecks{Describe: m.Describe, Starts: m.Starts, Follows: m.Follows}, adm,
		attemptOf(req.Namespace, req.RunID, req.TaskID, req.AdmissionSHA256, req.Item, req.Scopes), req.Attempt, req.TaskSpecSHA256, caller)
	if err != nil {
		return Issued{}, err
	}
	// Step 7's read comes before the slot, so an outage of the policy
	// spends no attempt; its check stays after 6b, the spec's order.
	policy, err := m.Policy(ctx, req.Namespace, adm.NamespaceID)
	switch {
	case errors.Is(err, ErrNamespaceGone):
		return Issued{}, refuse(ReasonRevoked, true, "namespace %q is gone", req.Namespace)
	case err != nil:
		return Issued{}, unavailableMint("namespace %q's identity policy could not be read: %v", req.Namespace, err)
	}
	// Step 6b: the attempt cap.
	now := m.Now().UTC()
	if err := m.takeSlot(ctx, a, req, held.PendingAttempt, now); err != nil {
		return Issued{}, err
	}
	// Step 7: still allowed now.
	for _, id := range a.Identities {
		if !AudienceAllowed(policy.Audiences, id.Audience) {
			return Issued{}, refuse(ReasonRevoked, true, "namespace %q no longer allows audience %q", req.Namespace, id.Audience)
		}
	}
	for _, g := range a.Grants {
		if why := grantWithdrawn(policy, adm, g); why != "" {
			return Issued{}, refuse(ReasonRevoked, true, "namespace %q: grant %q %s", req.Namespace, g.Name, why)
		}
	}
	// Step 8: sign, record, answer.
	tokens, err := m.sign(ctx, a, req, caller, now)
	if err != nil {
		return Issued{}, err
	}
	return Issued{Tokens: tokens, Received: held.Received}, nil
}

func (m *Minter) takeSlot(ctx context.Context, a Admitted, req MintRequest, pending PendingAttempt, now time.Time) error {
	slot, err := m.Stores.TakeAttemptSlot(ctx, artifacts.AttemptSlotRequest{
		Namespace: req.Namespace, RunID: req.RunID, TaskID: req.TaskID, Item: req.Item, Scope: req.Scopes.Resume,
		Slots: int(max(1, a.Task.IdentitySlots)), Holder: artifacts.SlotHolder{Attempt: req.Attempt, StartedAt: pending.StartedAt}, Now: now,
	})
	if err != nil {
		return unavailableMint("the attempt cap of task %q could not be read: %v", req.TaskID, err)
	}
	switch slot.Answer {
	case artifacts.SlotHeld:
	case artifacts.SlotExhausted:
		return refuse(ReasonAttemptsExhausted, true, "task %q has spent the %d attempts that may hold its identities", req.TaskID, max(1, a.Task.IdentitySlots))
	default:
		return unavailableMint("the attempt cap of task %q cannot be trusted (%s: %s)", req.TaskID, slot.Object, slot.Reason)
	}
	deadline := time.Duration(a.Task.AttemptDeadline) * time.Second
	if deadline > 0 && SlotExpired(slot.TakenAt, deadline, m.Skew, now) {
		return refuse(ReasonAttemptExpired, true, "attempt %d of task %q outlived its deadline", req.Attempt, req.TaskID)
	}
	return nil
}

// IssuedToken is one token a mint returns. Token is never logged or stored.
type IssuedToken struct {
	Name, Audience, Token, JTI, KID string
	IssuedAt                        time.Time
	ExpiresIn                       time.Duration
}

func (m *Minter) sign(ctx context.Context, a Admitted, req MintRequest, caller Caller, now time.Time) ([]IssuedToken, error) {
	signer, err := m.Signer()
	if err != nil {
		return nil, refuse(ReasonEngineRefused, false, "no signing key is eligible: %v", err)
	}
	record := artifacts.IssuedRecord{Namespace: req.Namespace, RunID: req.RunID, TaskID: req.TaskID, Item: req.Item,
		Attempt: req.Attempt, Principal: caller.Principal, OrkerID: caller.OrkerID, IssuedAt: now}
	out := make([]IssuedToken, 0, len(a.Identities)+len(a.Grants))
	one := func(name, grant, audience string, ttlSeconds int32) error {
		asked := time.Duration(ttlSeconds) * time.Second
		if asked == 0 {
			asked = time.Duration(a.Admission.TokenTTL) * time.Second
		}
		life := m.TTL.Clamp(asked)
		jti := m.NewJTI()
		claims, err := BuildClaims(ClaimsInput{Issuer: m.Issuer, Deployment: m.Deployment, Admission: a.Admission, TaskID: req.TaskID,
			Item: req.Item, Attempt: req.Attempt, Identity: name, Grant: grant, Audience: audience, Pool: a.Task.Pool, Orker: caller.OrkerID,
			Now: now, Lifetime: life, JTI: jti})
		if err != nil {
			return refuse(ReasonEngineRefused, false, "the claims of identity %q: %v", name, err)
		}
		token, err := signer.Sign("JWT", claims)
		if err != nil {
			return refuse(ReasonEngineRefused, false, "sign identity %q: %v", name, err)
		}
		out = append(out, IssuedToken{Name: name, Audience: audience, Token: token, JTI: jti, KID: signer.KeyID(), IssuedAt: now, ExpiresIn: life})
		record.Tokens = append(record.Tokens, artifacts.IssuedEntry{Name: name, Audience: audience, Subject: claims["sub"].(string),
			JTI: jti, KID: signer.KeyID(), IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(life).Unix()})
		return nil
	}
	for _, id := range a.Identities {
		if err := one(id.Name, "", id.Audience, id.TTLSeconds); err != nil {
			return nil, err
		}
	}
	for _, g := range a.Grants {
		if err := one(grantTokenPrefix+g.Name, g.Name, g.Audience, 0); err != nil {
			return nil, err
		}
	}
	// The record of what was issued is written before any token leaves.
	if !m.RecordIssuance {
		return out, nil
	}
	if _, err := m.Stores.WriteIssued(ctx, record); err != nil {
		return nil, unavailableMint("the record of what was issued could not be written: %v", err)
	}
	return out, nil
}
