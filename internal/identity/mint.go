package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
	Attempt                  int32
	TaskSpecSHA256           string
	AdmissionSHA256          string
	Tokens                   []TokenAsk
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
}

func (e *MintError) Error() string { return e.Reason + ": " + e.Message }

// Public is what the caller is told: the reason and a fixed sentence, so a
// store's key, a cluster's error or a run's admitted shape never reach an
// Orker (and through it a task). TASK_NOT_AS_ADMITTED alone carries its
// detail, which names the members that differ and both builds and nothing
// else. The whole message is the engine's log line and audit record.
func (e *MintError) Public() string {
	if e.Reason == ReasonTaskNotAsAdmitted {
		return e.Error()
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
}

func refuse(reason string, audit bool, format string, args ...any) *MintError {
	return &MintError{Reason: reason, Audit: audit, Message: fmt.Sprintf(format, args...)}
}

func unavailableMint(format string, args ...any) *MintError {
	return &MintError{Reason: ReasonUnavailable, Unavailable: true, Message: fmt.Sprintf(format, args...)}
}

// Admitted is a request that passed step 2: the run's admission, the
// task's row, and the identities the request asks for, as admitted.
type Admitted struct {
	Admission  artifacts.Admission
	Task       artifacts.AdmittedTask
	Identities []artifacts.AdmittedIdentity
}

// MintStores is what the mint reads and writes in object storage.
type MintStores interface {
	ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error)
	TakeAttemptSlot(ctx context.Context, req artifacts.AttemptSlotRequest) (artifacts.AttemptSlot, error)
	WriteIssued(ctx context.Context, r artifacts.IssuedRecord) (string, error)
}

// FollowsChecker is step 6a: each task the admission says this one follows
// has its record of success. nil is a build without it, and refuses an
// admission that names any (fail closed).
type FollowsChecker interface {
	CheckFollows(ctx context.Context, adm artifacts.Admission, taskID string, item artifacts.Item) error
}

// Minter is the mint's steps 2 and 4 to 8; the API holds steps 1 and 3,
// the replica's guard and the caller's permissions at the admitted pool.
type Minter struct {
	Stores MintStores
	Starts StartReader
	// Describe is a describe of the run, made after the request arrived.
	Describe func(ctx context.Context, runID string) (ExecutionFacts, error)
	// Audiences is the namespace's identity policy's audiences now (step 7),
	// for the incarnation whose id the run was admitted with: a namespace
	// made again under the name answers ErrNamespaceGone.
	Audiences func(ctx context.Context, namespace, namespaceID string) ([]string, error)
	Follows   FollowsChecker
	Signer    func() (*jwt.Signer, error)
	Issuer    string
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
	read, err := m.Stores.ReadAdmission(ctx, req.Namespace, req.RunID, req.AdmissionSHA256)
	if errors.Is(err, artifacts.ErrInvalidRecordKey) {
		return Admitted{}, refuse(ReasonNotAdmitted, true, "run %q names no admission record %q", req.RunID, req.AdmissionSHA256)
	}
	if err != nil {
		return Admitted{}, unavailableMint("the admission record of run %q could not be read: %v", req.RunID, err)
	}
	switch read.Answer {
	case artifacts.RecordFound:
	case artifacts.RecordNone:
		return Admitted{}, refuse(ReasonNotAdmitted, true, "run %q has no admission record %q", req.RunID, req.AdmissionSHA256)
	default:
		return Admitted{}, unavailableMint("the admission record of run %q cannot be trusted (%s: %s)", req.RunID, read.Object, read.Reason)
	}
	adm := read.Value
	if adm.Namespace != req.Namespace || adm.RunID != req.RunID {
		return Admitted{}, refuse(ReasonNotAdmitted, true, "the admission record is of run %q in namespace %q", adm.RunID, adm.Namespace)
	}
	row, ok := adm.Tasks[req.TaskID]
	if !ok {
		return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q of run %q was admitted no identity", req.TaskID, req.RunID)
	}
	switch fanOut := row.MaxItems > 0; {
	case req.Item.Set && !fanOut:
		return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q is not a fan-out, and the request names item %d", req.TaskID, req.Item.Index)
	case !req.Item.Set && fanOut:
		return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q is a fan-out, and the request names no item", req.TaskID)
	case req.Item.Set && (req.Item.Index < 0 || req.Item.Index >= row.MaxItems):
		return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q was admitted %d items, not item %d", req.TaskID, row.MaxItems, req.Item.Index)
	}
	declared := adm.SetOf(req.TaskID)
	out := Admitted{Admission: adm, Task: row}
	for _, ask := range req.Tokens {
		if strings.HasPrefix(ask.Name, "grant:") {
			// A grant's login lands with the provider framework; until
			// then no task can name a grant.
			return Admitted{}, refuse(ReasonNotAdmitted, true, "task %q asks for %q, and no grant can be used in this build", req.TaskID, ask.Name)
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

// Issue is steps 4 to 8 for a request Admit passed and the caller's
// permissions allowed: the attempt held by the caller's Orker, the run's
// start, the task's binding, what it follows, the attempt cap, the policy
// still allowing each audience, and the signing, with the record of what
// was issued written before any token is returned.
func (m *Minter) Issue(ctx context.Context, a Admitted, req MintRequest, caller Caller) ([]IssuedToken, error) {
	adm := a.Admission
	// A run is issued tokens only by the issuer it was admitted under: one
	// whose keys its relying parties were told to trust.
	if adm.Issuer != m.Issuer {
		return nil, refuse(ReasonEngineRefused, false, "run %q was admitted under issuer %q, and this engine issues as %q", req.RunID, adm.Issuer, m.Issuer)
	}
	// Step 4: the attempt.
	facts, err := m.Describe(ctx, req.RunID)
	if errors.Is(err, ErrNoSuchExecution) {
		return nil, refuse(ReasonAttemptNotHeld, false, "run %q is not executing", req.RunID)
	}
	if err != nil {
		return nil, unavailableMint("run %q could not be described: %v", req.RunID, err)
	}
	pending, verdict := HeldAttempt(facts, req.RunID, AttemptAsk{TaskID: req.TaskID, Item: req.Item, Attempt: req.Attempt, OrkerID: caller.OrkerID})
	switch verdict {
	case AttemptHeld:
	case AttemptNotHeld:
		return nil, refuse(ReasonAttemptNotHeld, false, "attempt %d of task %q is not held by Orker %q", req.Attempt, req.TaskID, caller.OrkerID)
	default:
		return nil, unavailableMint("the cluster's describe of run %q did not say who holds the attempt", req.RunID)
	}
	// Step 5: the start.
	switch start := CheckStart(ctx, m.Starts, adm, req.AdmissionSHA256, facts); start.Verdict {
	case StartAdmitted:
	case StartNotAdmitted:
		return nil, refuse(ReasonNotAdmitted, true, "%s", start.Reason)
	default:
		return nil, unavailableMint("%s", start.Reason)
	}
	// Step 6: the task binding.
	if why, ok := CheckBinding(adm, req.TaskID, req.TaskSpecSHA256); !ok {
		e := refuse(ReasonTaskNotAsAdmitted, true, "%s", why)
		e.EngineBuild = adm.EngineBuild
		return nil, e
	}
	// Step 6a: what the task follows; a build without the check refuses
	// an admission that names any.
	if len(adm.Follows[req.TaskID]) > 0 {
		if m.Follows == nil {
			return nil, refuse(ReasonEngineRefused, false, "task %q follows other tasks, and this engine cannot check them", req.TaskID)
		}
		if err := m.Follows.CheckFollows(ctx, adm, req.TaskID, req.Item); err != nil {
			var me *MintError
			if errors.As(err, &me) {
				return nil, me
			}
			return nil, unavailableMint("what task %q follows could not be checked: %v", req.TaskID, err)
		}
	}
	// Step 7's read comes before the slot, so an outage of the policy
	// spends no attempt; its check stays after 6b, the spec's order.
	allowed, err := m.Audiences(ctx, req.Namespace, adm.NamespaceID)
	switch {
	case errors.Is(err, ErrNamespaceGone):
		return nil, refuse(ReasonRevoked, true, "namespace %q is gone", req.Namespace)
	case err != nil:
		return nil, unavailableMint("namespace %q's identity policy could not be read: %v", req.Namespace, err)
	}
	// Step 6b: the attempt cap.
	now := m.Now().UTC()
	if err := m.takeSlot(ctx, a, req, pending, now); err != nil {
		return nil, err
	}
	// Step 7: still allowed now.
	for _, id := range a.Identities {
		if !AudienceAllowed(allowed, id.Audience) {
			return nil, refuse(ReasonRevoked, true, "namespace %q no longer allows audience %q", req.Namespace, id.Audience)
		}
	}
	// Step 8: sign, record, answer.
	return m.sign(ctx, a, req, caller, now)
}

func (m *Minter) takeSlot(ctx context.Context, a Admitted, req MintRequest, pending PendingAttempt, now time.Time) error {
	slot, err := m.Stores.TakeAttemptSlot(ctx, artifacts.AttemptSlotRequest{
		Namespace: req.Namespace, RunID: req.RunID, TaskID: req.TaskID, Item: req.Item,
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
	out := make([]IssuedToken, 0, len(a.Identities))
	for _, id := range a.Identities {
		asked := time.Duration(id.TTLSeconds) * time.Second
		if asked == 0 {
			asked = time.Duration(a.Admission.TokenTTL) * time.Second
		}
		life := m.TTL.Clamp(asked)
		jti := m.NewJTI()
		claims, err := BuildClaims(ClaimsInput{Issuer: m.Issuer, Deployment: m.Deployment, Admission: a.Admission, TaskID: req.TaskID,
			Item: req.Item, Attempt: req.Attempt, Identity: id.Name, Audience: id.Audience, Pool: a.Task.Pool, Orker: caller.OrkerID,
			Now: now, Lifetime: life, JTI: jti})
		if err != nil {
			return nil, refuse(ReasonEngineRefused, false, "the claims of identity %q: %v", id.Name, err)
		}
		token, err := signer.Sign("JWT", claims)
		if err != nil {
			return nil, refuse(ReasonEngineRefused, false, "sign identity %q: %v", id.Name, err)
		}
		out = append(out, IssuedToken{Name: id.Name, Audience: id.Audience, Token: token, JTI: jti, KID: signer.KeyID(), IssuedAt: now, ExpiresIn: life})
		record.Tokens = append(record.Tokens, artifacts.IssuedEntry{Name: id.Name, Audience: id.Audience, Subject: claims["sub"].(string),
			JTI: jti, KID: signer.KeyID(), IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(life).Unix()})
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
