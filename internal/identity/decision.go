package identity

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Verdict is what verifying a signed decision says (authn-authz.md §6.6).
type Verdict int

const (
	// VerdictUnknown permits nothing and concludes nothing: no such key
	// after a fresh read, an instant outside the key's life, bytes that do
	// not decode.
	VerdictUnknown Verdict = iota
	// VerdictValid permits acting on it and attributing by it.
	VerdictValid
	// VerdictBad: the key is the document's and a rule, the signature or
	// the kind is wrong. Only this says "forged".
	VerdictBad
	// VerdictUnavailable is no answer about the message: this engine has no
	// copy of the published keys it may use. The caller decides nothing.
	VerdictUnavailable
)

func (v Verdict) String() string {
	if v < 0 || int(v) >= len(verdictNames) {
		return fmt.Sprintf("verdict(%d)", int(v))
	}
	return verdictNames[v]
}

var verdictNames = [...]string{"unknown", "valid", "bad", "unavailable"}

// NoBinding is the bind of a decision whose body names nothing a caller
// must match; nil is refused, so no caller leaves the binding out by
// mistake.
var NoBinding = []byte{}

// Bind is the binding a decision's body begins with: each part, in order,
// as its length in four bytes big-endian followed by its bytes. A signer
// starts the body with it and a verifier passes the same parts as bind, so
// two different lists of as many parts never match each other.
func Bind(parts ...string) []byte {
	n := 0
	for _, p := range parts {
		n += 4 + len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = binary.BigEndian.AppendUint32(out, uint32(len(p)))
		out = append(out, p...)
	}
	return out
}

// unknownKidReadTimeout bounds the fresh read an unknown kid makes.
const unknownKidReadTimeout = 5 * time.Second

// failedKidReadRetry is how soon a read for an unknown kid may be tried
// again after one failed.
const failedKidReadRetry = time.Second

// UnknownKidReadInterval bounds how often a kid the copy lacks makes the
// engine read the document again: a flood of made-up kids costs the store
// one read per interval.
const UnknownKidReadInterval = 5 * time.Second

// decisionKeys answers a document's keys by kid, as the jwt verifier asks.
type decisionKeys artifacts.PublishedKeys

func (d decisionKeys) DecisionKey(kid string) (jwt.PublicKey, bool) {
	p, ok := artifacts.PublishedKeys(d).Key(kid)
	if !ok {
		return jwt.PublicKey{}, false
	}
	k, err := jwt.ParsePublicJWK(p.JWK)
	if err != nil || k.KeyID() != p.KID || string(k.Algorithm()) != p.Algorithm {
		return jwt.PublicKey{}, false // reported by the issuer's served-keys check
	}
	return k, true
}

// usable says the copy of the document may decide at now. Callers hold
// i.mu.
func (i *Issuer) usable(now time.Time) bool {
	age := now.Sub(i.readAt)
	return !i.readAt.IsZero() && age >= 0 && age <= i.cfg.CopyMaxAge
}

// decisionSigner is the key that signs decisions: among the keys this
// engine holds that the document lists unretired, the one published first,
// ties broken by kid. No activation delay applies: engines read the
// document itself. Callers hold i.mu.
func (i *Issuer) decisionSigner(now time.Time) (*jwt.Signer, error) {
	if !i.usable(now) {
		return nil, fmt.Errorf("%w: its copy of the published signing keys is missing or older than %s", ErrCannotSign, i.cfg.CopyMaxAge)
	}
	var best *jwt.Signer
	var bestAt time.Time
	for _, k := range i.cfg.Keys {
		p, ok := i.doc.Key(k.KeyID())
		if !ok || p.Retired() || i.retiredSeen[p.KID] || !i.storedIsHeld(p) {
			continue
		}
		if best == nil || p.PublishedAt.Before(bestAt) || (p.PublishedAt.Equal(bestAt) && p.KID < best.KeyID()) {
			best, bestAt = k, p.PublishedAt
		}
	}
	if best == nil {
		return nil, fmt.Errorf("%w: no key it holds is published and unretired", ErrCannotSign)
	}
	return best, nil
}

// SignDecision signs body as a decision of kind with the key that signs
// decisions.
func (i *Issuer) SignDecision(kind jwt.DecisionKind, body []byte) (string, error) {
	if i == nil {
		return "", fmt.Errorf("%w: this engine holds no signing key", ErrCannotSign)
	}
	now := i.now().UTC()
	i.mu.Lock()
	signer, err := i.decisionSigner(now)
	i.mu.Unlock()
	if err != nil {
		return "", err
	}
	return signer.SignDecision(kind, body)
}

// VerifyDecision checks msg as a decision of kind whose attested instant,
// read by the caller from the cluster or the store and never from the
// message, is at. bind is what the body must begin with: what the decision
// is about (its run, its scope), as the caller knows it, so a decision
// copied into another start is not taken there; a valid signature over a
// body bound to something else is bad. A kid this engine's copy lacks is
// looked for in a fresh read, at most once per UnknownKidReadInterval,
// before the answer is unknown. An engine with no keys (a nil Issuer)
// answers unavailable. A nil bind is bad: give NoBinding to say there is
// none.
func (i *Issuer) VerifyDecision(ctx context.Context, msg string, kind jwt.DecisionKind, at time.Time, bind []byte) ([]byte, string, Verdict) {
	if i == nil {
		return nil, "", VerdictUnavailable
	}
	if bind == nil {
		return nil, "", VerdictBad
	}
	if at.IsZero() {
		// The caller could not read the instant the kind attests: no
		// conclusion about the message.
		return nil, "", VerdictUnavailable
	}
	now := i.now().UTC()
	i.mu.Lock()
	ok, doc := i.usable(now), i.doc
	i.mu.Unlock()
	if !ok {
		return nil, "", VerdictUnavailable
	}
	body, kid, answer := jwt.VerifyDecision(msg, kind, decisionKeys(doc))
	if answer == jwt.DecisionUnknown && kid != "" {
		// "Unknown" is said only after a fresh read: one that could not be
		// made, or failed, is no conclusion.
		if !i.mayReadForKid(now) {
			return nil, kid, VerdictUnavailable
		}
		readCtx, cancel := context.WithTimeout(ctx, unknownKidReadTimeout)
		fresh, err := i.store.Read(readCtx)
		cancel()
		if err != nil {
			i.mu.Lock()
			// A failed read lets the next try sooner, but not at once.
			i.lastKidRead = now.Add(failedKidReadRetry - UnknownKidReadInterval)
			i.mu.Unlock()
			return nil, kid, VerdictUnavailable
		}
		{
			i.adopt(fresh, now)
			i.mu.Lock()
			doc = i.doc
			i.mu.Unlock()
			body, kid, answer = jwt.VerifyDecision(msg, kind, decisionKeys(doc))
		}
	}
	switch answer {
	case jwt.DecisionBad:
		return nil, kid, VerdictBad
	case jwt.DecisionUnknown:
		return nil, kid, VerdictUnknown
	}
	p, ok := doc.Key(kid)
	if !ok {
		return nil, kid, VerdictUnknown
	}
	if !i.withinLife(p, at.UTC()) {
		return nil, kid, VerdictUnknown
	}
	if !bytes.HasPrefix(body, bind) {
		return nil, kid, VerdictBad
	}
	return body, kid, VerdictValid
}

// mayReadForKid says an unknown kid may read the document now, and counts
// the read: at most one per UnknownKidReadInterval.
func (i *Issuer) mayReadForKid(now time.Time) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.lastKidRead.IsZero() && now.Sub(i.lastKidRead) < UnknownKidReadInterval {
		return false
	}
	i.lastKidRead = now
	return true
}

// withinLife says at lies inside p's life: not before its publication less
// the skew; for a key retired by rotation, not after its retirement plus
// the copy's age and the skew; for one retired as compromised, before
// compromised_from.
func (i *Issuer) withinLife(p artifacts.PublishedKey, at time.Time) bool {
	if at.IsZero() || at.Before(p.PublishedAt.Add(-i.cfg.ClockSkew)) {
		return false
	}
	from := p.CompromisedFrom
	i.mu.Lock()
	if seen, ok := i.compromisedSeen[p.KID]; ok && (from.IsZero() || seen.Before(from)) {
		from = seen // a compromise this engine read stands, whatever a later document says
	}
	i.mu.Unlock()
	if !from.IsZero() && !at.Before(from) {
		return false
	}
	if p.Retired() && at.After(p.RetiredAt.Add(i.cfg.CopyMaxAge+i.cfg.ClockSkew)) {
		return false
	}
	return true
}

// UnsignedVerdict is what an unsigned decision or record is worth.
type UnsignedVerdict int

const (
	// UnsignedBad: in observe or enforce, unsigned is forged.
	UnsignedBad UnsignedVerdict = iota
	// UnsignedAsserted: accepted as asserted, never as verified, and nothing
	// attaches to it.
	UnsignedAsserted
	// UnsignedUnavailable: in observe or enforce with no usable copy of the
	// document, no conclusion.
	UnsignedUnavailable
)

func (v UnsignedVerdict) String() string {
	if v < 0 || int(v) >= len(unsignedNames) {
		return fmt.Sprintf("unsigned(%d)", int(v))
	}
	return unsignedNames[v]
}

var unsignedNames = [...]string{"bad", "asserted", "unavailable"}

// UnsignedDecision judges an unsigned decision under the deployment's mode
// (the higher of the engine's own and the mode record's): bad in observe
// and enforce, asserted in asserted, and unavailable under a mode this
// release does not know. Nothing attaches to an unsigned decision in any mode.
func (i *Issuer) UnsignedDecision(mode string) UnsignedVerdict {
	switch mode {
	case "asserted":
		return UnsignedAsserted
	case "observe", "enforce":
		return UnsignedBad
	}
	// A mode the caller could not read, or one this release does not know:
	// no conclusion, and nothing attaches.
	return UnsignedUnavailable
}

// UnsignedRecord judges an unsigned termination record under the
// deployment's mode: in observe and enforce it is bad, except one whose run
// closed, by the cluster's time, before the earliest publication of any key
// in the document; in asserted it is accepted as asserted. A record with no
// close time, a mode this release does not know, or an engine with no keys
// is unavailable: no conclusion, and nothing attaches.
func (i *Issuer) UnsignedRecord(mode string, closedAt time.Time) UnsignedVerdict {
	switch mode {
	case "asserted":
		return UnsignedAsserted
	case "observe", "enforce":
	default:
		return UnsignedUnavailable
	}
	if i == nil || closedAt.IsZero() {
		// No keys to read the past by, or no close time to judge it: no
		// conclusion.
		return UnsignedUnavailable
	}
	now := i.now().UTC()
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.usable(now) {
		return UnsignedUnavailable
	}
	earliest := time.Time{}
	for _, k := range i.doc.Keys {
		if earliest.IsZero() || k.PublishedAt.Before(earliest) {
			earliest = k.PublishedAt
		}
	}
	if !earliest.IsZero() && closedAt.Before(earliest) {
		return UnsignedAsserted
	}
	return UnsignedBad
}

// MayIssue says whether an engine may issue a decision under the
// deployment's mode: one that holds no signing key (issuer nil) refuses
// above asserted, where every other engine would end its unsigned decision
// as forged.
func MayIssue(issuer *Issuer, mode string) error {
	// As UnsignedDecision judges it: only asserted takes an unsigned one.
	if issuer == nil && mode != "asserted" {
		return fmt.Errorf("%w: this engine holds no signing key, and this deployment verifies its callers", ErrCannotSign)
	}
	return nil
}
