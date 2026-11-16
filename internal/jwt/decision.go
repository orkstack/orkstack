package jwt

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/cryptosigner"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// DecisionType is the typ of a signed decision: what an engine hands
// another engine through the cluster (authn-authz.md §6.6). No token
// verifier takes it, and the decision verifier takes nothing else.
const DecisionType = "ork-decision+jws"

// MaxDecisionBytes bounds a signed decision; a longer one is refused before
// a byte of it is parsed.
const MaxDecisionBytes = 8 << 10

// ErrDecisionKind is a decision of a kind outside the closed set, and
// ErrDecisionTooLarge one whose signed form passes MaxDecisionBytes: neither
// is ever signed, however often it is asked.
var (
	ErrDecisionKind     = errors.New("the decision kind is not one this engine signs")
	ErrDecisionTooLarge = errors.New("the signed decision is too large")
)

// DecisionKind is what a signed decision is about: a closed set compiled
// into the engine, each naming the instant it attests. A signature made for
// one kind never verifies as another.
type DecisionKind string

const (
	// KindForceCancelRoot and KindForceCancelDerived attest the start of the
	// execution that carries the decision (E88).
	KindForceCancelRoot    DecisionKind = "fc.root-decision.v1"
	KindForceCancelDerived DecisionKind = "fc.derived-decision.v1"
	// KindForceCancelRecord attests the close of the run that was ended.
	KindForceCancelRecord DecisionKind = "fc.record.v2"
	// KindForceCancelEvent attests the close its terminal event belongs to.
	KindForceCancelEvent DecisionKind = "fc.event.v1"
	// KindRunEnding attests the start of the execution that publishes a
	// run's unwritten ending (E85).
	KindRunEnding DecisionKind = "run.ending.v1"
	// KindRunGap attests the store's own time for a gap record's object
	// (E85).
	KindRunGap DecisionKind = "run.gap.v1"
)

var decisionKinds = []DecisionKind{KindForceCancelRoot, KindForceCancelDerived, KindForceCancelRecord,
	KindForceCancelEvent, KindRunEnding, KindRunGap}

// DecisionKinds is the closed set.
func DecisionKinds() []DecisionKind { return slices.Clone(decisionKinds) }

// DecisionAnswer is what verifying a signed decision says.
type DecisionAnswer int

const (
	// DecisionUnknown: no such key, or bytes that do not decode. It permits
	// nothing and concludes nothing: an engine that has not read a new key
	// must not call a real decision a forgery.
	DecisionUnknown DecisionAnswer = iota
	// DecisionValid: the key is the document's, every rule holds, the
	// signature holds and the kind is the one asked for.
	DecisionValid
	// DecisionBad: the key is the document's and a rule, the signature or
	// the kind is wrong. Only this answer says "forged".
	DecisionBad
)

func (a DecisionAnswer) String() string {
	switch a {
	case DecisionValid:
		return "valid"
	case DecisionBad:
		return "bad"
	}
	return "unknown"
}

// DecisionKeys finds a key of the published keys document by kid. A key is
// never taken from the message.
type DecisionKeys interface {
	DecisionKey(kid string) (PublicKey, bool)
}

// decisionPayload is the signed payload, written by stablejson in this
// field order: {"v":1,"kind":…,"body":…}.
type decisionPayload struct {
	V    int    `json:"v"`
	Kind string `json:"kind"`
	Body string `json:"body"`
}

func encodeDecisionPayload(kind DecisionKind, body []byte) ([]byte, error) {
	return stablejson.Marshal(decisionPayload{V: 1, Kind: string(kind), Body: base64.RawURLEncoding.EncodeToString(body)})
}

// SignDecision signs body as a decision of kind: a compact JWS whose
// protected header is alg, kid and typ alone.
func (s *Signer) SignDecision(kind DecisionKind, body []byte) (string, error) {
	if !slices.Contains(decisionKinds, kind) {
		return "", fmt.Errorf("%w: %q", ErrDecisionKind, kind)
	}
	payload, err := encodeDecisionPayload(kind, body)
	if err != nil {
		return "", fmt.Errorf("encode the decision: %w", err)
	}
	var key any = s.signer
	switch s.signer.(type) {
	case *rsa.PrivateKey, *ecdsa.PrivateKey:
	default:
		key = cryptosigner.Opaque(s.signer)
	}
	opts := (&jose.SignerOptions{}).WithHeader(jose.HeaderKey("kid"), s.kid).WithType(jose.ContentType(DecisionType))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.SignatureAlgorithm(s.alg), Key: key}, opts)
	if err != nil {
		return "", fmt.Errorf("signer: %w", err)
	}
	obj, err := signer.Sign(payload)
	if err != nil {
		return "", fmt.Errorf("sign the decision: %w", err)
	}
	out, err := obj.CompactSerialize()
	if err != nil {
		return "", err
	}
	if s.alg == ES256 {
		if out, err = lowS(out); err != nil {
			return "", err
		}
	}
	if len(out) > MaxDecisionBytes {
		return "", fmt.Errorf("%w: %d bytes; at most %d", ErrDecisionTooLarge, len(out), MaxDecisionBytes)
	}
	return out, nil
}

// VerifyDecision checks msg as a decision of kind against keys, and answers
// its body and the kid that signed it when it is valid. The caller then
// holds the instant the kind attests, read from the cluster or the store,
// to that key's life; this function knows no clock.
func VerifyDecision(msg string, kind DecisionKind, keys DecisionKeys) (body []byte, kid string, answer DecisionAnswer) {
	if len(msg) > MaxDecisionBytes || !slices.Contains(decisionKinds, kind) {
		return nil, "", DecisionUnknown
	}
	parts := strings.Split(msg, ".")
	if len(parts) != 3 {
		return nil, "", DecisionUnknown
	}
	headerJSON, err := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if err != nil {
		return nil, "", DecisionUnknown
	}
	members, err := decodeObject(headerJSON)
	if err != nil {
		return nil, "", DecisionUnknown
	}
	kid, present, kErr := stringMember(members, "kid")
	if kErr != nil || !present || kid == "" {
		return nil, "", DecisionUnknown
	}
	key, ok := keys.DecisionKey(kid)
	if !ok || key.IsZero() {
		return nil, kid, DecisionUnknown
	}
	// From here the kid is a key of the document: every failure is bad.
	bad := func() ([]byte, string, DecisionAnswer) { return nil, kid, DecisionBad }
	if len(members) != 3 {
		return bad()
	}
	alg, _, aErr := stringMember(members, "alg")
	typ, _, tErr := stringMember(members, "typ")
	if aErr != nil || tErr != nil || Algorithm(alg) != key.Algorithm() || typ != DecisionType {
		return bad()
	}
	if !isBase64URL(parts[1]) || !isBase64URL(parts[2]) || parts[1] == "" || parts[2] == "" {
		return bad()
	}
	sig, err := base64.RawURLEncoding.Strict().DecodeString(parts[2])
	if err != nil || (key.Algorithm() == ES256 && (len(sig) != 64 || highS(sig[32:]))) {
		return bad()
	}
	obj, err := jose.ParseSignedCompact(msg, []jose.SignatureAlgorithm{jose.SignatureAlgorithm(key.Algorithm())})
	if err != nil {
		return bad()
	}
	payload, err := obj.Verify(key.Public())
	if err != nil {
		return bad()
	}
	var p decisionPayload
	if err := json.Unmarshal(payload, &p); err != nil || p.V != 1 || p.Kind != string(kind) {
		return bad()
	}
	body, err = base64.RawURLEncoding.Strict().DecodeString(p.Body)
	if err != nil {
		return bad()
	}
	if canonical, err := encodeDecisionPayload(kind, body); err != nil || !bytes.Equal(canonical, payload) {
		return bad()
	}
	return body, kid, DecisionValid
}

// DeriveLabel names what a derived value is for: a closed set.
type DeriveLabel string

const (
	// DeriveForceCancelRequestID is the request id of a retried force
	// cancel (E88).
	DeriveForceCancelRequestID DeriveLabel = "fc.request-id.v1"
	// DeriveResumeToken is a resume's token (E85).
	DeriveResumeToken DeriveLabel = "resume-token.v1"
)

const deriveInfo = "orkstack/derive/v1"

// ErrCannotDerive is a key whose private scalar this process cannot read,
// such as one held in a device.
var ErrCannotDerive = errors.New("this signing key cannot derive a value")

// Derive answers HMAC-SHA256 over label, a zero byte and data, under a key
// made by HKDF-SHA256 from the signing key's private scalar, with the
// signer's kid as its salt and orkstack/derive/v1 as its info. Only a
// holder of the key computes it, and every replica with the same key
// computes the same value; a rotation changes every derived value, so none
// is to be stored and compared later (authn-authz.md §6.6).
func (s *Signer) Derive(label DeriveLabel, data []byte) ([]byte, error) {
	if label != DeriveForceCancelRequestID && label != DeriveResumeToken {
		return nil, fmt.Errorf("derive label %q is not one this engine derives", label)
	}
	var scalar []byte
	switch k := s.signer.(type) {
	case *ecdsa.PrivateKey:
		b, err := k.Bytes()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCannotDerive, err)
		}
		scalar = b
	case *rsa.PrivateKey:
		scalar = k.D.FillBytes(make([]byte, (k.N.BitLen()+7)/8))
	default:
		return nil, ErrCannotDerive
	}
	key, err := hkdf.Key(sha256.New, scalar, []byte(s.kid), deriveInfo, sha256.Size)
	if err != nil {
		return nil, fmt.Errorf("derive: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(label))
	mac.Write([]byte{0})
	mac.Write(data)
	return mac.Sum(nil), nil
}

// p256HalfOrder is half of P-256's group order: an ECDSA signature (r, s)
// and (r, n-s) both verify, so only the low s is taken, and one decision
// has one encoding.
var p256HalfOrder = new(big.Int).Rsh(elliptic.P256().Params().N, 1)

func highS(s []byte) bool {
	return new(big.Int).SetBytes(s).Cmp(p256HalfOrder) > 0
}

// lowS rewrites an ES256 compact JWS to its low-s signature, the one
// encoding VerifyDecision takes.
func lowS(compact string) (string, error) {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return "", errors.New("the signed decision is not a compact JWS")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return "", errors.New("the ES256 signature is not 64 bytes")
	}
	if !highS(sig[32:]) {
		return compact, nil
	}
	n := elliptic.P256().Params().N
	low := new(big.Int).Sub(n, new(big.Int).SetBytes(sig[32:]))
	low.FillBytes(sig[32:])
	return parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
