package payloadcrypt

import (
	"crypto/rand"
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"runtime/debug"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

// Withheld is the whole text of an error that carries no key in a process
// that seals: a bug, written fail-closed, never in plaintext.
const Withheld = "withheld"

// sealedMessage is the plaintext message of a sealed cause.
const sealedMessage = "sealed"

func isMiss(err error) bool { return errors.Is(err, ErrEngineKeyMissing) }

// reasonShape is a reason code: a word of a closed vocabulary, never text a
// run supplied. Anything else is written as "sealed".
var reasonShape = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

// keyedError is an error leaving OrkStack's workflow or activity code
// through the boundary helper, with its run's key (Unsealed() for a run that
// is not sealed) and its reason code.
type keyedError struct {
	err    error
	scope  *Scope
	reason string
}

func (e *keyedError) Error() string { return e.err.Error() }
func (e *keyedError) Unwrap() error { return e.err }

// WithKey is the boundary helper: every error OrkStack's workflow and
// activity code returns leaves through it, with its run's key — Unsealed()
// for a run that is not sealed — and its reason code, a word of a closed
// vocabulary that becomes the plaintext message of a sealed failure. A nil
// scope is a key that was lost: in a process that seals, the error is
// withheld, never written in plaintext.
func WithKey(err error, s *Scope, reason string) error {
	if err == nil {
		return nil
	}
	return &keyedError{err: err, scope: s, reason: reason}
}

// keylessError marks the one error of a keyless run, which holds a reason
// code and fixed words and is written as it is.
type keylessError struct{ err error }

func (e *keylessError) Error() string { return e.err.Error() }
func (e *keylessError) Unwrap() error { return e.err }

// Keyless marks err as a keyless run's ending. It counts only as the
// outermost wrapper: Keyless deep inside another error exempts nothing.
func Keyless(err error) error {
	if err == nil {
		return nil
	}
	return &keylessError{err: err}
}

// Recover turns a panic into a keyed error, so its value and stack are
// sealed like any other: defer Recover(scope, reason, &err). The guard's
// panic (ErrEngineKeyMissing) is never recovered: it must fail the workflow
// task, so an engine that holds the key takes it.
func Recover(s *Scope, reason string, errp *error) {
	r := recover()
	if r == nil {
		return
	}
	if e, ok := r.(error); ok && isMiss(e) {
		panic(r)
	}
	stack := string(debug.Stack())
	cause, _ := r.(error)
	*errp = WithKey(temporal.NewApplicationErrorWithCause(fmt.Sprintf("panic: %v", r), "PanicError", cause, stack), s, reason)
}

// SealedFailure is a failure read back still sealed: the SDK's side of the
// failure converter decrypts nothing (its decode errors would be swallowed);
// the interpreter opens it with OpenFailure before it reads or classifies
// it. Its text is the plaintext message, a reason code.
type SealedFailure struct {
	f *failurepb.Failure
}

func (e *SealedFailure) Error() string { return e.f.GetMessage() }

// Reason is the reason code a sealed failure carries in plaintext: all a
// reader with no key has of it. It is not authenticated, as nothing in
// plaintext is.
func (e *SealedFailure) Reason() string { return e.f.GetMessage() }

// FailureConverter wraps the SDK's default failure converter. It seals an
// error that carries a key, every level of its cause chain and every detail;
// it reads a sealed failure back as a SealedFailure. With sealing off, an
// error that carries no key is written exactly as the default writes it.
type FailureConverter struct {
	dfc     *temporal.DefaultFailureConverter
	dc      converter.DataConverter
	sealing bool
}

var _ converter.FailureConverter = (*FailureConverter)(nil)

// NewFailureConverter builds the converter over dc, the process's data
// converter, so a failure's details take the same conversion as every other
// payload. sealing is payload_encryption.enabled.
func NewFailureConverter(dc converter.DataConverter, sealing bool) *FailureConverter {
	return &FailureConverter{
		dfc:     temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc}),
		dc:      dc,
		sealing: sealing,
	}
}

// ErrorToFailure writes err.
func (c *FailureConverter) ErrorToFailure(err error) *failurepb.Failure {
	if err == nil {
		return nil
	}
	// The outermost markers: what the boundary said of this error.
	top, leadKeyless, leadUnsealed := err, false, false
	for {
		if e, ok := top.(*keyedError); ok {
			leadUnsealed = leadUnsealed || e.scope.unsealed()
			top = e.err
			continue
		}
		if e, ok := top.(*keylessError); ok {
			leadKeyless = true
			top = e.err
			continue
		}
		break
	}
	var f *failurepb.Failure
	var sf *SealedFailure
	if errors.As(top, &sf) {
		f = proto.Clone(sf.f).(*failurepb.Failure)
	} else {
		f = c.dfc.ErrorToFailure(top)
	}
	// A key anywhere in the chain seals the whole failure.
	if ke := keyedWithScope(err); ke != nil {
		c.seal(f, ke.scope, ke.reason)
		return f
	}
	switch {
	case leadUnsealed, leadKeyless, !c.sealing:
		return f
	case sf != nil && sealedWhole(f):
		// Another run's failure, handed on still sealed: ciphertext and its
		// reason, as the cluster already holds it. The text of what wraps it is
		// not in f.
		return f
	case sdkOwn(top):
		stripSDKOwn(f)
		return f
	}
	withhold(f)
	return f
}

// sealedWhole reports that every level of f, and every detail, is sealed.
func sealedWhole(f *failurepb.Failure) bool {
	for l := f; l != nil; l = l.GetCause() {
		if !IsSealed(l.GetEncodedAttributes()) || l.GetStackTrace() != "" || l != f && l.GetMessage() != sealedMessage ||
			l == f && l.GetMessage() != sealedMessage && !reasonShape.MatchString(l.GetMessage()) {
			return false
		}
		for _, ps := range detailsOf(l) {
			for _, p := range ps.GetPayloads() {
				if !IsSealed(p) {
					return false
				}
			}
		}
	}
	return f != nil
}

// keyedWithScope is the first keyed error in err's chain that carries a
// run's key.
func keyedWithScope(err error) *keyedError {
	stack := []error{err}
	for len(stack) > 0 {
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if e == nil {
			continue
		}
		if ke, ok := e.(*keyedError); ok && ke.scope != nil && !ke.scope.unsealed() {
			return ke
		}
		switch u := e.(type) {
		case interface{ Unwrap() error }:
			stack = append(stack, u.Unwrap())
		case interface{ Unwrap() []error }:
			for i := len(u.Unwrap()) - 1; i >= 0; i-- {
				stack = append(stack, u.Unwrap()[i])
			}
		}
	}
	return nil
}

// sdkOwn reports the SDK's own errors, whose message quotes nothing of a
// run.
func sdkOwn(err error) bool {
	switch err.(type) {
	case *temporal.CanceledError, *temporal.TimeoutError, *temporal.TerminatedError:
		return true
	}
	return false
}

// stripSDKOwn keeps an SDK error's own top-level message and type, and
// withholds what it may carry of a run: its details and its causes.
func stripSDKOwn(f *failurepb.Failure) {
	clearDetails(f)
	f.EncodedAttributes = nil
	if f.GetCause() != nil {
		withhold(f.GetCause())
	}
}

// commonText is a level's text in the SDK's own encoded-attributes shape.
type commonText struct {
	Message    string `json:"message"`
	StackTrace string `json:"stack_trace"`
}

func (c *FailureConverter) seal(f *failurepb.Failure, s *Scope, reason string) {
	if !reasonShape.MatchString(reason) {
		reason = sealedMessage
	}
	sealed := proto.Clone(f).(*failurepb.Failure)
	if err := sealLevels(sealed, s); err != nil {
		// Fail closed: nothing of it in plaintext, its reason kept.
		withhold(f)
		f.Message = reason
		return
	}
	sealed.Message = reason
	proto.Reset(f)
	proto.Merge(f, sealed)
}

func sealLevels(f *failurepb.Failure, s *Scope) error {
	for l := f; l != nil; l = l.GetCause() {
		if !IsSealed(l.GetEncodedAttributes()) {
			text, err := json.Marshal(commonText{Message: l.GetMessage(), StackTrace: l.GetStackTrace()})
			if err != nil {
				return err
			}
			sealed, err := sealWith(failureLabel, s.key, s.ctx, &commonpb.Payload{
				Metadata: map[string][]byte{MetaEncoding: []byte("json/plain")}, Data: text,
			}, rand.Reader)
			if err != nil {
				return err
			}
			l.EncodedAttributes = sealed
			l.Message = sealedMessage
			l.StackTrace = ""
		}
		for _, ps := range detailsOf(l) {
			for i, p := range ps.GetPayloads() {
				if IsSealed(p) {
					continue
				}
				sp, err := SealPayload(s.key, s.ctx, p)
				if err != nil {
					return err
				}
				ps.Payloads[i] = sp
			}
		}
	}
	return nil
}

// detailsOf is every payload list a level carries.
func detailsOf(l *failurepb.Failure) []*commonpb.Payloads {
	var out []*commonpb.Payloads
	add := func(ps *commonpb.Payloads) {
		if ps != nil {
			out = append(out, ps)
		}
	}
	add(l.GetApplicationFailureInfo().GetDetails())
	add(l.GetCanceledFailureInfo().GetDetails())
	add(l.GetTimeoutFailureInfo().GetLastHeartbeatDetails())
	add(l.GetResetWorkflowFailureInfo().GetLastHeartbeatDetails())
	return out
}

func withholdLevel(l *failurepb.Failure) {
	l.Message = Withheld
	l.StackTrace = ""
	l.EncodedAttributes = nil
	clearDetails(l)
}

func clearDetails(l *failurepb.Failure) {
	if a := l.GetApplicationFailureInfo(); a != nil {
		a.Details = nil
	}
	if a := l.GetCanceledFailureInfo(); a != nil {
		a.Details = nil
	}
	if a := l.GetTimeoutFailureInfo(); a != nil {
		a.LastHeartbeatDetails = nil
	}
	if a := l.GetResetWorkflowFailureInfo(); a != nil {
		a.LastHeartbeatDetails = nil
	}
}

func withhold(f *failurepb.Failure) {
	for l := f; l != nil; l = l.GetCause() {
		withholdLevel(l)
	}
}

func failureSealed(f *failurepb.Failure) bool {
	for l := f; l != nil; l = l.GetCause() {
		if IsSealed(l.GetEncodedAttributes()) {
			return true
		}
		for _, ps := range detailsOf(l) {
			for _, p := range ps.GetPayloads() {
				if IsSealed(p) {
					return true
				}
			}
		}
	}
	return false
}

// FailureToError reads f. A sealed failure is handed back unopened, as a
// SealedFailure; any other is the SDK default's.
func (c *FailureConverter) FailureToError(f *failurepb.Failure) error {
	if failureSealed(f) {
		return &SealedFailure{f: proto.Clone(f).(*failurepb.Failure)}
	}
	return c.dfc.FailureToError(f)
}

// OpenFailure opens a failure that err holds still sealed, with s, the
// run's own key and context, and reads it as the SDK would have. An error
// holding no sealed failure is returned as it is. A sealed failure that s
// does not open is ErrUnreadable, never skipped.
func OpenFailure(s *Scope, c *FailureConverter, err error) (opened error, openErr error) {
	if err == nil {
		return nil, nil
	}
	var sf *SealedFailure
	if !errors.As(err, &sf) {
		return err, nil
	}
	if !s.Sealed() {
		return nil, unreadable("a sealed failure in a run that carries no key")
	}
	f, openErr := openLevels(s, sf.f)
	if openErr != nil {
		return nil, openErr
	}
	return c.dfc.FailureToError(f), nil
}

// OpenFailureProto is a failure as the cluster holds it, opened with s: f
// itself where nothing of it is sealed.
func OpenFailureProto(s *Scope, f *failurepb.Failure) (*failurepb.Failure, error) {
	if !s.Sealed() {
		return nil, unreadable("a failure read with no key")
	}
	return openLevels(s, f)
}

// openLevels is sealed opened with s, at every level and in every detail.
func openLevels(s *Scope, sealed *failurepb.Failure) (*failurepb.Failure, error) {
	f := proto.Clone(sealed).(*failurepb.Failure)
	for l := f; l != nil; l = l.GetCause() {
		if attrs := l.GetEncodedAttributes(); IsSealed(attrs) {
			text, openErr := openWith(failureLabel, s.key, s.ctx, attrs)
			if openErr != nil {
				return nil, openErr
			}
			var ct commonText
			if jsonErr := json.Unmarshal(text.GetData(), &ct); jsonErr != nil {
				return nil, unreadable("a failure's text does not parse")
			}
			l.Message, l.StackTrace, l.EncodedAttributes = ct.Message, ct.StackTrace, nil
		}
		for _, ps := range detailsOf(l) {
			for i, p := range ps.GetPayloads() {
				if !IsSealed(p) {
					continue
				}
				detail, openErr := s.Open(p)
				if openErr != nil {
					return nil, openErr
				}
				ps.Payloads[i] = detail
			}
		}
	}
	return f, nil
}

// FailureSealed reports whether anything of f is sealed.
func FailureSealed(f *failurepb.Failure) bool {
	for l := f; l != nil; l = l.GetCause() {
		if IsSealed(l.GetEncodedAttributes()) {
			return true
		}
		for _, ps := range detailsOf(l) {
			if slices.ContainsFunc(ps.GetPayloads(), IsSealed) {
				return true
			}
		}
	}
	return false
}
