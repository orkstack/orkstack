package aws

import (
	"context"
	"errors"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/orkstack/orkstack/internal/provider"
)

// The operations an error names.
const (
	opLogin = "login"
	opRead  = "read"
	opIssue = "issue"
)

// Codes of answers that are not AWS's own words.
const (
	// codeNoAnswer is a connection that failed or a call that ran out of
	// time.
	codeNoAnswer = "NoAnswer"
	// codeUnreadable is an answer that is not the service's: a proxy's page,
	// or an error in another protocol's form.
	codeUnreadable = "UnreadableAnswer"
	// codeUnlisted is an error code this table does not hold. The code
	// itself is not repeated: only the codes below are known to be codes.
	codeUnlisted = "UnlistedCode"
	// codeIncomplete is an answer that lacks what the call is for.
	codeIncomplete = "IncompleteAnswer"
	// codeExpired is a session whose expiry is at, before or too soon after
	// the answer's date.
	codeExpired = "AnswerAlreadyExpired"
	codeClosed  = "SessionClosed"
	codeNoName  = "CredentialUnnamed"
	codeToken   = "TokenUnavailable"
	// codeNoNamespace is a KMS secret of an attempt that names no namespace.
	codeNoNamespace = "NamespaceUnknown"
	// codeField is a secret that holds no such key, and codeNotJSON one that
	// is not a JSON object of plain values.
	codeField   = "KeyNotFound"
	codeNotJSON = "NotAJSONObject"
)

// loginClasses and readClasses are the whole classification of AWS's answers
// (providers.md §11). A code not listed is Unavailable: an unknown answer is
// retried and loud, never a silent permanent failure.
//
// InvalidIdentityToken is Unavailable on purpose: it is what STS answers
// while it still holds an old copy of the issuer's keys or cannot reach it.
var (
	loginClasses = map[string]provider.Class{
		"InvalidIdentityToken":  provider.Unavailable,
		"IDPCommunicationError": provider.Unavailable,
		"ExpiredTokenException": provider.Unavailable,
		"Throttling":            provider.Unavailable,
		"ThrottlingException":   provider.Unavailable,
		"RequestLimitExceeded":  provider.Unavailable,
		"ServiceUnavailable":    provider.Unavailable,
		"InternalFailure":       provider.Unavailable,

		"AccessDenied":            provider.Refused,
		"IDPRejectedClaim":        provider.Refused,
		"ValidationError":         provider.Refused,
		"MalformedPolicyDocument": provider.Refused,
		"PackedPolicyTooLarge":    provider.Refused,
		"RegionDisabledException": provider.Refused,
	}
	readClasses = map[string]provider.Class{
		"Throttling":                 provider.Unavailable,
		"ThrottlingException":        provider.Unavailable,
		"TooManyRequestsException":   provider.Unavailable,
		"RequestLimitExceeded":       provider.Unavailable,
		"KMSInternalException":       provider.Unavailable,
		"DependencyTimeoutException": provider.Unavailable,
		"KeyUnavailableException":    provider.Unavailable,
		"InternalServiceError":       provider.Unavailable,
		"InternalServerError":        provider.Unavailable,
		"ServiceUnavailable":         provider.Unavailable,
		"ExpiredTokenException":      provider.Unavailable,

		"ParameterNotFound":         provider.NotFound,
		"ParameterVersionNotFound":  provider.NotFound,
		"ResourceNotFoundException": provider.NotFound,
		"NotFoundException":         provider.NotFound,

		"AccessDeniedException":      provider.Refused,
		"IncorrectKeyException":      provider.Refused,
		"InvalidCiphertextException": provider.Refused,
		"DisabledException":          provider.Refused,
		"KMSInvalidStateException":   provider.Refused,
	}
)

// classify is err, an SDK call's, as the provider's one error. What the SDK
// returned is dropped but for a context's end: it holds the response, and may
// hold account ids, role names and paths.
func classify(op string, err error, classes map[string]provider.Class) *provider.Error {
	var response *smithyhttp.ResponseError
	// The SDK wraps a failed connection so too, with no status.
	answered := errors.As(err, &response) && response.HTTPStatusCode() > 0
	var api smithy.APIError
	if errors.As(err, &api) {
		code := api.ErrorCode()
		class, known := classes[code]
		if !known {
			code, class = codeUnlisted, provider.Unavailable
		}
		// A 5xx is AWS failing to answer, whatever code it carries.
		if answered && response.HTTPStatusCode() >= 500 {
			class = provider.Unavailable
		}
		return &provider.Error{Class: class, Op: op, Code: code}
	}
	end := contextEnd(err)
	code := codeNoAnswer
	if answered && end == nil {
		code = codeUnreadable
	}
	return &provider.Error{Class: provider.Unavailable, Op: op, Code: code, Err: end}
}

func contextEnd(err error) error {
	for _, end := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, end) {
			return end
		}
	}
	return nil
}
