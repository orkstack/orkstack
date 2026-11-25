package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// The storage scope check (docs/DEPLOY.md, "An Orker's own storage
// credential"): what an Orker's own credential can reach. The control plane's
// objects (the access document, definitions, namespaces, webhook secrets, the
// audit trail) are trusted for where they lie, so an Orker whose credential
// can write them can write itself a role. The store enforces the policy and
// an operator configures it; the check is how an Orker tests its own
// credential and says what it found. It decides nothing by itself.

// OrkerAllowedPrefixes is the documented Orker allow-list: every prefix an
// Orker's credential needs, for reading as for writing, and nothing else.
var OrkerAllowedPrefixes = []string{
	s3OrkflowNamespace,
	s3ManifestNamespace,
	CommitPrefix,
	AttestedPrefix,
	MemoPrefix,
	LogPrefix,
	AttemptPrefix,
	"metadata/probes",
	ReceiptPrefix,
	EventSourceStagingPrefix,
	RunContextPrefix,
}

// ScopeReadOnly is one prefix of the allow-list an Orker may only read.
type ScopeReadOnly struct {
	Prefix string
	// Listed says the Orker may also list the prefix, because it reads keys
	// there that may not exist.
	Listed bool
}

// ScopeReadOnlyPrefixes are the allow-list's prefixes only an engine may
// write: the results it attested, which an Orker that could write there could
// forge, and the run contexts, which an Orker gets by a key it was handed and
// so need not list. The check asks the store to refuse a write and a delete
// of each, and a listing of one that is not Listed.
var ScopeReadOnlyPrefixes = []ScopeReadOnly{
	{Prefix: AttestedPrefix, Listed: true},
	{Prefix: RunContextPrefix},
}

// ScopeCheckedPrefixes is every prefix outside the allow-list, each of which
// the check asks the store to refuse. A test ties the two tables to every
// prefix constant of this package.
var ScopeCheckedPrefixes = []string{
	IdentityPrefix,
	OrkflowPrefix,
	NamespacePrefix,
	SettingsPrefix,
	TriggerPrefix,
	EventSourcePrefix,
	EventSourceSpecPrefix,
	WebhookPrefix,
	AuditPrefix,
	DeliveryPrefix,
	DeliveryBodyPrefix,
	RunArchiveNamespace,
	"metadata/index",
	SnapshotPrefix,
	ScheduleBindingPrefix,
	"metadata/cleanup",
}

const (
	// ScopeSentinelKey is the one object the check tries to read: a missing
	// key cannot tell a reader from a non-reader, an object that exists can.
	// An engine writes it at boot when it is absent.
	ScopeSentinelKey = IdentityPrefix + "/sentinel"
	// ScopeProbePrefix holds the check's positive control, a conditional
	// write that must not succeed: nothing is ever stored there.
	ScopeProbePrefix = "metadata/probes/scope"
	// ScopeResultsSegment is a segment no namespace can be named: the
	// check's keys under metadata/commits/ and the checked prefixes use it.
	ScopeResultsSegment = "_scope-check"

	scopeSentinelBody = "orkstack scope sentinel: an Orker's credential must not read this\n"

	// maxScopeCode bounds a store's code in a round's detail.
	maxScopeCode = 64
	// MaxScopeDetail bounds a round's detail, in bytes, so a hostile store
	// can neither swell a report nor break its encoding.
	MaxScopeDetail = 512
	// scopeCleanupTimeout bounds the delete of a key a 2xx stored.
	scopeCleanupTimeout = 10 * time.Second
)

// ScopeOutcome is what one round found.
type ScopeOutcome int

const (
	// ScopeUnclear: the control did not pass, or some answer was neither a
	// refusal nor a reach. It moves no verdict.
	ScopeUnclear ScopeOutcome = iota
	// ScopeScoped: the control passed and every request was refused.
	ScopeScoped
	// ScopeUnscoped: the control passed and some request reached.
	ScopeUnscoped
)

func (o ScopeOutcome) String() string {
	switch o {
	case ScopeScoped:
		return "scoped"
	case ScopeUnscoped:
		return "unscoped"
	}
	return "unclear"
}

// ResultsReach is what the round's results request found: whether the
// credential's results cover every namespace or only named ones.
type ResultsReach int

const (
	// ResultsUnknown: the request answered neither way.
	ResultsUnknown ResultsReach = iota
	// ResultsEveryNamespace: the credential may write any namespace's
	// results.
	ResultsEveryNamespace
	// ResultsByNamespace: the credential's results are limited to named
	// namespaces.
	ResultsByNamespace
)

// ScopeRound is one round's result.
type ScopeRound struct {
	Outcome ScopeOutcome
	// Detail names, for an unscoped round, the first request that reached
	// ("reached: put metadata/identity/"), and for an unclear one the
	// store's code or what went wrong. It is empty for a scoped round and
	// never holds a credential.
	Detail string
	// Results is what the results request found; it decides nothing.
	Results ResultsReach
	// IgnoredCondition: the store answered 2xx to a write whose If-Match
	// names no object. The check deleted that key again.
	IgnoredCondition bool
}

// CheckScope runs one round of the scope check with this store's credential,
// as Orker orkerID.
func (s *S3CommitStore) CheckScope(ctx context.Context, orkerID string) ScopeRound {
	return checkStoreScope(ctx, s.client, s.bucket, orkerID)
}

// CheckStoreScopeOf runs one round with store's credential. The local
// backend has none to hold to a prefix: its round is unscoped, and says so.
func CheckStoreScopeOf(ctx context.Context, store CommitStore, orkerID string) ScopeRound {
	if checker, ok := store.(ScopeChecker); ok {
		return checker.CheckScope(ctx, orkerID)
	}
	if _, local := store.(*LocalCommitStore); local {
		return ScopeRound{Outcome: ScopeUnscoped, Detail: "the local backend has no credential to scope"}
	}
	return ScopeRound{Outcome: ScopeUnscoped, Detail: fmt.Sprintf("a commit store (%T) the scope check cannot ask", store)}
}

// ScopeChecker is a store whose credential the scope check can test.
type ScopeChecker interface {
	CheckScope(ctx context.Context, orkerID string) ScopeRound
}

// EnsureScopeSentinel writes the scope check's sentinel when it is absent.
// An engine calls it at boot; an object already there is left alone.
func (s *S3OrkflowStore) EnsureScopeSentinel(ctx context.Context) error {
	return ensureScopeSentinel(ctx, s.client, s.bucket)
}

// answer is how the check reads one store answer. Only the store's own error
// code counts, never the HTTP status alone.
type answer int

const (
	answerUnclear answer = iota
	answerRefused
	answerReached
	answerMissing // NoSuchKey
)

func readAnswer(err error) (answer, string) {
	if err == nil {
		return answerReached, ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return answerUnclear, "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return answerUnclear, "canceled"
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return answerUnclear, "network error"
	}
	switch code := apiErr.ErrorCode(); code {
	case "AccessDenied":
		return answerRefused, code
	case "PreconditionFailed":
		return answerReached, code
	case "NoSuchKey":
		return answerMissing, code
	case "":
		return answerUnclear, "no error code"
	default:
		return answerUnclear, boundText(oneLine(code), maxScopeCode)
	}
}

// reach reads a write's or a delete's NoSuchKey as what it is: the request
// reached the key.
func reach(a answer) answer {
	if a == answerMissing {
		return answerReached
	}
	return a
}

// checkStoreScope runs one round: the control, then a conditional write, a
// conditional delete and a listing of each checked prefix, and of each
// read-only one what it does not grant, then a read of the sentinel, then the
// results request.
func checkStoreScope(ctx context.Context, client *s3.Client, bucket, orkerID string) ScopeRound {
	var round ScopeRound
	// leftBehind is every key a 2xx stored that the check could not remove.
	var leftBehind []string
	put := func(key string) error {
		body := []byte("scope check\n")
		_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), IfMatch: aws.String(probeAbsentETag)})
		if err == nil {
			// The store stored a write its condition should have stopped:
			// remove it, bounded even when the round's own time is up.
			round.IgnoredCondition = true
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scopeCleanupTimeout)
			_, derr := client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			cancel()
			if a, code := readAnswer(derr); derr != nil && a != answerMissing {
				leftBehind = append(leftBehind, key+" ("+code+")")
			}
		}
		return err
	}

	switch a, code := readAnswer(put(ScopeProbePrefix + "/" + orkerID)); {
	case round.IgnoredCondition:
		round.Outcome = ScopeUnclear
		round.Detail = "the control was stored: the store ignored If-Match on a missing key"
		if len(leftBehind) > 0 {
			round.Detail += "; it could not be removed: " + strings.Join(leftBehind, ", ")
		}
		return round
	case a == answerReached, a == answerMissing:
	default:
		round.Outcome = ScopeUnclear
		round.Detail = "the control was answered " + code
		return round
	}

	var reached, unclear string
	note := func(what string, a answer, code string) {
		switch a {
		case answerReached:
			if reached == "" {
				reached = what
			}
		case answerRefused:
		default:
			if unclear == "" {
				unclear = what + ": " + code
			}
		}
	}
	refuse := func(prefix string, list bool) {
		key := prefix + "/" + ScopeResultsSegment + "/" + orkerID
		a, code := readAnswer(put(key))
		note("put "+prefix+"/", reach(a), code)

		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			IfMatch: aws.String(probeAbsentETag)})
		a, code = readAnswer(err)
		note("delete "+prefix+"/", reach(a), code)

		if !list {
			return
		}
		_, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket),
			Prefix: aws.String(prefix + "/"), MaxKeys: aws.Int32(1)})
		a, code = readAnswer(err)
		if a == answerMissing {
			a = answerUnclear
		}
		note("list "+prefix+"/", a, code)
	}
	for _, prefix := range ScopeCheckedPrefixes {
		refuse(prefix, true)
	}
	for _, p := range ScopeReadOnlyPrefixes {
		refuse(p.Prefix, !p.Listed)
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(ScopeSentinelKey)})
	if err == nil {
		_ = out.Body.Close()
	}
	// A missing sentinel cannot tell a reader from a non-reader: every
	// engine writes it at boot, so the round says the read went untested
	// rather than pass a reader as refused.
	if a, code := readAnswer(err); a == answerMissing {
		note("get "+ScopeSentinelKey, answerUnclear, "the sentinel is absent, so the read was not tested")
	} else {
		note("get "+ScopeSentinelKey, a, code)
	}

	switch a, _ := readAnswer(put(CommitPrefix + "/" + ScopeResultsSegment + "/" + orkerID)); reach(a) {
	case answerReached:
		round.Results = ResultsEveryNamespace
	case answerRefused:
		round.Results = ResultsByNamespace
	}

	switch {
	case reached != "":
		round.Outcome, round.Detail = ScopeUnscoped, "reached: "+reached
	case unclear != "":
		round.Outcome, round.Detail = ScopeUnclear, unclear
	case len(leftBehind) > 0:
		// Every control-plane request was refused, but the store kept a
		// probe object the check could not remove: say so, never scoped.
		round.Outcome = ScopeUnclear
	default:
		round.Outcome = ScopeScoped
	}
	if len(leftBehind) > 0 {
		if round.Detail != "" {
			round.Detail += "; "
		}
		round.Detail += "the store kept an object the check could not remove: " + strings.Join(leftBehind, ", ")
	}
	round.Detail = boundText(round.Detail, MaxScopeDetail)
	return round
}

func ensureScopeSentinel(ctx context.Context, client *s3.Client, bucket string) error {
	body := []byte(scopeSentinelBody)
	_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(ScopeSentinelKey),
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*"),
		ContentType: aws.String("text/plain")})
	if err == nil || isPreconditionFailed(err) {
		return nil
	}
	return fmt.Errorf("write the scope check's sentinel %s: %w", probeURI(bucket, ScopeSentinelKey), err)
}

// BoundText is s cut to at most n bytes on a rune boundary, valid UTF-8, so a
// proto string field always encodes it.
func BoundText(s string, n int) string { return boundText(s, n) }

func boundText(s string, n int) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
