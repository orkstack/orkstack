package eventsources

import (
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Reasons an attempt of an instance ends with that are not a service's own
// (reasons.go). Several are other packages' reasons under the same words, which
// internal/orker's tests hold to them: this package imports neither.
const (
	ReasonOrkerStopped            = "ORKER_STOPPED"
	ReasonOrkerCordoned           = "ORKER_CORDONED"
	ReasonServiceHostUnfit        = "SERVICE_HOST_UNFIT"
	ReasonImagePullFailed         = "IMAGE_PULL_FAILED"
	ReasonCredentialNotReloaded   = "CREDENTIAL_NOT_RELOADED"
	ReasonEngineUnavailable       = "ENGINE_UNAVAILABLE"
	ReasonIdentityUnavailable     = "IDENTITY_UNAVAILABLE"
	ReasonIdentityRefreshFailed   = "IDENTITY_REFRESH_FAILED"
	ReasonCredentialRefreshFailed = "CREDENTIAL_REFRESH_FAILED"
	ReasonCredentialRefused       = "CREDENTIAL_REFUSED"
	ReasonServiceNotReadyInTime   = "SERVICE_NOT_READY_IN_TIME"
	ReasonImageNotPermitted       = "IMAGE_NOT_PERMITTED"
	ReasonSecretNotPermitted      = "SECRET_NOT_PERMITTED"
	ReasonSecretUnresolved        = "SECRET_UNRESOLVED"
	ReasonMaskCapacityExceeded    = "MASK_CAPACITY_EXCEEDED"
	ReasonNamespaceNotServed      = "NAMESPACE_NOT_SERVED"
	ReasonResourcesUnsatisfiable  = "RESOURCES_UNSATISFIABLE"
	ReasonSpoolUnsatisfiable      = "SPOOL_UNSATISFIABLE"
	ReasonSourceRefreshBestEffort = "SOURCE_REFRESH_BEST_EFFORT"
	ReasonGrantNotPermitted       = "GRANT_NOT_PERMITTED"
	ReasonIdentityNotAdmitted     = "IDENTITY_NOT_ADMITTED"
	ReasonContainerStoppedOutside = "CONTAINER_STOPPED_BY_RUNTIME"
)

// Retry is how the cluster tries an attempt again that ended with a reason.
type Retry int

const (
	// RetryNone: the reason is never an attempt's failure, only what its
	// activity completes with.
	RetryNone Retry = iota
	// RetryNow: handed back before it started or evicted; after a second, on
	// another source host.
	RetryNow
	// RetryBackoff: after the activity's back-off.
	RetryBackoff
)

// EndCondition is what a result must carry, beside its reason and class, to
// be what its row says.
type EndCondition int

const (
	// EndAlways: the reason alone.
	EndAlways EndCondition = iota
	// EndPastBound: the refusals have gone on past their bound.
	EndPastBound
	// EndOnTrial: the instance was a replacement a roll was trying, which
	// only the workflow knows.
	EndOnTrial
	// EndFreshRead: the refusal was confirmed by a read made after the
	// request arrived.
	EndFreshRead
	// EndUnread: the source's own spec or record could not be read.
	EndUnread
)

// InstanceEnd is one row of how an attempt of an instance ends
// (docs/EVENT_SOURCES.md). The Orker returns a Retried reason as an error the
// cluster retries, and what Completes as a result; the workflow classifies a
// result by this table and by nothing the Orker says beside it.
type InstanceEnd struct {
	Reason string
	// Class is whose the end is.
	Class orkstackv1.InstanceEndClass
	// Retried is how the cluster retries an attempt that fails with it;
	// InstanceRetry where a cause is behind it.
	Retried Retry
	// Completes is, where Condition holds, how the activity completes instead.
	// A retried failure after the instance had been healthy completes
	// FAILED_AFTER_HEALTHY whatever this says.
	Completes orkstackv1.InstanceEnd
	Condition EndCondition
}

const (
	classHost   = orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_HOST
	classSource = orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE

	endStuck        = orkstackv1.InstanceEnd_INSTANCE_END_STUCK
	endStopped      = orkstackv1.InstanceEnd_INSTANCE_END_STOPPED
	endJudgment     = orkstackv1.InstanceEnd_INSTANCE_END_JUDGMENT_UNAVAILABLE
	endUnchangeable = orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE
)

var instanceEnds = []InstanceEnd{
	{Reason: ReasonOrkerStopped, Class: classHost, Retried: RetryNow},
	{Reason: ReasonOrkerCordoned, Class: classHost, Retried: RetryNow},
	{Reason: ReasonServiceHostUnfit, Class: classHost, Retried: RetryNow},

	{Reason: ReasonImagePullFailed, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonServiceNotReady, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonServiceExited, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonServiceFailed, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonServiceOOMKilled, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonServiceUnresponsive, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonServiceRunsAsRoot, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonContainerStoppedOutside, Class: classHost, Retried: RetryBackoff},
	{Reason: ReasonSecretUnresolved, Class: classHost, Retried: RetryBackoff},
	{Reason: ReasonNamespaceNotServed, Class: classHost, Retried: RetryBackoff},
	{Reason: ReasonCredentialNotReloaded, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonServiceHostStalled, Class: classHost, Retried: RetryBackoff},
	{Reason: ReasonEngineUnavailable, Class: classHost, Retried: RetryBackoff},
	{Reason: ReasonIdentityRefreshFailed, Class: classHost, Retried: RetryBackoff},
	{Reason: ReasonCredentialRefreshFailed, Class: classSource, Retried: RetryBackoff},
	{Reason: ReasonHostNotPermitted, Class: classHost, Retried: RetryBackoff},

	// The class is the retried failure's: as judgment-unavailable it is the
	// source's own spec or record, which its condition says.
	{Reason: ReasonIdentityUnavailable, Class: classHost, Retried: RetryBackoff, Completes: endJudgment, Condition: EndUnread},
	{Reason: ReasonCredentialRefused, Class: classSource, Retried: RetryBackoff, Completes: endUnchangeable, Condition: EndPastBound},

	{Reason: ReasonServiceStopStuck, Class: classHost, Completes: endStuck},
	{Reason: ReasonNotHeld, Class: classHost, Completes: endStopped},
	{Reason: ReasonPaused, Class: classSource, Completes: endStopped},
	{Reason: ReasonDeleting, Class: classSource, Completes: endStopped},
	{Reason: ReasonDeletedDocumentReappeared, Class: classSource, Completes: endStopped},
	{Reason: ReasonDisabled, Class: classHost, Completes: endStopped},

	{Reason: ReasonServiceNotReadyInTime, Class: classSource, Completes: endUnchangeable, Condition: EndOnTrial},
	{Reason: ReasonServiceContractUnknown, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonServiceProfileUnknown, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonServiceContractViolation, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonServiceConfigRejected, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonImageNotPermitted, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonSecretNotPermitted, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonMaskCapacityExceeded, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonResourcesUnsatisfiable, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonSpoolUnsatisfiable, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonSourceRefreshBestEffort, Class: classSource, Completes: endUnchangeable},
	{Reason: ReasonGrantNotPermitted, Class: classSource, Completes: endUnchangeable, Condition: EndFreshRead},
	{Reason: ReasonIdentityNotAdmitted, Class: classSource, Completes: endUnchangeable, Condition: EndFreshRead},
}

// InstanceEnds are the reasons an attempt of an instance ends with.
func InstanceEnds() []InstanceEnd { return append([]InstanceEnd(nil), instanceEnds...) }

// LookupInstanceEnd is reason's row. A reason without one is another release's,
// or no Orker's: nothing is ever decided from it.
func LookupInstanceEnd(reason string) (InstanceEnd, bool) {
	for _, e := range instanceEnds {
		if e.Reason == reason {
			return e, true
		}
	}
	return InstanceEnd{}, false
}

// InstanceRetry is how the cluster tries an attempt again that failed with
// reason, cause behind it. A host unfit for identity can be offered the task
// again, so one it hands back waits the back-off.
func InstanceRetry(reason, cause string) Retry {
	e, _ := LookupInstanceEnd(reason)
	if reason == ReasonServiceHostUnfit && identityUnfit[cause] {
		return RetryBackoff
	}
	return e.Retried
}

// EndFacts is the workflow's own account of a slot whose attempt ended.
type EndFacts struct {
	// OnTrial: it held a replacement a roll was trying.
	OnTrial bool
	// Now is the workflow's clock, and ProviderRefusedMax how long a provider
	// may refuse a credential before that is the source's fault, as pinned.
	Now                time.Time
	ProviderRefusedMax time.Duration
}

// EndIsUnchangeable reports that an attempt's result is one that trying again
// cannot change, which pauses its source or fails its roll: its reason's row
// says so, its class and end are the row's, and the row's condition holds.
func EndIsUnchangeable(res *orkstackv1.HostEventSourceResult, facts EndFacts) bool {
	e, ok := LookupInstanceEnd(res.GetReason())
	if !ok || e.Completes != endUnchangeable || res.GetEnd() != e.Completes || res.GetClass() != e.Class {
		return false
	}
	switch e.Condition {
	case EndAlways:
		return true
	case EndPastBound:
		since := res.GetRefusedSince()
		return res.GetPastBound() && since.IsValid() && !facts.Now.Before(since.AsTime().Add(facts.ProviderRefusedMax))
	case EndOnTrial:
		return facts.OnTrial
	case EndFreshRead:
		return res.GetFreshRead()
	}
	return false
}

// identityUnfit is what an Orker unfit for identity says it is. Its store is
// ORKER_STORE_UNSCOPED; IDENTITY_STORE_UNSCOPED is the engine's refusal of a
// submission for it.
var identityUnfit = map[string]bool{
	"IDENTITY_NOT_CONFIGURED": true, "IDENTITY_CLOCK_SKEW": true, "ORKER_STORE_UNSCOPED": true, "IDENTITY_SPEC_SKEW": true,
}

// instanceCauses are the codes that may stand behind a reason: a check's, an
// identity's, the image policy's rule.
var instanceCauses = map[string]bool{
	"CONFIG_INVALID": true, "NOT_AUTHORIZED": true, "UNREACHABLE": true,
	"OBJECT_MISSING": true, "OBJECT_INVALID": true, "NO_DEAD_LETTER": true, "MESSAGE_LIMIT": true,
	"SECRET_SCHEME_FORBIDDEN": true,
	// The spec asks for other resources than its activity reserved.
	"RESOURCE_ADMISSION_MISMATCH": true,
	"IMAGE_NOT_SIGNED":            true, "IMAGE_SIGNATURE_INVALID": true,
}

// InstanceCause is code where it is one that may stand behind a reason, and
// nothing otherwise: a code may be an image's word, and no text of an image's
// is a pause's.
func InstanceCause(code string) string {
	if instanceCauses[code] || identityUnfit[code] {
		return code
	}
	return ""
}
