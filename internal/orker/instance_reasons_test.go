package orker

import (
	"testing"

	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// TestInstanceEndReasons_AreTheSharedOnes: a reason an event source's instance
// ends with that a task's attempt, the engine's mint or the image policy also
// uses is one reason, spelled once each way.
func TestInstanceEndReasons_AreTheSharedOnes(t *testing.T) {
	for mine, theirs := range map[string]string{
		eventsources.ReasonOrkerStopped:            ReasonOrkerStopped,
		eventsources.ReasonOrkerCordoned:           task.ReasonOrkerCordoned,
		eventsources.ReasonImagePullFailed:         ReasonImagePullFailed,
		eventsources.ReasonImageNotPermitted:       task.ReasonImageNotPermitted,
		eventsources.ReasonContainerStoppedOutside: ReasonContainerStoppedByRuntime,
		eventsources.ReasonSecretNotPermitted:      ReasonSecretNotPermitted,
		eventsources.ReasonNamespaceNotServed:      ReasonNamespaceNotServed,
		eventsources.ReasonMaskCapacityExceeded:    ReasonMaskCapacityExceeded,
		eventsources.ReasonIdentityUnavailable:     identity.ReasonUnavailable,
		eventsources.ReasonIdentityNotAdmitted:     identity.ReasonNotAdmitted,
		eventsources.ReasonGrantNotPermitted:       provider.ReasonGrantNotPermitted,
		eventsources.ReasonCredentialRefused:       provider.ReasonCredentialRefused,
		eventsources.ReasonCredentialRefreshFailed: provider.ReasonCredentialRefreshFailed,
		eventsources.ReasonIdentityRefreshFailed:   ReasonIdentityRefreshFailed,
	} {
		if mine != theirs {
			t.Errorf("%q and %q are two spellings of one reason", mine, theirs)
		}
	}
	for _, code := range []string{ReasonResourceAdmissionMismatch, task.ReasonImageNotSigned, task.ReasonImageSignatureInvalid, ReasonIdentityNotConfigured,
		ReasonIdentityClockSkew, ReasonIdentitySpecSkew} {
		if eventsources.InstanceCause(code) != code {
			t.Errorf("%s is no cause", code)
		}
	}
}
