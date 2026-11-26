package orker

import (
	"context"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/internal/quantity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// instanceFailure is why an attempt of an event source's instance ends: a reason
// of pkg/eventsources' table, the code behind it, and a sentence for the Orker's
// own log and record. Its Detail never reaches a pause.
type instanceFailure struct {
	Reason string
	Cause  string
	Detail string
}

func (f *instanceFailure) Error() string { return f.Reason + ": " + f.Detail }

func instanceFail(reason, format string, args ...any) *instanceFailure {
	return &instanceFailure{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// checkInstancePolicy is the Orker's own check of a spec it was just given, before
// anything is pulled: the image policy the engine selected, what this Orker can
// host, its caps and its secret floor. A refusal is the same on every source host
// of the pool, whose settings are the same.
func (a *Activities) checkInstancePolicy(ctx context.Context, spec *eventsources.ServiceSpec, sel *orkstackv1.ImagePolicySelection) *instanceFailure {
	if sel == nil {
		// None given is not a policy that permits everything.
		return instanceFail(eventsources.ReasonEngineUnavailable, "the engine gave no image policy with its confirmation")
	}
	if f := a.dispatcher.checkInstanceImage(ctx, spec, sel); f != nil {
		return f
	}
	if _, ok := servicehost.Highest(spec.Contracts, servicehost.HostContracts); !ok {
		return instanceFail(eventsources.ReasonServiceContractUnknown, "the image implements contract %v; this Orker hosts %v", spec.Contracts, servicehost.HostContracts)
	}
	if _, ok := servicehost.Highest(spec.Profiles, servicehost.HostProfiles); !ok {
		return instanceFail(eventsources.ReasonServiceProfileUnknown, "the image implements %s %v; this Orker hosts %v",
			eventsources.ProfileEventSource, spec.Profiles, servicehost.HostProfiles)
	}
	request, err := instanceRequest(spec.Resources.CPU, spec.Resources.Memory)
	if err != nil {
		return instanceFail(eventsources.ReasonResourcesUnsatisfiable, "its resources do not read: %v", err)
	}
	ceiling, err := a.sourceHost.ceiling()
	if err != nil {
		return instanceFail(eventsources.ReasonServiceHostUnfit, "this Orker's own largest instance does not read: %v", err)
	}
	if request.exceeds(ceiling) {
		return instanceFail(eventsources.ReasonResourcesUnsatisfiable, "it asks for %s; an instance of this pool may ask for %s (%s, %s)",
			request, ceiling, SourceHostMaxRequestCPUVar, SourceHostMaxRequestMemoryVar)
	}
	if size := servicehost.AnswerBytes(spec.Concurrency, spec.Limits.MaxBodyBytes); size > a.sourceHost.SpoolBytes {
		return instanceFail(eventsources.ReasonSpoolUnsatisfiable, "its answer filesystem for %d lanes of %d bytes a message would be %d bytes; this pool's may be %d (%s)",
			spec.Concurrency, spec.Limits.MaxBodyBytes, size, a.sourceHost.SpoolBytes, SourceHostSpoolBytesVar)
	}
	for _, s := range spec.Secrets {
		if f := a.dispatcher.checkInstanceSecret(s); f != nil {
			return f
		}
	}
	return nil
}

// instanceRequest is what an instance asks for, as the ledger counts it.
func instanceRequest(cpu, memory string) (amount, error) {
	cores, err := quantity.CPU(cpu)
	if err != nil {
		return amount{}, err
	}
	bytes, err := quantity.Memory(memory)
	if err != nil {
		return amount{}, err
	}
	return amountOf(cores, int64(bytes)), nil
}

// checkInstanceImage holds the spec's image, by its digest, to the policy as it is
// now.
func (d *OCIDispatcher) checkInstanceImage(ctx context.Context, spec *eventsources.ServiceSpec, sel *orkstackv1.ImagePolicySelection) *instanceFailure {
	ref, err := dsl.ParseReference(spec.Image)
	if err != nil || ref.Digest == "" {
		return instanceFail(eventsources.ReasonImageNotPermitted, "its image is not named by a digest")
	}
	image := &orkstackv1.ImageRef{Reference: spec.Image, Registry: ref.Registry, Repository: ref.Repository, IndexDigest: ref.Digest, Pinned: ref.Pinned(ref.Digest)}
	what := fmt.Sprintf("event source %q", spec.Source.Name)
	if refusal := task.CheckImage(sel, 0, spec.Source.Name, image); refusal != nil {
		return instanceFail(eventsources.ReasonImageNotPermitted, "%s: %v", what, refusal)
	}
	f := d.checkSignature(ctx, sel, what, spec.Source.Namespace, image)
	if f == nil {
		return nil
	}
	switch f.Reason {
	case ReasonImageSignatureInvalid, task.ReasonImageNotSigned:
		return &instanceFailure{Reason: eventsources.ReasonImageNotPermitted, Cause: f.Reason, Detail: f.Message}
	case eventsources.ReasonImageNotPermitted:
		return &instanceFailure{Reason: f.Reason, Detail: f.Message}
	}
	// Whether it is signed could not be asked: its registry did not answer.
	return &instanceFailure{Reason: eventsources.ReasonImagePullFailed, Detail: f.Message}
}

// checkInstanceSecret holds one declared secret's source to what this Orker
// resolves and may read; it reads no value.
func (d *OCIDispatcher) checkInstanceSecret(s eventsources.SpecSecret) *instanceFailure {
	if !singlePathElement(s.Name) {
		return instanceFail(eventsources.ReasonSecretNotPermitted, "a secret's name is a file's name, which %q cannot be", sourcehost.SafeWord(s.Name, 64))
	}
	scheme, _ := SplitSecretSource(s.Source)
	if !d.Policy.AllowsSecretScheme(scheme) {
		return &instanceFailure{Reason: eventsources.ReasonSecretNotPermitted, Cause: ReasonSecretSchemeForbid,
			Detail: fmt.Sprintf("secret %q uses source scheme %s, which this orker does not resolve", s.Name, dsl.QuoteSecretSource(scheme))}
	}
	if f := d.secretFloorFailure(&orkstackv1.SecretRef{Name: s.Name, Source: s.Source}); f != nil {
		return instanceFail(eventsources.ReasonSecretNotPermitted, "%s", f.Message)
	}
	return nil
}

var _ error = (*instanceFailure)(nil)
