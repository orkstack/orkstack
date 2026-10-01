package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The image supply-chain policy at the engine's two doors (apply/validate
// and submit). The rule is internal/task.CheckImage; the signature is
// internal/dsl.SignatureVerifier. This file composes them with the stored
// floor and the namespace and maps each outcome onto the code the caller can
// act on.

// imagePolicy reads the deployment floor. A store that holds no document
// answers the EMPTY policy — permit everything — because that is what the
// deployment did before the store existed; a store that cannot be read is
// Unavailable, never "no policy". A nil store (Config.ImagePolicies unset)
// is the unit-test configuration and also answers the empty policy, on the
// terms Config.Resolver nil "applies without pinning".
func (s *Service) imagePolicy(ctx context.Context) (*orkstackv1.ImagePolicy, error) {
	if s.imagePolicies == nil {
		return &orkstackv1.ImagePolicy{}, nil
	}
	policy, err := s.imagePolicies.Get(ctx)
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return &orkstackv1.ImagePolicy{}, nil
		}
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("the image policy could not be read, so no image can be judged against it: %w", err))
	}
	return policy, nil
}

// imagePolicySelection composes the policy in force for a namespace
// (task.Selection): nil when there is none.
func (s *Service) imagePolicySelection(ctx context.Context, ns *orkstackv1.Namespace) (*orkstackv1.ImagePolicySelection, error) {
	policy, err := s.imagePolicy(ctx)
	if err != nil {
		return nil, err
	}
	sel, err := task.Selection(policy, ns)
	if err != nil {
		// A namespace naming a key the policy no longer holds. The API
		// refuses the writes that would produce it; a document written by
		// another release can still arrive here, and it is not the caller's
		// document to fix.
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return sel, nil
}

// checkImagePolicy holds every task's RESOLVED image to the selection: the
// pure half (task.CheckOrkflow) first, then signature verification of every
// DISTINCT pinned reference when the selection names a signer. The dedupe
// key is ImageRef.pinned ("{registry}/{repository}@sha256:<hex>"), exactly
// what a cosign signature is attached to: twelve `uses: shell` tasks are one
// image and one verification. It is NOT ResolveOrkflow's key, which memoizes
// by the raw `uses:` string (resolved[uses]): a catalog name and the
// registry reference it maps to are two resolutions there and one
// verification here, and one digest in two repositories is two
// verifications here, because cosign stores the .sig per repository. Tasks
// are walked in document order and the map is filled as they go, so the
// field an image's failure anchors to is its first task's. A nil selection
// returns nil without looking at a task. Errors:
//   - *task.ImagePolicyRefusal → connect InvalidArgument wrapped in
//     validationFieldError{Field: refusal.Field()} so ValidateOrkflow
//     anchors the issue;
//   - *dsl.SignatureError with IMAGE_NOT_SIGNED / IMAGE_SIGNATURE_INVALID →
//     InvalidArgument, field-anchored the same way (tasks[i].uses of the
//     first task using that image);
//   - IMAGE_SIGNATURE_UNAVAILABLE → Unavailable, the code applyResolveError
//     gives a registry that cannot be reached: nothing about the document is
//     wrong;
//   - a selection naming a signer while Config.Signatures is nil →
//     FailedPrecondition "this engine has no signature verifier
//     configured".
func (s *Service) checkImagePolicy(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	sel *orkstackv1.ImagePolicySelection,
	wf *orkstackv1.Orkflow,
) error {
	if sel == nil {
		return nil
	}
	if err := task.CheckOrkflow(sel, wf); err != nil {
		var refusal *task.ImagePolicyRefusal
		if errors.As(err, &refusal) {
			return connect.NewError(connect.CodeInvalidArgument,
				&validationFieldError{Field: refusal.Field(), Err: err})
		}
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	signer := sel.GetRequireSignedBy()
	if signer == "" {
		return nil
	}
	if s.signatures == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q requires %s, but this engine has no signature verifier configured",
			ns.GetName(), signer))
	}
	verified := map[string]bool{}
	for i, t := range wf.GetTasks() {
		image := t.GetImage()
		if image == nil || verified[image.GetPinned()] {
			continue
		}
		verified[image.GetPinned()] = true
		err := s.signatures.Verify(ctx, image, signer, sel.GetSigningKeyPem())
		if err == nil {
			continue
		}
		var sig *dsl.SignatureError
		if !errors.As(err, &sig) {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("task %q: %w", t.GetId(), err))
		}
		sentence := fmt.Errorf("task %q: %s; namespace %q requires %s", t.GetId(), sig.Error(), ns.GetName(), signer)
		switch sig.Code {
		case task.ReasonImageSignatureUnavailable:
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", sig.Code, sentence))
		case task.ReasonImageNotPermitted:
			// The engine's own selection is unusable: not the document's fault.
			return connect.NewError(connect.CodeFailedPrecondition, sentence)
		default:
			return connect.NewError(connect.CodeInvalidArgument,
				&validationFieldError{Field: fmt.Sprintf("tasks[%d].uses", i), Err: sentence})
		}
	}
	return nil
}

// submitPolicyError maps the InvalidArgument of a policy refusal to
// FailedPrecondition and leaves Unavailable and FailedPrecondition as they
// are: at submit the document is not what the caller can fix — it is a
// stored, immutable version that the deployment's policy now forbids — and
// FailedPrecondition is the code the admission hold and
// docs/ORKFLOW_TRIGGERS.md's 412 row already give "the deployment refuses
// this right now". The sentence is the same one.
func submitPolicyError(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeInvalidArgument {
		return connect.NewError(connect.CodeFailedPrecondition, connectErr.Unwrap())
	}
	return err
}

// validNamespaceSigner holds require_signed_by to the deployment: the
// document rule (task.ParseSigner) is the store's; that the NAMED KEY EXISTS
// in ImagePolicy.signing_keys is this engine's, because only it holds the
// policy store. The sentence names the known keys:
//
//	namespace "prod-payments" require_signed_by "cosign:acme-release" names a
//	signing key this deployment does not hold; known keys: acme-staging
//	(orkctl admin image-policy set --signing-key)
//
// A nil policy store with a non-empty require_signed_by is Unimplemented,
// as validNamespaceRetention answers with no retention controller.
func (s *Service) validNamespaceSigner(ctx context.Context, spec *orkstackv1.Namespace) error {
	signer := spec.GetRequireSignedBy()
	if signer == "" {
		return nil
	}
	_, name, err := task.ParseSigner(signer)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("namespace %q %w", spec.GetName(), err))
	}
	if s.imagePolicies == nil {
		return connect.NewError(connect.CodeUnimplemented, fmt.Errorf(
			"namespace %q require_signed_by %q cannot be checked: this engine has no image policy store configured",
			spec.GetName(), signer))
	}
	policy, err := s.imagePolicy(ctx)
	if err != nil {
		return err
	}
	if _, ok := policy.GetSigningKeys()[name]; !ok {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"namespace %q require_signed_by %q names a signing key this deployment does not hold; "+
				"known keys: %s (orkctl admin image-policy set --signing-key)",
			spec.GetName(), signer, task.KnownSigningKeys(policy)))
	}
	return nil
}

// imagePolicyStore resolves the store for the two RPCs.
func (s *Service) imagePolicyStore() (artifacts.ImagePolicyStore, error) {
	if s.imagePolicies == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("this engine has no image policy store configured"))
	}
	return s.imagePolicies, nil
}

// GetImagePolicy reads the deployment's image supply-chain floor. A
// deployment that never wrote one answers the EMPTY policy with updated_at
// unset: a real answer (everything is permitted), not a missing document.
func (s *Service) GetImagePolicy(
	ctx context.Context,
	_ *connect.Request[orkstackv1.GetImagePolicyRequest],
) (*connect.Response[orkstackv1.GetImagePolicyResponse], error) {
	store, err := s.imagePolicyStore()
	if err != nil {
		return nil, err
	}
	policy, err := store.Get(ctx)
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return connect.NewResponse(&orkstackv1.GetImagePolicyResponse{Policy: &orkstackv1.ImagePolicy{}}), nil
		}
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&orkstackv1.GetImagePolicyResponse{Policy: policy}), nil
}

// UpdateImagePolicy replaces the floor, whole. In order, all before any
// write: every entry and key is validated (InvalidArgument naming it); a
// registry list that excludes a catalog activity is FailedPrecondition naming
// the activities; removing a key a namespace still names is
// FailedPrecondition naming the namespaces. Stored definitions are untouched;
// they are re-checked at their next submit.
func (s *Service) UpdateImagePolicy(
	ctx context.Context,
	req *connect.Request[orkstackv1.UpdateImagePolicyRequest],
) (*connect.Response[orkstackv1.UpdateImagePolicyResponse], error) {
	store, err := s.imagePolicyStore()
	if err != nil {
		return nil, err
	}
	policy := req.Msg.GetPolicy()
	if policy == nil {
		policy = &orkstackv1.ImagePolicy{}
	}
	// 1. The document's own rule.
	if err := task.ValidRegistryPrefixes(policy.GetAllowedRegistries()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := task.ValidSigningKeys(policy.GetSigningKeys()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// 2. The catalog must stay runnable. require_digest is NOT applied to it:
	// catalog names are exempt from require_digest (task.CheckImage).
	if len(policy.GetAllowedRegistries()) > 0 {
		catalog := s.activityCatalog()
		var excluded []string
		for _, name := range catalog.Names() {
			reference := catalog[name]
			parsed, err := dsl.ParseReference(reference)
			if err != nil {
				excluded = append(excluded, fmt.Sprintf("%s (%s: %v)", name, reference, err))
				continue
			}
			if !task.RegistryAllowed(policy.GetAllowedRegistries(), parsed.Registry, parsed.Repository) {
				excluded = append(excluded, fmt.Sprintf("%s (%s/%s)", name, parsed.Registry, parsed.Repository))
			}
		}
		if len(excluded) > 0 {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"allowed registries exclude catalog activities %s; widen the list or change ORK_ACTIVITY_CATALOG first",
				strings.Join(excluded, ", ")))
		}
	}
	// 3. A key a namespace still names cannot go.
	if s.namespaces != nil {
		docs, err := s.namespaces.List(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("the image policy's signing keys cannot be checked against every namespace: %w", err))
		}
		stillNamed := map[string][]string{}
		for _, ns := range docs {
			_, name, err := task.ParseSigner(ns.GetRequireSignedBy())
			if err != nil || name == "" {
				continue
			}
			if _, kept := policy.GetSigningKeys()[name]; !kept {
				stillNamed[name] = append(stillNamed[name], ns.GetName())
			}
		}
		if len(stillNamed) > 0 {
			keys := make([]string, 0, len(stillNamed))
			for key := range stillNamed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			key := keys[0]
			namespaces := stillNamed[key]
			sort.Strings(namespaces)
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"signing key %q is still required by namespace(s) %s; clear their require_signed_by first",
				key, strings.Join(namespaces, ", ")))
		}
	}
	// 4. Write, whole.
	stored, err := store.Put(ctx, policy, artifacts.PutConditions{})
	if err != nil {
		if errors.Is(err, artifacts.ErrInvalidNamespace) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return nil, connect.NewError(retentionWriteErrorCode(err),
			fmt.Errorf("the image policy could not be written: %w", err))
	}
	return connect.NewResponse(&orkstackv1.UpdateImagePolicyResponse{Policy: stored}), nil
}

// imagePolicySummary renders a policy for an audit record: the flags, the
// registries, and each key's FINGERPRINT — never its PEM.
func imagePolicySummary(policy *orkstackv1.ImagePolicy) string {
	names := make([]string, 0, len(policy.GetSigningKeys()))
	for name := range policy.GetSigningKeys() {
		names = append(names, name)
	}
	sort.Strings(names)
	keys := make([]string, 0, len(names))
	for _, name := range names {
		fingerprint := task.SigningKeyPEMFingerprint(policy.GetSigningKeys()[name])
		if fingerprint == "" {
			fingerprint = "unparseable"
		}
		keys = append(keys, name+"="+fingerprint)
	}
	return fmt.Sprintf("require_digest=%t allowed_registries=[%s] signing_keys={%s}",
		policy.GetRequireDigest(), strings.Join(policy.GetAllowedRegistries(), " "), strings.Join(keys, " "))
}
