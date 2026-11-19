package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The object the API keeps for each schedule (artifacts.ScheduleBindingPrefix;
// docs/ORKFLOW_SCHEDULES.md, "Who a schedule runs as"): who bound it and the
// consent they gave, beside each submission the API accepted. It is a record:
// nothing here reads it to decide a fire, and the cluster's schedule is never
// changed because of it. A save writes it in three steps around the
// cluster's own write: (1) the save's entry is added; (2) the cluster's
// schedule is created or updated; (3) the set is trimmed to the save's entry
// and the one equal to what the cluster then holds. A save whose cluster
// write fails leaves an entry that submits nothing the cluster holds; the
// next save's trim removes it.

// scheduleRecordDetachedTimeout bounds a record write that runs on after
// its caller has gone: a trim, a drop, a restore.
const scheduleRecordDetachedTimeout = 30 * time.Second

// ScheduleBindingStore keeps the objects; *artifacts.ScheduleBindingStore in
// `ork`.
type ScheduleBindingStore interface {
	Get(ctx context.Context, namespace, id string) (artifacts.ScheduleBindings, error)
	Update(ctx context.Context, namespace, id string, change func(*artifacts.ScheduleBindings) error) (artifacts.ScheduleBindings, error)
	Delete(ctx context.Context, namespace, id string) error
	DeleteNamespace(ctx context.Context, namespace string) error
}

// scheduleSubmission is what rec submits: its Orkflow, its version as
// written, and the digest of its input.
func scheduleSubmission(rec *orkstackv1.ScheduleRecord) (artifacts.ScheduleBinding, error) {
	sum, err := scheduleInputSHA256(rec.GetInput())
	if err != nil {
		return artifacts.ScheduleBinding{}, err
	}
	return artifacts.ScheduleBinding{OrkflowName: rec.GetOrkflowName(), OrkflowVersion: rec.GetOrkflowVersion(), InputSHA256: sum}, nil
}

// scheduleInputSHA256 digests an input as its canonical JSON (null when
// there is none), so the same input always digests alike.
func scheduleInputSHA256(in *orkstackv1.Payload) (string, error) {
	var value any
	if in.GetData() != nil {
		value = in.GetData().AsMap()
	}
	b, err := stablejson.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("digest the schedule's input: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// boundBy makes the caller the binder of entry, with the consent the save
// decided (decideConsent). An asserted caller is kept as asserted, and an
// empty one leaves no binder.
func (s *Service) boundBy(entry artifacts.ScheduleBinding, who authn.RecordedPrincipal, granted *artifacts.BinderBinding, c consentRecord) artifacts.ScheduleBinding {
	now := s.now().UTC()
	entry.Binder, entry.BinderVerified, entry.BinderMethod = recordedBinder(who.ID), who.Verified && who.ID != "", ""
	entry.BinderBinding = nil
	if entry.BinderVerified {
		entry.BinderMethod = recordedBinder(who.Method)
		entry.BinderBinding = granted
	}
	entry.BoundAt, entry.SavedAt = now, now
	return withScheduleConsent(entry, c)
}

// withScheduleConsent records c in entry.
func withScheduleConsent(entry artifacts.ScheduleBinding, c consentRecord) artifacts.ScheduleBinding {
	entry.Consent, entry.ConsentAt, entry.ConsentBy = c.Digest, c.At.UTC(), c.By
	entry.ConsentAudiences, entry.ConsentGrants = c.Audiences, c.Grants
	return entry
}

// scheduleConsentOf is the consent entry records.
func scheduleConsentOf(entry artifacts.ScheduleBinding) consentRecord {
	return consentRecord{Digest: entry.Consent, At: entry.ConsentAt, By: entry.ConsentBy,
		Audiences: entry.ConsentAudiences, Grants: entry.ConsentGrants}
}

// recordedBinder is a principal as a record keeps it: valid UTF-8, no
// control or format character, and bounded, as an origin records one. An
// asserted header is whatever a gateway sent.
func recordedBinder(id string) string {
	id = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(id, "\uFFFD"))
	return boundOriginAssertion(id)
}

// keptBinder is entry with the binder and consent of the object's entry that
// submits what was, when a save changes nothing it submits: a re-apply, a
// new description or new times rebinds nothing. A schedule saved before
// this release has no such entry, and stays unbound until it is rebound.
func (s *Service) keptBinder(entry, was artifacts.ScheduleBinding, doc artifacts.ScheduleBindings) artifacts.ScheduleBinding {
	if prior, ok := doc.Find(was); ok {
		entry.Binder, entry.BinderVerified, entry.BinderMethod = prior.Binder, prior.BinderVerified, prior.BinderMethod
		entry.BoundAt, entry.BinderBinding = prior.BoundAt, prior.BinderBinding
		entry = withScheduleConsent(entry, scheduleConsentOf(prior))
	}
	entry.SavedAt = s.now().UTC()
	return entry
}

// addScheduleBinding is a save's step 1. A store that cannot take the entry
// refuses the save before the cluster is asked, so the cluster never holds a
// submission the API did not record.
func (s *Service) addScheduleBinding(ctx context.Context, ns, id string, entry artifacts.ScheduleBinding) error {
	_, err := s.addScheduleBindingFrom(ctx, ns, id, func(artifacts.ScheduleBindings) artifacts.ScheduleBinding { return entry })
	return err
}

// addScheduleBindingFrom is step 1 with the entry built from the object as
// the write reads it, so a binder kept from an earlier entry is read inside
// the same conditional write that adds the new one.
func (s *Service) addScheduleBindingFrom(ctx context.Context, ns, id string, build func(artifacts.ScheduleBindings) artifacts.ScheduleBinding) (artifacts.ScheduleBinding, error) {
	var entry artifacts.ScheduleBinding
	if s.scheduleBindings == nil {
		return build(artifacts.ScheduleBindings{}), nil
	}
	_, err := s.scheduleBindings.Update(ctx, ns, id, func(doc *artifacts.ScheduleBindings) error {
		entry = build(*doc)
		doc.Add(entry)
		return nil
	})
	return entry, scheduleBindingError(ctx, err)
}

// dropScheduleBinding removes the entry a save added, when the cluster
// definitely refused that save: an entry left for a submission the cluster
// never held could, at the cap, push out the one it does hold. Best effort,
// and only that exact entry.
func (s *Service) dropScheduleBinding(ctx context.Context, ns, id string, entry artifacts.ScheduleBinding) {
	if s.scheduleBindings == nil {
		return
	}
	detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), scheduleRecordDetachedTimeout)
	defer cancel()
	_, err := s.scheduleBindings.Update(detached, ns, id, func(doc *artifacts.ScheduleBindings) error {
		doc.Entries = slices.DeleteFunc(doc.Entries, func(e artifacts.ScheduleBinding) bool {
			return e.Submits(entry) && e.SavedAt.Equal(entry.SavedAt) && e.Binder == entry.Binder
		})
		return nil
	})
	if err != nil {
		s.logger.Warn("a refused schedule save left its entry in the schedule's record; the next save trims it",
			slog.String("namespace", ns), slog.String("schedule_id", id), slog.String("error", err.Error()))
	}
}

// trimScheduleBindings is a save's step 3: the set keeps the save's entry
// and the one equal to what the cluster holds now. When the cluster holds
// this save's submission and another save's trim removed its entry in the
// meantime, the entry is put back: the cluster never holds a submission the
// record has no entry for. The cluster has already accepted the save, so a
// trim that fails is logged, not answered, and runs to its end even when the
// caller has gone: the next save trims again.
func (s *Service) trimScheduleBindings(ctx context.Context, ns string, view engine.ScheduleView, entry artifacts.ScheduleBinding) {
	if s.scheduleBindings == nil || view.Record == nil {
		return
	}
	held, err := scheduleSubmission(view.Record)
	if err == nil {
		detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), scheduleRecordDetachedTimeout)
		defer cancel()
		_, err = s.scheduleBindings.Update(detached, ns, view.ID, func(doc *artifacts.ScheduleBindings) error {
			doc.Keep(held, entry)
			if _, ok := doc.Find(held); !ok && entry.Submits(held) {
				doc.Add(entry)
			}
			return nil
		})
	}
	if err != nil {
		s.logger.Warn("the schedule was saved, but its binding record was not trimmed; the next save trims it",
			slog.String("namespace", ns), slog.String("schedule", view.Name), slog.String("error", err.Error()))
	}
}

// rebindScheduleRecord is a --rebind's step 3, once the cluster holds the
// save: the caller's entry replaces the one for what the cluster holds. Its
// failure is answered, not logged, because the save asked for exactly this.
func (s *Service) rebindScheduleRecord(ctx context.Context, ns string, view engine.ScheduleView, entry artifacts.ScheduleBinding) error {
	if s.scheduleBindings == nil || view.Record == nil {
		return nil
	}
	held, err := scheduleSubmission(view.Record)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if !entry.Submits(held) {
		// Another save changed what the cluster holds meanwhile; it bound.
		return connect.NewError(connect.CodeAborted, fmt.Errorf("the schedule was changed by someone else while you rebound it; rebind again"))
	}
	_, err = s.scheduleBindings.Update(ctx, ns, view.ID, func(doc *artifacts.ScheduleBindings) error {
		doc.Add(entry)
		doc.Keep(held)
		return nil
	})
	if err != nil {
		return connect.NewError(clusterErrorCode(ctx, err, connect.CodeUnavailable),
			fmt.Errorf("the schedule is saved, but you were not recorded as its binder; rebind again: %w", err))
	}
	return nil
}

// priorScheduleConsent is the consent recorded for the submission was, read
// only to tell a refused consent what changed since. It is best effort: an
// unreadable object makes the refusal say what is asked for, not what
// changed, and decides nothing.
func (s *Service) priorScheduleConsent(ctx context.Context, ns, id string, was artifacts.ScheduleBinding) consentRecord {
	if s.scheduleBindings == nil {
		return consentRecord{}
	}
	doc, err := s.scheduleBindings.Get(ctx, ns, id)
	if err != nil {
		return consentRecord{}
	}
	if prior, ok := doc.Find(was); ok {
		return scheduleConsentOf(prior)
	}
	return consentRecord{}
}

// scheduleConsent fills a schedule's consent, what it asks for and whether
// that consent is stale: the consent recorded with the entry that submits
// what the cluster holds, and consent projection 1 of the version the
// schedule follows now, read once per version through memo (nil reads every
// time). Nothing is set unless both could be read, so a reader can tell
// "could not tell" from "none recorded".
func (s *Service) scheduleConsent(ctx context.Context, memo asksMemo, view engine.ScheduleView, out *orkstackv1.Schedule) {
	if view.Record == nil || out.GetUnavailableReason() != "" {
		return
	}
	asks, err := s.asksForMemo(ctx, memo, view.Namespace, view.Record.GetOrkflowName(), view.Record.GetOrkflowVersion())
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("what a schedule's Orkflow asks for could not be read; its consent is not shown",
				slog.String("namespace", view.Namespace), slog.String("schedule", view.Name), slog.String("error", err.Error()))
		}
		return
	}
	var consented *orkstackv1.ConsentView
	if s.scheduleBindings != nil {
		held, err := scheduleSubmission(view.Record)
		if err != nil {
			return
		}
		doc, err := s.scheduleBindings.Get(ctx, view.Namespace, view.ID)
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warn("a schedule's binding record could not be read; its consent is not shown",
					slog.String("namespace", view.Namespace), slog.String("schedule", view.Name), slog.String("error", err.Error()))
			}
			return
		}
		if entry, ok := doc.Find(held); ok {
			consented = scheduleConsentOf(entry).view()
		}
	}
	out.AsksFor = &orkstackv1.AsksFor{Digest: asks.Digest, Audiences: asks.Audiences, Grants: asks.Grants}
	out.Consented = consented
	out.ConsentStale = consentStale(consented, out.GetAsksFor())
}

// consentStale reports a consent that is not to what the Orkflow asks for
// now. Either one unknown is not stale: it is not shown.
func consentStale(given *orkstackv1.ConsentView, asks *orkstackv1.AsksFor) bool {
	return given.GetDigest() != "" && asks.GetDigest() != "" && given.GetDigest() != asks.GetDigest()
}

// scheduleProtoWith maps view and fills its consent.
func (s *Service) scheduleProtoWith(ctx context.Context, view engine.ScheduleView) *orkstackv1.Schedule {
	return s.scheduleProtoMemo(ctx, nil, view)
}

// scheduleProtoMemo is scheduleProtoWith reading each Orkflow version once
// through memo.
func (s *Service) scheduleProtoMemo(ctx context.Context, memo asksMemo, view engine.ScheduleView) *orkstackv1.Schedule {
	out := scheduleProto(view)
	s.scheduleConsent(ctx, memo, view, out)
	return out
}

func scheduleBindingError(ctx context.Context, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, artifacts.ErrScheduleBindingContended):
		return connect.NewError(connect.CodeAborted, err)
	}
	return connect.NewError(clusterErrorCode(ctx, err, connect.CodeUnavailable),
		fmt.Errorf("the schedule's record could not be written, so the schedule was not changed; the record itself may have been: %w", err))
}

// scheduleRefused reports a create the cluster definitely did not make: a
// name already taken, or a spec it refused. A lost answer or an outage is
// not one: the schedule may exist, so its record stays.
func scheduleRefused(err error) bool {
	return errors.Is(err, engine.ErrScheduleExists) || errors.Is(err, engine.ErrScheduleSpecRefused) ||
		errors.Is(err, engine.ErrOverlapPolicyUnknown) || errors.Is(err, engine.ErrScheduleNotFound)
}

// binderBindingFor is the binding that grants the caller run.submit on
// Orkflow orkflow of namespace ns, recorded with a verified binder so that
// later admissions judge the binder by it (workload-identity §3.2). nil,
// with no error, where no access document decides (asserted mode, or no
// access control), or where none grants: that binder's runs are then
// refused an identity, IDENTITY_STARTER_NOT_RECORDED. In enforce mode an
// access document that cannot be read refuses the save, unavailable, so a
// save that looked fine never leaves a schedule that cannot start its runs;
// in observe mode, which changes no answer, the binder is recorded with no
// binding and logged.
func (s *Service) binderBindingFor(ctx context.Context, ns, orkflow string) (*artifacts.BinderBinding, error) {
	if !s.access.enabled() || authn.ModeFrom(ctx) == authn.ModeAsserted {
		return nil, nil
	}
	s.access.init()
	// Only enforce refuses a save for it: observe never changes an answer,
	// so there the binder is recorded with no binding, and logged.
	unreadable := func(err error) (*artifacts.BinderBinding, error) {
		if !s.access.enforcing() {
			s.logger.Warn("the binding that lets a binder bind could not be read; it is recorded with none", "namespace", ns, "orkflow", orkflow, "error", err)
			return nil, nil
		}
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the binding that lets you bind could not be read; save again: %w", err))
	}
	policy, unavailable := s.access.policy(ctx)
	if unavailable {
		return unreadable(errors.New("the access document could not be read"))
	}
	res, err := s.withIDs(ctx, policy, authz.Resource{Namespace: artifacts.ResolveNamespace(ns), Orkflow: orkflow})
	if err != nil {
		return unreadable(err)
	}
	b, ok := policy.GrantingBinding(subjectOf(authn.FromContext(ctx)), authz.PermRunSubmit, res)
	if !ok {
		return nil, nil
	}
	return &artifacts.BinderBinding{Name: b.Name, Selector: b.Selector, Role: b.Role, Scope: b.Scope}, nil
}
