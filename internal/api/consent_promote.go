package api

import (
	"context"
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// promotionStaleConsent lists the schedules, triggers and event sources of
// namespace ns that follow Orkflow name's current pointer and whose consent
// does not cover what the version now current asks for: their runs are
// refused an identity until someone consents (workload-identity §3.2,
// "Promotion says what it will break"). A version that asks for nothing
// breaks nothing. unchecked says what could not be read or listed, and is
// returned to the caller, so the promotion never passes over it in silence.
func (s *Service) promotionStaleConsent(ctx context.Context, ns, name string) (stale []*orkstackv1.StaleConsent, unchecked string) {
	asks, err := s.asksFor(ctx, ns, name, "")
	if err != nil {
		return nil, fmt.Sprintf("what the version now current asks for could not be read: %v", err)
	}
	if asks.Empty() {
		return nil, ""
	}
	var missed []string
	add := func(kind, object string, given *orkstackv1.ConsentView) {
		if given.GetDigest() != asks.Digest {
			stale = append(stale, &orkstackv1.StaleConsent{Kind: kind, Name: object, Digest: asks.Digest})
		}
	}
	if s.schedules != nil {
		views, err := s.schedules.List(ctx, ns, nil)
		if err != nil {
			missed = append(missed, fmt.Sprintf("the schedules could not be listed: %v", err))
		}
		memo := asksMemo{}
		unread := 0
		for _, view := range views {
			if view.Record == nil {
				unread++ // the cluster's record could not be read: what it follows is unknown
				continue
			}
			if view.Record.GetOrkflowName() != name || view.Record.GetOrkflowVersion() != "" {
				continue
			}
			row := s.scheduleProtoMemo(ctx, memo, view)
			if row.GetAsksFor() == nil {
				unread++ // its consent could not be read: neither stale nor not
				continue
			}
			add("schedule", view.Name, row.GetConsented())
		}
		if unread > 0 {
			missed = append(missed, fmt.Sprintf("%d schedule(s) could not be read", unread))
		}
	}
	if s.triggers != nil {
		list, err := s.triggers.List(ctx, ns, name)
		if err != nil {
			missed = append(missed, fmt.Sprintf("the triggers could not be listed: %v", err))
		}
		for _, t := range list {
			if t.OrkflowVersion == "" {
				label := t.Name
				if label == "" {
					label = t.ID
				}
				add("trigger", label, triggerConsentOf(t.Binding).view())
			}
		}
	}
	if store, err := s.sourceStore(); err == nil {
		list, skipped, err := store.List(ctx, ns)
		switch {
		case err != nil:
			missed = append(missed, fmt.Sprintf("the event sources could not be listed: %v", err))
		case len(skipped) > 0:
			missed = append(missed, fmt.Sprintf("%d event source(s) could not be read", len(skipped)))
		}
		for _, src := range list {
			if src.GetOrkflowName() == name && src.GetOrkflowVersion() == "" {
				add("event-source", src.GetName(), src.GetConsented())
			}
		}
	} else if s.eventSourceSettings.Enabled {
		missed = append(missed, fmt.Sprintf("the event sources could not be listed: %v", err))
	}
	return stale, strings.Join(missed, "; ")
}
