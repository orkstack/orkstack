package dsl

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A task's identities (docs/DSL.md, "Task identity"): each a token for one
// audience, minted for each attempt. Rules 53, 54 and 55 are held here and
// in the awaited, sleeping and composed tasks' own lists.

// taskIdentityAvailable is whether this build runs task identity. Until the
// engine admits runs that hold one, a task that declares an identity is
// refused, so no definition is stored that a run could not honour.
//
// It stays false until a run that holds one is safe end to end: the
// Orker trusting only engine-recorded results (P14r, landed with it) is
// not enough while an awaited or a composed upstream of such a run fails
// closed for want of its own record (workload-identity §3.6). It is flipped
// by the change that lands P13d, P13e and P14v together, never earlier.
var taskIdentityAvailable = false

// TaskIdentityAvailable is whether this build runs task identity, for what a
// build that does not should not show.
func TaskIdentityAvailable() bool { return taskIdentityAvailable }

// MaxIdentitiesPerTask is how many identities one task may declare.
const MaxIdentitiesPerTask = 8

// MinIdentityTTLSeconds is the shortest life a task may ask for (rule 59);
// the deployment's identity.token.min_ttl may hold it higher.
const MinIdentityTTLSeconds = 600

const maxAudienceBytes = 255

var identityNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// identityDocument is one entry of a task's identities.
type identityDocument struct {
	Name       string `yaml:"name"`
	Audience   string `yaml:"audience"`
	TTLSeconds *int32 `yaml:"ttl_seconds"`
	// Refresh is required (absent) or best_effort.
	Refresh string `yaml:"refresh"`
}

var refreshWords = map[string]orkstackv1.RefreshPolicy{
	"":            orkstackv1.RefreshPolicy_REFRESH_POLICY_REQUIRED,
	"required":    orkstackv1.RefreshPolicy_REFRESH_POLICY_REQUIRED,
	"best_effort": orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT,
}

// compileIdentities maps a task's identities to the proto.
func compileIdentities(taskIndex int, td taskDocument) ([]*orkstackv1.IdentityRef, error) {
	var out []*orkstackv1.IdentityRef
	for i, d := range td.Identities {
		refresh, ok := refreshWords[d.Refresh]
		if !ok {
			return nil, validationError(fmt.Sprintf("tasks[%d].identities[%d].refresh", taskIndex, i), fmt.Errorf(
				"task %q: identity %q's refresh %q is required or best_effort", td.ID, d.Name, d.Refresh))
		}
		ref := &orkstackv1.IdentityRef{Name: d.Name, Audience: d.Audience, Refresh: refresh}
		if d.TTLSeconds != nil {
			ref.TtlSeconds = *d.TTLSeconds
		}
		out = append(out, ref)
	}
	return out, nil
}

// validateIdentities holds a container task's identities to rules 59 and
// 61, then refuses them while this build has no task identity.
func validateIdentities(taskIndex int, task *orkstackv1.Task) error {
	ids := task.GetIdentities()
	if len(ids) == 0 {
		return nil
	}
	at := func(i int, field string) string {
		return fmt.Sprintf("tasks[%d].identities[%d].%s", taskIndex, i, field)
	}
	if len(ids) > MaxIdentitiesPerTask {
		return validationError(fmt.Sprintf("tasks[%d].identities", taskIndex), fmt.Errorf(
			"task %q declares %d identities; at most %d", task.GetId(), len(ids), MaxIdentitiesPerTask))
	}
	// Rule 59: each identity's name, audience, life and refresh policy.
	seen := map[string]bool{}
	for i, id := range ids {
		switch {
		case !identityNamePattern.MatchString(id.GetName()):
			return validationError(at(i, "name"), fmt.Errorf("task %q: identity name %q must match %s", task.GetId(), id.GetName(), identityNamePattern))
		case seen[id.GetName()]:
			return validationError(at(i, "name"), fmt.Errorf("task %q declares identity %q twice", task.GetId(), id.GetName()))
		}
		seen[id.GetName()] = true
		if reason := audienceRefusal(id.GetAudience()); reason != "" {
			return validationError(at(i, "audience"), fmt.Errorf("task %q: identity %q's audience %s", task.GetId(), id.GetName(), reason))
		}
		if t := id.GetTtlSeconds(); t != 0 && t < MinIdentityTTLSeconds {
			return validationError(at(i, "ttl_seconds"), fmt.Errorf(
				"task %q: identity %q's ttl_seconds %d is under %d", task.GetId(), id.GetName(), t, MinIdentityTTLSeconds))
		}
		// Rule 61: every item of a fan-out holds the same identities.
		if task.GetEach() != nil {
			for _, f := range [...]struct{ field, v string }{{"name", id.GetName()}, {"audience", id.GetAudience()}} {
				field, v := f.field, f.v
				if strings.Contains(v, "{{") {
					return validationError(at(i, field), fmt.Errorf(
						"task %q: a fan-out's identities are the same for every item, so identity %q's %s is not a template", task.GetId(), id.GetName(), field))
				}
			}
		}
	}
	if !taskIdentityAvailable {
		return validationError(fmt.Sprintf("tasks[%d].identities", taskIndex), fmt.Errorf(
			"task %q declares identities, and task identity is not available in this build", task.GetId()))
	}
	return nil
}

// audienceRefusal says why audience cannot be a token's aud, or "".
func audienceRefusal(audience string) string {
	switch {
	case audience == "":
		return "is required"
	case len(audience) > maxAudienceBytes:
		return fmt.Sprintf("is %d bytes; at most %d", len(audience), maxAudienceBytes)
	case strings.IndexFunc(audience, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0:
		return "holds whitespace or a control character"
	}
	return ""
}

// canonicalIdentities is a task's identities as its canonical form holds
// them, or nil when it declares none.
func canonicalIdentities(t *orkstackv1.Task) []any {
	if len(t.GetIdentities()) == 0 {
		return nil
	}
	out := make([]any, 0, len(t.GetIdentities()))
	for _, id := range t.GetIdentities() {
		out = append(out, map[string]any{
			"name":       id.GetName(),
			"audience":   id.GetAudience(),
			"ttlSeconds": int64(id.GetTtlSeconds()),
			"refresh":    int64(id.GetRefresh()),
		})
	}
	return out
}
