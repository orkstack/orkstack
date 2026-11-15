package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/task"
)

// The engines' presence channel (task.EnginesQueue, docs/AUTHENTICATION.md,
// "Every engine enforces"): every `ork` process polls it, whatever it serves,
// and nothing dispatches to it. Its poller list is the deployment's engines,
// and each says what the fleet report and the mode record's lowering need to
// know of it in the identity it polls with, beside its capabilities: its
// mode, its idempotency key's life, the digests of its auth.admins and
// auth.orkers, its deployment and its build. Nothing is registered and
// nothing is stored; the cluster forgets a process minutes after it is gone,
// as it forgets an Orker on ork-orkers.

// ActivityEnginePresence is the one activity the presence worker registers,
// so that a worker with nothing else to run still polls. Nothing in OrkStack
// dispatches it (TestEnginesQueue_CarriesNoDispatch).
const ActivityEnginePresence = "EnginePresence"

// The description's keys, each written key=value in the identity's ork-caps
// list. A release that reads capabilities only sees tokens it does not know,
// and never matches one: no capability holds '='.
const (
	describeMode       = "mode"
	describeClaimTTL   = "claim-ttl"
	describeAdmins     = "admins"
	describeOrkers     = "orkers"
	describeDeployment = "deployment"
	describeBuild      = "build"
)

// ConfigDigestLen is the length of a configuration digest, in hex: the first
// eight bytes of its SHA-256, enough to tell two configurations apart.
const ConfigDigestLen = 16

// maxDescribedValue bounds one described value, so the whole identity stays
// well inside the cluster's bound on one (1000 bytes by default) beside a
// host name of 253.
const maxDescribedValue = 100

// MaxDescribedClaimTTL is the longest idempotency key life a description
// says or is believed for: ten years. An engine whose life is longer says
// this much, and a poller cannot hold the mode record's lowering off for
// longer by saying more.
const MaxDescribedClaimTTL = 10 * 366 * 24 * time.Hour

// EngineDescription is what one `ork` process says of itself on the presence
// channel. A field is empty when the process does not say it: a release that
// predates it, or a value that cannot travel in an identity.
type EngineDescription struct {
	Mode         string        // auth.mode: asserted, observe or enforce
	ClaimTTL     time.Duration // engine.idempotency_ttl
	AdminsDigest string        // ConfigDigest of auth.admins; set only when the mode authenticates
	OrkersDigest string        // ConfigDigest of auth.orkers; likewise
	Deployment   string        // ORK_DEPLOYMENT_NAME
	BuildID      string        // ORK_BUILD_ID; empty on an unversioned engine
}

// Tokens are d's tokens, in a fixed order. A claim life past
// MaxDescribedClaimTTL is said as that bound; any other value that cannot
// travel in an identity is left out, never altered (Unsaid names it).
func (d EngineDescription) Tokens() []string {
	var out []string
	add := func(key, value string, ok bool) {
		if ok && describable(value) {
			out = append(out, key+"="+value)
		}
	}
	add(describeMode, d.Mode, knownMode(d.Mode))
	add(describeClaimTTL, min(d.ClaimTTL, MaxDescribedClaimTTL).String(), d.ClaimTTL > 0)
	add(describeAdmins, d.AdminsDigest, validDigest(d.AdminsDigest))
	add(describeOrkers, d.OrkersDigest, validDigest(d.OrkersDigest))
	add(describeDeployment, d.Deployment, true)
	add(describeBuild, d.BuildID, true)
	return out
}

// Unsaid names the fields of d that Tokens leaves out although they are set:
// a value too long, or holding a space, comma, '=', or a byte that is not
// printable ASCII.
func (d EngineDescription) Unsaid() []string {
	var out []string
	if d.Mode != "" && !knownMode(d.Mode) {
		out = append(out, "auth.mode")
	}
	if d.Deployment != "" && !describable(d.Deployment) {
		out = append(out, "ORK_DEPLOYMENT_NAME")
	}
	if d.BuildID != "" && !describable(d.BuildID) {
		out = append(out, "ORK_BUILD_ID")
	}
	return out
}

// ParseEngineDescription reads what identity says of its engine. A key said
// twice, and a value Tokens would not write, leave that field empty: an
// identity is the cluster's record of whatever a poller sent, never trusted
// to be well formed.
func ParseEngineDescription(identity string) EngineDescription {
	values := map[string][]string{}
	for _, token := range IdentityCapabilities(identity) {
		if key, value, ok := strings.Cut(token, "="); ok {
			values[key] = append(values[key], value)
		}
	}
	one := func(key string) (string, bool) {
		v := values[key]
		if len(v) != 1 || !describable(v[0]) {
			return "", false
		}
		return v[0], true
	}
	var d EngineDescription
	if v, ok := one(describeMode); ok && knownMode(v) {
		d.Mode = v
	}
	if v, ok := one(describeClaimTTL); ok {
		if ttl, err := time.ParseDuration(v); err == nil && ttl > 0 && ttl <= MaxDescribedClaimTTL {
			d.ClaimTTL = ttl
		}
	}
	if v, ok := one(describeAdmins); ok && validDigest(v) {
		d.AdminsDigest = v
	}
	if v, ok := one(describeOrkers); ok && validDigest(v) {
		d.OrkersDigest = v
	}
	d.Deployment, _ = one(describeDeployment)
	d.BuildID, _ = one(describeBuild)
	return d
}

// describable reports a value that can travel in an identity's ork-caps
// list: non-empty, bounded, printable ASCII, and holding no separator (","
// or "=") or space.
func describable(value string) bool {
	if value == "" || len(value) > maxDescribedValue {
		return false
	}
	for i := 0; i < len(value); i++ {
		if b := value[i]; b <= 0x20 || b >= 0x7f || b == ',' || b == '=' {
			return false
		}
	}
	return true
}

func knownMode(mode string) bool {
	return mode == "asserted" || mode == "observe" || mode == "enforce"
}

func validDigest(digest string) bool {
	if len(digest) != ConfigDigestLen {
		return false
	}
	for i := 0; i < len(digest); i++ {
		if c := digest[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ConfigDigest digests a configured list whatever order it was written in:
// its entries sorted, each ended by a newline, hashed with SHA-256 and cut to
// ConfigDigestLen hex. Two engines whose lists hold the same entries agree.
func ConfigDigest(entries []string) string {
	sorted := slices.Clone(entries)
	slices.Sort(sorted)
	h := sha256.New()
	for _, e := range sorted {
		h.Write([]byte(e))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))[:ConfigDigestLen]
}

// NewPresenceWorker builds this process's worker on the engines' presence
// channel. It is unversioned, as the Orkers' presence worker is: nothing is
// dispatched to it, so a versioned routing would only register the channel
// with a release. One poller is its whole standing cost.
func NewPresenceWorker(c client.Client) worker.Worker {
	w := worker.New(c, task.EnginesQueue, worker.Options{
		DisableRegistrationAliasing:      true,
		DisableWorkflowWorker:            true,
		MaxConcurrentActivityTaskPollers: 1,
	})
	w.RegisterActivityWithOptions(enginePresence, activity.RegisterOptions{Name: ActivityEnginePresence})
	return w
}

// enginePresence is registered so a stray dispatch fails nothing.
func enginePresence(context.Context) error { return nil }

// EnginePresence is one engine the presence channel lists.
type EnginePresence struct {
	Identity    string
	LastSeen    time.Time
	Description EngineDescription
}

// EnginePresenceReader lists the engines the presence channel holds.
type EnginePresenceReader interface {
	ObserveEngines(ctx context.Context) ([]EnginePresence, error)
}

var _ EnginePresenceReader = (*TemporalReleaseCluster)(nil)

// ObserveEngines lists the fresh pollers of the engines' presence channel
// that are of this deployment, in identity order: a poller that names
// another deployment is skipped, and one that names none (a release that
// does not say) is listed. A failed describe is an error.
func (c *TemporalReleaseCluster) ObserveEngines(ctx context.Context) ([]EnginePresence, error) {
	described, err := c.describeChannel(ctx, task.EnginesQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, false)
	if err != nil {
		return nil, c.fail(ReleaseOpRead, "describe the engines' presence channel", "", err)
	}
	now, within := c.now(), c.freshness()
	var out []EnginePresence
	for _, poller := range described.GetPollers() {
		if !freshPoller(poller, now, within) {
			continue
		}
		identity := poller.GetIdentity()
		d := ParseEngineDescription(identity)
		if d.Deployment != "" && d.Deployment != c.deployment {
			continue
		}
		seen := now
		if at := poller.GetLastAccessTime(); at != nil && !at.AsTime().IsZero() {
			seen = at.AsTime()
		}
		out = append(out, EnginePresence{Identity: identity, LastSeen: seen, Description: d})
	}
	slices.SortFunc(out, func(a, b EnginePresence) int { return strings.Compare(a.Identity, b.Identity) })
	return out, nil
}

// LargestClaimTTL is the longest idempotency key life the engines listed
// advertise, and the identity of the first engine that advertises it; zero
// and "" when none says.
func LargestClaimTTL(engines []EnginePresence) (time.Duration, string) {
	var largest time.Duration
	var by string
	for _, e := range engines {
		if e.Description.ClaimTTL > largest {
			largest, by = e.Description.ClaimTTL, e.Identity
		}
	}
	return largest, by
}
