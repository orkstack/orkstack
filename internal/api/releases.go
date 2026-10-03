package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// Releases (orkctl-release §6): which build of OrkStack takes new runs, and
// the one judgement that decides whether another may.
//
// Nothing here holds a release. Every answer is the cluster's routing, drain
// verdict and run index, read on the call, plus the Orker inventory
// ListOrkers serves — so any replica answers identically, including a replica
// of another release. The two writes are one cluster call each, after the
// same check that fills Release.blockers, and the cluster's own guards stay on
// underneath it.

const (
	// releaseRunsListed is how many of a release's runs a read lists.
	releaseRunsListed = 10
	// releaseRoutingWait bounds how long a write waits for the engine's own
	// channel to report the new routing before answering unconfirmed.
	releaseRoutingWait = 10 * time.Second
)

// errNoReleases is the answer when this replica has no cluster seam.
var errNoReleases = errors.New("this engine is not wired to a cluster, so it cannot read or change releases")

// The fixed sentences of §5.8. A cluster error can carry a channel name or an
// internal attribute name, so its text is logged by the engine and never
// forwarded.
const (
	releaseMsgPermission  = "the cluster does not allow this engine to read or change this deployment's releases"
	releaseMsgRefusedFmt  = "the cluster refused to change which release takes new runs, and nothing was changed: the releases changed while this request ran, or a pool the current release served recently has no Orker of release %s polling it. Run \"orkctl release status\" and decide again."
	releaseMsgForgotFmt   = "release %s is no longer known to the cluster"
	releaseMsgWriteUnsure = "the cluster did not confirm the change in time; \"orkctl release status\" shows whether it happened"
	releaseMsgReadDown    = "the cluster did not answer for this deployment's releases"
	releaseMsgInternal    = "the cluster rejected the request; the engine log has the cause"
	releaseMsgRunsUnknown = "the run index could not be asked which runs execute on this release"
)

// releaseClusterError maps a cluster error onto a Connect code and one fixed
// sentence (§5.8), by its type and by whether it came from the write.
func releaseClusterError(err error, write bool, buildID string) *connect.Error {
	code, message := releaseErrorSentence(err, write, buildID)
	return connect.NewError(code, errors.New(message))
}

func releaseErrorSentence(err error, write bool, buildID string) (connect.Code, string) {
	var (
		denied      *serviceerror.PermissionDenied
		failed      *serviceerror.FailedPrecondition
		notFound    *serviceerror.NotFound
		deadline    *serviceerror.DeadlineExceeded
		unavailable *serviceerror.Unavailable
		canceled    *serviceerror.Canceled
	)
	switch {
	case errors.As(err, &denied):
		return connect.CodePermissionDenied, releaseMsgPermission
	case write && errors.As(err, &failed):
		return connect.CodeFailedPrecondition, fmt.Sprintf(releaseMsgRefusedFmt, buildID)
	case write && errors.As(err, &notFound):
		return connect.CodeNotFound, fmt.Sprintf(releaseMsgForgotFmt, buildID)
	case errors.As(err, &deadline), errors.As(err, &unavailable), errors.As(err, &canceled),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		if write {
			return connect.CodeUnavailable, releaseMsgWriteUnsure
		}
		return connect.CodeUnavailable, releaseMsgReadDown
	default:
		return connect.CodeInternal, releaseMsgInternal
	}
}

// releaseRunCount is one release's ReleaseRuns answer: the count, or the
// error that stands in for it.
type releaseRunCount struct {
	n    int64
	runs []*orkstackv1.Run
	err  error
}

// ListReleases is the deployment's releases as the cluster routes them.
func (s *Service) ListReleases(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListReleasesRequest],
) (*connect.Response[orkstackv1.ListReleasesResponse], error) {
	if s.releases == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errNoReleases)
	}
	resp, _, err := s.releaseSnapshot(ctx, req.Msg.GetBuildId())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

// releaseSnapshot is one ListReleases answer plus the observation a write
// judges and passes back. only != "" keeps one release in the response;
// every judgement still uses the whole deployment.
func (s *Service) releaseSnapshot(ctx context.Context, only string) (*orkstackv1.ListReleasesResponse, engine.ReleaseObservation, error) {
	obs, err := s.releases.ObserveReleases(ctx)
	if errors.Is(err, engine.ErrNotVersioned) {
		return &orkstackv1.ListReleasesResponse{
			DeploymentName: obs.DeploymentName,
			ObservedAt:     timestamppb.New(s.now()),
		}, obs, nil
	}
	if err != nil {
		return nil, obs, releaseClusterError(err, false, only)
	}

	known := map[string]bool{}
	for _, release := range obs.Releases {
		known[release.BuildID] = true
	}
	// Rule 5 of the judgement subtracts every release's count from the total
	// while no release is current, so every count is needed then even when
	// only one release is shown.
	var counted []string
	switch {
	case obs.Routing.Current == "" || only == "":
		for _, release := range obs.Releases {
			counted = append(counted, release.BuildID)
		}
	case known[only]:
		counted = []string{only}
	}

	var (
		wg         sync.WaitGroup
		inventory  []*orkstackv1.Orker
		invErr     error
		counts     = make([]releaseRunCount, len(counted))
		running    int64
		runningErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		inventory, invErr = s.orkerInventory(ctx)
	}()
	go func() {
		defer wg.Done()
		inParallel(len(counted), func(i int) {
			n, runs, err := s.releases.ReleaseRuns(ctx, counted[i], releaseRunsListed)
			counts[i] = releaseRunCount{n: n, runs: runs, err: err}
		})
		if obs.Routing.Current == "" {
			running, runningErr = s.releases.RunningRuns(ctx)
		}
	}()
	wg.Wait()

	runs := make(map[string]releaseRunCount, len(counted))
	for i, build := range counted {
		runs[build] = counts[i]
		if counts[i].err != nil {
			s.logger.Warn("list releases: the run index could not be asked about a release",
				"build_id", build, "error", counts[i].err)
		}
	}
	if runningErr != nil {
		s.logger.Warn("list releases: the runs in flight could not be counted", "error", runningErr)
	}

	resp := &orkstackv1.ListReleasesResponse{
		DeploymentName:            obs.DeploymentName,
		Versioned:                 true,
		UnversionedEngineReplicas: int32(obs.EngineReplicas[""]),
		Revision:                  obs.Routing.Revision(),
	}
	if invErr != nil {
		s.logger.Warn("list releases: the Orker inventory could not be read", "error", invErr)
		resp.OrkersUnavailableReason = "the Orker list could not be read: " + inventorySentence(invErr)
	}

	// Attribute the inventory: an answered row goes to its release when the
	// cluster still knows that release; every other row belongs to none.
	byBuild := map[string][]*orkstackv1.Orker{}
	for _, row := range inventory {
		if row.GetUnavailableReason() == "" && row.GetBuildId() != "" && known[row.GetBuildId()] {
			byBuild[row.GetBuildId()] = append(byBuild[row.GetBuildId()], row)
			continue
		}
		resp.UnattributedOrkers = append(resp.UnattributedOrkers, row)
	}
	incomplete := invErr != nil
	for _, row := range resp.UnattributedOrkers {
		if row.GetUnavailableReason() != "" {
			incomplete = true
		}
	}

	for _, observed := range obs.Releases {
		if only != "" && observed.BuildID != only {
			continue
		}
		release := &orkstackv1.Release{
			BuildId:        observed.BuildID,
			Role:           releaseRole(observed, obs.Routing),
			EngineReplicas: int32(obs.EngineReplicas[observed.BuildID]),
			Orkers:         byBuild[observed.BuildID],
			PoolsServed:    poolsServed(byBuild[observed.BuildID]),
			DrainState:     observed.DrainState,
		}
		if !observed.FirstSeenAt.IsZero() {
			release.FirstSeenAt = timestamppb.New(observed.FirstSeenAt)
		}
		switch release.Role {
		case orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT:
			if !obs.CurrentSince.IsZero() {
				release.RoleSince = timestamppb.New(obs.CurrentSince)
			}
		case orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING:
			release.RampPercent = obs.Routing.RampPercent
			if !obs.RampingSince.IsZero() {
				release.RoleSince = timestamppb.New(obs.RampingSince)
			}
		}
		if observed.DrainState != orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_UNSPECIFIED && !observed.DrainCheckedAt.IsZero() {
			release.DrainCheckedAt = timestamppb.New(observed.DrainCheckedAt)
		}
		if count, ok := runs[observed.BuildID]; ok && count.err == nil {
			release.RunsInFlight = int32(count.n)
			release.Runs = count.runs
		} else {
			release.RunsUnavailableReason = releaseMsgRunsUnknown
		}
		if release.Role != orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT {
			release.Blockers = judgeRelease(observed.BuildID, obs, inventory, invErr, runs, running, runningErr)
		}
		release.Drained = releaseDrained(release, incomplete)
		resp.Releases = append(resp.Releases, release)
	}
	sortReleases(resp.Releases)
	resp.ObservedAt = timestamppb.New(s.now())
	return resp, obs, nil
}

// inventorySentence is the fixed cause of an inventory that could not be
// read, in the words of §5.8's read rows.
func inventorySentence(err error) string {
	if errors.Is(err, errNoFleet) {
		return "this engine cannot list Orkers"
	}
	_, message := releaseErrorSentence(err, false, "")
	return message
}

// releaseRole is §6.2.
func releaseRole(observed engine.ObservedRelease, routing engine.Routing) orkstackv1.ReleaseRole {
	switch {
	case routing.Current != "" && observed.BuildID == routing.Current:
		return orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT
	case routing.Ramping != "" && observed.BuildID == routing.Ramping:
		return orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING
	case observed.HasServed:
		return orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS
	default:
		return orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE
	}
}

// poolsServed is every pool with an answered, uncordoned row, sorted.
func poolsServed(rows []*orkstackv1.Orker) []string {
	seen := map[string]bool{}
	pools := []string{}
	for _, row := range rows {
		if row.GetUnavailableReason() != "" || row.GetCordoned() || row.GetPool() == "" || seen[row.GetPool()] {
			continue
		}
		seen[row.GetPool()] = true
		pools = append(pools, row.GetPool())
	}
	sort.Strings(pools)
	return pools
}

// releaseDrained is §6.4: the one condition under which a release's processes
// may be stopped.
func releaseDrained(release *orkstackv1.Release, inventoryIncomplete bool) bool {
	role := release.GetRole()
	clusterDrained := role == orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS &&
		release.GetDrainState() == orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED
	if !clusterDrained && role != orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE {
		return false
	}
	if release.GetRunsUnavailableReason() != "" {
		// The cluster's own drain verdict is the authority; an unreadable
		// index is not evidence of a run. A candidate has no such verdict.
		if !clusterDrained {
			return false
		}
	} else if release.GetRunsInFlight() != 0 {
		return false
	}
	if inventoryIncomplete {
		return false
	}
	for _, row := range release.GetOrkers() {
		if row.GetOpenPins() > 0 {
			return false
		}
	}
	return true
}

// sortReleases is the contract's order: CURRENT, RAMPING, CANDIDATEs newest
// first, PREVIOUS newest first (by first_seen_at, then build_id).
func sortReleases(releases []*orkstackv1.Release) {
	rank := func(role orkstackv1.ReleaseRole) int {
		switch role {
		case orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT:
			return 0
		case orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING:
			return 1
		case orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE:
			return 2
		default:
			return 3
		}
	}
	sort.SliceStable(releases, func(i, j int) bool {
		a, b := releases[i], releases[j]
		if rank(a.GetRole()) != rank(b.GetRole()) {
			return rank(a.GetRole()) < rank(b.GetRole())
		}
		at, bt := a.GetFirstSeenAt().AsTime(), b.GetFirstSeenAt().AsTime()
		if !at.Equal(bt) {
			return at.After(bt)
		}
		return a.GetBuildId() < b.GetBuildId()
	})
}

// judgeRelease lists every reason target may not take new runs. It is pure:
// same inputs, same blockers, same order. It is the ONLY place the check
// lives; PromoteRelease and RampRelease refuse on exactly its output (§6.3).
func judgeRelease(target string, obs engine.ReleaseObservation, inventory []*orkstackv1.Orker,
	invErr error, runs map[string]releaseRunCount, running int64, runningErr error) []*orkstackv1.ReleaseBlocker {
	if obs.Routing.Current != "" && target == obs.Routing.Current {
		return nil
	}
	var blockers []*orkstackv1.ReleaseBlocker
	add := func(kind orkstackv1.ReleaseBlockerKind, pool, message string) {
		blockers = append(blockers, &orkstackv1.ReleaseBlocker{Kind: kind, Pool: pool, Message: message})
	}

	// 1. Engine.
	if obs.EngineReplicas[target] == 0 {
		add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_NO_ENGINE, "", fmt.Sprintf(
			"no engine of release %s is running, so a run sent to it would never start. "+
				"Start ork with ORK_BUILD_ID=%s and ORK_DEPLOYMENT_NAME=%s.", target, target, obs.DeploymentName))
	}

	// 2. Inventory.
	poolsJudged := true
	var answered []*orkstackv1.Orker
	if invErr != nil {
		add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERIFIED, "", fmt.Sprintf(
			"the Orker list could not be read, so whether release %s serves every pool in use cannot be "+
				"established; ask again.", target))
		poolsJudged = false
	} else {
		var silent []string
		for _, row := range inventory {
			if row.GetUnavailableReason() != "" {
				silent = append(silent, row.GetOrkerId())
				continue
			}
			answered = append(answered, row)
		}
		if len(silent) > 0 {
			sort.Strings(silent)
			add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERIFIED, "", fmt.Sprintf(
				"%d Orker(s) did not answer (%s), so their release and pool are unknown; an Orker that "+
					"stopped leaves the list within a minute — ask again.", len(silent), idList(silent)))
		}
	}
	sort.SliceStable(answered, func(i, j int) bool { return answered[i].GetOrkerId() < answered[j].GetOrkerId() })

	if poolsJudged {
		// 3. Pools in use.
		serves := map[string]bool{}
		cordonedIn := map[string]*orkstackv1.Orker{} // first cordoned row of the target, per pool
		for _, row := range answered {
			if row.GetBuildId() != target {
				continue
			}
			if row.GetCordoned() {
				if cordonedIn[row.GetPool()] == nil {
					cordonedIn[row.GetPool()] = row
				}
				continue
			}
			serves[row.GetPool()] = true
		}
		// evidence is the first reason each pool is in use: an Orker of another
		// release (or of none) first, by orker_id; then the cluster's own
		// evidence on the pools the current and ramping releases registered.
		evidence := map[string]string{}
		for _, row := range answered {
			if row.GetBuildId() == target || row.GetPool() == "" || evidence[row.GetPool()] != "" {
				continue
			}
			evidence[row.GetPool()] = orkerServesEvidence(row)
		}
		for _, other := range []string{obs.Routing.Current, obs.Routing.Ramping} {
			if other == "" || other == target {
				continue
			}
			observed := observedRelease(obs, other)
			if observed == nil {
				continue
			}
			if observed.DescribeErr != nil {
				add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERIFIED, "", fmt.Sprintf(
					"the pools release %s served could not be read; ask again.", other))
				continue
			}
			for _, pool := range observed.RegisteredPools {
				// A pool the target serves passes whatever its use, and one an
				// Orker already shows in use is judged on that evidence.
				if serves[pool] || evidence[pool] != "" {
					continue
				}
				use, ok := obs.PoolUse[pool]
				if !ok {
					continue
				}
				if use.Err != nil {
					add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERIFIED, pool, fmt.Sprintf(
						"whether pool %q has work waiting could not be read; ask again.", pool))
					evidence[pool] = unmeasured
					continue
				}
				if text := poolUseEvidence(use); text != "" {
					evidence[pool] = text
				}
			}
		}
		var inUse []string
		for pool, text := range evidence {
			if text != unmeasured && !serves[pool] {
				inUse = append(inUse, pool)
			}
		}
		sort.Strings(inUse)
		for _, pool := range inUse {
			nobody := "no Orker of release " + target + " serves it"
			if row := cordonedIn[pool]; row != nil {
				nobody = fmt.Sprintf("no uncordoned Orker of release %s serves it (%s of release %s is cordoned: %s)",
					target, row.GetOrkerId(), target, strconv.Quote(row.GetCordonReason()))
			}
			add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_POOL_NOT_SERVED, pool, fmt.Sprintf(
				"pool %q is in use (%s) and %s, so tasks its runs send there would not run on its own Orkers. "+
					"Start one with ORK_BUILD_ID=%s ORK_ORKER_POOL=%s.", pool, evidence[pool], nobody, target, pool))
		}

		// 4. Pin hosts.
		pinHost := map[string]*orkstackv1.Orker{}
		targetHosts := map[string]bool{}
		for _, row := range answered {
			if row.GetMaxConcurrentPins() <= 0 {
				continue
			}
			if row.GetBuildId() == target {
				if !row.GetCordoned() {
					targetHosts[row.GetPool()] = true
				}
				continue
			}
			if pinHost[row.GetPool()] == nil {
				pinHost[row.GetPool()] = row
			}
		}
		var pinPools []string
		for pool := range pinHost {
			if serves[pool] && !targetHosts[pool] {
				pinPools = append(pinPools, pool)
			}
		}
		sort.Strings(pinPools)
		for _, pool := range pinPools {
			add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_PINS_NOT_HOSTED, pool, fmt.Sprintf(
				"pool %q hosts pins (%s) and no Orker of release %s in it hosts pins, so a pin its runs open "+
					"there would open on another release's Orker. Start one with ORK_BUILD_ID=%s ORK_ORKER_POOL=%s "+
					"ORK_ORKER_MAX_PINS=1 (or more; set the same value on every Orker of the pool).",
				pool, pinHostEvidence(pinHost[pool]), target, target, pool))
		}
	}

	// 5. Runs started before versioning.
	if obs.Routing.Current == "" {
		failed := runningErr != nil
		var versioned int64
		for _, release := range obs.Releases {
			count, ok := runs[release.BuildID]
			if !ok || count.err != nil {
				failed = true
				continue
			}
			versioned += count.n
		}
		unversioned := running - versioned
		if unversioned < 0 {
			unversioned = 0
		}
		switch {
		case failed:
			add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERIFIED, "",
				"whether runs started before versioning are still executing could not be counted; ask again.")
		case unversioned > 0:
			add(orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_UNVERSIONED_RUNS, "", fmt.Sprintf(
				"%d run(s) started before any release was current are still executing; making release %s take "+
					"new runs moves them onto it at their next step, which fails them if its code differs from the "+
					"build that started them. Let them finish first — \"orkctl namespace update NS --hold REASON\" "+
					"stops new ones meanwhile (docs/DEPLOY.md §3.5).", unversioned, target))
		}
	}
	return blockers
}

// unmeasured marks a pool whose use could not be read: already reported as
// UNVERIFIED, and not in use by any evidence.
const unmeasured = "\x00unmeasured"

func observedRelease(obs engine.ReleaseObservation, build string) *engine.ObservedRelease {
	for i := range obs.Releases {
		if obs.Releases[i].BuildID == build {
			return &obs.Releases[i]
		}
	}
	return nil
}

// orkerServesEvidence is rule 3(a)'s evidence for one row of another release.
func orkerServesEvidence(row *orkstackv1.Orker) string {
	text := row.GetOrkerId() + " of release " + row.GetBuildId() + " serves it"
	if row.GetBuildId() == "" {
		text = row.GetOrkerId() + ", started without ORK_BUILD_ID, serves it"
	}
	if row.GetCordoned() {
		text += " (cordoned)"
	}
	return text
}

// pinHostEvidence names the pin host that makes a pool host pins.
func pinHostEvidence(row *orkstackv1.Orker) string {
	if row.GetBuildId() == "" {
		return row.GetOrkerId() + ", started without ORK_BUILD_ID, is a pin host"
	}
	return row.GetOrkerId() + " of release " + row.GetBuildId() + " is a pin host"
}

// poolUseEvidence is rule 3(b)'s evidence, the first that applies, or "" when
// the cluster shows no use.
func poolUseEvidence(use engine.PoolUse) string {
	switch {
	case use.Backlog == 1:
		return "1 task is waiting there"
	case use.Backlog > 1:
		return fmt.Sprintf("%d tasks are waiting there", use.Backlog)
	case use.AddRate > 0:
		return "it received work in the last minute"
	case use.LivePollers > 0:
		return "an Orker this list cannot show is polling it (one started before Orkers could be listed)"
	default:
		return ""
	}
}

// idList renders up to five ids, then "…".
func idList(ids []string) string {
	const shown = 5
	if len(ids) <= shown {
		return strings.Join(ids, ", ")
	}
	return strings.Join(ids[:shown], ", ") + ", …"
}

// releaseWritable is the steps both writes share: the snapshot, and the
// refusals that precede any judgement (§6.6 steps 2–5).
func (s *Service) releaseWritable(ctx context.Context, buildID, expected string) (*orkstackv1.ListReleasesResponse, *orkstackv1.Release, engine.ReleaseObservation, error) {
	resp, obs, err := s.releaseSnapshot(ctx, buildID)
	if err != nil {
		return nil, nil, obs, err
	}
	if !resp.GetVersioned() {
		return nil, nil, obs, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"no release of deployment %s has registered with the cluster; start ork and orker with "+
				"ORK_BUILD_ID set first (docs/DEPLOY.md §1)", resp.GetDeploymentName()))
	}
	var target *orkstackv1.Release
	for _, release := range resp.GetReleases() {
		if release.GetBuildId() == buildID {
			target = release
		}
	}
	if target == nil {
		return nil, nil, obs, connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"no release %s is known to the cluster; start its ork and orker with ORK_BUILD_ID=%s, "+
				"then run \"orkctl release status\"", buildID, buildID))
	}
	if expected != "" && expected != resp.GetRevision() {
		current, ramping := "none", "nothing is ramping"
		if obs.Routing.Current != "" {
			current = obs.Routing.Current
		}
		if obs.Routing.Ramping != "" {
			ramping = fmt.Sprintf("ramping is now %s at %g%%", obs.Routing.Ramping, obs.Routing.RampPercent)
		}
		return nil, nil, obs, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"the releases changed since you read them (current is now %s, %s); run \"orkctl release status\" "+
				"and decide again", current, ramping))
	}
	return resp, target, obs, nil
}

// refuseOnBlockers is §6.6 step 7.
func refuseOnBlockers(target *orkstackv1.Release) error {
	if len(target.GetBlockers()) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "release %s cannot take new runs yet:", target.GetBuildId())
	for _, blocker := range target.GetBlockers() {
		b.WriteString("\n  - ")
		b.WriteString(blocker.GetMessage())
	}
	return connect.NewError(connect.CodeFailedPrecondition, errors.New(b.String()))
}

// confirmRouting is §6.6 step 9: an AwaitRouting error is logged and read as
// unconfirmed, never as a failed write — the write already happened.
func (s *Service) confirmRouting(ctx context.Context, want engine.Routing) bool {
	confirmed, err := s.releases.AwaitRouting(ctx, want, releaseRoutingWait)
	if err != nil {
		s.logger.Warn("release routing could not be confirmed on the engine's channel",
			"current", want.Current, "ramping", want.Ramping, "error", err)
		return false
	}
	return confirmed
}

// PromoteRelease makes one release take every new run.
func (s *Service) PromoteRelease(
	ctx context.Context,
	req *connect.Request[orkstackv1.PromoteReleaseRequest],
) (*connect.Response[orkstackv1.PromoteReleaseResponse], error) {
	build := req.Msg.GetBuildId()
	if build == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("build_id is required: it names the release"))
	}
	if s.releases == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errNoReleases)
	}
	resp, target, obs, err := s.releaseWritable(ctx, build, req.Msg.GetExpectedRevision())
	if err != nil {
		return nil, err
	}
	if target.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT {
		return connect.NewResponse(&orkstackv1.PromoteReleaseResponse{
			Changed: false, PreviousBuildId: build, RoutingConfirmed: true, Revision: resp.GetRevision(),
		}), nil
	}
	if err := refuseOnBlockers(target); err != nil {
		return nil, err
	}
	want := engine.Routing{Current: build, Ramping: obs.Routing.Ramping, RampPercent: obs.Routing.RampPercent}
	if obs.Routing.Ramping == build {
		want.Ramping, want.RampPercent = "", 0
	}
	if err := s.releases.SetRouting(ctx, obs, want); err != nil {
		return nil, releaseClusterError(err, true, build)
	}
	confirmed := s.confirmRouting(ctx, want)
	s.logger.Info("release promoted", "build_id", build, "previous", obs.Routing.Current,
		"routing_confirmed", confirmed)
	return connect.NewResponse(&orkstackv1.PromoteReleaseResponse{
		Changed:          true,
		PreviousBuildId:  obs.Routing.Current,
		RoutingConfirmed: confirmed,
		Revision:         want.Revision(),
	}), nil
}

// RampRelease sends a share of new runs to one release that is not current.
func (s *Service) RampRelease(
	ctx context.Context,
	req *connect.Request[orkstackv1.RampReleaseRequest],
) (*connect.Response[orkstackv1.RampReleaseResponse], error) {
	build, percent := req.Msg.GetBuildId(), req.Msg.GetPercent()
	if build == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("build_id is required: it names the release"))
	}
	if percent < 0 || percent > 100 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("percent is 1–100 to ramp a release, or 0 to stop its ramp; %d is neither", percent))
	}
	if s.releases == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errNoReleases)
	}
	resp, target, obs, err := s.releaseWritable(ctx, build, req.Msg.GetExpectedRevision())
	if err != nil {
		return nil, err
	}
	if target.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"release %s is current; it already takes every new run that is not ramped elsewhere", build))
	}
	ramping := target.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_RAMPING
	want := engine.Routing{Current: obs.Routing.Current}
	if percent == 0 {
		if !ramping {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("release %s is not ramping", build))
		}
		// No judgement: stopping a ramp only takes new runs away from a release.
	} else {
		if ramping && obs.Routing.RampPercent == float32(percent) {
			return connect.NewResponse(&orkstackv1.RampReleaseResponse{
				Changed: false, PreviousBuildId: build, PreviousPercent: obs.Routing.RampPercent,
				RoutingConfirmed: true, Revision: resp.GetRevision(),
			}), nil
		}
		if err := refuseOnBlockers(target); err != nil {
			return nil, err
		}
		want.Ramping, want.RampPercent = build, float32(percent)
	}
	if err := s.releases.SetRouting(ctx, obs, want); err != nil {
		return nil, releaseClusterError(err, true, build)
	}
	confirmed := s.confirmRouting(ctx, want)
	s.logger.Info("release ramped", "build_id", build, "percent", percent,
		"previous", obs.Routing.Ramping, "previous_percent", obs.Routing.RampPercent,
		"routing_confirmed", confirmed)
	return connect.NewResponse(&orkstackv1.RampReleaseResponse{
		Changed:          true,
		PreviousBuildId:  obs.Routing.Ramping,
		PreviousPercent:  obs.Routing.RampPercent,
		RoutingConfirmed: confirmed,
		Revision:         want.Revision(),
	}), nil
}
