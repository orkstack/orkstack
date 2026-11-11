package dist_test

import (
	"encoding/json/v2"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The development stack's Temporal, and every proof's own (scripts/proof.sh
// and scripts/proof-part.sh run the image and command of this one service,
// through `proofrun temporal-args`), is started with three dynamic-config
// values. Two bring the cluster's worker-deployment drainage checks from
// minutes to seconds (docs/DEPLOY.md §4, "Underneath"), and one keeps
// overlapping deletes from holding each other up for minutes. They are
// development and proof settings, never a production recommendation: a
// production cluster keeps its defaults, and nothing in the quickstart carries
// them.
//
// The two drainage settings are read, per version that stopped taking new runs,
// by the cluster's version workflow: the first drainage check waits the grace
// period (the allowance for a workflow that started on the version just before
// it stopped being current to show up in visibility), and each later check
// waits the refresh interval.
//
// The third is how often the transfer queue saves its state. A closed
// execution's delete runs only once its close task reads as acknowledged in
// that saved state, which reaches only as far as the oldest task still pending,
// and this server has one shard, so one test's pending delete holds back
// another's.
const (
	drainageRefreshKey = "matching.wv.VersionDrainageStatusRefreshInterval"
	drainageGraceKey   = "matching.wv.VersionDrainageStatusVisibilityGracePeriod"
	queueStateSaveKey  = "history.transferProcessorUpdateAckInterval"

	dynamicConfigFlag = "--dynamic-config-value"
)

// dynamicConfigValues returns the KEY=VALUE pairs a `server start-dev` command
// line passes with --dynamic-config-value, in order, and fails the test when
// the flag has no value.
func dynamicConfigValues(t *testing.T, command []string) []string {
	t.Helper()
	var pairs []string
	for i, arg := range command {
		if arg != dynamicConfigFlag {
			continue
		}
		if i+1 >= len(command) {
			t.Fatalf("the command ends with %s and no KEY=VALUE: %v", dynamicConfigFlag, command)
		}
		pairs = append(pairs, command[i+1])
	}
	return pairs
}

// dynamicConfigDuration reads one value of the command the way the server
// does: the value after the first '=' is JSON, and a duration is a Go duration
// string or a number of seconds.
func dynamicConfigDuration(t *testing.T, pairs []string, key string) time.Duration {
	t.Helper()
	var found []string
	for _, pair := range pairs {
		if name, value, ok := strings.Cut(pair, "="); ok && name == key {
			found = append(found, value)
		}
	}
	if len(found) != 1 {
		t.Fatalf("the temporal command passes %s %d times (%q), want exactly once", key, len(found), pairs)
	}
	var decoded any
	if err := json.Unmarshal([]byte(found[0]), &decoded); err != nil {
		t.Fatalf("%s=%s: the value is not JSON, which %s requires (a string needs its quotes): %v", key, found[0], dynamicConfigFlag, err)
	}
	switch v := decoded.(type) {
	case string:
		d, err := time.ParseDuration(v)
		must.OKf(t, err, "%s=%s", key, found[0])
		return d
	case float64:
		return time.Duration(v * float64(time.Second))
	}
	t.Fatalf("%s=%s: neither a duration string nor a number of seconds", key, found[0])
	return 0
}

func TestDevCompose_TemporalDrainsWorkerDeploymentVersionsInSeconds(t *testing.T) {
	t.Parallel()
	command := parseCompose(t, devComposePath, readRepoFile(t, devComposePath)).service(t, "temporal").Command
	if len(command) < 2 || command[0] != "server" || command[1] != "start-dev" {
		t.Fatalf("the temporal service runs %v, want `server start-dev ...`, the only server the development stack and the proofs run", command)
	}
	pairs := dynamicConfigValues(t, command)
	refresh := dynamicConfigDuration(t, pairs, drainageRefreshKey)
	grace := dynamicConfigDuration(t, pairs, drainageGraceKey)

	// The cluster's defaults are three minutes each, which every test that
	// waits for a release to drain used to wait out. A refresh of seconds is
	// the point; a refresh of a fraction of a second would only churn the
	// version workflow's history.
	if refresh < time.Second || refresh > 10*time.Second {
		t.Errorf("%s is %s, want between 1s and 10s: seconds, not the default's minutes", drainageRefreshKey, refresh)
	}
	// The grace period is what keeps a version from being reported drained
	// while a workflow that started on it a moment ago has not reached
	// visibility, and a loaded host can hold a workflow task for seconds. It
	// is shortened, never removed.
	if grace < 5*time.Second || grace > 30*time.Second {
		t.Errorf("%s is %s, want between 5s and 30s: short enough for a test to wait out, long enough for visibility to catch up on a loaded host", drainageGraceKey, grace)
	}

	// The cluster's default is 30 seconds. With deletes overlapping, that made
	// a delete wait 31.9 to 136 seconds on this image and 5 seconds made it 3.9
	// to 11, so a test that gives a delete a minute can pass beside another one.
	if save := dynamicConfigDuration(t, pairs, queueStateSaveKey); save != 5*time.Second {
		t.Errorf("%s is %s, want 5s, the value measured", queueStateSaveKey, save)
	}

	// Nothing else is set by dynamic config: another knob is another way for
	// the development server to differ from the one an operator runs.
	if len(pairs) != 3 {
		t.Errorf("the temporal command passes %d dynamic-config values %q, want exactly the two drainage settings and %s", len(pairs), pairs, queueStateSaveKey)
	}
}

// The proofs take their Temporal from the compose file's service and run no
// second copy of its command, so the settings above reach a proof's own
// Temporal because the scripts read them from the file. A literal command in
// either script would let the two drift apart without anything failing.
func TestProofScripts_TakeTheirTemporalCommandFromTheComposeFile(t *testing.T) {
	t.Parallel()
	for _, script := range []string{"scripts/proof.sh", "scripts/proof-part.sh"} {
		body := string(readRepoFile(t, script))
		if !strings.Contains(body, `temporal-args -compose deploy/docker-compose.yml`) {
			t.Errorf("%s does not read the proof's Temporal image and command from deploy/docker-compose.yml with `proofrun temporal-args`", script)
		}
		for _, literal := range []string{"start-dev", dynamicConfigFlag, "VersionDrainageStatus"} {
			if strings.Contains(body, literal) {
				t.Errorf("%s names %q itself; the Temporal command belongs to deploy/docker-compose.yml alone", script, literal)
			}
		}
	}
}

// The quickstart is what a user runs to try OrkStack. Its Temporal keeps the
// cluster's own drainage timing, so a rollout there behaves as one anywhere
// else does.
func TestQuickstartCompose_TemporalKeepsTheClustersDrainageDefaults(t *testing.T) {
	t.Parallel()
	command := loadCompose(t).service(t, "temporal").Command
	if pairs := dynamicConfigValues(t, command); len(pairs) != 0 {
		t.Errorf("the quickstart's temporal passes dynamic-config values %q; they are development and proof settings", pairs)
	}
}
