package orker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestDialect_DefaultRuntimeIsRuncOrCrun: the engine's default OCI runtime is
// the one a CDI spec's hooks run under, so it must be runc or crun, by name and
// by the binary the name points at (design §4.10 item 2).
func TestDialect_DefaultRuntimeIsRuncOrCrun(t *testing.T) {
	for _, ok := range []runtimeFacts{{"runc", "runc"}, {"crun", "/usr/bin/crun"}, {"runc", "/usr/local/sbin/runc"}} {
		if err := checkDefaultRuntime(ok); err != nil {
			t.Errorf("checkDefaultRuntime(%+v) = %v", ok, err)
		}
	}
	for _, bad := range []runtimeFacts{
		{"", ""}, {"nvidia", "nvidia-container-runtime"}, {"runsc", "runsc"}, {"kata", "/usr/bin/kata-runtime"},
		{"RUNC", "runc"}, {"runc", ""}, {"runc", "/usr/bin/nvidia-container-runtime"}, {"crun", "/opt/runc-wrapper"},
		{"runc", "/usr/bin/runc/../nvidia-container-runtime"},
	} {
		if err := checkDefaultRuntime(bad); err == nil || !strings.Contains(err.Error(), "runc or crun") {
			t.Errorf("checkDefaultRuntime(%+v) = %v, want a refusal naming runc or crun", bad, err)
		}
	}
}

func TestDockerRuntime_AsksTheEnginesDefaultRuntime(t *testing.T) {
	for _, tt := range []struct {
		name, cli, answer, asked string
		want                     runtimeFacts
		refused                  bool
	}{
		{"docker, runc", CLIDocker, "", "info --format {\"name\":{{json .DefaultRuntime}}", runtimeFacts{"runc", "runc"}, false},
		{"docker, runc registered as the nvidia wrapper", CLIDocker, `{"name":"runc","runtimes":{"runc":{"path":"/usr/bin/nvidia-container-runtime"}}}`, "info --format {\"name\"",
			runtimeFacts{"runc", "/usr/bin/nvidia-container-runtime"}, true},
		{"docker, the nvidia runtime", CLIDocker, `{"name":"nvidia","runtimes":{"nvidia":{"path":"nvidia-container-runtime"},"runc":{"path":"runc"}}}`, "info --format {\"name\"",
			runtimeFacts{"nvidia", "nvidia-container-runtime"}, true},
		{"podman CLI, crun", CLIPodman, "", "info --format {{json .Host.OCIRuntime}}", runtimeFacts{"crun", "/usr/bin/crun"}, false},
		{"podman CLI, kata", CLIPodman, `{"name":"kata","path":"/usr/bin/kata"}`, "info --format {{json .Host.OCIRuntime}}", runtimeFacts{"kata", "/usr/bin/kata"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			if tt.answer != "" {
				fake.set("defaultruntime", tt.answer)
			}
			got, err := (&DockerRuntime{Bin: fake.bin, CLI: tt.cli}).DefaultRuntime(context.Background())
			if err != nil || got != tt.want {
				t.Fatalf("DefaultRuntime = %+v, %v; want %+v", got, err, tt.want)
			}
			if refused := checkDefaultRuntime(got) != nil; refused != tt.refused {
				t.Errorf("%+v: refused = %v, want %v", got, refused, tt.refused)
			}
			if calls := fake.calls(); countPrefix(calls, tt.asked) != 1 {
				t.Errorf("calls = %q; want the CLI asked %q once", calls, tt.asked)
			}
		})
	}
	for name, setup := range map[string]func(*countingDocker){
		"a failed question":          func(f *countingDocker) { f.set("infofail", "") },
		"an answer that is not JSON": func(f *countingDocker) { f.set("defaultruntime", "runc\n") },
	} {
		t.Run(name, func(t *testing.T) {
			fake := newCountingDocker(t)
			setup(fake)
			if got, err := (&DockerRuntime{Bin: fake.bin}).DefaultRuntime(context.Background()); err == nil {
				t.Errorf("answered %+v", got)
			}
		})
	}
}

type runtimeNamer struct {
	facts runtimeFacts
	err   error
}

func (r runtimeNamer) DefaultRuntime(context.Context) (runtimeFacts, error) { return r.facts, r.err }

// TestActivities_AuditDefaultRuntime: an Orker with any profile asks and
// refuses a default runtime that is not runc or crun (ORKER_PROFILE_INVALID,
// DEFAULT_RUNTIME_REFUSED); one that cannot say is refused too, and so is a
// runtime that cannot be asked at all (fail closed); one with no profile asks
// nothing.
func TestActivities_AuditDefaultRuntime(t *testing.T) {
	withProfile := newProfileState(gpuProfiles(), "")
	none := newProfileState(Profiles{}, "")
	for _, tt := range []struct {
		name    string
		state   *profileState
		runtime any
		want    string
	}{
		{"runc", withProfile, runtimeNamer{facts: runtimeFacts{"runc", "runc"}}, ""},
		{"nvidia", withProfile, runtimeNamer{facts: runtimeFacts{"nvidia", "nvidia-container-runtime"}}, DetailDefaultRuntimeRefused},
		{"runc that is the wrapper", withProfile, runtimeNamer{facts: runtimeFacts{"runc", "/usr/bin/nvidia-container-runtime"}}, DetailDefaultRuntimeRefused},
		{"cannot say", withProfile, runtimeNamer{err: context.DeadlineExceeded}, DetailDefaultRuntimeRefused},
		{"no profile", none, runtimeNamer{facts: runtimeFacts{"nvidia", "x"}}, ""},
		{"a runtime that cannot be asked, with a profile", withProfile, struct{}{}, DetailDefaultRuntimeRefused},
		{"a runtime that cannot be asked, no profile", none, struct{}{}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := auditDefaultRuntime(context.Background(), tt.state, tt.runtime)
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("= %v", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), ReasonProfileInvalid)):
				t.Fatalf("= %v, want %s under %s", err, tt.want, ReasonProfileInvalid)
			}
		})
	}
}

// TestProfileState_RuntimeChangeIsDrift: the runtime is asked again before
// create and by the periodic audit; another one than the boot audit found
// latches a drift (also one that is now refused), and one that cannot be asked
// fails the attempt as unavailable with nothing latched.
func TestProfileState_RuntimeChangeIsDrift(t *testing.T) {
	boot := runtimeFacts{"runc", "runc"}
	reason := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_VIEW_CHANGED
	for name, tc := range map[string]struct {
		now   runtimeNamer
		drift bool
		fail  string
	}{
		"unchanged": {runtimeNamer{facts: boot}, false, ""},
		"crun now, which is allowed but not what was audited": {runtimeNamer{facts: runtimeFacts{"crun", "crun"}}, true, ReasonProfileDrifted},
		"the wrapper now": {runtimeNamer{facts: runtimeFacts{"runc", "/usr/bin/nvidia-container-runtime"}}, true, ReasonProfileDrifted},
		"cannot be asked": {runtimeNamer{err: errors.New("daemon gone")}, false, ReasonRuntimeStateUnknown},
	} {
		t.Run(name, func(t *testing.T) {
			s := newProfileState(gpuProfiles(), "")
			s.useRuntime(tc.now, boot)
			failure := s.checkRuntime(context.Background(), []string{"amd"}, time.Now())
			switch {
			case tc.fail == "" && failure != nil:
				t.Fatalf("checkRuntime = %v", failure)
			case tc.fail != "" && (failure == nil || failure.Reason != tc.fail):
				t.Fatalf("checkRuntime = %v, want %s", failure, tc.fail)
			}
			if s.drifted("amd") != tc.drift {
				t.Fatalf("drifted = %v, want %v", s.drifted("amd"), tc.drift)
			}
			if tc.drift {
				r := &orkstackv1.DescribeOrkerResult{}
				s.report(r)
				if got := r.GetProfileCordons(); len(got) != 1 || got[0].GetReason() != reason || !strings.Contains(got[0].GetDetail(), DetailDefaultRuntimeRefused) {
					t.Fatalf("cordons = %v", got)
				}
			}
			// The periodic audit reaches the same verdict.
			p := newProfileState(gpuProfiles(), "")
			p.useRuntime(tc.now, boot)
			if found, askErr := p.auditRuntimeOnce(context.Background(), time.Now()); (askErr != nil) != (tc.now.err != nil) || (len(found) > 0) != tc.drift || p.drifted("amd") != tc.drift {
				t.Fatalf("auditRuntimeOnce = %v, drifted %v, want drift %v", found, p.drifted("amd"), tc.drift)
			}
		})
	}
	// With no runtime recorded (no profile asked it) nothing is checked.
	if f := newProfileState(gpuProfiles(), "").checkRuntime(context.Background(), []string{"amd"}, time.Now()); f != nil {
		t.Errorf("checkRuntime with no asker = %v", f)
	}
}

// TestDockerRuntime_CDISpecDirs: the engine's own list when it reports one, its
// documented defaults (said not to be reported) when it cannot, and the podman
// CLI's defaults.
func TestDockerRuntime_CDISpecDirs(t *testing.T) {
	for _, tt := range []struct {
		name, cli, flag, body string
		want                  string
		reported              bool
	}{
		{"docker's own list", CLIDocker, "cdidirs", `["/etc/cdi","/srv/cdi"]`, "/etc/cdi,/srv/cdi", true},
		{"docker, the default list", CLIDocker, "", "", "/etc/cdi,/var/run/cdi", true},
		{"docker, null", CLIDocker, "cdidirsnull", "", "/etc/cdi,/var/run/cdi", false},
		{"podman CLI", CLIPodman, "", "", "/etc/cdi,/var/run/cdi", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			if tt.flag != "" {
				fake.set(tt.flag, tt.body)
			}
			dirs, reported, err := (&DockerRuntime{Bin: fake.bin, CLI: tt.cli}).CDISpecDirs(context.Background())
			if err != nil || strings.Join(dirs, ",") != tt.want || reported != tt.reported {
				t.Fatalf("CDISpecDirs = %q, %v, %v; want %q, %v", dirs, reported, err, tt.want, tt.reported)
			}
		})
	}
	fake := newCountingDocker(t)
	fake.set("cdidirsfail", "")
	if dirs, _, err := (&DockerRuntime{Bin: fake.bin}).CDISpecDirs(context.Background()); err == nil || !strings.Contains(err.Error(), "exited") {
		t.Errorf("an engine that exits non-zero on the question gave %q, %v; want a failure, not the defaults", dirs, err)
	}
	fake = newCountingDocker(t)
	fake.set("cdidirs", "[not json")
	if dirs, _, err := (&DockerRuntime{Bin: fake.bin}).CDISpecDirs(context.Background()); err == nil {
		t.Errorf("a garbled answer gave %q", dirs)
	}
}

// countingRuntime counts how often the engine is asked.
type countingRuntime struct {
	facts runtimeFacts
	err   error
	asked int
	ctxs  []context.Context
}

func (c *countingRuntime) DefaultRuntime(ctx context.Context) (runtimeFacts, error) {
	c.asked++
	c.ctxs = append(c.ctxs, ctx)
	return c.facts, c.err
}

type attemptCtxKey struct{}

// TestRecheck_AsksTheRuntimeOncePerAttempt: the engine is asked at the
// pre-create recheck alone, once however many profiles the task names, with the
// attempt's own context; 3c and the recheck before the start do not ask. A
// question it cannot answer is a never-started RUNTIME_STATE_UNKNOWN, which the
// engine re-dispatches without spending the task's own attempt (emission.go).
func TestRecheck_AsksTheRuntimeOncePerAttempt(t *testing.T) {
	boot := runtimeFacts{"runc", "runc"}
	s, h, _ := nvidiaState(t)
	s.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
	asker := &countingRuntime{facts: boot}
	s.useRuntime(asker, boot)
	a, _ := s.audit("nv")
	refs := []*orkstackv1.ProfileRef{{Name: "nv", IdentityDigest: a.identity}, {Name: "nv", IdentityDigest: a.identity}}
	ctx := context.WithValue(context.Background(), attemptCtxKey{}, "attempt")

	if _, failure := s.gate(refs, h.inspect, time.Now()); failure != nil || asker.asked != 0 {
		t.Fatalf("3c: %v, asked %d times; want no question", failure, asker.asked)
	}
	if failure := s.recheck(ctx, refs, h.inspect, time.Now(), false); failure != nil || asker.asked != 0 {
		t.Fatalf("before the start: %v, asked %d times; want no question", failure, asker.asked)
	}
	if failure := s.recheck(ctx, refs, h.inspect, time.Now(), true); failure != nil || asker.asked != 1 {
		t.Fatalf("before create: %v, asked %d times; want once for two refs", failure, asker.asked)
	}
	if asker.ctxs[0].Value(attemptCtxKey{}) != "attempt" {
		t.Error("the question was asked on a context that is not the attempt's")
	}

	asker.err = errors.New("daemon gone")
	failure := s.recheck(ctx, refs, h.inspect, time.Now(), true)
	if failure == nil || failure.Reason != ReasonRuntimeStateUnknown || !failure.NeverStarted || s.drifted("nv") {
		t.Fatalf("an engine that cannot answer: %v, want a never-started RUNTIME_STATE_UNKNOWN with nothing latched", failure)
	}
	if !failure.mayEmit(false) {
		t.Error("RUNTIME_STATE_UNKNOWN never-started is not an interruption the Orker may emit")
	}
	// A cancelled attempt's context ends the question.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	asker.err = nil
	asker.facts = runtimeFacts{"runc", "/usr/bin/nvidia-container-runtime"}
	if failure := s.recheck(cancelled, refs, h.inspect, time.Now(), true); failure == nil {
		t.Error("a runtime now refused passed")
	}
}
