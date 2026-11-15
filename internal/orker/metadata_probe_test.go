package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseMetadataProbeMode(t *testing.T) {
	tests := []struct {
		raw     string
		want    MetadataProbeMode
		wantErr bool
	}{
		{"", MetadataProbeOff, false},
		{"off", MetadataProbeOff, false},
		{" Warn ", MetadataProbeWarn, false},
		{"REFUSE", MetadataProbeRefuse, false},
		{"on", "", true},
		{"true", "", true},
		{"refuse,warn", "", true},
	}
	for _, tt := range tests {
		got, err := ParseMetadataProbeMode(tt.raw)
		if tt.wantErr {
			if err == nil || !strings.Contains(err.Error(), "orker.metadata_probe ("+MetadataProbeVar+")") {
				t.Errorf("ParseMetadataProbeMode(%q) = %q, %v; want a refusal naming the setting", tt.raw, got, err)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("ParseMetadataProbeMode(%q) = %q, %v; want %q", tt.raw, got, err, tt.want)
		}
	}
}

// probeTargets are two addresses: one asked twice, one asked once.
func probeTargets() []metadataTarget {
	return []metadataTarget{
		{Address: "a", Host: "a", Port: 80, Requests: []metadataRequest{httpGet("a", "/", ""), ec2TokenRequest("a")}},
		{Address: "b", Host: "b", Port: 80, Requests: []metadataRequest{httpGet("b", "/", "")}},
	}
}

// TestJudgeMetadataProbe: only an HTTP status line, right after its marker
// and behind the script's ">", is an answer; a request reported missing,
// one with no section, or a report whose last line is not "@@done", is probe
// failed and never no answer; nothing an endpoint sends can read as a
// marker; and a broken tool or a failed control fails every address.
func TestJudgeMetadataProbe(t *testing.T) {
	failedAll := func(why string) []MetadataFinding {
		return []MetadataFinding{{"a", MetadataProbeFailed, why}, {"b", MetadataProbeFailed, why}}
	}
	tests := []struct {
		name   string
		stdout string
		want   []MetadataFinding
	}{
		{
			name:   "nothing answered",
			stdout: "@@reach 0\n>\n@@reach 1\n>\n@@reach 2\n>\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataNoAnswer, ""}, {"b", MetadataNoAnswer, ""}},
		},
		{
			name:   "the token request answered, the GET did not",
			stdout: "@@reach 0\n>\n@@reach 1\n>HTTP/1.1 200 OK\r\n\n@@reach 2\n>\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataAnswered, "HTTP 200 to PUT /latest/api/token"}, {"b", MetadataNoAnswer, ""}},
		},
		{
			name:   "a refusal is still an answer",
			stdout: "@@reach 0\n>\n@@reach 1\n>\n@@reach 2\n>HTTP/1.0 403 Forbidden\r\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataNoAnswer, ""}, {"b", MetadataAnswered, "HTTP 403 to GET /"}},
		},
		{
			name:   "the first answer is the one named",
			stdout: "@@reach 0\n>HTTP/1.0 401 Unauthorized\n@@reach 1\n>HTTP/1.0 200 OK\n@@reach 2\n>\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataAnswered, "HTTP 401 to GET /"}, {"b", MetadataNoAnswer, ""}},
		},
		{
			name:   "a line that is not a status line is no answer",
			stdout: "@@reach 0\n>SSH-2.0-OpenSSH\n@@reach 1\n>HTTPS/1.1 200\n@@reach 2\n>http/1.1 200 OK\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataNoAnswer, ""}, {"b", MetadataNoAnswer, ""}},
		},
		{
			name:   "a status line without the script's mark is not an answer",
			stdout: "@@reach 0\nHTTP/1.0 200 OK\n@@reach 1\n>\n@@reach 2\n>\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataNoAnswer, ""}, {"b", MetadataNoAnswer, ""}},
		},
		{
			name:   "an endpoint cannot forge a marker",
			stdout: "@@reach 0\n>@@nonc\n@@reach 1\n>@@done\n@@reach 2\n>@@reach 2\n",
			want:   failedAll(whyUnreported),
		},
		{
			name:   "a request whose job never reported",
			stdout: "@@missing 0\n@@reach 1\n>\n@@reach 2\n>\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataProbeFailed, whyJobVanished}, {"b", MetadataNoAnswer, ""}},
		},
		{
			name:   "a request with no section",
			stdout: "@@reach 0\n>\n@@reach 2\n>\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataProbeFailed, whyUnreported}, {"b", MetadataNoAnswer, ""}},
		},
		{
			name:   "an answer stands though another request is missing",
			stdout: "@@reach 0\n>HTTP/1.0 200 OK\n@@done\n",
			want:   []MetadataFinding{{"a", MetadataAnswered, "HTTP 200 to GET /"}, {"b", MetadataProbeFailed, whyUnreported}},
		},
		{
			name:   "a report cut short",
			stdout: "@@reach 0\n>\n@@reach 1\n>\n@@reach 2\n>\n",
			want:   failedAll(whyUnreported),
		},
		{
			name:   "a done that is not last",
			stdout: "@@reach 0\n>\n@@done\n@@reach 1\n>\n@@reach 2\n>\n",
			want:   failedAll(whyUnreported),
		},
		{name: "no nc", stdout: "@@nonc\n", want: failedAll(whyNoNC)},
		{name: "no timeout", stdout: "@@notimeout\n", want: failedAll(whyNoTimeout)},
		{name: "no scratch directory", stdout: "@@notmp\n", want: failedAll(whyNoTmp)},
		{name: "the control failed", stdout: "@@ncbroken\n", want: failedAll(whyNCBroken)},
		{name: "empty", stdout: "", want: failedAll(whyUnreported)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := judgeMetadataProbe(probeTargets(), tt.stdout)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("judgeMetadataProbe =\n %v\nwant\n %v", got, tt.want)
			}
		})
	}
}

// TestMetadataProbeArgv: one (host, port, request) triple per request, in
// target order, after the script and its $0.
func TestMetadataProbeArgv(t *testing.T) {
	argv := metadataProbeArgv(probeTargets())
	if len(argv) != 4+3*3 {
		t.Fatalf("argv has %d words; want %d: %q", len(argv), 4+3*3, argv)
	}
	if argv[0] != "/bin/sh" || argv[1] != "-c" || argv[2] != metadataProbeScript {
		t.Fatalf("argv does not run the probe's script: %q", argv[:3])
	}
	want := []string{"a", "80", httpGet("a", "/", "").Raw, "a", "80", ec2TokenRequest("a").Raw, "b", "80", httpGet("b", "/", "").Raw}
	if !slices.Equal(argv[4:], want) {
		t.Fatalf("argv[4:] = %q; want %q", argv[4:], want)
	}
}

// TestDefaultMetadataTargets: every address of docs/DEPLOY.md's table, EC2's
// two with the token request.
func TestDefaultMetadataTargets(t *testing.T) {
	want := []string{"169.254.169.254", "fd00:ec2::254", "169.254.170.2", "169.254.170.23", "metadata.google.internal", "168.63.129.16"}
	var got []string
	for _, tg := range defaultMetadataTargets() {
		got = append(got, tg.Address)
		hasToken := slices.ContainsFunc(tg.Requests, func(r metadataRequest) bool { return r.Name == "PUT /latest/api/token" })
		if wantToken := tg.Address == "169.254.169.254" || tg.Address == "fd00:ec2::254"; hasToken != wantToken {
			t.Errorf("%s: token request %t; want %t", tg.Address, hasToken, wantToken)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("addresses = %q; want %q", got, want)
	}
}

func bootWithMetadataProbe(t *testing.T, mode MetadataProbeMode, set func(*countingDocker)) (*Activities, *countingDocker, string, error) {
	t.Helper()
	fake := newCountingDocker(t)
	fake.set("present", "")
	if set != nil {
		set(fake)
	}
	var logs bytes.Buffer
	id := fmt.Sprintf("orker-metaprobe-%d", os.Getpid())
	activities := NewActivitiesWithOptions(nil, Options{
		OrkerID:       id,
		WorkRoot:      t.TempDir(),
		Runtime:       &DockerRuntime{Bin: fake.bin},
		MetadataProbe: mode,
		Logger:        slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	t.Cleanup(activities.Close)
	activities.dispatcher.signalProbeGrace = 300 * time.Millisecond
	activities.dispatcher.metadataTargets = probeTargets()
	err := activities.Boot(context.Background())
	return activities, fake, logs.String(), err
}

// TestBoot_MetadataProbe: off runs no container; warn and refuse run one, on
// the default network, labelled with the Orker's identity and removed before
// Boot returns; an endpoint that answered refuses boot only under refuse,
// and a probe that failed refuses it there too, never read as safe.
func TestBoot_MetadataProbe(t *testing.T) {
	const answered = "@@reach 0\n>HTTP/1.0 200 OK\n@@reach 1\n>\n@@reach 2\n>\n@@done\n"
	const silent = "@@reach 0\n>\n@@reach 1\n>\n@@reach 2\n>\n@@done\n"
	tests := []struct {
		name       string
		mode       MetadataProbeMode
		out        string
		runFail    bool
		wantRun    bool
		wantReason string
		wantLog    string
		wantFirst  MetadataOutcome
	}{
		{name: "off", mode: MetadataProbeOff, out: answered},
		{name: "unset", mode: "", out: answered},
		{name: "warn answered", mode: MetadataProbeWarn, out: answered, wantRun: true,
			wantLog: "an instance metadata endpoint answered", wantFirst: MetadataAnswered},
		{name: "warn silent", mode: MetadataProbeWarn, out: silent, wantRun: true,
			wantLog: "no instance metadata endpoint answered", wantFirst: MetadataNoAnswer},
		{name: "warn failed", mode: MetadataProbeWarn, runFail: true, wantRun: true,
			wantLog: "this is not read as safe", wantFirst: MetadataProbeFailed},
		{name: "refuse answered", mode: MetadataProbeRefuse, out: answered, wantRun: true,
			wantReason: ReasonMetadataEndpointReachable, wantFirst: MetadataAnswered},
		{name: "refuse silent", mode: MetadataProbeRefuse, out: silent, wantRun: true,
			wantLog: "no instance metadata endpoint answered", wantFirst: MetadataNoAnswer},
		{name: "refuse failed", mode: MetadataProbeRefuse, runFail: true, wantRun: true,
			wantReason: ReasonMetadataProbeFailed, wantFirst: MetadataProbeFailed},
		{name: "refuse cut short", mode: MetadataProbeRefuse, out: "@@reach 0\n>\n", wantRun: true,
			wantReason: ReasonMetadataProbeFailed, wantFirst: MetadataProbeFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activities, fake, logs, err := bootWithMetadataProbe(t, tt.mode, func(f *countingDocker) {
				f.set("metaout", tt.out)
				if tt.runFail {
					f.set("metarunfail", "")
				}
			})
			switch {
			case tt.wantReason == "" && err != nil:
				t.Fatalf("Boot = %v; want it to start", err)
			case tt.wantReason != "" && (err == nil || !strings.HasPrefix(err.Error(), tt.wantReason+": ")):
				t.Fatalf("Boot = %v; want a refusal %s", err, tt.wantReason)
			}
			if tt.wantReason == ReasonMetadataEndpointReachable && !errors.Is(err, ErrMetadataEndpointReachable) {
				t.Errorf("Boot = %v; want it to wrap ErrMetadataEndpointReachable", err)
			}
			if tt.wantReason == ReasonMetadataProbeFailed && !errors.Is(err, ErrMetadataProbeFailed) {
				t.Errorf("Boot = %v; want it to wrap ErrMetadataProbeFailed", err)
			}
			var runs []string
			for _, call := range fake.calls() {
				if strings.HasPrefix(call, "run ") && strings.Contains(call, "--label ork.task="+metadataProbeTaskID) {
					runs = append(runs, call)
				}
			}
			if !tt.wantRun {
				if len(runs) != 0 {
					t.Fatalf("mode %q ran the metadata probe: %q", tt.mode, runs)
				}
				if activities.metadata.Load() != nil {
					t.Fatal("findings recorded though no probe ran")
				}
				return
			}
			if len(runs) == 0 {
				t.Fatalf("mode %q ran no metadata probe container", tt.mode)
			}
			for _, call := range runs {
				if !strings.Contains(call, "--network "+NetworkDefault+" ") {
					t.Errorf("the metadata probe does not run on the default network: %s", call)
				}
				if !strings.Contains(call, "--label ork.orker="+activities.dispatcher.OrkerID+" ") {
					t.Errorf("the metadata probe is not labelled with the Orker's identity: %s", call)
				}
			}
			if !tt.runFail {
				ran, removed := probeContainerNames(t, fake.calls())
				for _, name := range ran {
					if !slices.Contains(removed, name) {
						t.Errorf("probe container %s not removed; removed %q", name, removed)
					}
				}
			}
			found := activities.metadata.Load()
			if found == nil || len(*found) != 2 || (*found)[0].Outcome != tt.wantFirst {
				t.Fatalf("recorded findings = %v; want two, the first %s", found, tt.wantFirst)
			}
			if tt.wantReason == "" {
				described, err := activities.DescribeOrker(context.Background())
				if err != nil {
					t.Fatalf("DescribeOrker = %v", err)
				}
				if got := described.GetMetadataProbes(); len(got) != 2 || got[0].GetAddress() != "a" || got[0].GetOutcome() != tt.wantFirst.proto() {
					t.Errorf("DescribeOrker's metadata_probes = %v; want two, the first a %s", got, tt.wantFirst)
				}
				wantReachable := []string(nil)
				if tt.wantFirst == MetadataAnswered {
					wantReachable = []string{"a"}
				}
				if got := described.GetMetadataReachable(); !slices.Equal(got, wantReachable) {
					t.Errorf("DescribeOrker's metadata_reachable = %q; want %q", got, wantReachable)
				}
			}
			if tt.wantLog != "" && !strings.Contains(logs, tt.wantLog) {
				t.Errorf("logs do not say %q:\n%s", tt.wantLog, logs)
			}
		})
	}
}

// detailForms are every Detail judgeMetadataProbe may give: words of its
// own, a three-digit status and a request's own name, never more of what the
// container printed.
var detailForms = regexp.MustCompile(`^(|HTTP [0-9]{3} to (GET /|PUT /latest/api/token)|` +
	regexp.QuoteMeta(whyNoNC) + `|` + regexp.QuoteMeta(whyNoTimeout) + `|` + regexp.QuoteMeta(whyNoTmp) + `|` +
	regexp.QuoteMeta(whyNCBroken) + `|` + regexp.QuoteMeta(whyNCBroken6) + `|` + regexp.QuoteMeta(whyBadAddress) + `|` + regexp.QuoteMeta(whyUnreported) + `|` + regexp.QuoteMeta(whyJobVanished) + `)$`)

// FuzzJudgeMetadataProbe holds what the reader of the probe's container
// output claims: it never panics; it gives one finding per target, in
// order, each with one of the three outcomes; an answer is reported only
// where the output held an HTTP status line; and a detail never carries more
// of the output than a status code.
func FuzzJudgeMetadataProbe(f *testing.F) {
	f.Add("@@reach 0\n>HTTP/1.0 200 OK\n@@reach 1\n>\n@@reach 2\n>\n@@done\n")
	f.Add("@@reach 0\n>\n@@reach 2\n>\n@@done\n")
	f.Add("@@missing 0\n@@reach 1\n>@@done\n@@ncbroken\n")
	f.Add("@@nonc\n")
	f.Add("@@reach -1\n>HTTP/1.1 200\n@@reach 99999999999999999999\n@@done")
	f.Add("")
	f.Fuzz(func(t *testing.T, stdout string) {
		targets := probeTargets()
		got := judgeMetadataProbe(targets, stdout)
		if len(got) != len(targets) {
			t.Fatalf("%d findings for %d targets", len(got), len(targets))
		}
		for i, fd := range got {
			if fd.Address != targets[i].Address {
				t.Fatalf("finding %d names %q; want %q", i, fd.Address, targets[i].Address)
			}
			switch fd.Outcome {
			case MetadataAnswered:
				if !strings.Contains(stdout, ">HTTP/") {
					t.Fatalf("answered with no status line in %q", stdout)
				}
			case MetadataNoAnswer, MetadataProbeFailed:
			default:
				t.Fatalf("finding %d has outcome %d", i, fd.Outcome)
			}
			if !detailForms.MatchString(fd.Detail) {
				t.Fatalf("detail %q is not one of the reader's own", fd.Detail)
			}
		}
	})
}

// TestJudgeMetadataProbe_AnAddressNCCouldNotUse: nc refusing an IP literal is
// a request never asked, so probe failed; a name that does not resolve is
// unreachable to a task as well, so no answer. A container with IPv6 whose
// IPv6 loopback exchange failed fails every address.
func TestJudgeMetadataProbe_AnAddressNCCouldNotUse(t *testing.T) {
	targets := []metadataTarget{
		{Address: "fd00:ec2::254", Host: "fd00:ec2::254", Port: 80, Requests: []metadataRequest{httpGet("[fd00:ec2::254]", "/", "")}},
		{Address: "metadata.google.internal", Host: "metadata.google.internal", Port: 80, Requests: []metadataRequest{httpGet("metadata.google.internal", "/", "")}},
	}
	got := judgeMetadataProbe(targets, "@@reach 0\n>\n@@badaddr 0\n@@reach 1\n>\n@@badaddr 1\n@@done\n")
	want := []MetadataFinding{{"fd00:ec2::254", MetadataProbeFailed, whyBadAddress}, {"metadata.google.internal", MetadataNoAnswer, ""}}
	if !slices.Equal(got, want) {
		t.Fatalf("judgeMetadataProbe = %v; want %v", got, want)
	}
	got = judgeMetadataProbe(targets, "@@ncbroken6\n")
	if got[0].Outcome != MetadataProbeFailed || got[0].Detail != whyNCBroken6 || got[1].Outcome != MetadataProbeFailed {
		t.Fatalf("judgeMetadataProbe(@@ncbroken6) = %v; want every address probe failed", got)
	}
}

// TestBoot_AProbeThatDidNotRunRepeatsNothingTheContainerPrinted: a runtime's
// error can quote the container's stdout, which holds what an endpoint sent;
// neither the refusal, the findings DescribeOrker carries, nor the log repeats
// it, a control character included.
func TestBoot_AProbeThatDidNotRunRepeatsNothingTheContainerPrinted(t *testing.T) {
	activities, _, logs, err := bootWithMetadataProbe(t, MetadataProbeRefuse, func(f *countingDocker) {
		f.set("metarunsaid", "")
	})
	if err == nil || !strings.HasPrefix(err.Error(), ReasonMetadataProbeFailed+": ") {
		t.Fatalf("Boot = %v; want %s", err, ReasonMetadataProbeFailed)
	}
	described := MetadataFindings(activities)
	if len(described) == 0 {
		t.Fatal("no findings recorded")
	}
	for _, text := range append([]string{err.Error(), logs}, described[0].Detail) {
		if strings.Contains(text, "secret-token") || strings.Contains(text, "\x1b") {
			t.Errorf("repeats what the container printed: %q", text)
		}
	}
	if described[0].Detail != "its container did not run: the container runtime refused it or it exited non-zero" {
		t.Errorf("detail = %q; want the Orker's own words", described[0].Detail)
	}
}

// TestJudgeMetadataProbe_ATargetWithNoRequestIsNeverClean: nothing asked is
// probe failed, never no answer.
func TestJudgeMetadataProbe_ATargetWithNoRequestIsNeverClean(t *testing.T) {
	got := judgeMetadataProbe([]metadataTarget{{Address: "x", Host: "x", Port: 80}}, "@@done\n")
	if len(got) != 1 || got[0].Outcome != MetadataProbeFailed {
		t.Fatalf("judgeMetadataProbe = %v; want probe failed", got)
	}
}

// TestBoot_AModeOutsideTheThreeIsRefusedOrRead: a mode handed straight to
// Options is read as the setting is, so "Refuse " refuses; anything else
// refuses boot naming the setting, never a probe that only warns.
func TestBoot_AModeOutsideTheThreeIsRefusedOrRead(t *testing.T) {
	const answered = "@@reach 0\n>HTTP/1.0 200 OK\n@@reach 1\n>\n@@reach 2\n>\n@@done\n"
	_, _, _, err := bootWithMetadataProbe(t, "Refuse ", func(f *countingDocker) { f.set("metaout", answered) })
	if err == nil || !strings.HasPrefix(err.Error(), ReasonMetadataEndpointReachable+": ") {
		t.Fatalf("Boot(%q) = %v; want %s", "Refuse ", err, ReasonMetadataEndpointReachable)
	}
	_, fake, _, err := bootWithMetadataProbe(t, "loud", func(f *countingDocker) { f.set("metaout", answered) })
	if err == nil || !strings.Contains(err.Error(), MetadataProbeVar) {
		t.Fatalf("Boot(%q) = %v; want a refusal naming %s", "loud", err, MetadataProbeVar)
	}
	for _, call := range fake.calls() {
		if strings.Contains(call, "ork.task="+metadataProbeTaskID) {
			t.Fatalf("an unreadable mode ran the probe: %s", call)
		}
	}
}
