package docscheck

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v4"

	"github.com/orkstack/orkstack/internal/orker"
)

// stopBase is one shipped way of running an Orker, with the grace the Orker
// stops with there and the time its process manager gives it to do so.
type stopBase struct {
	name, kind string
	grace      time.Duration
	// graceFrom says where the grace came from, for the failure message: a
	// setting the base names, or the Orker's own default when it names none.
	graceFrom string
	timeout   time.Duration
	// infinite: the manager never kills the Orker (systemd's `infinity`).
	infinite bool
	// timeoutFrom says where the timeout came from, or that the base names
	// none and the platform's own default applies.
	timeoutFrom string
}

// The platforms' own stop timeouts when a base names none (docs/DEPLOY.md,
// "Stop timeouts"): Kubernetes' 30 s, Compose's 10 s, systemd's 90 s.
const (
	kubernetesDefaultStop = 30 * time.Second
	composeDefaultStop    = 10 * time.Second
	systemdDefaultStop    = 90 * time.Second
)

// TestDeploy_BasesStopTimeoutCoverTheGrace: every base the repository ships
// for running an Orker gives it a stop timeout of at least its stop grace plus
// the tail a stop can take past it (orker.StopTimeoutTail, DEPLOY.md "Stop
// timeouts"): a Kubernetes Deployment's terminationGracePeriodSeconds, a
// Compose service's stop_grace_period, a systemd unit's TimeoutStopSec. A
// manager that kills the Orker sooner loses the tasks the grace was to let
// finish. A base that names no setting gets its platform's default, which is
// far too short, and a base that is not there at all is a failure, never a
// skip. internal/dist's tests hold the quickstart and the two Kubernetes
// shapes to their exact figures; this holds every base found, wherever it
// is, to the rule, so a new one cannot ship short.
func TestDeploy_BasesStopTimeoutCoverTheGrace(t *testing.T) {
	root := repoRoot(t)
	var bases []stopBase
	bases = append(bases, kubernetesBases(t, root)...)
	bases = append(bases, composeBases(t, root)...)
	bases = append(bases, systemdBases(t, root)...)

	have := map[string]int{}
	for _, b := range bases {
		have[b.kind]++
	}
	for _, want := range []struct {
		kind string
		min  int
	}{{"kubernetes", 2}, {"compose", 1}, {"systemd", 1}} {
		if have[want.kind] < want.min {
			t.Errorf("found %d %s bases of an Orker, want at least %d: a base this test cannot find is not held to the rule",
				have[want.kind], want.kind, want.min)
		}
	}
	for _, b := range bases {
		need := b.grace + orker.StopTimeoutTail
		t.Logf("%s (%s): grace %v (%s), stop timeout %v (%s), needs %v", b.name, b.kind, b.grace, b.graceFrom, b.timeout, b.timeoutFrom, need)
		if !b.infinite && b.timeout < need {
			t.Errorf("%s (%s): stop timeout %v (%s) is below its stop grace %v (%s) plus the stop's tail %v, %v: "+
				"its running tasks would be lost, not handed back (set it to at least %v)",
				b.name, b.kind, b.timeout, b.timeoutFrom, b.grace, b.graceFrom, orker.StopTimeoutTail, need, need)
		}
	}
}

// walkFiles calls visit with the repository-relative path of every regular
// file under dir that match accepts.
func walkFiles(t *testing.T, root, dir string, match func(rel string) bool, visit func(rel string, raw []byte)) {
	t.Helper()
	err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "node_modules" || d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if !match(rel) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		visit(rel, raw)
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
}

// graceOf is the stop grace a base's environment gives its Orker, or the
// Orker's default when it names none.
func graceOf(t *testing.T, name, raw string, set bool) (time.Duration, string) {
	t.Helper()
	if !set {
		return orker.DefaultStopGrace, "the Orker's default, the base sets none"
	}
	grace, err := orker.ParseStopGrace(raw)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return grace, orker.StopGraceVar + "=" + raw
}

// kubernetesBases are the Deployments (and any other pod template) under
// deploy/kubernetes that run a container named orker. The check's
// kustomizations are patches of a deployment for a cluster check, not a base
// of one: they run a zero grace and short tasks within a 120 s timeout on
// purpose, and are held by internal/dist's tests to what they do.
func kubernetesBases(t *testing.T, root string) []stopBase {
	type container struct {
		Name string `yaml:"name"`
		Env  []struct {
			Name  string `yaml:"name"`
			Value string `yaml:"value"`
		} `yaml:"env"`
	}
	type doc struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Spec struct {
			Template struct {
				Spec struct {
					Termination *int64      `yaml:"terminationGracePeriodSeconds"`
					Containers  []container `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	var bases []stopBase
	walkFiles(t, root, "deploy/kubernetes", func(rel string) bool {
		return strings.HasSuffix(rel, ".yaml") && !strings.HasPrefix(rel, "deploy/kubernetes/check/")
	}, func(rel string, raw []byte) {
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		for {
			var d doc
			if err := dec.Decode(&d); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				t.Fatalf("%s: %v", rel, err)
			}
			for _, c := range d.Spec.Template.Spec.Containers {
				if c.Name != "orker" {
					continue
				}
				var value string
				var set bool
				for _, e := range c.Env {
					if e.Name == orker.StopGraceVar {
						value, set = e.Value, true
					}
				}
				name := fmt.Sprintf("%s %s/%s", rel, d.Kind, d.Metadata.Name)
				grace, from := graceOf(t, name, value, set)
				b := stopBase{name: name, kind: "kubernetes", grace: grace, graceFrom: from,
					timeout: kubernetesDefaultStop, timeoutFrom: "the platform's default, the base sets none"}
				if d.Spec.Template.Spec.Termination != nil {
					b.timeout = time.Duration(*d.Spec.Template.Spec.Termination) * time.Second
					b.timeoutFrom = "terminationGracePeriodSeconds: " + strconv.FormatInt(*d.Spec.Template.Spec.Termination, 10)
				}
				bases = append(bases, b)
			}
		}
	})
	return bases
}

// composeOverrides are Compose files merged over another base, which inherit
// its stop grace and stop timeout and set neither of their own.
var composeOverrides = map[string]string{
	"deploy/release-smoke.yaml": "the release smoke's override of compose.yaml, which keeps its Orker's grace and stop_grace_period",
}

// composeBases are the services running the Orker in a Compose file at the
// repository root or under deploy/: the service called orker, or built from
// the orker target.
func composeBases(t *testing.T, root string) []stopBase {
	type service struct {
		Build       yaml.Node `yaml:"build"`
		Environment yaml.Node `yaml:"environment"`
		StopGrace   *string   `yaml:"stop_grace_period"`
	}
	type file struct {
		Services map[string]service `yaml:"services"`
	}
	var bases []stopBase
	visit := func(rel string, raw []byte) {
		if reason, ok := composeOverrides[rel]; ok {
			t.Logf("%s is not a base: %s", rel, reason)
			return
		}
		var f file
		if err := yaml.Unmarshal(raw, &f); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		for name, svc := range f.Services {
			target := ""
			if svc.Build.Kind == yaml.MappingNode {
				for i := 0; i+1 < len(svc.Build.Content); i += 2 {
					if svc.Build.Content[i].Value == "target" {
						target = svc.Build.Content[i+1].Value
					}
				}
			}
			if name != "orker" && target != "orker" {
				continue
			}
			env, set := composeEnv(svc.Environment, orker.StopGraceVar)
			label := fmt.Sprintf("%s service %s", rel, name)
			grace, from := graceOf(t, label, env, set)
			b := stopBase{name: label, kind: "compose", grace: grace, graceFrom: from,
				timeout: composeDefaultStop, timeoutFrom: "the platform's default, the base sets none"}
			if svc.StopGrace != nil {
				d, err := time.ParseDuration(*svc.StopGrace)
				if err != nil {
					t.Fatalf("%s: stop_grace_period %q: %v", label, *svc.StopGrace, err)
				}
				b.timeout, b.timeoutFrom = d, "stop_grace_period: "+*svc.StopGrace
			}
			bases = append(bases, b)
		}
	}
	match := func(rel string) bool {
		return strings.HasSuffix(rel, ".yml") || strings.HasSuffix(rel, ".yaml")
	}
	for _, name := range []string{"compose.yaml", "compose.yml", "docker-compose.yml", "docker-compose.yaml"} {
		if raw, err := os.ReadFile(filepath.Join(root, name)); err == nil {
			visit(name, raw)
		}
	}
	walkFiles(t, root, "deploy", func(rel string) bool {
		return match(rel) && !strings.HasPrefix(rel, "deploy/kubernetes/") && !strings.HasPrefix(rel, "deploy/observability/")
	}, visit)
	return bases
}

// composeEnv reads one variable of a Compose service's environment, written
// as a mapping or as a list of KEY=value.
func composeEnv(n yaml.Node, key string) (string, bool) {
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == key {
				return n.Content[i+1].Value, true
			}
		}
	case yaml.SequenceNode:
		for _, item := range n.Content {
			if k, v, ok := strings.Cut(item.Value, "="); ok && k == key {
				return v, true
			}
		}
	}
	return "", false
}

var iniBlock = regexp.MustCompile("(?s)```ini\n(.*?)```")

// systemdBases are the units of an Orker the repository ships: a .service
// file, or an ini block of docs/DEPLOY.md whose ExecStart runs the orker
// binary.
func systemdBases(t *testing.T, root string) []stopBase {
	var bases []stopBase
	unit := func(label, text string) {
		var execs, timeoutRaw, graceRaw string
		var graceSet, timeoutSet bool
		for _, line := range strings.Split(text, "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
			if !ok {
				continue
			}
			switch key {
			case "ExecStart":
				execs += value + "\n"
			case "TimeoutStopSec":
				timeoutRaw, timeoutSet = value, true
			case "Environment":
				for _, assignment := range strings.Fields(strings.Trim(value, `"`)) {
					if k, v, ok := strings.Cut(strings.Trim(assignment, `"`), "="); ok && k == orker.StopGraceVar {
						graceRaw, graceSet = v, true
					}
				}
			}
		}
		if !strings.Contains(execs, "orker") {
			return
		}
		grace, from := graceOf(t, label, graceRaw, graceSet)
		b := stopBase{name: label, kind: "systemd", grace: grace, graceFrom: from,
			timeout: systemdDefaultStop, timeoutFrom: "systemd's default, the unit sets no TimeoutStopSec"}
		if timeoutSet {
			d, infinite, err := parseSystemdTimespan(timeoutRaw)
			if err != nil {
				t.Fatalf("%s: TimeoutStopSec=%q: %v", label, timeoutRaw, err)
			}
			b.timeout, b.infinite, b.timeoutFrom = d, infinite, "TimeoutStopSec="+timeoutRaw
		}
		bases = append(bases, b)
	}
	walkFiles(t, root, ".", func(rel string) bool {
		return strings.HasSuffix(rel, ".service") || rel == "docs/DEPLOY.md"
	}, func(rel string, raw []byte) {
		if strings.HasSuffix(rel, ".service") {
			unit(rel, string(raw))
			return
		}
		for i, m := range iniBlock.FindAllStringSubmatch(string(raw), -1) {
			unit(fmt.Sprintf("%s ini block %d", rel, i+1), m[1])
		}
	})
	return bases
}

var systemdSpan = regexp.MustCompile(`(\d+(?:\.\d+)?)\s*([a-zµ]*)`)

// parseSystemdTimespan reads a TimeoutStopSec value: `infinity`, a bare
// number of seconds, or numbers with units (`24h15min`, `87300s`, `1d 2h`).
// systemd-analyze timespan is the established reader but needs systemd on the
// host, which a unit test must not; the orker package's reader is unexported.
func parseSystemdTimespan(raw string) (d time.Duration, infinite bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "infinity" {
		return 0, true, nil
	}
	units := map[string]time.Duration{
		"": time.Second, "s": time.Second, "sec": time.Second, "second": time.Second, "seconds": time.Second,
		"ms": time.Millisecond, "msec": time.Millisecond, "us": time.Microsecond, "µs": time.Microsecond, "usec": time.Microsecond,
		"m": time.Minute, "min": time.Minute, "minute": time.Minute, "minutes": time.Minute,
		"h": time.Hour, "hr": time.Hour, "hour": time.Hour, "hours": time.Hour,
		"d": 24 * time.Hour, "day": 24 * time.Hour, "days": 24 * time.Hour,
	}
	matches := systemdSpan.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 || strings.Join(strings.Fields(systemdSpan.ReplaceAllString(raw, "")), "") != "" {
		return 0, false, fmt.Errorf("not a systemd time span")
	}
	for _, m := range matches {
		unit, ok := units[m[2]]
		if !ok {
			return 0, false, fmt.Errorf("unknown unit %q", m[2])
		}
		n, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, false, err
		}
		d += time.Duration(n * float64(unit))
	}
	return d, false, nil
}

// TestParseSystemdTimespan holds the reader the bases test uses to the forms a
// unit writes TimeoutStopSec in.
func TestParseSystemdTimespan(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		want     time.Duration
		infinite bool
		bad      bool
	}{
		{raw: "24h15min", want: 24*time.Hour + 15*time.Minute},
		{raw: "87300", want: 87300 * time.Second},
		{raw: "87300s", want: 87300 * time.Second},
		{raw: "1d 2h 3min", want: 26*time.Hour + 3*time.Minute},
		{raw: "90s", want: 90 * time.Second},
		{raw: "infinity", infinite: true},
		{raw: "", bad: true},
		{raw: "soon", bad: true},
		{raw: "5 parsecs", bad: true},
	} {
		got, infinite, err := parseSystemdTimespan(tc.raw)
		if tc.bad {
			if err == nil {
				t.Errorf("parseSystemdTimespan(%q) = (%v, %v, nil), want an error", tc.raw, got, infinite)
			}
			continue
		}
		if err != nil || got != tc.want || infinite != tc.infinite {
			t.Errorf("parseSystemdTimespan(%q) = (%v, %v, %v), want (%v, %v, nil)", tc.raw, got, infinite, err, tc.want, tc.infinite)
		}
	}
}
