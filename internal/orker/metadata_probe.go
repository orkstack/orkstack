package orker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The metadata probe (docs/DEPLOY.md, "Keeping tasks away from the host's
// identity").
//
// A container on the default network may be able to reach the host's
// instance metadata service, which hands out the host's role. OrkStack
// cannot close that from inside a container's network and adds nothing to a
// container to try (AGENTS.md §6). What it can do, when asked, is look: at
// boot, one container built exactly as a task's is, on the default network,
// makes for each address the request a task would make, and the Orker judges
// what came back. A TCP connection alone says little: with EC2's hop limit
// of 1 the connection can open while the token request is what fails, so
// only an HTTP status line counts as an answer.
//
// Each address has one of three outcomes, never two: it answered, it did
// not, or the probe could not tell (its container did not start, its image
// was missing, the runtime erred, the container's own tools were not there).
// A probe that failed is never read as safe.

// MetadataProbeMode says whether the boot probe asks the host's instance
// metadata endpoints from a container on the default network, and what an
// answer does.
type MetadataProbeMode string

const (
	// MetadataProbeOff runs no extra container at boot.
	MetadataProbeOff MetadataProbeMode = "off"
	// MetadataProbeWarn probes and logs an endpoint that answered, or a
	// probe that failed.
	MetadataProbeWarn MetadataProbeMode = "warn"
	// MetadataProbeRefuse probes and refuses boot when an endpoint answered
	// or the probe failed.
	MetadataProbeRefuse MetadataProbeMode = "refuse"

	// MetadataProbeVar is the setting's environment variable
	// (orker.metadata_probe).
	MetadataProbeVar = "ORK_ORKER_METADATA_PROBE"
	// DefaultMetadataProbe is off: a probe costs every boot a container and
	// has nothing to find on a laptop.
	DefaultMetadataProbe = MetadataProbeOff
)

// ParseMetadataProbeMode reads orker.metadata_probe. Empty is the default;
// anything but off, warn or refuse is refused, naming the setting.
func ParseMetadataProbeMode(raw string) (MetadataProbeMode, error) {
	switch v := MetadataProbeMode(strings.ToLower(strings.TrimSpace(raw))); v {
	case "":
		return DefaultMetadataProbe, nil
	case MetadataProbeOff, MetadataProbeWarn, MetadataProbeRefuse:
		return v, nil
	}
	return "", fmt.Errorf("orker.metadata_probe (%s): %q is not off, warn or refuse", MetadataProbeVar, raw)
}

// MetadataOutcome is what one address's probe found.
type MetadataOutcome int

const (
	// MetadataAnswered: an HTTP status line came back.
	MetadataAnswered MetadataOutcome = iota + 1
	// MetadataNoAnswer: every request ran and none was answered.
	MetadataNoAnswer
	// MetadataProbeFailed: the probe could not tell.
	MetadataProbeFailed
)

func (o MetadataOutcome) String() string {
	switch o {
	case MetadataAnswered:
		return "answered"
	case MetadataNoAnswer:
		return "no answer"
	case MetadataProbeFailed:
		return "probe failed"
	}
	return "unknown"
}

// MetadataFinding is one address's outcome. Detail says which request was
// answered and with what status, or why the probe could not tell; it never
// holds what an endpoint sent beyond its status code.
type MetadataFinding struct {
	Address string
	Outcome MetadataOutcome
	Detail  string
}

// metadataRequest is one request the probe makes: what an operator reads in
// a finding, and the raw HTTP/1.0 request, written as printf's %b reads it.
type metadataRequest struct {
	Name string
	Raw  string
}

// metadataTarget is one address the probe asks: Address is what a finding
// names, Host and Port what the container dials.
type metadataTarget struct {
	Address  string
	Host     string
	Port     int
	Requests []metadataRequest
}

// httpGet is a plain GET of path with extra header lines, each ending \r\n.
func httpGet(host, path, headers string) metadataRequest {
	return metadataRequest{
		Name: "GET " + path,
		Raw:  "GET " + path + " HTTP/1.0\\r\\nHost: " + host + "\\r\\n" + headers + "\\r\\n",
	}
}

// ec2TokenRequest is the PUT that asks EC2's IMDSv2 for a session token: the
// request a hop limit of 1 is there to stop.
func ec2TokenRequest(host string) metadataRequest {
	return metadataRequest{
		Name: "PUT /latest/api/token",
		Raw: "PUT /latest/api/token HTTP/1.0\\r\\nHost: " + host +
			"\\r\\nX-aws-ec2-metadata-token-ttl-seconds: 21600\\r\\nContent-Length: 0\\r\\n\\r\\n",
	}
}

// defaultMetadataTargets are the addresses of docs/DEPLOY.md's table, each
// with the requests a task on that platform would make. 169.254.169.254 is
// EC2's, GCE's and Azure's at once, so it is asked as each of them asks.
func defaultMetadataTargets() []metadataTarget {
	const linkLocal = "169.254.169.254"
	return []metadataTarget{
		{Address: linkLocal, Host: linkLocal, Port: 80, Requests: []metadataRequest{
			httpGet(linkLocal, "/", ""),
			ec2TokenRequest(linkLocal),
			httpGet(linkLocal, "/computeMetadata/v1/", "Metadata-Flavor: Google\\r\\n"),
			httpGet(linkLocal, "/metadata/instance?api-version=2021-02-01", "Metadata: true\\r\\n"),
		}},
		{Address: "fd00:ec2::254", Host: "fd00:ec2::254", Port: 80, Requests: []metadataRequest{
			httpGet("[fd00:ec2::254]", "/", ""),
			ec2TokenRequest("[fd00:ec2::254]"),
		}},
		{Address: "169.254.170.2", Host: "169.254.170.2", Port: 80, Requests: []metadataRequest{
			httpGet("169.254.170.2", "/", ""),
		}},
		{Address: "169.254.170.23", Host: "169.254.170.23", Port: 80, Requests: []metadataRequest{
			httpGet("169.254.170.23", "/", ""),
		}},
		{Address: "metadata.google.internal", Host: "metadata.google.internal", Port: 80, Requests: []metadataRequest{
			httpGet("metadata.google.internal", "/computeMetadata/v1/", "Metadata-Flavor: Google\\r\\n"),
		}},
		{Address: "168.63.129.16", Host: "168.63.129.16", Port: 80, Requests: []metadataRequest{
			httpGet("168.63.129.16", "/", ""),
		}},
	}
}

// metadataProbeWait is how long the container's nc waits for one request's
// connection and then for its answer, in seconds; metadataProbeBound is how
// long one request may take in all, its name's lookup included, before
// timeout ends it. Every request runs at once, so the probe's container ends
// within about the bound.
const (
	metadataProbeWait  = 3
	metadataProbeBound = 5
)

// metadataProbeTaskID names the probe's container (ork-metaprobe-…).
const metadataProbeTaskID = "metaprobe"

// metadataProbeControlPort is the loopback port of the script's own positive
// control: before it asks any endpoint, the container serves a status line on
// it and asks for it, so "no answer" is only ever read from a container whose
// nc can complete an HTTP exchange.
const metadataProbeControlPort = 18079

// metadataProbeScript is what the probe's container runs, as `sh -c`, with
// one (host, port, request) triple of arguments per request. It first proves
// its own nc works on loopback, over IPv6 too where the container has it,
// then sends every request at once, each
// bounded by timeout, keeps at most the first line of each answer, and
// prints, in order, for each request either "@@reach N" followed by that line
// behind a ">" (so nothing an endpoint sends can read as a marker) or
// "@@missing N" for a request whose job never reported, "@@badaddr N" after
// a request whose nc could not use its address, and last "@@done". It
// judges nothing and always exits 0. A tool missing, no scratch directory, or
// a control that failed is a marker of its own, so the host can say the
// probe could not tell.
var metadataProbeScript = `command -v nc >/dev/null 2>&1 || { echo '@@nonc'; exit 0; }
command -v timeout >/dev/null 2>&1 || { echo '@@notimeout'; exit 0; }
d=$(mktemp -d) || { echo '@@notmp'; exit 0; }
control() {
  ( printf 'HTTP/1.0 204 No Content\r\n\r\n' | timeout ` + strconv.Itoa(metadataProbeBound) + ` nc -l -p "$2" >/dev/null 2>&1 ) &
  n=0
  while [ $n -lt 30 ]; do
    if printf 'GET / HTTP/1.0\r\n\r\n' | timeout ` + strconv.Itoa(metadataProbeWait) + ` nc -w ` + strconv.Itoa(metadataProbeWait) + ` "$1" "$2" 2>/dev/null | head -n 1 | grep -q '^HTTP/'; then return 0; fi
    n=$((n+1)); sleep 0.1
  done
  return 1
}
control 127.0.0.1 ` + strconv.Itoa(metadataProbeControlPort) + ` || { echo '@@ncbroken'; exit 0; }
if grep -q ' lo$' /proc/net/if_inet6 2>/dev/null; then
  control ::1 ` + strconv.Itoa(metadataProbeControlPort+1) + ` || { echo '@@ncbroken6'; exit 0; }
fi
i=0
while [ $# -ge 3 ]; do
  ( printf '%b' "$3" | timeout ` + strconv.Itoa(metadataProbeBound) + ` nc -w ` + strconv.Itoa(metadataProbeWait) + ` "$1" "$2" 2>"$d/$i.err" | head -c 256 | head -n 1 > "$d/$i"; echo done > "$d/$i.end" ) &
  i=$((i+1)); shift 3
done
wait
j=0
while [ $j -lt $i ]; do
  if [ -f "$d/$j.end" ]; then
    echo "@@reach $j"; printf '>'; cat "$d/$j" 2>/dev/null; echo
    if grep -q 'bad address' "$d/$j.err" 2>/dev/null; then echo "@@badaddr $j"; fi
  else echo "@@missing $j"; fi
  j=$((j+1))
done
echo '@@done'
exit 0
`

// statusLine is the start of an HTTP response behind the script's ">": what
// counts as an answer.
var statusLine = regexp.MustCompile(`^>HTTP/[0-9](?:\.[0-9])? ([0-9]{3})`)

// metadataProbeArgv is the container's command for targets.
func metadataProbeArgv(targets []metadataTarget) []string {
	argv := []string{"/bin/sh", "-c", metadataProbeScript, "ork-metaprobe"}
	for _, t := range targets {
		for _, r := range t.Requests {
			argv = append(argv, t.Host, strconv.Itoa(t.Port), r.Raw)
		}
	}
	return argv
}

// The reasons judgeMetadataProbe gives for a probe that could not tell.
const (
	whyNoNC        = "the probe's image has no nc"
	whyNoTimeout   = "the probe's image has no timeout"
	whyNoTmp       = "the probe's container could not make a scratch directory"
	whyNCBroken    = "the probe's container could not complete an HTTP exchange on its own loopback"
	whyNCBroken6   = "the probe's container could not complete an HTTP exchange on its own IPv6 loopback"
	whyBadAddress  = "the probe's nc could not use the address"
	whyUnreported  = "the probe's container ended before it reported every request"
	whyJobVanished = "a request of the probe's container never reported"
)

// judgeMetadataProbe reads what the probe's container printed into one
// finding per target, in order. Only a line that does not begin with ">" is
// a marker, and only the line right after "@@reach N" is that request's
// answer. A request reported missing, one with no section, or a report whose
// last line is not "@@done", is one the probe could not tell about: its target
// is probe failed unless another of its requests was answered.
func judgeMetadataProbe(targets []metadataTarget, stdout string) []MetadataFinding {
	lines := strings.Split(stdout, "\n")
	answers := map[int]string{}
	seen := map[int]bool{}
	vanished := map[int]bool{}
	badAddr := map[int]bool{}
	last := ""
	for i := 0; i < len(lines); i++ {
		l := strings.TrimSpace(lines[i])
		if l == "" || strings.HasPrefix(l, ">") {
			continue
		}
		last = l
		switch l {
		case "@@nonc":
			return metadataFailedAll(targets, whyNoNC)
		case "@@notimeout":
			return metadataFailedAll(targets, whyNoTimeout)
		case "@@notmp":
			return metadataFailedAll(targets, whyNoTmp)
		case "@@ncbroken":
			return metadataFailedAll(targets, whyNCBroken)
		case "@@ncbroken6":
			return metadataFailedAll(targets, whyNCBroken6)
		}
		if rest, ok := strings.CutPrefix(l, "@@badaddr "); ok {
			if n, err := strconv.Atoi(rest); err == nil && n >= 0 {
				badAddr[n] = true
			}
			continue
		}
		if rest, ok := strings.CutPrefix(l, "@@missing "); ok {
			if n, err := strconv.Atoi(rest); err == nil && n >= 0 {
				vanished[n] = true
			}
			continue
		}
		rest, ok := strings.CutPrefix(l, "@@reach ")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(rest)
		if err != nil || n < 0 {
			continue
		}
		seen[n] = true
		if i+1 < len(lines) {
			if m := statusLine.FindStringSubmatch(strings.TrimSpace(lines[i+1])); m != nil {
				answers[n] = m[1]
			}
		}
	}
	done := last == "@@done"
	findings := make([]MetadataFinding, 0, len(targets))
	n := 0
	for _, t := range targets {
		f := MetadataFinding{Address: t.Address, Outcome: MetadataNoAnswer}
		why := ""
		if len(t.Requests) == 0 {
			// Nothing asked is never no answer.
			why = whyUnreported
		}
		for _, r := range t.Requests {
			code, answered := answers[n]
			switch {
			case answered:
				if f.Outcome != MetadataAnswered {
					f.Outcome = MetadataAnswered
					f.Detail = "HTTP " + code + " to " + r.Name
				}
			case vanished[n]:
				if why == "" {
					why = whyJobVanished
				}
			case badAddr[n] && net.ParseIP(t.Host) != nil:
				// A name that does not resolve is unreachable to a task too,
				// and on every cloud host but GCE metadata.google.internal never
				// resolves: reading it as failed would refuse them all. GCE's
				// endpoint is asked by address as well, with its own header, so
				// a lookup that failed at boot hides nothing. An address nc
				// could not use was never asked. The image is pinned, so nc's
				// wording is fixed.
				if why == "" {
					why = whyBadAddress
				}
			case !seen[n] || !done:
				why = whyUnreported
			}
			n++
		}
		if why != "" && f.Outcome != MetadataAnswered {
			f.Outcome = MetadataProbeFailed
			f.Detail = why
		}
		findings = append(findings, f)
	}
	return findings
}

func metadataFailedAll(targets []metadataTarget, why string) []MetadataFinding {
	findings := make([]MetadataFinding, 0, len(targets))
	for _, t := range targets {
		findings = append(findings, MetadataFinding{Address: t.Address, Outcome: MetadataProbeFailed, Detail: why})
	}
	return findings
}

// ProbeMetadata runs the metadata probe's container under root and reports
// one finding per address. It never returns an error: a probe that could not
// run is every address's probe failed, with why.
func (d *OCIDispatcher) ProbeMetadata(ctx context.Context, root string) []MetadataFinding {
	targets := d.metadataTargets
	if targets == nil {
		targets = defaultMetadataTargets()
	}
	prober, ok := d.Runtime.(bootProber)
	if !ok {
		return metadataFailedAll(targets, "this Orker's runtime cannot run a probe container")
	}
	dir, err := os.MkdirTemp(root, "ork-metaprobe-*")
	if err != nil {
		return metadataFailedAll(targets, fmt.Sprintf("no probe directory under %s: %v", root, err))
	}
	defer func() {
		if err := removeSealedTree(dir); err != nil {
			d.logger().Warn("metadata probe: its directory could not be removed and is left on disk",
				"path", dir, "error", err)
		}
	}()
	ws, err := NewWorkspace(dir)
	if err == nil {
		err = ws.Seal()
	}
	if err == nil {
		err = ws.SealSecrets()
	}
	if err != nil {
		return metadataFailedAll(targets, fmt.Sprintf("the probe's workspace under %s: %v", root, err))
	}
	spec, err := d.metadataProbeSpec(ws, targets)
	if err != nil {
		return metadataFailedAll(targets, err.Error())
	}
	run, err := prober.RunBootProbe(ctx, spec)
	d.warnProbeLeftovers(run.Leftovers)
	if err != nil {
		// Never the runtime's own text: it can quote the container's output,
		// which holds what an endpoint sent.
		return metadataFailedAll(targets, metadataRunFailure(err))
	}
	d.removeProbeContainer(ctx, run.Container)
	return judgeMetadataProbe(targets, run.Stdout)
}

// metadataProbeSpec is the probe's container: built by containerSpec, which
// every task's container is built by, for a task that requests nothing and
// sets no user, on the default network.
func (d *OCIDispatcher) metadataProbeSpec(ws *Workspace, targets []metadataTarget) (ContainerSpec, error) {
	cpu, memory, failure := d.Policy.ResolveResources(nil)
	if failure != nil {
		return ContainerSpec{}, fmt.Errorf("the probe's resources: %s", failure.Message)
	}
	in := &orkstackv1.ExecuteTaskInput{TaskId: metadataProbeTaskID, Command: metadataProbeArgv(targets)}
	image := &orkstackv1.ImageRef{Pinned: BindProbeImage}
	spec, failure := d.containerSpec(in, ws, image, 1, NetworkDefault, cpu, memory, d.Policy.ForceUser)
	if failure != nil {
		return ContainerSpec{}, fmt.Errorf("the probe's container: %s", failure.Message)
	}
	spec.Labels = map[string]string{"ork.task": metadataProbeTaskID}
	if d.OrkerID != "" {
		spec.Labels["ork.orker"] = d.OrkerID
	}
	return spec, nil
}

// ErrMetadataEndpointReachable is the refusal under refuse when an address
// answered.
var ErrMetadataEndpointReachable = errors.New("an instance metadata endpoint answered a container on the default network")

// ErrMetadataProbeFailed is the refusal under refuse when the probe could not
// tell for an address.
var ErrMetadataProbeFailed = errors.New("the metadata probe could not tell whether an endpoint answers")

// metadataVerdict logs findings as mode asks and returns the refusal, or nil.
func (d *OCIDispatcher) metadataVerdict(mode MetadataProbeMode, findings []MetadataFinding) error {
	var answered, failed []string
	for _, f := range findings {
		switch f.Outcome {
		case MetadataAnswered:
			answered = append(answered, f.Address+" ("+f.Detail+")")
			d.logger().Warn("metadata probe: an instance metadata endpoint answered a container on the default network; "+
				"a task could ask it for this host's role (docs/DEPLOY.md, \"Keeping tasks away from the host's identity\")",
				"address", f.Address, "detail", f.Detail, "setting", MetadataProbeVar, "mode", string(mode))
		case MetadataProbeFailed:
			failed = append(failed, f.Address+" ("+f.Detail+")")
			d.logger().Error("metadata probe: could not tell whether an endpoint answers; this is not read as safe",
				"address", f.Address, "detail", f.Detail, "setting", MetadataProbeVar, "mode", string(mode))
		}
	}
	if len(answered) == 0 && len(failed) == 0 {
		d.logger().Info("metadata probe: from this host's default network, at boot, no instance metadata endpoint answered",
			"addresses", len(findings))
	}
	if mode != MetadataProbeRefuse {
		return nil
	}
	if len(answered) > 0 {
		return fmt.Errorf("%s: %w: %s; close it on the host, or set %s=warn to start anyway",
			ReasonMetadataEndpointReachable, ErrMetadataEndpointReachable, strings.Join(answered, ", "), MetadataProbeVar)
	}
	if len(failed) > 0 {
		return fmt.Errorf("%s: %w: %s", ReasonMetadataProbeFailed, ErrMetadataProbeFailed, strings.Join(failed, ", "))
	}
	return nil
}

// ReasonMetadataEndpointReachable is a boot refusal under
// orker.metadata_probe=refuse: an instance metadata endpoint answered the
// probe's container on the default network.
const ReasonMetadataEndpointReachable = "METADATA_ENDPOINT_REACHABLE"

// ReasonMetadataProbeFailed is a boot refusal under
// orker.metadata_probe=refuse: the probe could not tell whether an endpoint
// answers, which is never read as safe.
const ReasonMetadataProbeFailed = "METADATA_PROBE_FAILED"

func (o MetadataOutcome) proto() orkstackv1.MetadataProbeOutcome {
	switch o {
	case MetadataAnswered:
		return orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_ANSWERED
	case MetadataNoAnswer:
		return orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_NO_ANSWER
	case MetadataProbeFailed:
		return orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_PROBE_FAILED
	}
	return orkstackv1.MetadataProbeOutcome_METADATA_PROBE_OUTCOME_UNSPECIFIED
}

// describeMetadata fills what DescribeOrker reports of the boot's metadata
// probe: every finding, and the addresses that answered. Nothing when no
// probe ran.
func (a *Activities) describeMetadata(result *orkstackv1.DescribeOrkerResult) {
	found := a.metadata.Load()
	if found == nil {
		return
	}
	for _, f := range *found {
		result.MetadataProbes = append(result.MetadataProbes,
			&orkstackv1.MetadataProbe{Address: f.Address, Outcome: f.Outcome.proto(), Detail: f.Detail})
		if f.Outcome == MetadataAnswered {
			result.MetadataReachable = append(result.MetadataReachable, f.Address)
		}
	}
}

// metadataRunFailure is why the probe's container did not run, in words of the
// Orker's own: a runtime's error can quote the container's stdout, which holds
// what an endpoint sent, so its text goes nowhere.
func metadataRunFailure(err error) string {
	switch {
	case errors.Is(err, ErrProbeImageUnavailable):
		return "its container did not run: the probe's image is not available"
	case errors.Is(err, ErrProbeTimedOut):
		return "its container did not run: the container daemon was too slow"
	case errors.Is(err, ErrNotBindable):
		return "its container did not run: the daemon refused its workspace"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "its container did not run: the boot was interrupted"
	}
	return "its container did not run: the container runtime refused it or it exited non-zero"
}
