package orker

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/orkstack/orkstack/internal/orker/servicehost/reachprobe"
)

// The host-native rule (design §4.12): a profile's devices are what the
// Orker inspected only if a container the runtime starts sees the same
// objects. At boot and in every periodic audit a probe container is started
// with the devices (and read-only binds) the Orker passes, `orker probe view`
// reports what it sees at each path, and judgeHostView compares that with
// the Orker's own view. It proves the two views are equal, not that the
// Orker runs natively on the host: a privileged container bind-mounting the
// same paths still passes, and DEPLOY.md calls that shape unsupported.

// View kinds: what one path is.
const (
	viewChar = "char"
	viewDir  = "dir"
	viewFile = "file"
)

// maxProbeViewBytes bounds what the Orker reads of a probe's report. A
// profile names at most a handful of paths, so a report anywhere near it is
// not the probe's.
const maxProbeViewBytes = 64 << 10

// viewEntry is what one process sees at one path, stat'ed and never opened.
// Error is set, and Kind empty, when the path could not be stat'ed.
type viewEntry struct {
	Path  string `json:"path"`
	Kind  string `json:"kind,omitempty"`
	Major uint32 `json:"major,omitzero"`
	Minor uint32 `json:"minor,omitzero"`
	GID   uint32 `json:"gid,omitzero"`
	Dev   uint64 `json:"dev,omitzero"`
	Ino   uint64 `json:"ino,omitzero"`
	// Driver is what a unit probe's driver said of a gpu node (unit_probe.go):
	// its DRM driver name and version, or kfd's version. Empty elsewhere.
	Driver string `json:"driver,omitempty"`
	Error  string `json:"error,omitempty"`
}

// probeView is the probe's report.
type probeView struct {
	View []viewEntry `json:"view"`
}

// viewCheck is how one path is compared.
type viewCheck int

const (
	// checkDevice: the same character node, by device number and group.
	// Never by dev:inode: --device makes a node of the container's own.
	checkDevice viewCheck = iota + 1
	// checkBind: the same object, by kind and dev:inode.
	checkBind
	// checkCDINode: the same character node, by device number only. The
	// runtime gives a CDI device node the container user's uid and gid, never
	// the host node's, and a task reaches the host group through the spec's
	// additionalGids (cdi_audit.go), so the gid says nothing about the unit.
	checkCDINode
)

// viewWant is one path the probe must report, and the Orker's own view of it.
type viewWant struct {
	Check viewCheck
	Own   viewEntry
}

// statView is what this process sees at p. It is what `orker probe view`
// reports inside the probe, and the Orker's own view on the host.
func statView(p string) viewEntry {
	v := viewEntry{Path: p}
	info, err := os.Lstat(p)
	if err != nil {
		v.Error = err.Error()
		return v
	}
	st, ok := nodeStatOf(info)
	if !ok {
		v.Error = "stat " + p + ": no device numbers on this platform"
		return v
	}
	mode := info.Mode()
	switch {
	case mode&os.ModeCharDevice != 0:
		v.Kind = viewChar
		v.Major, v.Minor = deviceNumbers(st.Rdev)
	case mode.IsDir():
		v.Kind = viewDir
	case mode.IsRegular():
		v.Kind = viewFile
	default:
		v.Error = fmt.Sprintf("%s is a %s, not a character device, directory or regular file", p, mode.Type())
		return v
	}
	v.GID = st.GID
	v.Dev, v.Ino = st.Dev, st.Ino
	return v
}

// writeProbeView writes the probe's report.
func writeProbeView(w io.Writer, view []viewEntry) error {
	if view == nil {
		view = []viewEntry{}
	}
	b, err := json.Marshal(probeView{View: view})
	if err != nil {
		return fmt.Errorf("encode probe view: %w", err)
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// parseProbeView reads a probe's report, held to its exact shape: one JSON
// object, no unknown member, every entry an absolute path with a known kind
// or an error.
func parseProbeView(raw []byte) ([]viewEntry, error) {
	if len(raw) > maxProbeViewBytes {
		return nil, fmt.Errorf("probe view is larger than %d bytes", maxProbeViewBytes)
	}
	var pv probeView
	if err := json.Unmarshal(bytes.TrimSpace(raw), &pv, json.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("probe view: %w", err)
	}
	if pv.View == nil {
		return nil, errors.New("probe view: no view")
	}
	for i, v := range pv.View {
		if !path.IsAbs(v.Path) || path.Clean(v.Path) != v.Path {
			return nil, fmt.Errorf("probe view entry %d: %q is not an absolute clean path", i, v.Path)
		}
		switch {
		case v.Error != "" && v.Kind == "":
		case v.Error == "" && (v.Kind == viewChar || v.Kind == viewDir || v.Kind == viewFile):
		case v.Error == "" && v.Kind == "":
			return nil, fmt.Errorf("probe view entry %s: neither a kind or an error", v.Path)
		default:
			return nil, fmt.Errorf("probe view entry %s: kind %q", v.Path, v.Kind)
		}
	}
	return pv.View, nil
}

// judgeHostView is nil when the probe saw at every wanted path what the
// Orker sees, and otherwise names every difference.
func judgeHostView(want []viewWant, probed []viewEntry) error {
	seen := make(map[string]viewEntry, len(probed))
	var errs []string
	for _, p := range probed {
		if _, dup := seen[p.Path]; dup {
			errs = append(errs, fmt.Sprintf("the probe reported %s twice", p.Path))
			continue
		}
		seen[p.Path] = p
	}
	for _, w := range want {
		p, ok := seen[w.Own.Path]
		if !ok {
			errs = append(errs, "the probe did not report "+w.Own.Path)
			continue
		}
		delete(seen, w.Own.Path)
		if msg := viewDiff(w, p); msg != "" {
			errs = append(errs, msg)
		}
	}
	for p := range seen {
		errs = append(errs, "the probe reported "+p+", which was not asked")
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New("the probe container's view differs from the Orker's: " + strings.Join(errs, "; "))
}

// viewDiff is how p differs from what w wants, or "".
func viewDiff(w viewWant, p viewEntry) string {
	own := w.Own
	if p.Error != "" {
		return fmt.Sprintf("%s: the probe could not stat it: %s", own.Path, p.Error)
	}
	if p.Kind != own.Kind {
		return fmt.Sprintf("%s is a %s to the Orker and a %s to the probe", own.Path, own.Kind, p.Kind)
	}
	switch w.Check {
	case checkDevice:
		if p.Major != own.Major || p.Minor != own.Minor {
			return fmt.Sprintf("%s is %d:%d to the Orker and %d:%d to the probe", own.Path, own.Major, own.Minor, p.Major, p.Minor)
		}
		if p.GID != own.GID {
			return fmt.Sprintf("%s has gid %d to the Orker and gid %d to the probe", own.Path, own.GID, p.GID)
		}
	case checkCDINode:
		if p.Major != own.Major || p.Minor != own.Minor {
			return fmt.Sprintf("%s is %d:%d to the Orker and %d:%d to the probe", own.Path, own.Major, own.Minor, p.Major, p.Minor)
		}
	case checkBind:
		if p.Dev != own.Dev || p.Ino != own.Ino {
			return fmt.Sprintf("%s is %d:%d to the Orker and %d:%d to the probe", own.Path, own.Dev, own.Ino, p.Dev, p.Ino)
		}
	default:
		return fmt.Sprintf("%s: no check %d", own.Path, w.Check)
	}
	return ""
}

// maxProbePaths bounds the paths one probe is asked about.
const maxProbePaths = 64

// Probe is the hidden `orker probe` subcommand, run inside a probe container
// from the orker image (design §2.1). `orker probe view PATH...` writes one
// report of what the container sees at each path; a path it cannot stat is
// reported with its error, for the Orker to judge. It reads no configuration
// and reaches nothing: its only output is the report. Exit 2 is a malformed
// invocation, which writes nothing to stdout.
func Probe(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "reach" {
		// The reach probe (reach_probe.go): stopped by SIGTERM, as a canary is.
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
		defer stop()
		return reachprobe.Main(ctx, args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "unit" {
		return probeUnitNodes(args[1:], stdout, stderr)
	}
	if len(args) < 2 || args[0] != "view" || len(args)-1 > maxProbePaths {
		fmt.Fprintf(stderr, "usage: orker probe view PATH... (1 to %d absolute paths)\n", maxProbePaths)
		return 2
	}
	paths := args[1:]
	for _, p := range paths {
		if !path.IsAbs(p) || path.Clean(p) != p {
			fmt.Fprintf(stderr, "orker probe view: %q is not an absolute clean path\n", p)
			return 2
		}
	}
	view := make([]viewEntry, 0, len(paths))
	for _, p := range paths {
		view = append(view, statView(p))
	}
	if err := writeProbeView(stdout, view); err != nil {
		fmt.Fprintf(stderr, "orker probe view: %v\n", err)
		return 1
	}
	return 0
}
