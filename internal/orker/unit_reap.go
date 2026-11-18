package orker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Labels on every container, task or probe, bound to gpu units: the lock it
// was bound under and its units.
const (
	labelUnitsLock = "ork.units.lock"
	labelUnits     = "ork.units"
)

// unitsLabel is ork.units' value: <profile>:<i,…>, ascending.
func unitsLabel(profile string, indices []int) string {
	parts := make([]string, len(indices))
	for i, index := range indices {
		parts[i] = strconv.Itoa(index)
	}
	return profile + ":" + strings.Join(parts, ",")
}

// parseUnitsLabel reads ork.units for profile's n units: distinct canonical
// indices in range, returned ascending. Anything else is not read (false), and
// the caller fails closed.
func parseUnitsLabel(label, profile string, n int) ([]int, bool) {
	rest, ok := strings.CutPrefix(label, profile+":")
	if !ok || rest == "" {
		return nil, false
	}
	var indices []int
	for _, part := range strings.Split(rest, ",") {
		i, err := strconv.Atoi(part)
		if err != nil || i < 0 || i >= n || strconv.Itoa(i) != part || slices.Contains(indices, i) {
			return nil, false
		}
		indices = append(indices, i)
	}
	slices.Sort(indices)
	return indices, true
}

// UnitContainer is one container carrying ork.units.lock, with the attempt it
// ran.
type UnitContainer struct {
	OwnContainer
	Lock  string // ork.units.lock
	Units string // ork.units
	Orker string // ork.orker
	State string // the runtime's state word: running, exited, created…
}

// unitContainersFormat is one container a line, tab-separated, spelled as
// ownContainersFormat is, so it needs no dialect.
const unitContainersFormat = `{{.ID}}` + "\t" + `{{.State}}` + "\t" + `{{.Label "` + labelUnitsLock + `"}}` + "\t" + `{{.Label "` + labelUnits + `"}}` +
	"\t" + `{{.Label "ork.orker"}}` + "\t" + `{{.Label "ork.run"}}` + "\t" + `{{.Label "ork.task"}}` +
	"\t" + `{{.Label "ork.item"}}` + "\t" + `{{.Label "ork.attempt"}}`

// parseUnitContainers reads unitContainersFormat's answer; a line without its
// nine fields, or without an id, is not one of its answers.
func parseUnitContainers(answer string) []UnitContainer {
	var out []UnitContainer
	for _, line := range strings.Split(answer, "\n") {
		f := strings.Split(strings.TrimRight(line, "\r"), "\t")
		if len(f) != 9 || f[0] == "" {
			continue
		}
		out = append(out, UnitContainer{
			State: f[1], Lock: f[2], Units: f[3], Orker: f[4],
			OwnContainer: OwnContainer{ID: f[0], RunID: f[5], TaskID: f[6], Item: f[7], Attempt: f[8]},
		})
	}
	return out
}

// unitContainerLister is the runtime capability the lock-scoped reap lists
// with. A runtime without it cannot host a gpu profile.
type unitContainerLister interface {
	UnitContainers(ctx context.Context) ([]UnitContainer, error)
}

// unitReapDeps is what reapUnitsUnderLock acts through.
type unitReapDeps struct {
	list     func(context.Context) ([]UnitContainer, error)
	remove   func(context.Context, ContainerID) error
	release  func(context.Context, artifacts.AttemptRef) error
	log      func(msg string, args ...any)
	capacity *Capacity
}

// reapUnitsUnderLock is the lock-scoped reap (design §4.7.6): it runs once the
// GPU host lock is held, after the by-id reap and before any device-bearing
// probe or bind. Holding the lock's exclusive flock proves every earlier
// holder of lock in this boot has exited, so every container labelled with
// it, running or exited, is force-removed, whichever Orker id labelled it, and
// its attempt released. A container whose removal fails is seeded with its
// exact units, or with every unit of the profile when its label cannot be
// read, and freed when a later removal succeeds. A container under another
// lock is logged and never touched; one of them still running with units of
// this profile is returned, foreign: it may hold a GPU this Orker would bind,
// so the profile is held unavailable (FOREIGN_UNITS_HELD) until it is gone.
// Only a failure to list is an error.
func reapUnitsUnderLock(ctx context.Context, lock, profile string, n int, d unitReapDeps) ([]UnitContainer, error) {
	listed, err := d.list(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the containers bound to gpu units: %w", err)
	}
	var foreign []UnitContainer
	all := make([]int, n)
	for i := range all {
		all[i] = i
	}
	for _, c := range listed {
		if c.Lock != lock {
			d.log("a container bound under another GPU host lock is left alone",
				"container", c.ID, "lock", c.Lock, "units", c.Units, "orker_id", c.Orker, "state", c.State)
			if foreignUnits(c, profile) {
				foreign = append(foreign, c)
			}
			continue
		}
		if err := d.remove(ctx, ContainerID(c.ID)); err != nil {
			indices, ok := parseUnitsLabel(c.Units, profile, n)
			if !ok {
				d.log("a unit container's units label cannot be read; every unit of the profile stays held until it is removed",
					"container", c.ID, "units", c.Units, "profile", profile)
				indices = all
			}
			d.log("a unit container could not be removed; its units stay held until a later removal",
				"container", c.ID, "units", indices, "error", err)
			d.capacity.seedUnits(ContainerID(c.ID), indices)
			continue
		}
		if ref, ok := reapedAttempt(c.OwnContainer); ok && d.release != nil {
			if err := d.release(ctx, ref); err != nil {
				d.log("a reaped unit container's attempt release marker was not written",
					"run_id", c.RunID, "task_id", c.TaskID, "attempt", c.Attempt, "error", err)
			}
		}
	}
	return foreign, nil
}

// foreignUnits reports whether c, bound under another lock, may still hold a
// unit of profile: it is not exited or dead, and its units are this
// profile's (or cannot be read, which fails closed).
func foreignUnits(c UnitContainer, profile string) bool {
	if c.State == "exited" || c.State == "dead" {
		return false
	}
	name, _, _ := strings.Cut(c.Units, ":")
	return name == profile || name == ""
}

// foreignDetail names the containers foreignUnits found, for the control
// channel.
func foreignDetail(foreign []UnitContainer) string {
	ids := make([]string, len(foreign))
	for i, c := range foreign {
		ids[i] = c.ID
	}
	return "containers bound under another GPU host lock still run with units of this profile: " + strings.Join(ids, ", ")
}
