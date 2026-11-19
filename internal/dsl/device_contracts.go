package dsl

import (
	"fmt"
	"regexp"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Device contracts (E91, rollout-revision §2.2–2.3). An image may declare, in
// its interface's devices: map, which contract of a device profile it needs;
// an author may pin one with a devices: entry written name@label. Create
// copies the image's declaration into the task's device_contracts, which is
// part of the definition, so of its digest. The Orker serves the contract the
// run pinned (internal/orker).

// contractLabelPattern is a contract label's grammar: the Orker's revision
// grammar (internal/orker/profiles.go), which a profile's contract labels use.
var contractLabelPattern = regexp.MustCompile(`^[A-Za-z0-9_.:+-]{1,64}$`)

// deviceEntryPattern is a devices: entry's shape, for the document's JSON
// Schema: a profile name, optionally @ a contract label.
var deviceEntryPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*(@[A-Za-z0-9_.:+-]{1,64})?$`)

// splitDeviceEntries reads a task's devices: entries, each a profile name or
// name@label, into the bare names and the contracts the author pinned. The
// names are judged by validateDevices; only the suffix's shape is judged here.
func splitDeviceEntries(taskIndex int, taskID string, entries []string) ([]string, map[string]string, error) {
	names := make([]string, 0, len(entries))
	var contracts map[string]string
	for i, entry := range entries {
		name, label, pinned := strings.Cut(entry, "@")
		if !pinned {
			names = append(names, entry)
			continue
		}
		if name == "" || !contractLabelPattern.MatchString(label) {
			return nil, nil, validationError(fmt.Sprintf("tasks[%d].devices[%d]", taskIndex, i),
				fmt.Errorf("task %q: `devices:` entry %q is not a profile name or name@contract (a contract is %s)",
					taskID, entry, contractLabelPattern))
		}
		names = append(names, name)
		if contracts == nil {
			contracts = map[string]string{}
		}
		contracts[name] = label
	}
	return names, contracts, nil
}

// checkInterfaceDevices holds an interface's devices: map to its rules.
func checkInterfaceDevices(devices map[string]string) error {
	if n := len(devices); n > MaxTaskDevices {
		return fmt.Errorf("devices names %d profiles, limit is %d", n, MaxTaskDevices)
	}
	for _, name := range mapKeys(devices) {
		if len(name) > MaxSlug || !slugPattern.MatchString(name) {
			return fmt.Errorf("devices: %q is not a profile name (%s)", name, slugPattern)
		}
		if label := devices[name]; !contractLabelPattern.MatchString(label) {
			return fmt.Errorf("devices.%s: contract %q is not a contract label (%s)", name, label, contractLabelPattern)
		}
	}
	return nil
}

// PlatformDevices is what one more platform of a multi-platform image
// declares in its interface's devices:, read for a task that names a profile
// (every platform must agree): nil Devices when it declares none, or Err when
// its label could not be read.
type PlatformDevices struct {
	Platform string
	Devices  map[string]string
	Err      string
}

// Rule 54: a task's devices: against its image's declared contracts.
//
// checkDeviceContracts is the device half of CheckOrkflowInterfaces for one
// task: every profile its image needs is named, an author's pin agrees with
// the image, every platform declares the same contracts, and, for a task
// naming a profile it does not pin, a declaration that cannot be known is an
// error. A pin the image cannot be read to confirm is a warning. It reports
// whether its issues replace the unreadable-label warning.
func checkDeviceContracts(i int, task *orkstackv1.Task, res *Resolution) (issues []*orkstackv1.ValidationIssue, replacesWarning bool) {
	field := fmt.Sprintf("tasks[%d].devices", i)
	add := func(severity orkstackv1.ValidationSeverity, format string, args ...any) {
		issues = append(issues, &orkstackv1.ValidationIssue{Field: field, Message: fmt.Sprintf(format, args...), Severity: severity})
	}
	refuse := func(format string, args ...any) {
		add(orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR, format, args...)
	}
	unpinned, pinned := "", ""
	for _, name := range task.GetDevices() {
		if label, ok := task.GetDeviceContracts()[name]; ok {
			if pinned == "" {
				pinned = name + "@" + label
			}
		} else if unpinned == "" {
			unpinned = name
		}
	}
	// unknowable is a declaration that cannot be read: an error for a task
	// naming a profile it does not pin, a warning that its pins are unchecked
	// otherwise.
	var unknown []string
	unknowable := func(why string) { unknown = append(unknown, why) }
	report := func() bool {
		if len(unknown) == 0 {
			return false
		}
		why := strings.Join(unknown, "; ")
		switch {
		case unpinned != "":
			refuse("the image's device contracts cannot be read (%s); write the contract explicitly, as %s@<contract>", why, unpinned)
		case pinned != "":
			add(orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING,
				"the image's device contracts cannot be read (%s); %s is not checked against the image", why, pinned)
		}
		return unpinned != ""
	}
	switch {
	case res == nil:
		if len(task.GetDevices()) > 0 {
			unknowable("no image resolution was made")
		}
		report()
		return issues, false
	case res.InterfaceError != "":
		if len(task.GetDevices()) > 0 {
			unknowable(res.InterfaceError)
		}
		return issues, report()
	}
	image := task.GetImage().GetPinned()
	declared := res.Interface.GetDevices()
	named := map[string]bool{}
	for _, name := range task.GetDevices() {
		named[name] = true
	}
	for _, name := range mapKeys(declared) {
		label := declared[name]
		if !named[name] {
			refuse("image %s needs profile %s (contract %s); add it to devices:", image, name, label)
			continue
		}
		if pin, ok := task.GetDeviceContracts()[name]; ok && pin != label {
			refuse("devices names %s@%s, but image %s declares contract %s of %s; write %s or %s@%s",
				name, pin, image, label, name, name, name, label)
		}
	}
	if len(task.GetDevices()) == 0 {
		return issues, false
	}
	// Every platform declares the same contracts: each profile any of them
	// declares, so a platform needing a profile the first does not is caught.
	for _, other := range res.OtherPlatforms {
		if other.Err != "" {
			unknowable(other.Platform + ": " + other.Err)
			continue
		}
		profiles := map[string]bool{}
		for name := range declared {
			profiles[name] = true
		}
		for name := range other.Devices {
			profiles[name] = true
		}
		for _, name := range mapKeys(profiles) {
			here, there := declared[name], other.Devices[name]
			if here == there {
				continue
			}
			refuse("image %s declares %s of %s on %s and %s on %s; every platform must declare the same device contracts",
				image, contractWords(here), name, res.Platform, contractWords(there), other.Platform)
		}
	}
	report()
	return issues, false
}

func contractWords(label string) string {
	if label == "" {
		return "no contract"
	}
	return "contract " + label
}

// ApplyDeviceContracts copies, at create, each resolved image's declaration
// into every task that names the profile, beside the author's own pins
// (which CheckOrkflowInterfaces already held to agree). It runs before the
// definition's digests are taken, so they cover it, and writes nothing for a
// task its image declares nothing for.
func ApplyDeviceContracts(wf *orkstackv1.Orkflow, resolutions Resolutions) {
	for _, task := range wf.GetTasks() {
		res := resolutions[task.GetUses()]
		if res == nil {
			continue
		}
		declared := res.Interface.GetDevices()
		for _, name := range task.GetDevices() {
			label, ok := declared[name]
			if !ok {
				continue
			}
			if task.DeviceContracts == nil {
				task.DeviceContracts = map[string]string{}
			}
			task.DeviceContracts[name] = label
		}
	}
}
