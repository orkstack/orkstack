package orker

import (
	"fmt"
	"slices"

	cdi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdispec "tags.cncf.io/container-device-interface/specs-go"
)

// nullEditsValidator refuses a CDI spec with an empty (null) entry in an
// edits list. The library decodes one as a nil pointer and dereferences it in
// its own version check and validation (v1.1.1, and v1.1.2 still), so without
// this a file in a CDI directory would crash the Orker that reads it. It is the library's
// extra validator, which ReadSpec runs on the decoded spec before anything
// reads its edits, so the one read of the file is still the library's.
type nullEditsValidator struct{}

func init() { cdi.SetSpecValidator(nullEditsValidator{}) }

func (nullEditsValidator) Validate(spec *cdispec.Spec) error {
	if err := refuseNullEdits("containerEdits", spec.ContainerEdits); err != nil {
		return err
	}
	for i, d := range spec.Devices {
		if err := refuseNullEdits(fmt.Sprintf("devices[%d] (%s) containerEdits", i, d.Name), d.ContainerEdits); err != nil {
			return err
		}
	}
	return nil
}

func refuseNullEdits(at string, e cdispec.ContainerEdits) error {
	for _, l := range []struct {
		name string
		null bool
	}{
		{"deviceNodes", slices.Contains(e.DeviceNodes, nil)},
		{"netDevices", slices.Contains(e.NetDevices, nil)},
		{"hooks", slices.Contains(e.Hooks, nil)},
		{"mounts", slices.Contains(e.Mounts, nil)},
	} {
		if l.null {
			return fmt.Errorf("%s: %s has an empty entry", at, l.name)
		}
	}
	return nil
}
