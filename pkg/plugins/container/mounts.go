package containerplugin

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// ephemeralDir is the workdir subdirectory holding scratch mounts. It lives
// inside the per-attempt workdir so an abandoned attempt cannot leak it, and
// the plugin still removes each directory itself once the container exits.
const ephemeralDir = "ephemeral"

// mount is one bind mount handed to `docker run`.
type mount struct {
	// hostPath is the absolute path on the Orker host.
	hostPath string
	// containerPath is the absolute path inside the container.
	containerPath string
	// readOnly marks the mount immutable to the container.
	readOnly bool
	// source describes where the mount came from, for logs and errors.
	source string
	// ephemeral marks a scratch directory this plugin created and must remove.
	ephemeral bool
}

// bindSpec renders the mount as a `--mount type=bind` argument.
//
// `--mount` rather than `--volume` on purpose: when the daemon cannot see the
// host path — a VM that shares only part of the filesystem, a remote daemon —
// `--volume` silently mounts an empty directory and the task reads nothing,
// while `--mount` refuses to start. A task that cannot see its input must say
// so, not succeed on emptiness.
func (m mount) bindSpec() string {
	spec := "type=bind,source=" + m.hostPath + ",target=" + m.containerPath
	if m.readOnly {
		spec += ",readonly"
	}
	return spec
}

// mountSpec is one declared entry of the `mounts` list, before resolution.
type mountSpec struct {
	artifact    string
	ephemeral   string
	path        string
	readOnly    bool
	readOnlySet bool
}

// parseMounts reads the `mounts` list. Every error here is the orkflow
// author's, so all of them are permanent: no retry turns a bad path into a
// good one.
func parseMounts(config map[string]any) ([]mountSpec, error) {
	raw, ok := config["mounts"]
	if !ok || raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("`mounts` must be a list, got %T", raw)
	}

	specs := make([]mountSpec, 0, len(items))
	seen := map[string]int{}
	for i, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("`mounts`[%d] must be a map, got %T", i, item)
		}
		spec := mountSpec{
			artifact:  strings.TrimSpace(configString(entry, "artifact")),
			ephemeral: strings.TrimSpace(configString(entry, "ephemeral")),
			path:      strings.TrimSpace(configString(entry, "path")),
		}
		if value, present := entry["read_only"]; present && value != nil {
			readOnly, err := asBool(value)
			if err != nil {
				return nil, fmt.Errorf("`mounts`[%d].read_only: %w", i, err)
			}
			spec.readOnly, spec.readOnlySet = readOnly, true
		}

		switch {
		case spec.artifact != "" && spec.ephemeral != "":
			return nil, fmt.Errorf(
				"`mounts`[%d] sets both `artifact` and `ephemeral`; a mount has exactly one source", i)
		case spec.artifact == "" && spec.ephemeral == "":
			return nil, fmt.Errorf(
				"`mounts`[%d] needs a source: `artifact: \"<task>/<name>\"` or `ephemeral: <name>`", i)
		}
		if spec.artifact != "" && spec.readOnlySet && !spec.readOnly {
			return nil, fmt.Errorf(
				"`mounts`[%d] mounts artifact %q read_only: false; upstream artifacts are immutable",
				i, spec.artifact)
		}
		if err := validateContainerPath(i, spec.path); err != nil {
			return nil, err
		}
		if first, duplicate := seen[spec.path]; duplicate {
			return nil, fmt.Errorf("`mounts`[%d] and `mounts`[%d] both mount %q", first, i, spec.path)
		}
		seen[spec.path] = i
		specs = append(specs, spec)
	}
	return specs, nil
}

// validateContainerPath rejects anything that would not be a single, absolute,
// unambiguous target inside the container. ',' would split the `--mount`
// argument and ':' would split a volume spec, so neither can ever pass.
func validateContainerPath(index int, target string) error {
	switch {
	case target == "":
		return fmt.Errorf("`mounts`[%d] needs a container `path`", index)
	case !strings.HasPrefix(target, "/"):
		return fmt.Errorf("`mounts`[%d].path %q must be absolute inside the container", index, target)
	case target == "/":
		return fmt.Errorf("`mounts`[%d].path may not be the container root", index)
	case strings.ContainsAny(target, ":,"):
		return fmt.Errorf("`mounts`[%d].path %q may not contain ':' or ','", index, target)
	case path.Clean(target) != strings.TrimSuffix(target, "/"):
		return fmt.Errorf("`mounts`[%d].path %q must be a clean path (no '.', '..' or repeated '/')",
			index, target)
	}
	return nil
}

// resolveMounts turns declared mounts into bind mounts: an upstream artifact is
// materialized into the workdir and mounted read-only, an ephemeral mount is a
// fresh writable directory created for this attempt.
func resolveMounts(
	ctx context.Context,
	ec *plugin.ExecContext,
	specs []mountSpec,
	scratchRoot string,
) ([]mount, error) {
	mounts := make([]mount, 0, len(specs))
	names := map[string]struct{}{}
	for i, spec := range specs {
		var resolved mount
		var err error
		switch {
		case spec.artifact != "":
			resolved, err = resolveArtifactMount(ctx, ec, i, spec)
		default:
			if _, duplicate := names[spec.ephemeral]; duplicate {
				return mounts, plugin.Permanent(fmt.Errorf(
					"container: `mounts`[%d] reuses the ephemeral name %q", i, spec.ephemeral))
			}
			names[spec.ephemeral] = struct{}{}
			resolved, err = createEphemeralMount(i, spec, scratchRoot)
		}
		if err != nil {
			return mounts, err
		}
		if strings.ContainsRune(resolved.hostPath, ',') {
			// A ',' would split the --mount argument into nonsense fields.
			return mounts, plugin.Permanent(fmt.Errorf(
				"container: host path %q for %s contains ',' and cannot be bind-mounted",
				resolved.hostPath, resolved.source))
		}
		mounts = append(mounts, resolved)
	}
	return mounts, nil
}

func resolveArtifactMount(
	ctx context.Context,
	ec *plugin.ExecContext,
	index int,
	spec mountSpec,
) (mount, error) {
	if ec.Artifacts == nil {
		return mount{}, plugin.Permanent(fmt.Errorf(
			"container: `mounts`[%d] wants artifact %q but no artifact resolver is configured",
			index, spec.artifact))
	}
	local, err := ec.Artifacts.Resolve(ctx, spec.artifact)
	if err != nil {
		return mount{}, fmt.Errorf("container: `mounts`[%d]: %w", index, err)
	}
	absolute, err := filepath.Abs(local)
	if err != nil {
		return mount{}, fmt.Errorf("container: `mounts`[%d]: resolve %s: %w", index, local, err)
	}
	return mount{
		hostPath:      absolute,
		containerPath: spec.path,
		// Upstream artifacts are shared, immutable inputs: a container that
		// could rewrite one would corrupt every other task reading it.
		readOnly: true,
		source:   fmt.Sprintf("artifact %q", spec.artifact),
	}, nil
}

func createEphemeralMount(index int, spec mountSpec, scratchRoot string) (mount, error) {
	name := safeNameComponent(spec.ephemeral)
	if name != spec.ephemeral {
		return mount{}, plugin.Permanent(fmt.Errorf(
			"container: `mounts`[%d].ephemeral %q must be a plain name ([A-Za-z0-9._-])",
			index, spec.ephemeral))
	}
	dir := filepath.Join(scratchRoot, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return mount{}, fmt.Errorf("container: create ephemeral mount %q: %w", spec.ephemeral, err)
	}
	// The container's user is the image's business, not ours, so the scratch
	// directory is writable by whoever it turns out to be. umask would
	// otherwise strip these bits from MkdirAll.
	if err := os.Chmod(dir, 0o777); err != nil {
		return mount{}, fmt.Errorf("container: prepare ephemeral mount %q: %w", spec.ephemeral, err)
	}
	return mount{
		hostPath:      dir,
		containerPath: spec.path,
		readOnly:      spec.readOnlySet && spec.readOnly,
		source:        fmt.Sprintf("ephemeral %q", spec.ephemeral),
		ephemeral:     true,
	}, nil
}

// cleanEphemeral removes every scratch directory this attempt created and
// reports what it removed. The workdir is torn down by the Orker afterwards,
// but a scratch mount is the plugin's own resource: it is released as soon as
// the container that used it is gone, on success and on failure alike.
func cleanEphemeral(mounts []mount, log plugin.Logger) {
	for _, m := range mounts {
		if !m.ephemeral {
			continue
		}
		if err := os.RemoveAll(m.hostPath); err != nil {
			log("container: could not remove %s at %s: %v", m.source, m.hostPath, err)
			continue
		}
		log("container: removed %s (%s)", m.source, m.hostPath)
	}
}
