package dist

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// QuickstartAsset is the bundle's file name; like every asset it carries
	// no version, so releases/latest/download/ keeps working.
	QuickstartAsset = "orkstack-quickstart.tar.gz"
	// QuickstartDir is the one top-level directory every entry of the bundle
	// sits under.
	QuickstartDir = "orkstack-quickstart"
	// QuickstartComposeFile and QuickstartReadmeFile are the bundle's compose
	// file and README, relative to the repository root. The bundle holds them
	// as they are; deploy/quickstart/smoke.yaml, the release smoke's
	// override, is never part of it.
	QuickstartComposeFile = "deploy/quickstart/compose.yaml"
	QuickstartReadmeFile  = "deploy/quickstart/README.txt"
	// ExamplesDir holds the Orkflows the bundle ships: the ExampleSuffix files
	// directly in it, and nothing below it.
	ExamplesDir   = "examples"
	ExampleSuffix = ".ork.yaml"
)

// QuickstartOptions say what Quickstart packs.
type QuickstartOptions struct {
	// Root is the repository root the bundle's files are read from.
	Root string
	// Version is named in the .env's comment.
	Version Version
	// Images are the digests of the ten images a release publishes.
	Images []Image
	// Registry and Tag are the host[:port] and tag every image is named
	// under: ghcr.io and the version without its "v" in a release, a
	// registry of the smoke's own and its token in the smoke.
	Registry string
	Tag      string
	// CommitTime stamps every entry.
	CommitTime time.Time
}

var (
	// registryHost is a dot-separated host name or an IPv4 address, built of
	// the domain components the OCI reference grammar allows.
	registryHost = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*$`)
	registryPort = regexp.MustCompile(`^[0-9]{1,5}$`)
	imageTag     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$`)
)

// ValidateRegistry refuses a registry that is not a bare host[:port]: no
// scheme, no path, no credentials.
func ValidateRegistry(registry string) error {
	host, port, hasPort := strings.Cut(registry, ":")
	if !registryHost.MatchString(host) {
		return fmt.Errorf("registry %q is not a host or host:port (for example ghcr.io or 127.0.0.1:5000)", registry)
	}
	if hasPort {
		if n, err := strconv.Atoi(port); !registryPort.MatchString(port) || err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("registry %q: %q is not a port from 1 to 65535", registry, port)
		}
	}
	return nil
}

// ValidateTag refuses a tag an image reference cannot carry.
func ValidateTag(tag string) error {
	if !imageTag.MatchString(tag) {
		return fmt.Errorf("tag %q is not an image tag: 1 to 128 letters, digits, '_', '.' or '-', not starting with '.' or '-'", tag)
	}
	return nil
}

// envVariable is the compose variable that names an image.
func envVariable(image string) string {
	return "ORKSTACK_" + strings.ToUpper(image) + "_IMAGE"
}

// QuickstartEnv renders the bundle's .env: a comment naming the version, the
// four platform images by tag and digest, and the activity catalog as the
// one-line YAML document ORK_ACTIVITY_CATALOG reads. The release writes
// `registry/orkstack/<name>:<tag>@<digest>` for each image, which is the
// reference the engine pins by its digest and the Docker daemon pulls by it
// too.
func QuickstartEnv(version Version, registry, tag string, images []Image) ([]byte, error) {
	if err := ValidateRegistry(registry); err != nil {
		return nil, err
	}
	if err := ValidateTag(tag); err != nil {
		return nil, err
	}
	checked, err := checkImages(images)
	if err != nil {
		return nil, err
	}
	digests := make(map[string]string, len(checked))
	for _, image := range checked {
		digests[image.Name] = image.Digest
	}
	reference := func(name string) string {
		return registry + "/" + ImageNamespace + "/" + name + ":" + tag + "@" + digests[name]
	}

	var out strings.Builder
	fmt.Fprintf(&out, "# OrkStack %s: every image by tag and digest.\n", version)
	for _, name := range serviceImages {
		fmt.Fprintf(&out, "%s=%s\n", envVariable(name), reference(name))
	}
	var activities []string
	for _, name := range activityNames {
		activities = append(activities, fmt.Sprintf("%s: %q", name, reference(ActivityPrefix+name)))
	}
	fmt.Fprintf(&out, "ORKSTACK_ACTIVITY_CATALOG={activities: {%s}}\n", strings.Join(activities, ", "))
	return []byte(out.String()), nil
}

var requiredVariable = regexp.MustCompile(`\$\{([A-Z][A-Z0-9_]*):?\?`)

// requiredVariables lists the variables a compose file cannot start without
// (${NAME:?message} or ${NAME?message}), outside its comments.
func requiredVariables(compose []byte) []string {
	var kept []string
	for _, line := range strings.Split(string(compose), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			kept = append(kept, line)
		}
	}
	var names []string
	for _, m := range requiredVariable.FindAllStringSubmatch(strings.Join(kept, "\n"), -1) {
		if !slices.Contains(names, m[1]) {
			names = append(names, m[1])
		}
	}
	sort.Strings(names)
	return names
}

// readNonEmpty reads a file the bundle holds and refuses an empty one.
func readNonEmpty(root, rel string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", rel, err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%s is empty; every bundle carries it", rel)
	}
	return data, nil
}

// Quickstart packs the quickstart bundle: orkstack-quickstart/ holding the
// repository's compose file and README as they are, a .env that names the
// release's images (QuickstartEnv), the LICENSE and the Orkflows directly in
// examples/. Like the other archives it is deterministic: sorted, root-owned,
// stamped with the commit time.
//
// It refuses a compose file that needs a variable the .env does not set, so a
// bundle that could not start is never packed.
func Quickstart(opts QuickstartOptions) (Asset, error) {
	if opts.CommitTime.IsZero() {
		return Asset{}, errors.New("no commit time: the bundle is stamped with the commit's time, never the clock")
	}
	env, err := QuickstartEnv(opts.Version, opts.Registry, opts.Tag, opts.Images)
	if err != nil {
		return Asset{}, err
	}
	compose, err := readNonEmpty(opts.Root, QuickstartComposeFile)
	if err != nil {
		return Asset{}, err
	}
	readme, err := readNonEmpty(opts.Root, QuickstartReadmeFile)
	if err != nil {
		return Asset{}, err
	}
	license, err := readNonEmpty(opts.Root, LicenseFile)
	if err != nil {
		return Asset{}, err
	}
	for _, variable := range requiredVariables(compose) {
		if !strings.Contains("\n"+string(env), "\n"+variable+"=") {
			return Asset{}, fmt.Errorf("%s requires %s, which the release's .env does not set; "+
				"a bundle that cannot start is not packed", QuickstartComposeFile, variable)
		}
	}

	entries := []Entry{
		{Name: "compose.yaml", Data: compose, Mode: modeFile},
		{Name: ".env", Data: env, Mode: modeFile},
		{Name: "README.txt", Data: readme, Mode: modeFile},
		{Name: "LICENSE", Data: license, Mode: modeFile},
	}
	examples, err := os.ReadDir(filepath.Join(opts.Root, ExamplesDir))
	if err != nil {
		return Asset{}, fmt.Errorf("read %s: %w", ExamplesDir, err)
	}
	shipped := 0
	for _, example := range examples {
		if !strings.HasSuffix(example.Name(), ExampleSuffix) {
			continue
		}
		if !example.Type().IsRegular() {
			return Asset{}, fmt.Errorf("%s/%s is not a regular file", ExamplesDir, example.Name())
		}
		data, err := os.ReadFile(filepath.Join(opts.Root, ExamplesDir, example.Name()))
		if err != nil {
			return Asset{}, fmt.Errorf("read %s/%s: %w", ExamplesDir, example.Name(), err)
		}
		entries = append(entries, Entry{Name: ExamplesDir + "/" + example.Name(), Data: data, Mode: modeFile})
		shipped++
	}
	if shipped == 0 {
		return Asset{}, fmt.Errorf("%s holds no *%s file; the bundle's README applies one", ExamplesDir, ExampleSuffix)
	}
	data, err := TarGzTree(QuickstartDir, entries, opts.CommitTime)
	if err != nil {
		return Asset{}, fmt.Errorf("pack the quickstart bundle: %w", err)
	}
	return Asset{Name: QuickstartAsset, Data: data}, nil
}
