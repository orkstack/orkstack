package dist_test

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
	"github.com/orkstack/orkstack/internal/dsl"
)

const bundleDir = "orkstack-quickstart/"

func releaseImages() []dist.Image {
	var images []dist.Image
	for _, name := range dist.Images() {
		images = append(images, dist.Image{Name: name, Digest: fakeDigest(name)})
	}
	return images
}

// realOptions packs the repository's own quickstart files, as a release does.
func realOptions(t *testing.T) dist.QuickstartOptions {
	t.Helper()
	return dist.QuickstartOptions{
		Root: repoRoot(t), Version: mustVersion(t, "v0.1.0"), Images: releaseImages(),
		Registry: "ghcr.io", Tag: "0.1.0", CommitTime: commitTime,
	}
}

// bundleRoot writes a repository root holding the files a bundle is made of:
// the real compose file and README (so the variables are the real ones), a
// LICENSE, two examples, and the things a bundle must leave out.
func bundleRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel string, data []byte) {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("LICENSE", fakeLicense)
	write(dist.QuickstartComposeFile, readRepoFile(t, dist.QuickstartComposeFile))
	write(dist.QuickstartReadmeFile, readRepoFile(t, dist.QuickstartReadmeFile))
	write("examples/alpha.ork.yaml", []byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: alpha\n"))
	write("examples/beta.ork.yaml", []byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: beta\n"))
	// Not part of a bundle.
	write("deploy/quickstart/smoke.yaml", []byte("services: {registry: {image: registry:3}}\n"))
	write("deploy/quickstart/.env.example", []byte("ORKSTACK_ORK_IMAGE=x\n"))
	write("examples/README.md", []byte("# examples\n"))
	write("examples/automation/nested.ork.yaml", []byte("nested\n"))
	write("examples/custom-image/Dockerfile", []byte("FROM scratch\n"))
	return root
}

func optionsFor(t *testing.T, root string) dist.QuickstartOptions {
	t.Helper()
	opts := realOptions(t)
	opts.Root = root
	return opts
}

func TestQuickstartEnv_NamesEveryImageByTagAndDigest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ registry, tag, version string }{
		{"ghcr.io", "0.1.0", "v0.1.0"},
		{"127.0.0.1:34567", "smoke-4242-a1b2c3", "v0.1.0"},
		{"registry.example.com:5000", "1.20.3", "v1.20.3"},
	} {
		version := mustVersion(t, tc.version)
		data, err := dist.QuickstartEnv(version, tc.registry, tc.tag, releaseImages())
		if err != nil {
			t.Fatalf("%s %s: %v", tc.registry, tc.tag, err)
		}
		text := string(data)
		lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
		if !strings.HasSuffix(text, "\n") || len(lines) != 6 {
			t.Fatalf("the .env has %d lines (final newline %v), want a comment and five variables:\n%s", len(lines), strings.HasSuffix(text, "\n"), text)
		}
		if !strings.HasPrefix(lines[0], "# ") || !strings.Contains(lines[0], tc.version) {
			t.Errorf("the first line %q must be one comment naming the version %s", lines[0], tc.version)
		}
		env := parseEnvFile(t, ".env", data)

		ref := func(name string) string {
			return tc.registry + "/orkstack/" + name + ":" + tc.tag + "@" + fakeDigest(name)
		}
		for variable, name := range map[string]string{
			"ORKSTACK_ORK_IMAGE": "ork", "ORKSTACK_ORKER_IMAGE": "orker", "ORKSTACK_UI_IMAGE": "ui", "ORKSTACK_ORKCTL_IMAGE": "orkctl",
		} {
			if env[variable] != ref(name) {
				t.Errorf("%s = %q, want %q", variable, env[variable], ref(name))
			}
			parsed, err := dsl.ParseReference(env[variable])
			if err != nil || parsed.Registry != tc.registry || parsed.Repository != "orkstack/"+name ||
				parsed.Tag != tc.tag || parsed.Digest != fakeDigest(name) {
				t.Errorf("%s = %q does not parse to registry %s, orkstack/%s, tag %s and its digest: %+v (%v)",
					variable, env[variable], tc.registry, name, tc.tag, parsed, err)
			}
		}

		want := `{activities: {shell: "` + ref("activity-shell") + `", http: "` + ref("activity-http") +
			`", file: "` + ref("activity-file") + `", json: "` + ref("activity-json") +
			`", git: "` + ref("activity-git") + `", template: "` + ref("activity-template") + `"}}`
		if env["ORKSTACK_ACTIVITY_CATALOG"] != want {
			t.Errorf("ORKSTACK_ACTIVITY_CATALOG =\n  %s\nwant\n  %s", env["ORKSTACK_ACTIVITY_CATALOG"], want)
		}
		// And it is what the engine reads: a catalog document that names
		// each activity by its image, pinned.
		catalog, err := dsl.ParseCatalog([]byte(env["ORKSTACK_ACTIVITY_CATALOG"]))
		if err != nil {
			t.Fatalf("the engine does not accept the catalog: %v", err)
		}
		if !slices.Equal(catalog.Names(), dsl.BuiltinActivityCatalog().Names()) {
			t.Errorf("the catalog names %v, want the built-in names %v", catalog.Names(), dsl.BuiltinActivityCatalog().Names())
		}
		for name, reference := range catalog {
			if reference != ref("activity-"+name) {
				t.Errorf("catalog %s = %q, want %q", name, reference, ref("activity-"+name))
			}
		}
	}
}

func TestQuickstartEnv_SetsExactlyTheVariablesComposeRequires(t *testing.T) {
	t.Parallel()
	data, err := dist.QuickstartEnv(mustVersion(t, "v0.1.0"), "ghcr.io", "0.1.0", releaseImages())
	if err != nil {
		t.Fatal(err)
	}
	required, _ := composeVariables(string(readRepoFile(t, composePath)))
	var set []string
	for key := range parseEnvFile(t, ".env", data) {
		set = append(set, key)
	}
	sort.Strings(set)
	sort.Strings(required)
	if !slices.Equal(set, required) {
		t.Errorf("the release's .env sets %v, but compose.yaml requires %v", set, required)
	}
}

func TestQuickstartEnv_DoesNotDependOnTheOrderOfTheImages(t *testing.T) {
	t.Parallel()
	forward, err := dist.QuickstartEnv(mustVersion(t, "v0.1.0"), "ghcr.io", "0.1.0", releaseImages())
	if err != nil {
		t.Fatal(err)
	}
	reversed := releaseImages()
	slices.Reverse(reversed)
	backward, err := dist.QuickstartEnv(mustVersion(t, "v0.1.0"), "ghcr.io", "0.1.0", reversed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forward, backward) {
		t.Errorf("the order of the images changed the .env:\n%s\n%s", forward, backward)
	}
}

func TestQuickstartBundle_IsDeterministicAndNeverHoldsTheSmokeOverride(t *testing.T) {
	t.Parallel()
	for name, root := range map[string]string{"the repository": repoRoot(t), "a root with the smoke's files": bundleRoot(t)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts := optionsFor(t, root)
			first, err := dist.Quickstart(opts)
			if err != nil {
				t.Fatal(err)
			}
			if first.Name != "orkstack-quickstart.tar.gz" {
				t.Errorf("asset named %q, want orkstack-quickstart.tar.gz (no version, so releases/latest/download keeps working)", first.Name)
			}
			again, err := dist.Quickstart(opts)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.Data, again.Data) {
				t.Error("packing twice gave different bytes")
			}
			reversed := opts
			reversed.Images = slices.Clone(opts.Images)
			slices.Reverse(reversed.Images)
			if shuffled, err := dist.Quickstart(reversed); err != nil || !bytes.Equal(first.Data, shuffled.Data) {
				t.Errorf("the order of the images changed the bundle (%v)", err)
			}
			later := opts
			later.CommitTime = commitTime.Add(time.Hour)
			if other, err := dist.Quickstart(later); err != nil || bytes.Equal(first.Data, other.Data) {
				t.Errorf("a different commit time gave the same bytes (%v), so the time is not in the bundle", err)
			}
			retagged := opts
			retagged.Tag = "9.9.9"
			if other, err := dist.Quickstart(retagged); err != nil || bytes.Equal(first.Data, other.Data) {
				t.Errorf("a different tag gave the same bytes (%v), so the .env is not in the bundle", err)
			}

			checkBundle(t, first.Data, root, opts)
		})
	}
}

// checkBundle holds a packed bundle to its layout.
func checkBundle(t *testing.T, data []byte, root string, opts dist.QuickstartOptions) {
	t.Helper()
	headers, contents := readTarGz(t, data)
	var names []string
	for _, h := range headers {
		names = append(names, h.Name)
		if !strings.HasPrefix(h.Name, bundleDir) {
			t.Errorf("%s is outside the top-level directory %s", h.Name, bundleDir)
		}
		if h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" {
			t.Errorf("%s is owned by %d:%d %q:%q, want root", h.Name, h.Uid, h.Gid, h.Uname, h.Gname)
		}
		if !h.ModTime.Equal(opts.CommitTime) {
			t.Errorf("%s has mtime %v, want the commit time", h.Name, h.ModTime)
		}
		if strings.HasSuffix(h.Name, "/") {
			if h.Typeflag != tar.TypeDir || h.Mode != 0o755 {
				t.Errorf("directory %s: type %q mode %o, want a 0755 directory", h.Name, h.Typeflag, h.Mode)
			}
		} else if h.Typeflag != tar.TypeReg || h.Mode != 0o644 {
			t.Errorf("file %s: type %q mode %o, want a regular 0644 file", h.Name, h.Typeflag, h.Mode)
		}
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("entries are not sorted: %q", names)
	}
	if len(names) == 0 || names[0] != bundleDir {
		t.Errorf("the first entry is %q, want the directory %s", names, bundleDir)
	}

	// Exactly these entries: the files, and the examples at the top of
	// examples/ and nothing deeper.
	want := []string{bundleDir, bundleDir + ".env", bundleDir + "LICENSE", bundleDir + "README.txt", bundleDir + "compose.yaml", bundleDir + "examples/"}
	matches, err := filepath.Glob(filepath.Join(root, "examples", "*.ork.yaml"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no examples under %s (%v)", root, err)
	}
	for _, m := range matches {
		want = append(want, bundleDir+"examples/"+filepath.Base(m))
	}
	sort.Strings(want)
	if !slices.Equal(names, want) {
		t.Errorf("entries:\n  %q\nwant\n  %q", names, want)
	}
	for _, name := range names {
		if strings.Contains(name, "smoke") || strings.Contains(name, ".env.example") {
			t.Errorf("the bundle holds %s", name)
		}
	}

	byteForByte := map[string]string{
		"compose.yaml": dist.QuickstartComposeFile, "README.txt": dist.QuickstartReadmeFile, "LICENSE": "LICENSE",
	}
	for name, source := range byteForByte {
		original, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(source)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(contents[bundleDir+name], original) {
			t.Errorf("%s is not %s byte for byte", name, source)
		}
	}
	for _, m := range matches {
		original, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(contents[bundleDir+"examples/"+filepath.Base(m)], original) {
			t.Errorf("examples/%s is not the repository's", filepath.Base(m))
		}
	}
	env, err := dist.QuickstartEnv(opts.Version, opts.Registry, opts.Tag, opts.Images)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents[bundleDir+".env"], env) {
		t.Errorf(".env is %q, want what QuickstartEnv renders", contents[bundleDir+".env"])
	}
	// The override's bytes are nowhere in the bundle, whatever it was named.
	if smoke, err := os.ReadFile(filepath.Join(root, "deploy", "quickstart", "smoke.yaml")); err == nil {
		for name, body := range contents {
			if bytes.Contains(body, smoke) {
				t.Errorf("%s holds the smoke override", name)
			}
		}
	}
}

func TestQuickstart_AcceptsTheReleaseAndTheSmokesValues(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ registry, tag string }{
		{"ghcr.io", "0.1.0"},
		{"127.0.0.1:34567", "smoke-12345-a1b2c3"},
		{"localhost:5000", "dev"},
		{"registry.example.com", "1.2.3-rc.1"},
		{"my-registry.internal:443", "_x"},
		{"registry", strings.Repeat("a", 128)},
	} {
		if err := dist.ValidateRegistry(tc.registry); err != nil {
			t.Errorf("registry %q refused: %v", tc.registry, err)
		}
		if err := dist.ValidateTag(tc.tag); err != nil {
			t.Errorf("tag %q refused: %v", tc.tag, err)
		}
	}
}

func TestQuickstart_RefusesABadRegistry(t *testing.T) {
	t.Parallel()
	for _, registry := range []string{
		"", " ", "ghcr.io/", "ghcr.io/orkstack", "https://ghcr.io", "ghcr.io:", ":5000", "ghcr.io:0", "ghcr.io:65536",
		"ghcr.io:99999", "ghcr.io:port", "ghcr.io:5000:1", "ghcr io", "ghcr.io\n", "-ghcr.io", "ghcr-.io", ".ghcr.io",
		"ghcr.io.", "ghcr..io", "user@ghcr.io", "[::1]:5000", "ghcr.io:+5000", "ghcr_io",
	} {
		if err := dist.ValidateRegistry(registry); err == nil {
			t.Errorf("registry %q accepted; want host or host:port", registry)
		}
		if _, err := dist.QuickstartEnv(mustVersion(t, "v0.1.0"), registry, "0.1.0", releaseImages()); err == nil {
			t.Errorf("QuickstartEnv accepted registry %q", registry)
		}
	}
}

func TestQuickstart_RefusesABadTag(t *testing.T) {
	t.Parallel()
	for _, tag := range []string{
		"", " ", "-1", ".1", "a b", "a/b", "a:b", "a@b", "v1\n", strings.Repeat("a", 129), "ü", "a+b",
		"sha256:" + strings.Repeat("a", 64),
	} {
		if err := dist.ValidateTag(tag); err == nil {
			t.Errorf("tag %q accepted", tag)
		}
		if _, err := dist.QuickstartEnv(mustVersion(t, "v0.1.0"), "ghcr.io", tag, releaseImages()); err == nil {
			t.Errorf("QuickstartEnv accepted tag %q", tag)
		}
	}
}

func TestQuickstart_RefusesAnImageListThatIsNotTheTen(t *testing.T) {
	t.Parallel()
	opts := optionsFor(t, bundleRoot(t))
	withOut := func(drop string) []dist.Image {
		var images []dist.Image
		for _, image := range releaseImages() {
			if image.Name != drop {
				images = append(images, image)
			}
		}
		return images
	}
	for name, images := range map[string][]dist.Image{
		"none":              nil,
		"without the ork":   withOut("ork"),
		"without the ui":    withOut("ui"),
		"without a shell":   withOut("activity-shell"),
		"an unknown image":  append(releaseImages(), dist.Image{Name: "activity-curl", Digest: fakeDigest("curl")}),
		"an image twice":    append(releaseImages(), releaseImages()[0]),
		"a bad digest":      append(withOut("ui"), dist.Image{Name: "ui", Digest: "sha256:abc"}),
		"an uppercase name": append(withOut("ui"), dist.Image{Name: "UI", Digest: fakeDigest("ui")}),
	} {
		bad := opts
		bad.Images = images
		if _, err := dist.Quickstart(bad); err == nil {
			t.Errorf("Quickstart accepted an image list %s", name)
		}
		if _, err := dist.QuickstartEnv(opts.Version, opts.Registry, opts.Tag, images); err == nil {
			t.Errorf("QuickstartEnv accepted an image list %s", name)
		}
	}
}

func TestQuickstart_RefusesARepositoryThatCannotMakeABundle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(root string) error
		want   string
	}{
		{"no compose file", func(root string) error { return os.Remove(filepath.Join(root, dist.QuickstartComposeFile)) }, "compose.yaml"},
		{"no README", func(root string) error { return os.Remove(filepath.Join(root, dist.QuickstartReadmeFile)) }, "README.txt"},
		{"no LICENSE", func(root string) error { return os.Remove(filepath.Join(root, "LICENSE")) }, "LICENSE"},
		{"an empty LICENSE", func(root string) error { return os.WriteFile(filepath.Join(root, "LICENSE"), nil, 0o644) }, "LICENSE"},
		{"an empty compose file", func(root string) error {
			return os.WriteFile(filepath.Join(root, dist.QuickstartComposeFile), nil, 0o644)
		}, "compose.yaml"},
		{"no examples", func(root string) error {
			matches, _ := filepath.Glob(filepath.Join(root, "examples", "*.ork.yaml"))
			for _, m := range matches {
				if err := os.Remove(m); err != nil {
					return err
				}
			}
			return nil
		}, "examples"},
		{"an example that is not a file", func(root string) error {
			return os.Symlink("alpha.ork.yaml", filepath.Join(root, "examples", "link.ork.yaml"))
		}, "link.ork.yaml"},
		{"a compose file that needs a variable the .env does not set", func(root string) error {
			compose := filepath.Join(root, dist.QuickstartComposeFile)
			data, err := os.ReadFile(compose)
			if err != nil {
				return err
			}
			return os.WriteFile(compose, append(data, []byte("# x\n  extra:\n    image: ${ORKSTACK_EXTRA_IMAGE:?set in .env}\n")...), 0o644)
		}, "ORKSTACK_EXTRA_IMAGE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := bundleRoot(t)
			if err := tc.mutate(root); err != nil {
				if os.IsNotExist(err) || strings.Contains(err.Error(), "operation not permitted") {
					t.Skipf("cannot set the case up here: %v", err)
				}
				t.Fatal(err)
			}
			if _, err := dist.Quickstart(optionsFor(t, root)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %v; want a refusal naming %s", err, tc.want)
			}
		})
	}
	t.Run("no commit time", func(t *testing.T) {
		t.Parallel()
		opts := optionsFor(t, bundleRoot(t))
		opts.CommitTime = time.Time{}
		if _, err := dist.Quickstart(opts); err == nil || !strings.Contains(err.Error(), "commit time") {
			t.Errorf("error %v; want a refusal naming the commit time", err)
		}
	})
}

// TestQuickstart_TheRegistryAndTagAreInTheLines: the registry and tag a caller
// passes reach every reference, and nothing else of the host does.
func TestQuickstart_TheRegistryAndTagAreInTheLines(t *testing.T) {
	t.Parallel()
	data, err := dist.QuickstartEnv(mustVersion(t, "v0.1.0"), "127.0.0.1:5555", "tok", releaseImages())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "ghcr.io") {
		t.Errorf("the .env still names ghcr.io:\n%s", data)
	}
	references := regexp.MustCompile(`127\.0\.0\.1:5555/orkstack/[a-z-]+:tok@sha256:[0-9a-f]{64}`).FindAllString(string(data), -1)
	if len(references) != 10 {
		t.Errorf("found %d references, want 10 (four images and six activities):\n%s", len(references), data)
	}
}

// TestQuickstartReadme_NamesOnlyWhatTheBundleShips: the commands the bundle's
// README and compose file show run against the files the bundle holds, so an
// example that is renamed or removed cannot leave them describing one that is
// not there.
func TestQuickstartReadme_NamesOnlyWhatTheBundleShips(t *testing.T) {
	t.Parallel()
	named := regexp.MustCompile(`examples/([A-Za-z0-9._-]+\.ork\.yaml)`)
	submit := regexp.MustCompile(`run submit ([a-z][a-z0-9-]*)`)
	for _, file := range []string{dist.QuickstartReadmeFile, dist.QuickstartComposeFile} {
		text := string(readRepoFile(t, file))
		if file == dist.QuickstartReadmeFile && !named.MatchString(text) {
			t.Errorf("%s names no example to apply", file)
		}
		for _, m := range named.FindAllStringSubmatch(text, -1) {
			if _, err := os.Stat(filepath.Join(repoRoot(t), dist.ExamplesDir, m[1])); err != nil {
				t.Errorf("%s names examples/%s, which the bundle does not ship: %v", file, m[1], err)
			}
		}
		for _, m := range submit.FindAllStringSubmatch(text, -1) {
			found := false
			matches, err := filepath.Glob(filepath.Join(repoRoot(t), "examples", "*"+dist.ExampleSuffix))
			if err != nil {
				t.Fatal(err)
			}
			for _, example := range matches {
				found = found || regexp.MustCompile(`(?m)^name:\s*`+regexp.QuoteMeta(m[1])+`\s*$`).Match(mustRead(t, example))
			}
			if !found {
				t.Errorf("%s submits %q, which no shipped example declares as its name", file, m[1])
			}
		}
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
