package orker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestDockerRuntime_ImageLabel reads one label from `image inspect`'s
// labels, tells an absent label from an absent image, and refuses output
// that is not a label map.
func TestDockerRuntime_ImageLabel(t *testing.T) {
	for _, tc := range []struct {
		name, answer string
		code         int
		value        string
		ok, err      bool
	}{
		{name: "labelled", answer: `{"org.orkstack.build":"c0ffee","a":"b"}`, value: "c0ffee", ok: true},
		{name: "another label only", answer: `{"a":"b"}`},
		{name: "no labels", answer: `null`},
		{name: "no image", answer: "Error: No such image", code: 1, err: true},
		{name: "not a map", answer: `["x"]`, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := "#!/bin/sh\n[ \"$1 $2 $3 $4\" = 'image inspect --format {{json .Config.Labels}}' ] || exit 9\n" +
				"printf '%s\\n' " + shellQuote(tc.answer) + "\nexit " + string(rune('0'+tc.code)) + "\n"
			bin := filepath.Join(t.TempDir(), "docker")
			must.OK(t, os.WriteFile(bin, []byte(script), 0o755))
			value, ok, err := (&DockerRuntime{Bin: bin}).ImageLabel(t.Context(), "img@sha256:x", LabelBuild)
			if (err != nil) != tc.err || ok != tc.ok || value != tc.value {
				t.Fatalf("ImageLabel = %q, %v, %v", value, ok, err)
			}
		})
	}
}

// TestDockerRuntime_LocalDaemon refuses an environment that points the CLI
// at another daemon, and never names the value, which can hold a remote's
// user or credentials.
func TestDockerRuntime_LocalDaemon(t *testing.T) {
	for _, tc := range []struct {
		name, key, value string
		refused          bool
	}{
		{name: "nothing set", key: "DOCKER_HOST", value: ""},
		{name: "the local socket", key: "DOCKER_HOST", value: "unix:///run/docker.sock"},
		{name: "a podman socket", key: "CONTAINER_HOST", value: "unix:///run/user/1000/podman/podman.sock"},
		{name: "the default context", key: "DOCKER_CONTEXT", value: "default"},
		{name: "a tcp daemon", key: "DOCKER_HOST", value: "tcp://secret-host:2376", refused: true},
		{name: "an ssh daemon", key: "DOCKER_HOST", value: "ssh://admin@secret-host", refused: true},
		{name: "a remote podman", key: "CONTAINER_HOST", value: "ssh://admin@secret-host/run/podman.sock", refused: true},
		{name: "a context", key: "DOCKER_CONTEXT", value: "secret-host", refused: true},
		{name: "a connection", key: "CONTAINER_CONNECTION", value: "secret-host", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range []string{"DOCKER_HOST", "CONTAINER_HOST", "DOCKER_CONTEXT", "CONTAINER_CONNECTION"} {
				t.Setenv(k, "")
			}
			t.Setenv(tc.key, tc.value)
			err := (&DockerRuntime{}).LocalDaemon()
			if (err != nil) != tc.refused {
				t.Fatalf("LocalDaemon = %v", err)
			}
			if err != nil && (strings.Contains(err.Error(), "secret-host") || !strings.Contains(err.Error(), tc.key)) {
				t.Fatalf("LocalDaemon = %v; want it to name %s and not its value", err, tc.key)
			}
		})
	}
}

// TestDockerRuntime_ImageRunsAsRoot reads the user an image declares: none, root by
// name or by number, in any group, is root; what cannot be read is an error.
func TestDockerRuntime_ImageRunsAsRoot(t *testing.T) {
	for answer, want := range map[string]struct {
		code      int
		root, err bool
	}{
		`""`: {root: true}, `"root"`: {root: true}, `"0"`: {root: true}, `"0:1000"`: {root: true}, `"root:app"`: {root: true}, `"000"`: {root: true},
		`"1000"`: {}, `"app"`: {}, `"1000:0"`: {}, `"rooty"`: {}, `"10"`: {},
		"Error: No such image": {code: 1, err: true}, "{": {err: true},
	} {
		script := "#!/bin/sh\n[ \"$1 $2 $3 $4\" = 'image inspect --format {{json .Config.User}}' ] || exit 9\n" +
			"printf '%s\\n' " + shellQuote(answer) + "\nexit " + string(rune('0'+want.code)) + "\n"
		bin := filepath.Join(t.TempDir(), "docker")
		must.OK(t, os.WriteFile(bin, []byte(script), 0o755))
		root, err := (&DockerRuntime{Bin: bin}).ImageRunsAsRoot(t.Context(), "img@sha256:x")
		if root != want.root || (err != nil) != want.err {
			t.Errorf("%s: %v, %v", answer, root, err)
		}
	}
}
