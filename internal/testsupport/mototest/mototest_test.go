package mototest

import (
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/dextest"
)

func TestImage_IsPinned(t *testing.T) {
	root, err := dextest.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Image(root); err != nil {
		t.Fatal(err)
	}
}

func TestImageFrom(t *testing.T) {
	digest := "@sha256:" + strings.Repeat("a", 64)
	for name, tc := range map[string]struct {
		data string
		ok   bool
	}{
		"pinned, after a comment": {"# why\n\nmotoserver/moto:5.2.5" + digest + "\n", true},
		"a tag alone":             {"motoserver/moto:5.2.5\n", false},
		"latest":                  {"motoserver/moto:latest" + digest, false},
		"another image":           {"example.test/moto:5.2.5" + digest, false},
		"two images":              {"motoserver/moto:5.2.5" + digest + "\nmotoserver/moto:5.2.6" + digest, false},
		"none":                    {"# nothing\n", false},
	} {
		if got, err := imageFrom(tc.data); (err == nil) != tc.ok {
			t.Errorf("%s: %q, %v", name, got, err)
		}
	}
}

func TestRunArgs(t *testing.T) {
	args := RunArgs("img", "p1", "host:1:2")
	for _, want := range []string{"never", "127.0.0.1::5000", "orkstack.proof=p1", "orkstack.proof.owner=host:1:2", "no-new-privileges"} {
		if !slices.Contains(args, want) {
			t.Errorf("docker %v lacks %s", args, want)
		}
	}
	if args[len(args)-1] != "img" {
		t.Errorf("docker %v does not end with the image", args)
	}
	if slices.ContainsFunc(RunArgs("img", "p1", ""), func(a string) bool { return strings.HasPrefix(a, "orkstack.proof") }) {
		t.Error("a Moto with no owner is labelled as a proof's")
	}
}
