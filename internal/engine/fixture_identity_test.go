package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The recorded histories under testdata/history are real Temporal histories,
// and a real history names the machine that recorded it twice: in every
// `identity` the SDK stamps on a worker's or client's event (`<pid>@<host>@`)
// and in the sticky task queue's name (`<host>:<uuid>`). Neither is an input
// to the replay guard — the replayer compares commands, the payload half
// compares what the engine put into its calls — so a fixture carries them
// scrubbed: the identity reads `0@fixture@` and the sticky queue `fixture:<uuid>`.
// Every other identity (an Orker's id, a replica's name) is the test's own
// and stays. Nothing else in a fixture changes, which is what keeps this on
// the right side of the README's rule against hand-editing: a fixture scrubbed
// of its host says exactly what it said about the engine.
//
// TestFixtures_CarryNoHostIdentity fails for a fixture that still names a
// host — a freshly recorded one — and names the command that scrubs it:
//
//	ORK_SCRUB_FIXTURES=1 go test ./internal/engine/ -run TestFixtures_CarryNoHostIdentity

var (
	// The SDK's default identity: the process id, the host, a trailing `@`.
	hostIdentityRE = regexp.MustCompile(`("identity":\s*")\d+@[^@"]+@(")`)
	// A sticky task queue's name: the host, a colon, a UUID.
	stickyQueueRE = regexp.MustCompile(`("name":\s*")[^":]+:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")`)
)

// scrubHostIdentity rewrites a recorded history's host-bearing identities and
// sticky queue names to their fixture forms, and nothing else.
func scrubHostIdentity(encoded []byte) []byte {
	out := hostIdentityRE.ReplaceAll(encoded, []byte(`${1}0@fixture@${2}`))
	return stickyQueueRE.ReplaceAll(out, []byte(`${1}fixture:${2}`))
}

func TestScrubHostIdentity(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"a worker identity", `"identity": "2770562@some-host.local@"`, `"identity": "0@fixture@"`},
		{"a sticky queue", `"name": "some-host.local:d5c19ab9-ed0c-49bf-814a-43a7ee5bc363"`, `"name": "fixture:d5c19ab9-ed0c-49bf-814a-43a7ee5bc363"`},
		{"an Orker's own identity stays", `"identity": "orker-1"`, `"identity": "orker-1"`},
		{"a replica's name stays", `"identity": "replica-a"`, `"identity": "replica-a"`},
		{"a normal task queue stays", `"name": "ork-tasks"`, `"name": "ork-tasks"`},
		{"an Orker control queue stays", `"name": "ork-orker-some-host-8365cf02"`, `"name": "ork-orker-some-host-8365cf02"`},
		{"already scrubbed is a fixed point", `"identity": "0@fixture@"`, `"identity": "0@fixture@"`},
	} {
		if got := string(scrubHostIdentity([]byte(tc.in))); got != tc.want {
			t.Errorf("%s: %s -> %s, want %s", tc.name, tc.in, got, tc.want)
		}
	}
}

func TestFixtures_CarryNoHostIdentity(t *testing.T) {
	paths, err := filepath.Glob("testdata/history/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no fixtures under testdata/history")
	}
	scrub := os.Getenv("ORK_SCRUB_FIXTURES") == "1"
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		scrubbed := scrubHostIdentity(raw)
		if bytes.Equal(raw, scrubbed) {
			continue
		}
		if !scrub {
			t.Errorf("%s names the host that recorded it; ORK_SCRUB_FIXTURES=1 go test ./internal/engine/ -run TestFixtures_CarryNoHostIdentity scrubs it", p)
			continue
		}
		if err := os.WriteFile(p, scrubbed, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("scrubbed %s", p)
	}
}
