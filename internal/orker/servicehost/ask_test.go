package servicehost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWrite_OnlyUnderAsk(t *testing.T) {
	root := t.TempDir()
	ask := filepath.Join(root, "ask")
	if err := os.Mkdir(ask, 0o755); err != nil {
		t.Fatal(err)
	}
	a, err := OpenAskDir(ask)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.WriteJSON("lanes/2/poll.json", map[string]any{"schemaVersion": 1, "seq": 1}); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(ask, "lanes", "2", "poll.json")); err != nil || string(got) != `{"schemaVersion":1,"seq":1}` {
		t.Fatalf("poll.json = %q, %v", got, err)
	}
	if err := a.Write("lanes/2/poll.json", []byte(`{"seq":2}`)); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(ask, "lanes", "2"))
	if len(entries) != 1 {
		t.Fatalf("a temporary file was left: %v", entries)
	}
	for _, name := range []string{"../escape.json", "/abs.json", "", "lanes/../../x"} {
		if err := a.Write(name, []byte("x")); err == nil {
			t.Errorf("Write(%q) succeeded", name)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "escape.json")); !os.IsNotExist(err) {
		t.Fatal("a write escaped ask/")
	}
	// A link planted in ask/ (the container cannot, but the Orker never
	// trusts a path) is not followed out of it.
	if err := os.Symlink(root, filepath.Join(ask, "out")); err != nil {
		t.Fatal(err)
	}
	if err := a.Write("out/escape.json", []byte("x")); err == nil {
		t.Fatal("a write followed a link out of ask/")
	}
}
