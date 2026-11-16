package authn

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadTokenFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if got, err := ReadTokenFile(write("ok", "eyJ.a.b\n"), "x"); err != nil || got != "eyJ.a.b" {
		t.Fatalf("ReadTokenFile = %q, %v", got, err)
	}
	for name, path := range map[string]string{
		"empty":    write("empty", " \n"),
		"two":      write("two", "a b\n"),
		"oversize": write("big", strings.Repeat("a", MaxTokenFileBytes+1)),
		"missing":  filepath.Join(dir, "none"),
	} {
		_, err := ReadTokenFile(path, "the token file")
		if !errors.Is(err, ErrTokenFile) || !strings.HasPrefix(err.Error(), "the token file") {
			t.Errorf("%s: %v; want ErrTokenFile under its label", name, err)
		}
	}
}

func TestLoopbackHost(t *testing.T) {
	for host, want := range map[string]bool{
		"localhost": true, "ork.localhost": false, "LOCALHOST": true, "127.0.0.1": true, "127.9.9.9": true, "::1": true,
		"ork.example.com": false, "10.0.0.1": false, "": false, "localhost.example.com": false,
	} {
		if got := LoopbackHost(host); got != want {
			t.Errorf("LoopbackHost(%q) = %v; want %v", host, got, want)
		}
	}
}
