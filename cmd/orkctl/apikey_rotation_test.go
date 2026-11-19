package main

import (
	"fmt"
	"slices"
	"testing"
)

// keysInUse reports whether every key of want carried a call among seen.
// TestIntegration_APIKeyReplacedMidRun gives the engine and the Orker keys
// of their own, so a key names the process that sent it: a wait on one key
// both processes share is satisfied by whichever re-reads its file first.
func keysInUse(seen []string, want ...string) error {
	var missing []string
	for _, k := range want {
		if !slices.Contains(seen, k) {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("no call yet with %v", missing)
	}
	return nil
}

// oldKeyAfterNew reports a call on old after the first call on new among
// seen: the process sending them went back to a key it had replaced.
func oldKeyAfterNew(seen []string, old, new string) error {
	first := slices.Index(seen, new)
	if first < 0 {
		return fmt.Errorf("no call with %s", new)
	}
	if i := slices.Index(seen[first:], old); i >= 0 {
		return fmt.Errorf("call %d carried %s after call %d carried %s", first+i, old, first, new)
	}
	return nil
}

// TestOldKeyAfterNew_RefusesAProcessThatKeptItsOldKey: a call on the old key
// before the first on the new one is a straggler; one after it means the
// process went back to the old key.
func TestOldKeyAfterNew_RefusesAProcessThatKeptItsOldKey(t *testing.T) {
	tests := []struct {
		name string
		seen []string
		ok   bool
	}{
		{"only the new key", []string{"ork-key-two", "ork-key-two"}, true},
		{"a straggler before the new key", []string{"ork-key-one", "ork-key-two"}, true},
		{"the other process's calls do not count", []string{"ork-key-two", "orker-key-one"}, true},
		{"the old key after the new one", []string{"ork-key-two", "ork-key-one"}, false},
		{"no call on the new key", []string{"ork-key-one"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := oldKeyAfterNew(tt.seen, "ork-key-one", "ork-key-two")
			if (err == nil) != tt.ok {
				t.Fatalf("oldKeyAfterNew(%v) = %v, want ok %v", tt.seen, err, tt.ok)
			}
		})
	}
}

// TestKeysInUse_WaitsForEveryProcessesKey: the rotation test's wait holds
// until each process's own new key carried a call. The sequence that failed
// a proof, the Orker's calls on its new key while the engine's still carry
// its old one, keeps it waiting.
func TestKeysInUse_WaitsForEveryProcessesKey(t *testing.T) {
	tests := []struct {
		name string
		seen []string
		ok   bool
	}{
		{"nothing yet", nil, false},
		{"only the Orker re-read its file", []string{"ork-key-one", "orker-key-two", "orker-key-two"}, false},
		{"only the engine re-read its file", []string{"ork-key-two", "orker-key-one"}, false},
		{"both carry their new keys", []string{"orker-key-two", "ork-key-one", "ork-key-two"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := keysInUse(tt.seen, "ork-key-two", "orker-key-two")
			if (err == nil) != tt.ok {
				t.Fatalf("keysInUse(%v) = %v, want ok %v", tt.seen, err, tt.ok)
			}
		})
	}
}
