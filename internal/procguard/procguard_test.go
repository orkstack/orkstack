package procguard

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestParseAllowDebug(t *testing.T) {
	tests := []struct {
		raw     string
		want    bool
		wantErr bool
	}{
		{"", false, false},
		{"false", false, false},
		{"0", false, false},
		{" TRUE ", true, false},
		{"1", true, false},
		{"yes", false, true},
		{"on", false, true},
	}
	for _, tt := range tests {
		got, err := ParseAllowDebug(tt.raw)
		if tt.wantErr {
			if err == nil || !strings.HasPrefix(err.Error(), SettingAllowDebug+" ("+AllowDebugVar+"): ") {
				t.Errorf("ParseAllowDebug(%q) = %t, %v; want a refusal naming the setting", tt.raw, got, err)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("ParseAllowDebug(%q) = %t, %v; want %t", tt.raw, got, err, tt.want)
		}
	}
}

// TestSettle_RefusesAValueItCannotRead: the one error Settle returns.
func TestSettle_RefusesAValueItCannotRead(t *testing.T) {
	var logs bytes.Buffer
	err := Settle(func(string) string { return "maybe" }, slog.New(slog.NewTextHandler(&logs, nil)), nil)
	if err == nil || !strings.Contains(err.Error(), AllowDebugVar) {
		t.Fatalf("Settle = %v; want a refusal naming %s", err, AllowDebugVar)
	}
}

// TestSettle_AHardenThatFailedIsLoggedAndNotFatal: hardening is never a
// reason to be down.
func TestSettle_AHardenThatFailedIsLoggedAndNotFatal(t *testing.T) {
	if !supported {
		t.Skip("procguard does nothing off Linux")
	}
	var logs bytes.Buffer
	err := Settle(func(string) string { return "" }, slog.New(slog.NewTextHandler(&logs, nil)), errors.New("prctl: operation not permitted"))
	if err != nil {
		t.Fatalf("Settle = %v; want nil", err)
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "could not make itself undumpable") {
		t.Fatalf("logs = %q; want the failure at error", logs.String())
	}
}

// FuzzParseAllowDebug: it never panics; it accepts only the four spellings,
// in any case and padding, and a refusal quotes no more than the value.
func FuzzParseAllowDebug(f *testing.F) {
	for _, s := range []string{"", "true", " 0 ", "yes", "TRUE\n"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := ParseAllowDebug(raw)
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "", "false", "0":
			if err != nil || got {
				t.Fatalf("ParseAllowDebug(%q) = %t, %v; want false", raw, got, err)
			}
		case "true", "1":
			if err != nil || !got {
				t.Fatalf("ParseAllowDebug(%q) = %t, %v; want true", raw, got, err)
			}
		default:
			if err == nil || !strings.HasPrefix(err.Error(), SettingAllowDebug+" ("+AllowDebugVar+"): ") {
				t.Fatalf("ParseAllowDebug(%q) = %t, %v; want a refusal", raw, got, err)
			}
		}
	})
}
