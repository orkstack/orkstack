package logstream_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/logstream"
)

func TestSecretFinder_NamesTheSecretWhoseFormItFinds(t *testing.T) {
	password := "correct-horse-battery"
	token := "tok-0123456789abcdef"
	finder, err := logstream.NewSecretFinder([]logstream.NamedValue{
		{Name: "db", Value: []byte(password)},
		{Name: "api", Value: []byte(token)},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		input string
		want  string
		found bool
	}{
		{"the value", `{"x":"` + password + `"}`, "db", true},
		{"its base64", `{"x":"` + base64.StdEncoding.EncodeToString([]byte(password)) + `"}`, "db", true},
		{"the other secret", "a " + token + " b", "api", true},
		{"neither", `{"x":"correct-horse"}`, "", false},
		{"the name only", `{"secret":"db"}`, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := finder.Find([]byte(tt.input))
			if got != tt.want || found != tt.found {
				t.Fatalf("Find = %q, %v; want %q, %v", got, found, tt.want, tt.found)
			}
		})
	}
}

func TestSecretFinder_AFormTwoSecretsShareNamesTheFirst(t *testing.T) {
	finder, err := logstream.NewSecretFinder([]logstream.NamedValue{
		{Name: "first", Value: []byte("shared-value-1234")},
		{Name: "second", Value: []byte("shared-value-1234")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, found := finder.Find([]byte("shared-value-1234")); !found || got != "first" {
		t.Fatalf("Find = %q, %v; want first", got, found)
	}
}

func TestSecretFinder_AShortValueHasNoForm(t *testing.T) {
	finder, err := logstream.NewSecretFinder([]logstream.NamedValue{{Name: "pin", Value: []byte("1234")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := finder.Find([]byte("1234")); found {
		t.Fatal("a value under 8 bytes was found")
	}
	if !finder.Empty() {
		t.Fatal("a finder of only short values is not empty")
	}
}

func TestSecretFinder_RefusesASetPastTheBounds(t *testing.T) {
	values := make([]logstream.NamedValue, 0, 16)
	for i := range 16 {
		// 70 KiB of distinct lines: registered by its lines only, and
		// sixteen of them pass the 512 KiB bound.
		var v strings.Builder
		for j := 0; v.Len() < 70<<10; j++ {
			v.WriteString(strings.Repeat(string(rune('a'+i)), 40))
			v.WriteString(strings.Repeat("0", j%7+1) + "-" + string(rune('A'+j%26)) + strings.Repeat("9", j/26+1) + "\n")
		}
		values = append(values, logstream.NamedValue{Name: strings.Repeat("s", i+1), Value: []byte(v.String())})
	}
	_, err := logstream.NewSecretFinder(values)
	var capacity *logstream.MaskCapacityError
	if !errors.As(err, &capacity) {
		t.Fatalf("NewSecretFinder = %v; want a *MaskCapacityError", err)
	}
}

func TestMaskWriter_CountsTheRunsItReplaced(t *testing.T) {
	m := logstream.NewMasker()
	if _, err := m.Add("s", []byte("hunter2hunter2")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := m.Writer(&out)
	_, _ = w.Write([]byte("a hunter2hunter2 b hunter2hunter2\nhunter2hunt"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := w.Masked(); got != 3 {
		t.Fatalf("Masked = %d; want 3 (two occurrences and the held tail): %q", got, out.String())
	}
}
