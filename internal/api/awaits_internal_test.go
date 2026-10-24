package api

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/triggers"
)

func TestVerifyAwaitToken(t *testing.T) {
	salt := []byte("0123456789abcdef")
	token := strings.Repeat("ab", 32)
	cred := engine.AwaitCredential{
		Salt:   hex.EncodeToString(salt),
		Digest: hex.EncodeToString(triggers.HashSecret(salt, token)),
	}
	cases := []struct {
		name      string
		cred      engine.AwaitCredential
		presented string
		want      bool
	}{
		{"right", cred, token, true},
		{"wrong", cred, strings.Repeat("cd", 32), false},
		{"empty", cred, "", false},
		{"undecodable salt", engine.AwaitCredential{Salt: "zz", Digest: cred.Digest}, token, false},
		{"undecodable digest", engine.AwaitCredential{Salt: cred.Salt, Digest: "not hex"}, token, false},
		{"empty credential", engine.AwaitCredential{}, token, false},
	}
	for _, tc := range cases {
		if got := verifyAwaitToken(tc.cred, tc.presented); got != tc.want {
			t.Errorf("%s: verifyAwaitToken = %v, want %v", tc.name, got, tc.want)
		}
	}
	// The comparison is constant time: a timing measurement is not evidence
	// (docs/ORKFLOW_TRIGGERS.md), so the source is held to it instead.
	source, err := os.ReadFile("awaits.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "subtle.ConstantTimeCompare(digest, triggers.HashSecret(salt, presented))") {
		t.Fatal("verifyAwaitToken no longer compares with crypto/subtle.ConstantTimeCompare")
	}
}

func TestMintAwaitCallbacks_LeavesAPlanWithNoCallbackAwaitUntouched(t *testing.T) {
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{
		{Id: "a", Uses: "shell"},
		{Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
	}}
	plan := engine.RunPlan{}
	minted, err := mintAwaitCallbacks(wf, &plan)
	if err != nil || len(minted) != 0 || plan.Awaits != nil {
		t.Fatalf("mint = %v, %v, plan.Awaits = %v; want nothing", minted, err, plan.Awaits)
	}
}
