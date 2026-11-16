package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

// TestAuthAssertion_PrintsAnAssertionBoundToTheRequest: the printed
// assertion verifies for exactly the method, path and body it was asked
// for, and for no other body.
func TestAuthAssertion_PrintsAnAssertionBoundToTheRequest(t *testing.T) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(k)
	key := writeFile(t, "sa.pem", string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})))
	body := writeFile(t, "body.json", `{"orkflow":"x"}`)
	var out, errOut strings.Builder
	args := []string{"auth", "assertion", "--server", "https://ork.example.test", "--service-account", "bot",
		"--service-account-key", key, "--method", "post", "--url", "https://ork.example.test/v1/x?namespace=a", "--body-file", body}
	if code := Execute(args, &out, &errOut); code != exitOK {
		t.Fatalf("auth assertion = %d: %s", code, errOut.String())
	}
	pub, _ := jwt.NewPublicKey(&k.PublicKey)
	keys, _ := jwt.StaticByThumbprint(pub)
	public, _ := url.Parse("https://ork.example.test")
	verify := func(b string) error {
		_, err := jwt.VerifyRequest(context.Background(), strings.TrimSpace(out.String()),
			jwt.AssertionPolicy{Account: "bot", Audience: "https://ork.example.test", Leeway: time.Minute}, keys,
			jwt.Request{Method: "POST", Path: "/v1/x", RawQuery: "namespace=a", BodySHA256: jwt.BodySHA256([]byte(b)),
				HostAllowed: func(string) bool { return true }, PublicURL: public}, jwt.NewReplayCache(), time.Now())
		return err
	}
	if err := verify(`{"orkflow":"y"}`); err == nil {
		t.Error("the assertion verified for another body")
	}
	if err := verify(`{"orkflow":"x"}`); err != nil {
		t.Errorf("the assertion does not verify for its request: %v", err)
	}
	errOut.Reset()
	if code := Execute([]string{"auth", "assertion", "--method", "GET", "--url", "https://ork.example.test/x"}, &out, &errOut); code == exitOK {
		t.Error("an assertion with no service account was printed")
	}
}
