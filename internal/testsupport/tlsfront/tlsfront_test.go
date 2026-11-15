package tlsfront_test

import (
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/testpki"
	"github.com/orkstack/orkstack/internal/testsupport/tlsfront"
)

func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must.OK(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	return ln.Addr().String()
}

func TestFront_CopiesBytesForAClientItTrustsAndRefusesOneWithout(t *testing.T) {
	serverCA := testpki.NewCA(t, "server")
	clientCA := testpki.NewCA(t, "client")
	leaf := clientCA.Issue(t, testpki.LeafOptions{Name: "orker", Client: true})
	f := tlsfront.Start(t, tlsfront.Options{
		Upstream:    echoServer(t),
		Certificate: serverCA.Issue(t, testpki.LeafOptions{Name: "front", Hosts: []string{"127.0.0.1"}}).TLS(t),
		ClientCAs:   clientCA.Pool(),
	})

	cert := leaf.TLS(t)
	conn, err := tls.Dial("tcp", f.Addr, &tls.Config{RootCAs: serverCA.Pool(), ServerName: "127.0.0.1", Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	must.OKf(t, err, "dial with a client certificate")
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte("ping"))
	must.OK(t, err)
	got := make([]byte, 4)
	_, err = io.ReadFull(conn, got)
	must.OKf(t, err, "read the echo")
	conn.Close()
	if string(got) != "ping" {
		t.Fatalf("echo = %q, want ping", got)
	}

	bare, err := tls.Dial("tcp", f.Addr, &tls.Config{RootCAs: serverCA.Pool(), ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12})
	if err == nil {
		_ = bare.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = bare.Write([]byte("x"))
		_, err = bare.Read(make([]byte, 1))
		bare.Close()
	}
	if err == nil {
		t.Fatal("a client without a certificate got through")
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(f.Handshakes()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	hs := f.Handshakes()
	if len(hs) != 2 || hs[0].Err != nil || hs[0].Client.Cmp(leaf.Cert.SerialNumber) != 0 || hs[1].Err == nil {
		t.Fatalf("handshakes = %+v, want the client's certificate and then a refusal", hs)
	}
	f.Close()
}
