// Package testpki makes the certificate authorities and certificates a test
// of OrkStack's TLS needs: a CA, server and client certificates it signs,
// written as PEM. Keys are ECDSA P-256, generated per call; nothing is cached
// and nothing is read from the host.
package testpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

var serials atomic.Int64

// CA is a certificate authority a test made.
type CA struct {
	Cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// PEM is the CA certificate, PEM-encoded.
	PEM []byte
}

// Pool is a pool holding only this CA.
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	return pool
}

// NewCA makes a CA named name, valid from an hour ago for a year.
func NewCA(t testing.TB, name string) *CA {
	t.Helper()
	key := newKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serials.Add(1)),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("testpki: create CA %s: %v", name, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("testpki: parse CA %s: %v", name, err)
	}
	return &CA{Cert: cert, key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// Leaf is a certificate a CA signed, with its key.
type Leaf struct {
	Cert    *x509.Certificate
	CertPEM []byte
	KeyPEM  []byte
}

// TLS is the leaf as a tls.Certificate.
func (l Leaf) TLS(t testing.TB) tls.Certificate {
	t.Helper()
	pair, err := tls.X509KeyPair(l.CertPEM, l.KeyPEM)
	if err != nil {
		t.Fatalf("testpki: key pair: %v", err)
	}
	return pair
}

// LeafOptions says what a leaf is for.
type LeafOptions struct {
	// Name is the subject's common name.
	Name string
	// Hosts are its DNS names and IP addresses, for a server certificate.
	Hosts []string
	// Client makes it a client certificate; otherwise it is a server's.
	Client bool
	// NotAfter is its end; zero means a year from now.
	NotAfter time.Time
}

// Issue signs a leaf.
func (ca *CA) Issue(t testing.TB, o LeafOptions) Leaf {
	t.Helper()
	key := newKey(t)
	notAfter := o.NotAfter
	if notAfter.IsZero() {
		notAfter = time.Now().Add(365 * 24 * time.Hour)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serials.Add(1)),
		Subject:      pkix.Name{CommonName: o.Name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if o.Client {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	for _, h := range o.Hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("testpki: issue %s: %v", o.Name, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("testpki: parse %s: %v", o.Name, err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("testpki: marshal key: %v", err)
	}
	return Leaf{
		Cert:    cert,
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}
}

// Write writes data to dir/name with mode 0600 and returns its path.
func Write(t testing.TB, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("testpki: write %s: %v", path, err)
	}
	return path
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("testpki: generate key: %v", err)
	}
	return key
}
