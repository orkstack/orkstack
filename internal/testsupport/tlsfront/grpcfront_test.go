package tlsfront_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/testpki"
	"github.com/orkstack/orkstack/internal/testsupport/tlsfront"
)

func TestKeyFront_ForwardsAnAcceptedKeyAndRefusesAnyOther(t *testing.T) {
	var (
		mu         sync.Mutex
		upstreamMD []metadata.MD
	)
	upstream := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mu.Lock()
		upstreamMD = append(upstreamMD, md)
		mu.Unlock()
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(upstream, health.NewServer())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must.OK(t, err)
	go func() { _ = upstream.Serve(ln) }()
	t.Cleanup(upstream.Stop)

	ca := testpki.NewCA(t, "ca")
	f := tlsfront.StartKeyFront(t, ln.Addr().String(), ca.Issue(t, testpki.LeafOptions{Name: "front", Hosts: []string{"127.0.0.1"}}).TLS(t), "old")
	conn, err := grpc.NewClient(f.Addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: ca.Pool(), MinVersion: tls.VersionTLS12})))
	must.OK(t, err)
	t.Cleanup(func() { conn.Close() })
	check := func(key string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if key != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
		}
		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	}

	must.OKf(t, check("old"), "the accepted key")
	for _, key := range []string{"", "new"} {
		if err := check(key); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("key %q: err = %v, want unauthenticated", key, err)
		}
	}
	f.SetKeys("new")
	must.OKf(t, check("new"), "the key after SetKeys")
	if err := check("old"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("the replaced key: err = %v, want unauthenticated", err)
	}
	if got, want := fmt.Sprintf("%q", f.Seen()), `["old" "" "new" "new" "old"]`; got != want {
		t.Fatalf("Seen() = %s, want %s", got, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(upstreamMD) != 2 {
		t.Fatalf("the upstream saw %d calls, want the 2 accepted", len(upstreamMD))
	}
	for _, md := range upstreamMD {
		if len(md.Get("authorization")) != 0 {
			t.Fatal("the key reached the upstream")
		}
	}
}
