//go:build integration

package orker_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// metadataStandIn is an HTTP endpoint on the host, reached from a container
// at host.docker.internal as every Go integration suite reaches the host. It
// records each request's method, path and EC2 token header.
type metadataStandIn struct {
	port int
	mu   sync.Mutex
	seen []string
}

func startMetadataStandIn(t *testing.T) *metadataStandIn {
	t.Helper()
	s := &metadataStandIn{}
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	must.OKf(t, err, "listen")
	s.port = ln.Addr().(*net.TCPAddr).Port
	srv := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.seen = append(s.seen, r.Method+" "+r.URL.Path+" ttl="+r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds"))
		s.mu.Unlock()
		if r.Method == http.MethodPut {
			_, _ = w.Write([]byte("a-session-token-the-orker-never-repeats"))
			return
		}
		http.Error(w, "forbidden", http.StatusForbidden)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return s
}

func (s *metadataStandIn) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

// startBanner is a TCP endpoint that accepts, writes a line that is not HTTP
// and closes: a connection that opens and is no answer. It counts the
// connections it accepted.
func startBanner(t *testing.T) (port int, accepted func() int) {
	t.Helper()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	must.OKf(t, err, "listen")
	var mu sync.Mutex
	n := 0
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			n++
			mu.Unlock()
			_, _ = c.Write([]byte("SSH-2.0-not-a-metadata-service\r\n"))
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().(*net.TCPAddr).Port, func() int { mu.Lock(); defer mu.Unlock(); return n }
}

// TestIntegration_MetadataProbe is the metadata probe on a real daemon, its
// container built as a task's is and run on the default network. An HTTP
// stand-in it can reach answers, and under refuse the Orker does not start:
// METADATA_ENDPOINT_REACHABLE, naming the address and the request answered.
// The stand-in saw the GET and EC2's token PUT with its TTL header: the
// requests a task would make, not a TCP connection alone. In the second case
// the container reaches an endpoint that accepts and answers with a line that
// is not HTTP, which is no answer, and an unrouted address, which is no answer
// either: the Orker starts. The banner's count is that window's positive
// control: the container did reach the network.
func TestIntegration_MetadataProbe(t *testing.T) {
	requireDocker(t)
	realDocker := containerCLI(t)
	setup, cancelSetup := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelSetup()
	requireProbeImage(setup, t, realDocker)

	standIn := startMetadataStandIn(t)
	bannerPort, accepted := startBanner(t)

	cases := []struct {
		name       string
		targets    []orker.MetadataTargetForTest
		wantReason string
		want       []orker.MetadataOutcome
	}{
		{
			name: "an endpoint that answers refuses boot",
			targets: []orker.MetadataTargetForTest{
				{Address: "stand-in", Host: "host.docker.internal", Port: standIn.port, TokenRequest: true},
			},
			wantReason: orker.ReasonMetadataEndpointReachable,
			want:       []orker.MetadataOutcome{orker.MetadataAnswered},
		},
		{
			name: "a connection that is not HTTP and an unrouted address are no answer",
			targets: []orker.MetadataTargetForTest{
				{Address: "banner", Host: "host.docker.internal", Port: bannerPort},
				{Address: "unrouted", Host: "192.0.2.1", Port: 80, TokenRequest: true},
			},
			want: []orker.MetadataOutcome{orker.MetadataNoAnswer, orker.MetadataNoAnswer},
		},
	}
	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			store, err := artifacts.NewLocalStore(t.TempDir())
			must.OK(t, err)
			t.Cleanup(func() { _ = store.Close() })
			activities := orker.NewActivitiesWithOptions(store, orker.Options{
				Runtime:       &orker.DockerRuntime{Bin: realDocker, BindProbeTimeout: 90 * time.Second},
				OrkerID:       proof.OrkerID(fmt.Sprintf("metaprobe-%d", i)),
				WorkRoot:      postureWorkRoot(t, "ork-metaprobe"),
				MetadataProbe: orker.MetadataProbeRefuse,
			})
			t.Cleanup(activities.Close)
			orker.SetSignalProbeGrace(activities, 3*time.Second)
			orker.SetMetadataTargets(activities, tt.targets)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()

			err = activities.Boot(ctx)
			switch {
			case tt.wantReason == "" && err != nil:
				t.Fatalf("Boot = %v; want it to start", err)
			case tt.wantReason != "" && (err == nil || !strings.HasPrefix(err.Error(), tt.wantReason+": ")):
				t.Fatalf("Boot = %v; want %s", err, tt.wantReason)
			}
			findings := orker.MetadataFindings(activities)
			if len(findings) != len(tt.want) {
				t.Fatalf("findings = %v; want %d", findings, len(tt.want))
			}
			for j, f := range findings {
				if f.Outcome != tt.want[j] {
					t.Errorf("%s: %s (%s); want %s", f.Address, f.Outcome, f.Detail, tt.want[j])
				}
			}
			if tt.wantReason != "" {
				if !strings.Contains(err.Error(), "stand-in (HTTP 403 to GET /)") {
					t.Errorf("the refusal does not name the address and the request answered: %v", err)
				}
				if strings.Contains(err.Error(), "a-session-token") {
					t.Errorf("the refusal repeats what the endpoint sent: %v", err)
				}
				seen := strings.Join(standIn.requests(), "; ")
				if !strings.Contains(seen, "GET / ttl=") || !strings.Contains(seen, "PUT /latest/api/token ttl=21600") {
					t.Errorf("the stand-in saw %q; want the GET and the token PUT with its TTL header", seen)
				}
			} else if accepted() == 0 {
				t.Fatal("the banner accepted no connection: the container never reached the network, so no answer proves nothing")
			}
		})
	}
}
