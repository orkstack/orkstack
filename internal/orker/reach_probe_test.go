package orker

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
)

// clusterAddresses: the Orker resolves a cluster named by name, and a name
// that resolves to nothing is an error, which ends the probe as not run.
func TestClusterAddresses(t *testing.T) {
	answer := func(ips ...string) func(context.Context, string) ([]net.IPAddr, error) {
		return func(_ context.Context, host string) ([]net.IPAddr, error) {
			if host != "temporal" {
				t.Errorf("looked up %q", host)
			}
			var out []net.IPAddr
			for _, ip := range ips {
				out = append(out, net.IPAddr{IP: net.ParseIP(ip)})
			}
			return out, nil
		}
	}
	never := func(context.Context, string) ([]net.IPAddr, error) {
		t.Error("an address was looked up")
		return nil, nil
	}
	tests := []struct {
		name    string
		cluster string
		lookup  func(context.Context, string) ([]net.IPAddr, error)
		want    []string
		wantErr bool
	}{
		{name: "an address", cluster: "10.0.0.5:7233", lookup: never},
		{name: "an IPv6 address", cluster: "[fd00::5]:7233", lookup: never},
		{name: "no cluster", lookup: never},
		{name: "a name", cluster: "temporal:7233", lookup: answer("10.0.0.5", "fd00::5"), want: []string{"10.0.0.5", "fd00::5"}},
		{name: "a name at this host", cluster: "temporal:7233", lookup: answer("192.168.1.20", "10.0.0.5"), want: []string{"127.0.0.1", "10.0.0.5"}},
		{name: "a name at this host's IPv6 address", cluster: "temporal:7233", lookup: answer("fd00::20"), want: []string{"127.0.0.1"}},
		{name: "this host's IPv6 address", cluster: "[fd00:0::20]:7233", lookup: never, want: []string{"127.0.0.1"}},
		{name: "a name the probe cannot show", cluster: "témporal:7233", lookup: never, wantErr: true},
		{name: "too many addresses", cluster: "temporal:7233", lookup: answer(slices.Repeat([]string{"10.0.0.5"}, maxClusterAddresses+1)...), wantErr: true},
		{name: "a name that resolves to nothing", cluster: "temporal:7233", lookup: answer(), wantErr: true},
		{name: "a name that does not resolve", cluster: "temporal:7233",
			lookup:  func(context.Context, string) ([]net.IPAddr, error) { return nil, errors.New("no such host") },
			wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := clusterAddresses(context.Background(), tt.cluster, []string{"192.168.1.20", "fd00::20"}, tt.lookup)
			if !slices.Equal(got, tt.want) || (err != nil) != tt.wantErr {
				t.Fatalf("%q, %v; want %q, error %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}
