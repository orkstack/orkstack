package dockerport

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPublish_LeavesTheHostPortToDocker(t *testing.T) {
	if got := Publish("8333"); got != "127.0.0.1::8333" {
		t.Fatalf("Publish = %q", got)
	}
}

func TestParse_ReadsTheLoopbackMapping(t *testing.T) {
	cases := []struct {
		name, out, want string
		ok              bool
	}{
		{"one mapping", "127.0.0.1:32850\n", "127.0.0.1:32850", true},
		{"ipv6 first", "[::1]:32851\n127.0.0.1:32850\n", "127.0.0.1:32850", true},
		{"wildcard is not loopback", "0.0.0.0:32850\n", "", false},
		{"nothing published", "", "", false},
		{"no port", "127.0.0.1:\n", "", false},
		{"port zero", "127.0.0.1:0\n", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parse(c.out)
			if c.ok != (err == nil) || got != c.want {
				t.Fatalf("parse(%q) = %q, %v; want %q, ok=%v", c.out, got, err, c.want, c.ok)
			}
		})
	}
}

func TestAddressInUse_IsDockersOwnBindRefusal(t *testing.T) {
	cases := map[string]bool{
		"driver failed programming external connectivity on endpoint x (y): Bind for 127.0.0.1:32851 failed: port is already allocated": true,
		"failed to set up container networking: failed to bind host port 127.0.0.1:52667/tcp: address already in use":                   true,
		"Unable to find image 'dex:latest' locally": false,
		"": false,
	}
	for out, want := range cases {
		if got := AddressInUse([]byte(out)); got != want {
			t.Errorf("AddressInUse(%q) = %v; want %v", out, got, want)
		}
	}
}

// A run that Docker refuses for the port is run again on another; any other
// failure, and a run that succeeds, end it at once.
func TestRunPinned_TriesAnotherPortOnlyWhenTheBindIsRefused(t *testing.T) {
	inUse := []byte("Bind for 127.0.0.1:1 failed: port is already allocated")
	t.Run("retries a refused bind on a new port", func(t *testing.T) {
		var addrs []string
		addr, out, err := RunPinned(context.Background(), 3, func(addr string) ([]byte, error) {
			addrs = append(addrs, addr)
			if len(addrs) < 3 {
				return inUse, errors.New("exit status 125")
			}
			return []byte("cid\n"), nil
		})
		if err != nil || strings.TrimSpace(string(out)) != "cid" || addr != addrs[2] {
			t.Fatalf("RunPinned = %q, %q, %v after %v", addr, out, err, addrs)
		}
		for _, a := range addrs {
			if !strings.HasPrefix(a, "127.0.0.1:") || strings.HasSuffix(a, ":0") {
				t.Fatalf("handed %q, not a loopback port", a)
			}
		}
	})
	t.Run("never retries another failure", func(t *testing.T) {
		calls := 0
		_, _, err := RunPinned(context.Background(), 3, func(string) ([]byte, error) {
			calls++
			return []byte("no such image"), errors.New("exit status 125")
		})
		if err == nil || calls != 1 {
			t.Fatalf("err=%v after %d calls; want a failure after one", err, calls)
		}
	})
	t.Run("gives up after its tries, naming the refusal", func(t *testing.T) {
		calls := 0
		_, _, err := RunPinned(context.Background(), 2, func(string) ([]byte, error) {
			calls++
			return inUse, errors.New("exit status 125")
		})
		if err == nil || calls != 2 || !strings.Contains(err.Error(), "already allocated") {
			t.Fatalf("err=%v after %d calls", err, calls)
		}
	})
}

func TestRunPinned_StopsOnACancelledContextAndTriesAtLeastOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	if _, _, err := RunPinned(ctx, 3, func(string) ([]byte, error) { calls++; return nil, nil }); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("cancelled: err=%v after %d calls", err, calls)
	}
	if _, _, err := RunPinned(context.Background(), 0, func(string) ([]byte, error) { calls++; return nil, nil }); err != nil || calls != 1 {
		t.Fatalf("tries 0: err=%v after %d calls; want one try", err, calls)
	}
}
