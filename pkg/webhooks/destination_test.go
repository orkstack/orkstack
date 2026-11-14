package webhooks_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// policy is a destination policy allowing cidrs.
func policy(t *testing.T, cidrs ...string) webhooks.DestinationPolicy {
	t.Helper()
	prefixes, err := webhooks.ParseNetworks(cidrs)
	if err != nil {
		t.Fatalf("ParseNetworks(%q): %v", cidrs, err)
	}
	return webhooks.NewDestinationPolicy(prefixes)
}

func TestCheckAddr_RefusesInternalDestinations(t *testing.T) {
	p := policy(t)
	for _, raw := range []string{
		"127.0.0.1", "127.8.9.10", "::1", "0.0.0.0", "::",
		"169.254.169.254", "169.254.0.1", "fe80::1", "fd00:ec2::254",
		"10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1", "fd12:3456::1", "fc00::1",
		"100.64.0.1", "198.18.0.1", "224.0.0.1", "ff02::1", "255.255.255.255", "240.0.0.1",
		"::ffff:127.0.0.1", "::ffff:169.254.169.254", "::ffff:10.0.0.1",
		"64:ff9b::7f00:1", "2002:7f00:1::1", "fec0::1",
		"::ffff:0:127.0.0.1", "::ffff:0:7f00:1", "2001:1::1", "2001:3::1", "2001:4:112::1", "2001:20::1", "2001:10::1", "5f00::1", "3fff::1", "::7f00:1",
	} {
		err := p.CheckAddr(netip.MustParseAddr(raw))
		if !errors.Is(err, webhooks.ErrDestinationBlocked) {
			t.Errorf("CheckAddr(%s) = %v, want ErrDestinationBlocked", raw, err)
		}
	}
}

func TestCheckAddr_AcceptsPublicDestinations(t *testing.T) {
	p := policy(t)
	for _, raw := range []string{"8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111", "::ffff:8.8.8.8"} {
		if err := p.CheckAddr(netip.MustParseAddr(raw)); err != nil {
			t.Errorf("CheckAddr(%s) = %v, want nil", raw, err)
		}
	}
}

func TestCheckAddr_AllowListAdmitsOnlyItsOwnRanges(t *testing.T) {
	p := policy(t, "10.1.0.0/16", "127.0.0.1/32")
	for raw, wantBlocked := range map[string]bool{
		"10.1.2.3": false, "127.0.0.1": false, "::ffff:10.1.2.3": false,
		"10.2.0.1": true, "127.0.0.2": true, "169.254.169.254": true, "192.168.0.1": true,
	} {
		err := p.CheckAddr(netip.MustParseAddr(raw))
		if blocked := errors.Is(err, webhooks.ErrDestinationBlocked); blocked != wantBlocked {
			t.Errorf("CheckAddr(%s) = %v, blocked want %v", raw, err, wantBlocked)
		}
	}
}

func TestParseNetworks(t *testing.T) {
	for _, bad := range []string{"", "10.0.0.0", "10.0.0.0/33", "nonsense", "10.0.0.1/8", "::1/129", "::ffff:0:0/80"} {
		if _, err := webhooks.ParseNetworks([]string{bad}); err == nil {
			t.Errorf("ParseNetworks(%q) accepted, want an error", bad)
		}
	}
	got, err := webhooks.ParseNetworks([]string{" 10.0.0.0/8 ", "fd00::/8"})
	if err != nil || len(got) != 2 || got[0] != netip.MustParsePrefix("10.0.0.0/8") {
		t.Errorf("ParseNetworks = %v, %v", got, err)
	}
}

func TestValidateDestination_RefusesLiteralInternalHosts(t *testing.T) {
	p := policy(t)
	for _, raw := range []string{
		"http://127.0.0.1/hook", "http://localhost:8080/hook", "https://LOCALHOST./x", "http://a.localhost/x",
		"http://[::1]:9000/", "http://169.254.169.254/latest/meta-data/", "http://10.0.0.5/", "http://192.168.0.1:81/",
		"http://[fd00::1]/", "http://[::ffff:127.0.0.1]/", "http://0.0.0.0/", "http://2130706433/", "http://0x7f.1/",
		"http://017700000001/", "http://127.1/",
	} {
		_, err := p.ValidateDestination(raw)
		if !errors.Is(err, webhooks.ErrDestinationBlocked) {
			t.Errorf("ValidateURL(%q) = %v, want ErrDestinationBlocked", raw, err)
		}
	}
}

func TestValidateDestination_AcceptsPublicAndNamedHosts(t *testing.T) {
	p := policy(t)
	for _, raw := range []string{"https://example.com/hook", "http://8.8.8.8/x", "https://hooks.internal.example:8443/p"} {
		if _, err := p.ValidateDestination(raw); err != nil {
			t.Errorf("ValidateURL(%q) = %v, want nil (a name is checked when it is dialled)", raw, err)
		}
	}
}

func TestValidateDestination_AllowListAdmitsLiteralHost(t *testing.T) {
	p := policy(t, "10.0.0.0/8", "127.0.0.0/8")
	for _, raw := range []string{"http://10.1.2.3/x", "http://127.0.0.1:1/x", "http://localhost/x"} {
		if _, err := p.ValidateDestination(raw); err != nil {
			t.Errorf("ValidateURL(%q) = %v, want nil with the range allowed", raw, err)
		}
	}
	if _, err := p.ValidateDestination("http://192.168.0.1/"); !errors.Is(err, webhooks.ErrDestinationBlocked) {
		t.Errorf("an address outside the allow-list was accepted: %v", err)
	}
}

func TestDispatcher_RefusesLoopbackAtDialWithReason(t *testing.T) {
	p := policy(t)
	rec := newReceiver(t, http.StatusOK)
	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: p}).Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}, sampleDelivery())
	if err == nil || result.Delivered {
		t.Fatalf("Deliver = %+v, %v; want a refusal", result, err)
	}
	if rec.count() != 0 {
		t.Errorf("the receiver saw %d requests, want none", rec.count())
	}
	attempt := result.LastAttempt()
	if !strings.Contains(attempt.Error, webhooks.ReasonDestinationBlocked) {
		t.Errorf("attempt error = %q, want it to carry %s", attempt.Error, webhooks.ReasonDestinationBlocked)
	}
	if attempt.Retryable() {
		t.Error("a blocked destination is retryable; repeating it cannot change the answer")
	}
}

// Each dispatcher holds its own policy: one that allows loopback and one
// that does not deliver side by side in one process.
func TestDispatcher_PolicyIsPerInstance(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	hook := webhooks.Webhook{ID: "wh-1", URL: rec.server.URL, Secret: "s"}
	open := webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: policy(t, "127.0.0.0/8", "::1/128")})
	closed := webhooks.NewDispatcher(webhooks.DispatcherOptions{})
	if result, err := open.Deliver(context.Background(), hook, sampleDelivery()); err != nil || !result.Delivered {
		t.Fatalf("the loopback-allowing dispatcher: %+v, %v; want delivered", result, err)
	}
	result, err := closed.Deliver(context.Background(), hook, sampleDelivery())
	if err == nil || !strings.Contains(result.LastAttempt().Error, webhooks.ReasonDestinationBlocked) {
		t.Fatalf("the default dispatcher: %+v, %v; want %s", result, err, webhooks.ReasonDestinationBlocked)
	}
	if rec.count() != 1 {
		t.Errorf("the receiver saw %d requests, want 1", rec.count())
	}
}

// The zero policy admits public addresses only.
func TestDestinationPolicy_ZeroValueAdmitsPublicOnly(t *testing.T) {
	var p webhooks.DestinationPolicy
	if err := p.CheckAddr(netip.MustParseAddr("8.8.8.8")); err != nil {
		t.Errorf("CheckAddr(8.8.8.8) = %v", err)
	}
	if err := p.CheckAddr(netip.MustParseAddr("127.0.0.1")); !errors.Is(err, webhooks.ErrDestinationBlocked) {
		t.Errorf("CheckAddr(127.0.0.1) = %v, want ErrDestinationBlocked", err)
	}
}

func TestDispatcher_RefusesARedirectToAnInternalAddress(t *testing.T) {
	hop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(hop.Close)
	// The first hop is allowed; the address it redirects to is not.
	p := policy(t, "127.0.0.1/32")
	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: p}).Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: hop.URL}, sampleDelivery())
	if err == nil || result.Delivered {
		t.Fatalf("Deliver = %+v, %v; want the redirect refused", result, err)
	}
	if !strings.Contains(result.LastAttempt().Error, webhooks.ReasonDestinationBlocked) {
		t.Errorf("attempt error = %q, want %s", result.LastAttempt().Error, webhooks.ReasonDestinationBlocked)
	}
}

func FuzzParseNetworks(f *testing.F) {
	for _, seed := range []string{"10.0.0.0/8", "fd00::/8", "", "10.0.0.1/8", "::ffff:10.0.0.0/104", " 127.0.0.1/32 ", "0.0.0.0/0"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		prefixes, err := webhooks.ParseNetworks([]string{raw})
		if err != nil {
			return
		}
		if len(prefixes) != 1 || !prefixes[0].IsValid() || prefixes[0] != prefixes[0].Masked() {
			t.Fatalf("ParseNetworks(%q) = %v, want one valid masked prefix", raw, prefixes)
		}
	})
}

func TestValidateURL_ChecksSyntaxOnly(t *testing.T) {
	if _, err := webhooks.ValidateURL("http://169.254.169.254/x"); err != nil {
		t.Errorf("ValidateURL consulted the destination policy, which a client does not hold: %v", err)
	}
}

func TestDispatcher_RefusesANameThatResolvesToLoopback(t *testing.T) {
	p := policy(t)
	rec := newReceiver(t, http.StatusOK)
	port := rec.server.URL[strings.LastIndex(rec.server.URL, ":"):]
	result, err := webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: p}).Deliver(context.Background(),
		webhooks.Webhook{ID: "wh-1", URL: "http://localhost" + port}, sampleDelivery())
	if err == nil || rec.count() != 0 {
		t.Fatalf("Deliver = %+v, %v; receiver saw %d; want a refusal at dial", result, err, rec.count())
	}
	if !strings.HasPrefix(result.LastAttempt().Error, webhooks.ReasonDestinationBlocked) {
		t.Errorf("attempt error = %q, want it to lead with %s", result.LastAttempt().Error, webhooks.ReasonDestinationBlocked)
	}
}

// A subscription registered before the destination policy keeps its URL
// through an update that leaves it alone; a new URL meets the registry's
// policy.
func TestRegistry_AnUpdateThatLeavesTheURLAloneIsNotRefusedForIt(t *testing.T) {
	store := &countingStore{hooks: []webhooks.Webhook{{ID: "wh-1", URL: "http://10.0.0.5/hook", Secret: "s"}}}
	registry := webhooks.NewDurableRegistry(store, webhooks.WithDestinationPolicy(policy(t, "10.0.0.6/32")))
	next, _, err := registry.UpdateContext(context.Background(), "wh-1", func(w *webhooks.Webhook) error { w.Name = ""; return nil })
	if err != nil || next.URL != "http://10.0.0.5/hook" {
		t.Fatalf("an update without a URL change = %+v, %v; want it accepted", next, err)
	}
	_, _, err = registry.UpdateContext(context.Background(), "wh-1", func(w *webhooks.Webhook) error { w.URL = "http://10.0.0.7/hook"; return nil })
	if !errors.Is(err, webhooks.ErrDestinationBlocked) {
		t.Errorf("an update to a new internal URL = %v, want ErrDestinationBlocked", err)
	}
	if _, _, err := registry.UpdateContext(context.Background(), "wh-1", func(w *webhooks.Webhook) error { w.URL = "http://10.0.0.6/hook"; return nil }); err != nil {
		t.Errorf("an update to a URL the policy allows = %v", err)
	}
}

func TestWholeFamilyNetworks(t *testing.T) {
	parse := func(cidrs ...string) []netip.Prefix {
		p, err := webhooks.ParseNetworks(cidrs)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	got := webhooks.WholeFamilyNetworks(parse("10.0.0.0/8", "0.0.0.0/0", "::/0", "::ffff:0:0/96", "fd00::/8"))
	want := []string{"0.0.0.0/0", "::/0", "::ffff:0:0/96"}
	if len(got) != len(want) {
		t.Fatalf("WholeFamilyNetworks = %v, want %v", got, want)
	}
	for i, p := range got {
		if p != netip.MustParsePrefix(want[i]) {
			t.Errorf("entry %d = %s, want %s", i, p, want[i])
		}
	}
	if got := webhooks.WholeFamilyNetworks(parse("10.0.0.0/8", "::1/128")); len(got) != 0 {
		t.Errorf("WholeFamilyNetworks(narrow) = %v, want none", got)
	}
}

// A store-backed registry judges a registration's URL before the store sees
// it, as the in-memory one does.
func TestRegistry_DurableRegistrationMeetsThePolicy(t *testing.T) {
	registry := webhooks.NewDurableRegistry(&countingStore{})
	_, err := registry.RegisterContext(context.Background(), webhooks.Registration{URL: "http://169.254.169.254/latest/meta-data/"})
	if !errors.Is(err, webhooks.ErrDestinationBlocked) {
		t.Fatalf("RegisterContext = %v, want ErrDestinationBlocked before the store is asked", err)
	}
}
