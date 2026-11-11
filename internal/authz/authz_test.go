package authz_test

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
)

func callee(shares ...*orkstackv1.OrkflowShare) authz.Callee {
	return authz.Callee{Namespace: "platform", Name: "deploy", Version: "1.2.3", SharedWith: shares}
}

func TestAuthorize(t *testing.T) {
	every := &orkstackv1.OrkflowShare{Namespace: "*"}
	payments := &orkstackv1.OrkflowShare{Namespace: "payments"}
	release := &orkstackv1.OrkflowShare{Orkflow: "search/release"}
	cases := []struct {
		name    string
		callee  authz.Callee
		caller  authz.Caller
		rule    string
		refused bool
	}{
		{"same namespace needs no entry", callee(), authz.Caller{Namespace: "platform", Orkflow: "nightly"}, authz.RuleSameNamespace, false},
		{"empty namespaces are default", authz.Callee{Name: "deploy", Version: "1"}, authz.Caller{Namespace: "default", Orkflow: "x"}, authz.RuleSameNamespace, false},
		{"not shared", callee(), authz.Caller{Namespace: "payments", Orkflow: "nightly"}, "", true},
		{"every namespace", callee(every), authz.Caller{Namespace: "payments", Orkflow: "nightly"}, "namespace:*", false},
		{"one namespace", callee(payments), authz.Caller{Namespace: "payments", Orkflow: "nightly"}, "namespace:payments", false},
		{"another namespace", callee(payments), authz.Caller{Namespace: "search", Orkflow: "release"}, "", true},
		{"one orkflow", callee(release), authz.Caller{Namespace: "search", Orkflow: "release"}, "orkflow:search/release", false},
		{"another orkflow of that namespace", callee(release), authz.Caller{Namespace: "search", Orkflow: "nightly"}, "", true},
		{"the same name in another namespace", callee(release), authz.Caller{Namespace: "payments", Orkflow: "release"}, "", true},
		{"the most specific entry wins", callee(every, payments), authz.Caller{Namespace: "payments", Orkflow: "nightly"}, "namespace:payments", false},
		{"an orkflow entry beats a namespace entry", callee(every, release, &orkstackv1.OrkflowShare{Namespace: "search"}), authz.Caller{Namespace: "search", Orkflow: "release"}, "orkflow:search/release", false},
		{"default is a namespace like any other", callee(&orkstackv1.OrkflowShare{Namespace: "default"}), authz.Caller{Orkflow: "x"}, "namespace:default", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision, err := authz.Authorize(authz.Request{Action: authz.ActionRun, Callee: tc.callee, Caller: tc.caller})
			if tc.refused {
				var notShared *authz.NotSharedError
				if !errors.As(err, &notShared) {
					t.Fatalf("Authorize = %+v, %v; want *NotSharedError", decision, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Authorize: %v", err)
			}
			if decision.Rule != tc.rule || decision.Action != authz.ActionRun {
				t.Fatalf("decision = %+v, want rule %q", decision, tc.rule)
			}
		})
	}
}

func TestAuthorize_TheDecisionNamesBothSides(t *testing.T) {
	decision, err := authz.Authorize(authz.Request{
		Action: authz.ActionRun,
		Callee: callee(&orkstackv1.OrkflowShare{Namespace: "payments"}),
		Caller: authz.Caller{Namespace: "payments", Orkflow: "nightly"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Callee != "platform/deploy@1.2.3" || decision.Caller != "payments/nightly" || !decision.CrossNamespace() {
		t.Fatalf("decision = %+v", decision)
	}
	same, err := authz.Authorize(authz.Request{Action: authz.ActionRun, Callee: callee(), Caller: authz.Caller{Namespace: "platform", Orkflow: "x"}})
	if err != nil || same.CrossNamespace() {
		t.Fatalf("same-namespace decision = %+v, %v", same, err)
	}
}

func TestAuthorize_RefusalSaysWhatToChange(t *testing.T) {
	_, err := authz.Authorize(authz.Request{
		Action: authz.ActionRun,
		Callee: callee(&orkstackv1.OrkflowShare{Orkflow: "search/release"}),
		Caller: authz.Caller{Namespace: "payments", Orkflow: "nightly"},
	})
	for _, want := range []string{
		authz.ReasonNotShared, `"platform/deploy@1.2.3"`, `"payments/nightly"`, "orkflow:search/release",
		"namespace: payments", "orkflow: payments/nightly", "new version",
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v does not contain %q", err, want)
		}
	}
	_, err = authz.Authorize(authz.Request{Action: authz.ActionRun, Callee: callee(), Caller: authz.Caller{Namespace: "payments", Orkflow: "nightly"}})
	if err == nil || !strings.Contains(err.Error(), "shares with no other namespace") {
		t.Errorf("an unshared refusal %v does not say it is shared with nobody", err)
	}
}

func TestAuthorize_RefusesAnUnknownAction(t *testing.T) {
	_, err := authz.Authorize(authz.Request{Action: "use_pool", Callee: callee(&orkstackv1.OrkflowShare{Namespace: "*"}),
		Caller: authz.Caller{Namespace: "payments", Orkflow: "x"}})
	var unknown *authz.UnknownActionError
	if !errors.As(err, &unknown) || !strings.Contains(err.Error(), `"use_pool"`) {
		t.Fatalf("Authorize = %v, want *UnknownActionError naming the action", err)
	}
}

func TestAuthorize_RefusesAMalformedStoredEntry(t *testing.T) {
	_, err := authz.Authorize(authz.Request{Action: authz.ActionRun,
		Callee: callee(&orkstackv1.OrkflowShare{Orkflow: "release"}),
		Caller: authz.Caller{Namespace: "payments", Orkflow: "x"}})
	if err == nil || !strings.Contains(err.Error(), "shared_with[0]") {
		t.Fatalf("Authorize = %v, want the malformed entry named", err)
	}
	var notShared *authz.NotSharedError
	if errors.As(err, &notShared) {
		t.Fatal("a malformed entry is reported as not shared, hiding a corrupt definition")
	}
}

func TestAuthorize_RequiresACallingOrkflow(t *testing.T) {
	_, err := authz.Authorize(authz.Request{Action: authz.ActionRun, Callee: callee(&orkstackv1.OrkflowShare{Namespace: "*"}),
		Caller: authz.Caller{Namespace: "payments"}})
	if err == nil || !strings.Contains(err.Error(), "only through composition") {
		t.Fatalf("Authorize with no calling Orkflow = %v", err)
	}
}
