package main

import (
	"bytes"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A child run of another namespace's Orkflow names that namespace wherever
// the CLI names its Orkflow, and an Orkflow's shared_with is shown with it.

func TestRenderRun_NamesASharedOrkflowsNamespace(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-1", Namespace: "search", OrkflowName: "deploy", OrkflowVersion: "1.0.0",
		OrkflowNamespace: "platform", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	var out bytes.Buffer
	if err := renderRun(&out, run); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(collapseSpaces(out.String()), "Orkflow: platform/deploy (shared from namespace platform)") {
		t.Fatalf("run get = %q", out.String())
	}
	out.Reset()
	if err := renderRunList(&out, []*orkstackv1.Run{run}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(collapseSpaces(out.String()), "ork-1 platform/deploy 1.0.0") {
		t.Fatalf("run list = %q", out.String())
	}
	own := &orkstackv1.Run{Id: "ork-2", Namespace: "search", OrkflowName: "deploy", OrkflowVersion: "1"}
	out.Reset()
	if err := renderRun(&out, own); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "shared") || strings.Contains(out.String(), "/deploy") {
		t.Fatalf("an own run names a share: %q", out.String())
	}
}

func TestChildLabel_NamesASharedChildsNamespace(t *testing.T) {
	if got := childLabel(&orkstackv1.TaskChild{OrkflowName: "deploy", OrkflowVersion: "1", OrkflowNamespace: "platform"}); got != "platform/deploy@1" {
		t.Fatalf("childLabel = %q", got)
	}
	if got := childLabel(&orkstackv1.TaskChild{OrkflowName: "deploy", OrkflowVersion: "1"}); got != "deploy@1" {
		t.Fatalf("childLabel = %q", got)
	}
}

func TestRenderOrkflow_ShowsWhoItIsSharedWith(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", Tasks: []*orkstackv1.Task{{Id: "ship", Uses: "shell"}},
		SharedWith: []*orkstackv1.OrkflowShare{{Namespace: "*"}, {Orkflow: "search/release"}}}
	var out bytes.Buffer
	if err := renderOrkflow(&out, wf, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(collapseSpaces(out.String()), "Shared with: namespace:*, orkflow:search/release") {
		t.Fatalf("orkflow get = %q", out.String())
	}
	out.Reset()
	wf.SharedWith = nil
	if err := renderOrkflow(&out, wf, ""); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Shared with") {
		t.Fatalf("an unshared orkflow get names a share: %q", out.String())
	}
}

func TestStatsRows_NameASharedOrkflowsOwner(t *testing.T) {
	byOrkflow := orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW
	if got := failureCell(&orkstackv1.FailureGroup{OrkflowName: "deploy", OrkflowNamespace: "platform"}, byOrkflow); got != "platform/deploy" {
		t.Fatalf("failure cell = %q", got)
	}
	if got := flakeCell(&orkstackv1.FlakeGroup{OrkflowName: "deploy", OrkflowNamespace: "platform"}, byOrkflow); got != "platform/deploy" {
		t.Fatalf("flake cell = %q", got)
	}
	if got := usageCell(&orkstackv1.UsageRow{OrkflowName: "deploy", OrkflowNamespace: "platform"}, byOrkflow); got != "platform/deploy" {
		t.Fatalf("usage cell = %q", got)
	}
	if got := usageCell(&orkstackv1.UsageRow{OrkflowName: "deploy"}, byOrkflow); got != "deploy" {
		t.Fatalf("own usage cell = %q", got)
	}
}

// An Orkflow that declares a concurrency limit names it, in the words the
// UI's Concurrency fact uses (ui/src/lib/concurrency.ts, concurrencyLabel).
func TestRenderOrkflow_ShowsItsConcurrencyLimit(t *testing.T) {
	cases := []struct {
		name string
		spec *orkstackv1.ConcurrencySpec
		want string
	}{
		{"a key, a timeout and a bound", &orkstackv1.ConcurrencySpec{Limit: 1, Key: "db-migrate:{{ .inputs.env }}",
			OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE, QueueTimeoutSeconds: 7200, MaxQueued: 50},
			"Concurrency: limit 1 on key db-migrate:{{ .inputs.env }}; the rest wait in line up to 2h, at most 50"},
		{"its own key, the default timeout", &orkstackv1.ConcurrencySpec{Limit: 2},
			"Concurrency: limit 2 on this Orkflow's own key; the rest wait in line up to the deployment's default"},
		{"half an hour", &orkstackv1.ConcurrencySpec{Limit: 3, QueueTimeoutSeconds: 1800},
			"Concurrency: limit 3 on this Orkflow's own key; the rest wait in line up to 30m"},
		{"an hour and five seconds", &orkstackv1.ConcurrencySpec{Limit: 1, QueueTimeoutSeconds: 3605},
			"Concurrency: limit 1 on this Orkflow's own key; the rest wait in line up to 1h0m5s"},
		{"another strategy, as the DSL writes it", &orkstackv1.ConcurrencySpec{Limit: 2, OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_CANCEL_OLDEST},
			"Concurrency: limit 2 on this Orkflow's own key; on_limit cancel_oldest"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", Tasks: []*orkstackv1.Task{{Id: "ship", Uses: "shell"}}, Concurrency: c.spec}
			var out bytes.Buffer
			if err := renderOrkflow(&out, wf, ""); err != nil {
				t.Fatal(err)
			}
			if got := collapseSpaces(detailLine(t, out.String(), "Concurrency:")); got != c.want {
				t.Fatalf("Concurrency line = %q, want %q", got, c.want)
			}
		})
	}
}

// An Orkflow that declares no concurrency limit names none.
func TestRenderOrkflow_NamesNoLimitItDoesNotDeclare(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", Tasks: []*orkstackv1.Task{{Id: "ship", Uses: "shell"}}}
	var out bytes.Buffer
	if err := renderOrkflow(&out, wf, ""); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Concurrency:") {
		t.Fatalf("an Orkflow with no limit names one:\n%s", out.String())
	}
}
