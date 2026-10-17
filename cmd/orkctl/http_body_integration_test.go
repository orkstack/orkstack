//go:build integration

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestIntegration_HTTPBodyIsSentAsWritten runs the activity-http image built
// from this tree through `orkctl task run` — the Orker's own dispatcher —
// against the examples stack's stub: a mapping or a list is sent as compact
// JSON with Content-Type: application/json unless headers sets one, a string
// is sent byte for byte with no newline added, and any other body fails the
// task naming its type.
func TestIntegration_HTTPBodyIsSentAsWritten(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	x := newExamplesStack(ctx, t, examplesOptions{})
	defer x.finish(t, "http-body")
	url := x.services.base + "/post"
	doc := `apiVersion: orkstack/v1
kind: Orkflow
name: http-body
version: "0.1.0"
tasks:
  - id: mapping
    uses: http
    with:
      op: post
      url: "` + url + `"
      body:
        status: ok
        count: 2
        nested: {list: [1, "two"]}
  - id: list
    uses: http
    with:
      op: post
      url: "` + url + `"
      body: [1, "two", {three: 3}]
  - id: own-type
    uses: http
    with:
      op: post
      url: "` + url + `"
      headers:
        Content-Type: application/merge-patch+json
      body:
        status: ok
  - id: text
    uses: http
    with:
      op: post
      url: "` + url + `"
      body: "a"
  - id: number
    uses: http
    with:
      op: post
      url: "` + url + `"
      body: 12
`
	path := filepath.Join(t.TempDir(), "http-body.ork.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ task, body, contentType string }{
		{"mapping", `{"count":2,"nested":{"list":[1,"two"]},"status":"ok"}`, "application/json"},
		{"list", `[1,"two",{"three":3}]`, "application/json"},
		{"own-type", `{"status":"ok"}`, "application/merge-patch+json"},
		{"text", `a`, ""},
	} {
		before := len(x.services.Requests("/post"))
		stdout, stderr, code := x.taskRun("--json", "--file", path, "--task", tc.task)
		if code != 0 {
			t.Fatalf("task run %s exit %d\n%s\n%s", tc.task, code, stdout, stderr)
		}
		requests := x.services.Requests("/post")
		if len(requests) != before+1 {
			t.Fatalf("%s: %d new POST requests, want 1", tc.task, len(requests)-before)
		}
		got := requests[len(requests)-1]
		if string(got.Body) != tc.body {
			t.Errorf("%s: body %q, want %q", tc.task, got.Body, tc.body)
		}
		if tc.contentType != "" && (got.ContentType != tc.contentType || got.ContentTypes != 1) {
			t.Errorf("%s: Content-Type %q (%d headers), want %q once", tc.task, got.ContentType, got.ContentTypes, tc.contentType)
		}
		x.note("task %s sent %q with Content-Type %q", tc.task, got.Body, got.ContentType)
	}

	before := len(x.services.Requests("/post"))
	stdout, stderr, code := x.taskRun("--json", "--file", path, "--task", "number")
	if code == 0 {
		t.Fatalf("a number body succeeded:\n%s\n%s", stdout, stderr)
	}
	if want := "activity-http: with.body must be a string, a mapping or a list, got number"; !strings.Contains(stdout+stderr, want) {
		t.Fatalf("the failure does not say %q:\n%s\n%s", want, stdout, stderr)
	}
	if after := len(x.services.Requests("/post")); after != before {
		t.Fatalf("a refused body was sent")
	}
	x.note("task number failed: with.body must be a string, a mapping or a list, got number")
}
