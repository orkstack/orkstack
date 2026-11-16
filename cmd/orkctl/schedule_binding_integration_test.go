//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_AScheduleRecordConvergesWhenItsAnswersAreLost: the
// engine's writes of a schedule's record land and then have their answer
// lost, once on the create, twice on an update (the entry and the trim) and
// once on the delete. Each save still answers as it should; the record
// converges to the one entry that submits what the cluster holds; and the
// delete leaves neither a Temporal Schedule nor a record, so nothing is
// orphaned.
func TestIntegration_AScheduleRecordConvergesWhenItsAnswersAreLost(t *testing.T) {
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, cfg)
	proxy := s3scope.New(t, cfg.S3Endpoint, cfg.S3Bucket, s3scope.Everything())
	s := startScopeStack(t, []string{"ORK_S3_ENDPOINT=" + proxy.URL}, nil)
	fmt.Fprintf(&s.log, "# a schedule record whose writes lose their answers (%s)\nengine %s, store %s through %s\n\n",
		time.Now().UTC().Format(time.RFC3339), s.engine.addr, cfg.S3Endpoint, proxy.URL)
	records, err := artifacts.NewScheduleBindingStore(s.ctx, cfg)
	must.OK(t, err)
	recordWrite := func(r s3scope.Request) bool {
		return r.Verb == s3scope.Put && strings.HasPrefix(r.Key, artifacts.ScheduleBindingPrefix+"/")
	}

	flow := "binding-" + fmt.Sprint(time.Now().UnixNano()%1_000_000_000)
	s.create(flow, fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
description: A schedule's target; the schedule stays paused.
tasks:
  - id: echo
    uses: shell
    with:
      op: exec
      command:
        - "true"
`, flow))

	get := func() *orkstackv1.Schedule {
		t.Helper()
		out := s.cli("schedule", "get", "nightly", "--json")
		resp := &orkstackv1.GetScheduleResponse{}
		must.OKf(t, api.UnmarshalJSON([]byte(out), resp), "schedule get --json: %s", out)
		return resp.GetSchedule()
	}
	converged := func(step string, want func(artifacts.ScheduleBinding) bool) artifacts.ScheduleBindings {
		t.Helper()
		sch := get()
		doc, err := records.Get(s.ctx, "default", sch.GetId())
		must.OKf(t, err, "read the record after %s", step)
		if len(doc.Entries) != 1 || !want(doc.Entries[0]) {
			t.Fatalf("after %s the record is %+v, want one entry for what the cluster holds", step, doc.Entries)
		}
		if sch.GetConsented().GetDigest() != consent.Empty() {
			t.Fatalf("after %s the schedule's consent is %v", step, sch.GetConsented())
		}
		fmt.Fprintf(&s.log, "after %s: one entry, version %q, binder %q, consent %s\n\n", step,
			doc.Entries[0].OrkflowVersion, doc.Entries[0].Binder, doc.Entries[0].Consent)
		return doc
	}

	proxy.LoseAnswer(recordWrite, 1)
	s.cli("schedule", "create", "nightly", "--orkflow", flow, "--cron", "0 2 * * *", "--paused")
	if proxy.LostSeen() != 1 {
		t.Fatalf("the create lost %d answers, want 1, so nothing here was tested", proxy.LostSeen())
	}
	first := converged("a create whose record write lost its answer", func(e artifacts.ScheduleBinding) bool { return e.OrkflowVersion == "" })

	proxy.LoseAnswer(recordWrite, 2)
	s.cli("schedule", "update", "nightly", "--input-json", `{"who":"b"}`)
	if proxy.LostSeen() != 3 {
		t.Fatalf("the update lost %d answers in all, want 3", proxy.LostSeen())
	}
	converged("an update whose entry and trim lost their answers", func(e artifacts.ScheduleBinding) bool {
		return e.InputSHA256 != first.Entries[0].InputSHA256
	})
	id := first.ScheduleID

	proxy.LoseAnswer(recordWrite, 1)
	s.cli("schedule", "delete", "nightly")
	if proxy.LostSeen() != 4 {
		t.Fatalf("the delete lost %d answers in all, want 4", proxy.LostSeen())
	}
	_, err = s.temporal.ScheduleClient().GetHandle(s.ctx, engine.TemporalScheduleID("default", "nightly")).Describe(s.ctx)
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("after the delete the cluster's schedule answers %v, want NotFound: an orphaned schedule", err)
	}
	after, err := records.Get(s.ctx, "default", id)
	must.OK(t, err)
	if len(after.Entries) != 0 {
		t.Fatalf("after the delete the record holds %+v", after.Entries)
	}
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	iter, err := s.temporal.ScheduleClient().List(ctx, client.ScheduleListOptions{})
	must.OK(t, err)
	for iter.HasNext() {
		entry, err := iter.Next()
		must.OK(t, err)
		if strings.Contains(entry.ID, "nightly") {
			t.Fatalf("the cluster still lists %s", entry.ID)
		}
	}
	t.Logf("EVIDENCE schedule record with %d lost answers: converged to one entry after each save; the delete left no Temporal Schedule and no record", proxy.LostSeen())
	proof.Write(t, "orkctl", "schedule-record-lost-answers", []byte(s.log.String()))
}
