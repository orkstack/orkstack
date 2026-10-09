//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestNamespaceStore_PutGetListAndDeleteRespectOrkflowOwnership(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	store, err := artifacts.NewS3NamespaceStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	ctx := context.Background()
	nsName := "reg-ns-" + uniqueToken(t)
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, artifacts.NamespacePrefix+"/"+nsName+".json")
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, artifacts.OrkflowPrefix+"/"+nsName+"/")
	})

	if _, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:         nsName,
		Description:  "namespace registry",
		DefaultPool:  "main",
		AllowedPools: []string{"main", "gpu"},
	}, artifacts.PutConditions{}); err != nil {
		t.Fatalf("Put(%s): %v", nsName, err)
	}

	key := fmt.Sprintf("%s/%s.json", artifacts.NamespacePrefix, nsName)
	keys := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.NamespacePrefix+"/")
	found := false
	for _, got := range keys {
		if got == key {
			found = true
		}
	}
	if !found {
		t.Fatalf("namespace keys = %v, want %s", keys, key)
	}

	got, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s): %v", nsName, err)
	}
	if got.GetName() != nsName || got.GetDescription() != "namespace registry" {
		t.Fatalf("Get(%s) = %v, want the stored document", nsName, got)
	}
	defaultNS, err := store.Get(ctx, "")
	if err != nil {
		t.Fatalf("Get(default): %v", err)
	}
	if defaultNS.GetName() != artifacts.DefaultNamespace {
		t.Fatalf("Get(default).Name = %q, want %q", defaultNS.GetName(), artifacts.DefaultNamespace)
	}

	listed, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	seen := map[string]bool{}
	for _, item := range listed {
		seen[item.GetName()] = true
	}
	if !seen[artifacts.DefaultNamespace] || !seen[nsName] {
		t.Fatalf("List names = %v, want default and %s", seen, nsName)
	}

	// The refusal has to say HOW MANY orkflows are in the way, so the count is
	// asserted as a count. Three distinct names, one of them holding two
	// versions: the message must read 3, not 4 — it counts orkflows, not
	// objects — and each name that goes must move it. A bare substring like
	// "1" proves nothing here: the namespace name supplies one.
	orkflowStore, err := artifacts.NewS3OrkflowStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore: %v", err)
	}
	stored := []struct{ name, version string }{
		{"owned", "1.0.0"},
		{"owned", "2.0.0"},
		{"second", "1.0.0"},
		{"third", "1.0.0"},
	}
	for _, o := range stored {
		orkflow := testOrkflow(t, o.name, o.version, time.Now().UTC())
		orkflow.Namespace = nsName
		if _, _, err := orkflowStore.Put(ctx, orkflow, artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put %s@%s: %v", o.name, o.version, err)
		}
	}
	refusalSays := func(want string) {
		t.Helper()
		err := store.Delete(ctx, nsName)
		if !errors.Is(err, artifacts.ErrNamespaceNotEmpty) {
			t.Fatalf("Delete(non-empty) = %v, want ErrNamespaceNotEmpty", err)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Delete(non-empty) said %q, want it to say %q", err.Error(), want)
		}
	}
	dropOrkflow := func(name, version string) {
		t.Helper()
		key := fmt.Sprintf("%s/%s/%s/%s.json", artifacts.OrkflowPrefix, nsName, name, version)
		if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(cfg.S3Bucket),
			Key:    aws.String(key),
		}); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
	}

	refusalSays("3 orkflow(s)")
	dropOrkflow("owned", "1.0.0")
	refusalSays("3 orkflow(s)") // a version went; the orkflow did not
	dropOrkflow("owned", "2.0.0")
	refusalSays("2 orkflow(s)")
	dropOrkflow("second", "1.0.0")
	refusalSays("1 orkflow(s)")
	dropOrkflow("third", "1.0.0")

	if err := store.Delete(ctx, nsName); err != nil {
		t.Fatalf("Delete(empty): %v", err)
	}
	if _, err := store.Get(ctx, nsName); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get(deleted) = %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, artifacts.DefaultNamespace); !errors.Is(err, artifacts.ErrNamespaceNotEmpty) {
		t.Fatalf("Delete(default) = %v, want ErrNamespaceNotEmpty", err)
	}
}

// TestNamespaceStore_StampsCreatedAndUpdatedItself is why those two fields are
// on the wire at all: something writes them. The registry does — created_at
// once, updated_at on every write — and a caller cannot dictate either.
func TestNamespaceStore_StampsCreatedAndUpdatedItself(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	nsName := "reg-clock-" + uniqueToken(t)
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket,
			artifacts.NamespacePrefix+"/"+nsName+".json")
	})

	// The caller sends times of its own; they are discarded.
	backdated := timestamppb.New(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	before := time.Now().UTC().Add(-time.Minute)
	if _, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:        nsName,
		Description: "created",
		CreatedAt:   backdated,
		UpdatedAt:   backdated,
	}, artifacts.PutConditions{}); err != nil {
		t.Fatalf("Put(%s): %v", nsName, err)
	}
	first, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s): %v", nsName, err)
	}
	if first.GetCreatedAt() == nil || first.GetUpdatedAt() == nil {
		t.Fatalf("Get(%s) = created_at %v, updated_at %v; want both stamped",
			nsName, first.GetCreatedAt(), first.GetUpdatedAt())
	}
	if !first.GetCreatedAt().AsTime().After(before) || !first.GetUpdatedAt().AsTime().After(before) {
		t.Fatalf("Get(%s) = created_at %s, updated_at %s; want the store's clock, not the caller's %s",
			nsName, first.GetCreatedAt().AsTime(), first.GetUpdatedAt().AsTime(), backdated.AsTime())
	}

	// A second write keeps the birth and moves the change.
	if _, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:        nsName,
		Description: "edited",
		CreatedAt:   backdated,
	}, artifacts.PutConditions{}); err != nil {
		t.Fatalf("Put(%s) again: %v", nsName, err)
	}
	second, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s) after edit: %v", nsName, err)
	}
	if second.GetDescription() != "edited" {
		t.Fatalf("Get(%s).Description = %q, want the second write's", nsName, second.GetDescription())
	}
	if !second.GetCreatedAt().AsTime().Equal(first.GetCreatedAt().AsTime()) {
		t.Fatalf("created_at moved from %s to %s; a rewrite is not a birth",
			first.GetCreatedAt().AsTime(), second.GetCreatedAt().AsTime())
	}
	if second.GetUpdatedAt().AsTime().Before(first.GetUpdatedAt().AsTime()) {
		t.Fatalf("updated_at went backwards: %s then %s",
			first.GetUpdatedAt().AsTime(), second.GetUpdatedAt().AsTime())
	}
}

// TestNamespaceStore_ConditionalWritesArbitrateBetweenTwoWriters measures the
// compare-and-swap against a real backend.
//
// It matters that this is not a unit test. The whole point of the conditions
// is that the STORE arbitrates: two engine replicas share one bucket, neither
// coordinates with the other, and an unconditional write is a
// last-writer-wins in which the loser never learns it lost. A double that
// implemented IfMatch itself would prove only that the double works.
//
// Three facts, in the order a caller meets them: a create cannot overwrite, an
// update with the etag it read succeeds, and the same etag used twice is
// refused — which is exactly the second replica's edit landing on top of the
// first's.
func TestNamespaceStore_ConditionalWritesArbitrateBetweenTwoWriters(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	store, err := artifacts.NewS3NamespaceStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	ctx := context.Background()
	nsName := "reg-cas-" + uniqueToken(t)
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket,
			artifacts.NamespacePrefix+"/"+nsName+".json")
	})

	created, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:        nsName,
		Description: "first",
	}, artifacts.PutConditions{IfAbsent: true})
	if err != nil {
		t.Fatalf("Put(%s, IfAbsent) on an absent name: %v", nsName, err)
	}
	if created.GetEtag() == "" {
		t.Fatal("a conditional create returned no etag, so nothing can compare-and-swap on it")
	}

	// A second create of the same name is refused by the backend, not by us.
	if _, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:        nsName,
		Description: "second",
	}, artifacts.PutConditions{IfAbsent: true}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("Put(%s, IfAbsent) over an existing name = %v, want ErrPreconditionFailed",
			nsName, err)
	}

	// The etag a read hands back is the one that works.
	read, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s): %v", nsName, err)
	}
	if read.GetEtag() != created.GetEtag() {
		t.Fatalf("Get etag %q != Put etag %q; the token has to name the same bytes both ways",
			read.GetEtag(), created.GetEtag())
	}
	winner, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:        nsName,
		Description: "the winner",
	}, artifacts.PutConditions{IfMatch: read.GetEtag()})
	if err != nil {
		t.Fatalf("Put(%s, IfMatch the current etag): %v", nsName, err)
	}
	if winner.GetEtag() == read.GetEtag() {
		t.Fatal("the etag did not move after a successful conditional write")
	}

	// The second writer still holds the etag it read before the winner wrote.
	// This is the two-replica race, with the interleaving forced.
	if _, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:        nsName,
		Description: "the loser",
	}, artifacts.PutConditions{IfMatch: read.GetEtag()}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("Put(%s, IfMatch a stale etag) = %v, want ErrPreconditionFailed", nsName, err)
	}
	final, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s) after the race: %v", nsName, err)
	}
	if final.GetDescription() != "the winner" {
		t.Fatalf("stored description = %q, want %q: the loser's write landed",
			final.GetDescription(), "the winner")
	}

	// The etag is never stored inside the document it names: a copy in the
	// body could only ever be the previous write's token.
	body := getObject(t, ctx, raw, cfg.S3Bucket,
		artifacts.NamespacePrefix+"/"+nsName+".json")
	if strings.Contains(string(body), "etag") {
		t.Fatalf("the stored document mentions an etag:\n%s", body)
	}
}

// TestNamespaceStore_RoundTripsATelemetryPolicyAndRefusesAReservedKey is the
// store half of the telemetry contract: a policy with a sink, a task-log
// override and attributes survives Put/Get unchanged, and a document whose
// attributes shadow a platform prefix is refused before anything is written
// — validNamespaceTelemetry's document-only rule, exercised through the real
// S3-backed store rather than called directly.
func TestNamespaceStore_RoundTripsATelemetryPolicyAndRefusesAReservedKey(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	store, err := artifacts.NewS3NamespaceStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	ctx := context.Background()
	nsName := "reg-ns-telemetry-" + uniqueToken(t)
	t.Cleanup(func() {
		_ = store.Delete(ctx, nsName)
	})

	policy := &orkstackv1.TelemetryPolicy{
		Sink:       "datadog",
		TaskLogs:   orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED,
		Attributes: map[string]string{"team": "payments"},
	}
	if _, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:      nsName,
		Telemetry: policy,
	}, artifacts.PutConditions{}); err != nil {
		t.Fatalf("Put with a telemetry policy: %v", err)
	}

	got, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s): %v", nsName, err)
	}
	if got.GetTelemetry().GetSink() != "datadog" {
		t.Errorf("sink = %q, want datadog", got.GetTelemetry().GetSink())
	}
	if got.GetTelemetry().GetTaskLogs() != orkstackv1.TaskLogExport_TASK_LOG_EXPORT_ENABLED {
		t.Errorf("task_logs = %v, want ENABLED", got.GetTelemetry().GetTaskLogs())
	}
	if got.GetTelemetry().GetAttributes()["team"] != "payments" {
		t.Errorf("attributes = %v, want team=payments", got.GetTelemetry().GetAttributes())
	}

	// A reserved-prefix attribute is refused by the store itself: nothing
	// downstream of internal/api gets to decide this differently.
	_, err = store.Put(ctx, &orkstackv1.Namespace{
		Name: "reg-ns-telemetry-reserved-" + uniqueToken(t),
		Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"ork.reserved": "1"},
		},
	}, artifacts.PutConditions{})
	if !errors.Is(err, artifacts.ErrInvalidNamespace) {
		t.Fatalf("Put with a reserved attribute key = %v, want ErrInvalidNamespace", err)
	}
}

// TestNamespaceStore_RoundTripsHoldAndMaxRunningAndStampsSinceItself: the
// hold's since is the registry's, set on the write that set the hold and kept
// across a reason edit; releasing it is a write with no hold; an empty reason
// is refused and leaves the stored document as it was.
func TestNamespaceStore_RoundTripsHoldAndMaxRunningAndStampsSinceItself(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	nsName := "reg-ns-hold-" + uniqueToken(t)
	t.Cleanup(func() { _ = store.Delete(context.Background(), nsName) })

	forged := timestamppb.New(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	before := time.Now().UTC().Add(-time.Minute)
	put, err := store.Put(ctx, &orkstackv1.Namespace{
		Name:       nsName,
		MaxRunning: 20,
		Hold:       &orkstackv1.NamespaceHold{Reason: "partner CI loop, INC-4412", Since: forged},
	}, artifacts.PutConditions{IfAbsent: true})
	if err != nil {
		t.Fatalf("Put with a hold: %v", err)
	}
	first, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s): %v", nsName, err)
	}
	if got := first.GetHold().GetReason(); got != "partner CI loop, INC-4412" {
		t.Fatalf("hold.reason = %q, want the one written", got)
	}
	if first.GetMaxRunning() != 20 {
		t.Fatalf("max_running = %d, want 20", first.GetMaxRunning())
	}
	since := first.GetHold().GetSince()
	if since == nil || !since.AsTime().After(before) {
		t.Fatalf("hold.since = %v, want the registry's clock, not the client's %s", since, forged.AsTime())
	}

	// A reason edit under the etag keeps since and moves updated_at.
	edited := proto.Clone(first).(*orkstackv1.Namespace)
	edited.Hold.Reason = "partner CI loop, INC-4412 (vendor paged)"
	edited.Hold.Since = forged
	if _, err := store.Put(ctx, edited, artifacts.PutConditions{IfMatch: put.GetEtag()}); err != nil {
		t.Fatalf("Put reason edit: %v", err)
	}
	second, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get after edit: %v", err)
	}
	if second.GetHold().GetReason() != edited.GetHold().GetReason() {
		t.Fatalf("hold.reason = %q, want the edit", second.GetHold().GetReason())
	}
	if !second.GetHold().GetSince().AsTime().Equal(since.AsTime()) {
		t.Fatalf("hold.since moved from %s to %s on a reason edit", since.AsTime(), second.GetHold().GetSince().AsTime())
	}
	if !second.GetUpdatedAt().AsTime().After(first.GetUpdatedAt().AsTime()) &&
		!second.GetUpdatedAt().AsTime().Equal(first.GetUpdatedAt().AsTime()) {
		t.Fatalf("updated_at went backwards: %s then %s", first.GetUpdatedAt().AsTime(), second.GetUpdatedAt().AsTime())
	}
	if second.GetEtag() == first.GetEtag() {
		t.Fatalf("etag unchanged across a write: %q", second.GetEtag())
	}

	// Release: a write with no hold.
	released := proto.Clone(second).(*orkstackv1.Namespace)
	released.Hold = nil
	if _, err := store.Put(ctx, released, artifacts.PutConditions{IfMatch: second.GetEtag()}); err != nil {
		t.Fatalf("Put release: %v", err)
	}
	third, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get after release: %v", err)
	}
	if third.GetHold() != nil {
		t.Fatalf("hold = %v after release, want none", third.GetHold())
	}

	// An empty reason is refused, and the stored document is unchanged.
	bad := proto.Clone(third).(*orkstackv1.Namespace)
	bad.Hold = &orkstackv1.NamespaceHold{Reason: "  "}
	if _, err := store.Put(ctx, bad, artifacts.PutConditions{IfMatch: third.GetEtag()}); !errors.Is(err, artifacts.ErrInvalidNamespace) {
		t.Fatalf("Put with an empty reason = %v, want ErrInvalidNamespace", err)
	}
	after, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get after refused write: %v", err)
	}
	if after.GetEtag() != third.GetEtag() || after.GetHold() != nil {
		t.Fatalf("refused write changed the document: etag %q -> %q, hold %v",
			third.GetEtag(), after.GetEtag(), after.GetHold())
	}
}

// TestNamespaceStore_RoundTripsAllowedSecretsInOrderAndRefusesABadOne: the
// list is stored and returned in the order given, an empty list under the
// etag clears it, and a traversal entry is refused by the store itself with
// the stored document left as it was.
func TestNamespaceStore_RoundTripsAllowedSecretsInOrderAndRefusesABadOne(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	nsName := "reg-ns-secrets-" + uniqueToken(t)
	t.Cleanup(func() { _ = store.Delete(context.Background(), nsName) })

	want := []string{"file:/etc/orkstack/secrets/sandbox/*", "env:SANDBOX_*"}
	put, err := store.Put(ctx, &orkstackv1.Namespace{Name: nsName, AllowedSecrets: want},
		artifacts.PutConditions{IfAbsent: true})
	if err != nil {
		t.Fatalf("Put with two allowed_secrets: %v", err)
	}
	first, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get(%s): %v", nsName, err)
	}
	if got := first.GetAllowedSecrets(); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("allowed_secrets = %q, want %q in that order", got, want)
	}

	cleared := proto.Clone(first).(*orkstackv1.Namespace)
	cleared.AllowedSecrets = nil
	if _, err := store.Put(ctx, cleared, artifacts.PutConditions{IfMatch: put.GetEtag()}); err != nil {
		t.Fatalf("Put clearing allowed_secrets: %v", err)
	}
	second, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get after clear: %v", err)
	}
	if len(second.GetAllowedSecrets()) != 0 {
		t.Fatalf("allowed_secrets = %q after clear, want none", second.GetAllowedSecrets())
	}

	bad := proto.Clone(second).(*orkstackv1.Namespace)
	bad.AllowedSecrets = []string{"file:/a/../b"}
	_, err = store.Put(ctx, bad, artifacts.PutConditions{IfMatch: second.GetEtag()})
	if !errors.Is(err, artifacts.ErrInvalidNamespace) || !strings.Contains(err.Error(), `"file:/a/../b"`) {
		t.Fatalf("Put with file:/a/../b = %v, want ErrInvalidNamespace naming the entry", err)
	}
	after, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get after refused write: %v", err)
	}
	if after.GetEtag() != second.GetEtag() || len(after.GetAllowedSecrets()) != 0 {
		t.Fatalf("refused write changed the document: etag %q -> %q, allowed_secrets %q",
			second.GetEtag(), after.GetEtag(), after.GetAllowedSecrets())
	}
}

// getObject reads one object's bytes through the raw client, so an assertion
// about what was STORED is made against the bucket rather than against what
// the store hands back.
func getObject(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string) []byte {
	t.Helper()
	object, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get %s/%s: %v", bucket, key, err)
	}
	defer object.Body.Close()
	body, err := io.ReadAll(object.Body)
	if err != nil {
		t.Fatalf("read %s/%s: %v", bucket, key, err)
	}
	return body
}

// TestNamespaceStore_RoundTripsABudget: a budget is stored and read back, a
// budget that bounds nothing is refused by the store with the stored
// document unchanged, and a write with no budget clears it.
func TestNamespaceStore_RoundTripsABudget(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	store, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	nsName := "reg-ns-budget-" + uniqueToken(t)
	t.Cleanup(func() { _ = store.Delete(context.Background(), nsName) })

	budget := &orkstackv1.Budget{CpuSeconds: 5000000, Period: durationpb.New(720 * time.Hour), Enforce: true}
	put, err := store.Put(ctx, &orkstackv1.Namespace{Name: nsName, Budget: budget}, artifacts.PutConditions{IfAbsent: true})
	if err != nil {
		t.Fatalf("Put with a budget: %v", err)
	}
	first, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !proto.Equal(first.GetBudget(), budget) {
		t.Fatalf("budget = %v, want %v", first.GetBudget(), budget)
	}

	bad := proto.Clone(first).(*orkstackv1.Namespace)
	bad.Budget = &orkstackv1.Budget{Period: durationpb.New(720 * time.Hour)}
	if _, err := store.Put(ctx, bad, artifacts.PutConditions{IfMatch: put.GetEtag()}); !errors.Is(err, artifacts.ErrInvalidNamespace) ||
		!strings.Contains(err.Error(), "bounds nothing") {
		t.Fatalf("Put of a zero-dimension budget = %v, want ErrInvalidNamespace naming 'bounds nothing'", err)
	}
	unchanged, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get after refused write: %v", err)
	}
	if unchanged.GetEtag() != first.GetEtag() || !proto.Equal(unchanged.GetBudget(), budget) {
		t.Fatalf("refused write changed the document: etag %q -> %q, budget %v", first.GetEtag(), unchanged.GetEtag(), unchanged.GetBudget())
	}

	cleared := proto.Clone(unchanged).(*orkstackv1.Namespace)
	cleared.Budget = nil
	if _, err := store.Put(ctx, cleared, artifacts.PutConditions{IfMatch: unchanged.GetEtag()}); err != nil {
		t.Fatalf("Put with no budget: %v", err)
	}
	last, err := store.Get(ctx, nsName)
	if err != nil {
		t.Fatalf("Get after clear: %v", err)
	}
	if last.GetBudget() != nil {
		t.Fatalf("budget = %v after a write with none, want unset", last.GetBudget())
	}
}

// TestNamespaceStore_ListNamesIsSortedByNameAndReadsNoDocument runs against
// real MinIO in a bucket private to this test, so the exact set of names is
// known.
//
// It proves the two facts a paged ListNamespaces rests on. The names come
// back in NAME order, not key order: the bucket lists `a-b.json` before
// `a.json` because `-` sorts before `.`, and a cursor over keys would skip
// or repeat a name. And no document is read: one namespace document holds
// bytes that are not a namespace at all, so List — which reads every
// document — fails on it, while ListNames still answers.
func TestNamespaceStore_ListNamesIsSortedByNameAndReadsNoDocument(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	cfg.S3Bucket = "orkstack-ns-names-" + strings.ToLower(uniqueToken(t))
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		t.Fatalf("create private bucket %s: %v", cfg.S3Bucket, err)
	}
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, "")
		_, _ = raw.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(cfg.S3Bucket)})
	})
	store, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}

	for _, name := range []string{"ab", "a-b", "a"} {
		if _, err := store.Put(ctx, &orkstackv1.Namespace{Name: name}, artifacts.PutConditions{}); err != nil {
			t.Fatalf("Put(%s): %v", name, err)
		}
	}
	// zz's document is not a namespace, and Bad_Name.json is no key this
	// package writes: the first must be listed by name, the second skipped.
	for key, body := range map[string]string{
		artifacts.NamespacePrefix + "/zz.json":       "this is not a namespace document",
		artifacts.NamespacePrefix + "/Bad_Name.json": "{}",
	} {
		if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(cfg.S3Bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(body),
		}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	keys := listKeys(t, ctx, raw, cfg.S3Bucket, artifacts.NamespacePrefix+"/")
	t.Logf("bucket key order: %v", keys)
	if strings.Join(keys, " ") != strings.Join([]string{
		artifacts.NamespacePrefix + "/Bad_Name.json",
		artifacts.NamespacePrefix + "/a-b.json",
		artifacts.NamespacePrefix + "/a.json",
		artifacts.NamespacePrefix + "/ab.json",
		artifacts.NamespacePrefix + "/zz.json",
	}, " ") {
		t.Fatalf("bucket key order = %v; the premise of this test (a-b.json before a.json) does not hold", keys)
	}

	names, err := store.ListNames(ctx)
	if err != nil {
		t.Fatalf("ListNames: %v", err)
	}
	t.Logf("ListNames: %v", names)
	if want := "a a-b ab default zz"; strings.Join(names, " ") != want {
		t.Fatalf("ListNames = %v, want [%s]: every stored name plus default, by name", names, want)
	}
	if _, err := store.List(ctx); err == nil {
		t.Fatal("List succeeded over a document that is not a namespace; it should read and refuse it, " +
			"which is what shows ListNames did not read it")
	} else {
		t.Logf("List over the same bucket fails, as it reads every document: %v", err)
	}
}
