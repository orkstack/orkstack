//go:build integration

package api_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// mintedReplicas builds count API replicas sharing one bucket and nothing else:
// each has its own store client and its own process-local state.
func mintedReplicas(t *testing.T, count int) ([]*api.Service, *artifacts.S3OrkflowStore, artifacts.Config) {
	t.Helper()
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireAuditMinIO(t, cfg)
	ctx := context.Background()
	replicas := make([]*api.Service, count)
	var first *artifacts.S3OrkflowStore
	for i := range replicas {
		store, err := artifacts.NewS3OrkflowStore(ctx, cfg)
		if err != nil {
			t.Fatalf("NewS3OrkflowStore: %v", err)
		}
		if first == nil {
			first = store
		}
		replicas[i] = api.NewService(api.Config{
			Orkflows: store,
			Runs:     engine.NewRunStore(),
			Bus:      engine.NewEventBus(),
			Starter:  &fakeStarter{},
		})
	}
	return replicas, first, cfg
}

// mintedDocument is a document under a name of its own, so a test reads only what
// it wrote.
func mintedDocument(t *testing.T, comment string) (name, doc string) {
	t.Helper()
	name = fmt.Sprintf("minted-%d-%d", os.Getpid(), time.Now().UnixNano())
	return name, "apiVersion: orkstack/v1\nkind: Orkflow\nname: " + name + "\ntasks:\n  - id: one\n    uses: busybox:latest\n" + comment
}

// objectETags is every object under one Orkflow's prefix with its ETag, which is
// how "wrote nothing" is measured: not by what an apply reports, by the bucket.
func objectETags(t *testing.T, ctx context.Context, raw *s3.Client, bucket, name string) map[string]string {
	t.Helper()
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	out, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	if err != nil {
		t.Fatalf("list %s: %v", prefix, err)
	}
	etags := make(map[string]string, len(out.Contents))
	for _, object := range out.Contents {
		etags[aws.ToString(object.Key)] = aws.ToString(object.ETag)
	}
	return etags
}

// TestIntegration_ConcurrentAppliesAcrossReplicasConverge is the race of tracker
// item E44, item 3, on the real conditional write: replicas that share only a
// bucket apply one new document at one instant. They end on one version, exactly
// one of them created it, the head names it and one version object exists.
func TestIntegration_ConcurrentAppliesAcrossReplicasConverge(t *testing.T) {
	const appliers, rounds = 8, 10
	replicas, store, cfg := mintedReplicas(t, appliers)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	raw := auditS3Client(t, cfg)

	for round := range rounds {
		name, doc := mintedDocument(t, fmt.Sprintf("# round %d\n", round))
		start := make(chan struct{})
		responses := make([]*orkstackv1.ApplyOrkflowResponse, appliers)
		var wg sync.WaitGroup
		for i, replica := range replicas {
			wg.Go(func() {
				<-start
				resp, err := replica.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
				if err != nil {
					t.Errorf("round %d, replica %d: %v", round, i, err)
					return
				}
				responses[i] = resp.Msg
			})
		}
		close(start)
		wg.Wait()
		if t.Failed() {
			t.FailNow()
		}

		version := responses[0].GetOrkflow().GetVersion()
		if !mintedVersionShape.MatchString(version) {
			t.Fatalf("round %d: version %q, want a minted one", round, version)
		}
		creators := 0
		for i, resp := range responses {
			if got := resp.GetOrkflow().GetVersion(); got != version {
				t.Fatalf("round %d: replica %d landed on %q, replica 0 on %q", round, i, got, version)
			}
			if resp.GetCreated() {
				creators++
			}
		}
		if creators != 1 {
			t.Fatalf("round %d: %d replicas reported created=true, want exactly one", round, creators)
		}
		catalog, err := store.Versions(ctx, "", name)
		if err != nil {
			t.Fatalf("round %d: Versions: %v", round, err)
		}
		if len(catalog.All) != 1 || catalog.Head != version {
			t.Fatalf("round %d: catalog = %d versions, head %q; want one, head %q", round, len(catalog.All), catalog.Head, version)
		}
		// The version object, the head and one claim, and nothing else.
		if etags := objectETags(t, ctx, raw, cfg.S3Bucket, name); len(etags) != 3 {
			t.Fatalf("round %d: objects = %v, want 3", round, sortedKeysOf(etags))
		}
		if err := store.Delete(ctx, "", name); err != nil {
			t.Fatalf("round %d: Delete: %v", round, err)
		}
	}
}

func sortedKeysOf(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestIntegration_ReapplyOnAnotherReplicaWritesNothing is docs/DSL.md's no-op, by
// the bucket's own ETags: a replica that never saw the first apply finds the
// version from the store, and no object changes.
func TestIntegration_ReapplyOnAnotherReplicaWritesNothing(t *testing.T) {
	replicas, store, cfg := mintedReplicas(t, 2)
	ctx := context.Background()
	raw := auditS3Client(t, cfg)
	name, doc := mintedDocument(t, "")
	t.Cleanup(func() { _ = store.Delete(context.Background(), "", name) })

	first, err := replicas[0].ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	before := objectETags(t, ctx, raw, cfg.S3Bucket, name)
	for i, replica := range replicas {
		again, err := replica.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
		if err != nil {
			t.Fatalf("re-apply on replica %d: %v", i, err)
		}
		if again.Msg.GetCreated() || !proto.Equal(again.Msg.GetOrkflow(), first.Msg.GetOrkflow()) {
			t.Fatalf("re-apply on replica %d = created %v, %v; want the stored %v",
				i, again.Msg.GetCreated(), again.Msg.GetOrkflow(), first.Msg.GetOrkflow())
		}
		validated, err := replica.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
		if err != nil {
			t.Fatalf("validate on replica %d: %v", i, err)
		}
		if got := validated.Msg.GetOrkflow().GetVersion(); got != first.Msg.GetOrkflow().GetVersion() {
			t.Fatalf("validate on replica %d reports %q, want %q", i, got, first.Msg.GetOrkflow().GetVersion())
		}
	}
	after := objectETags(t, ctx, raw, cfg.S3Bucket, name)
	if len(after) != len(before) {
		t.Fatalf("objects went from %v to %v", sortedKeysOf(before), sortedKeysOf(after))
	}
	for key, etag := range before {
		if after[key] != etag {
			t.Errorf("%s changed: ETag %s then %s", key, etag, after[key])
		}
	}
}

// TestIntegration_AStoredSHA256VersionCoexistsWithMintedOnes is the upgrade on
// real storage: a catalog written by an earlier release has no claims. Re-applying
// its document writes nothing, an edit mints a version beside it, and both read.
func TestIntegration_AStoredSHA256VersionCoexistsWithMintedOnes(t *testing.T) {
	replicas, store, cfg := mintedReplicas(t, 1)
	svc := replicas[0]
	ctx := context.Background()
	raw := auditS3Client(t, cfg)
	name, doc := mintedDocument(t, "")
	t.Cleanup(func() { _ = store.Delete(context.Background(), "", name) })

	validated, err := svc.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	wf := proto.Clone(validated.Msg.GetOrkflow()).(*orkstackv1.Orkflow)
	hash, err := dsl.ContentHash([]byte(doc), wf)
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	wf.Version = hash
	if wf.DefinitionDigest, err = dsl.DefinitionDigest(wf); err != nil {
		t.Fatalf("DefinitionDigest: %v", err)
	}
	wf.CreatedAt = timestamppb.New(time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond))
	legacy, created, err := store.Put(ctx, wf, artifacts.NoHeadExpectation())
	if err != nil || !created {
		t.Fatalf("seed the stored version: created %v, %v", created, err)
	}
	if !legacyVersionShape.MatchString(legacy.GetVersion()) {
		t.Fatalf("seeded version %q, want a hex sha256", legacy.GetVersion())
	}

	before := objectETags(t, ctx, raw, cfg.S3Bucket, name)
	again, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if again.Msg.GetCreated() || again.Msg.GetOrkflow().GetVersion() != legacy.GetVersion() {
		t.Fatalf("re-apply = created %v, version %q; want no write and %q",
			again.Msg.GetCreated(), again.Msg.GetOrkflow().GetVersion(), legacy.GetVersion())
	}
	after := objectETags(t, ctx, raw, cfg.S3Bucket, name)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("re-applying a stored sha256 version changed the bucket:\n%v\n%v", before, after)
	}

	edited, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc + "# an edit\n"}))
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if !edited.Msg.GetCreated() || !mintedVersionShape.MatchString(edited.Msg.GetOrkflow().GetVersion()) {
		t.Fatalf("edit = created %v, version %q; want a minted version", edited.Msg.GetCreated(), edited.Msg.GetOrkflow().GetVersion())
	}
	catalog, err := store.Versions(ctx, "", name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 2 || catalog.Head != edited.Msg.GetOrkflow().GetVersion() {
		t.Fatalf("catalog = %d versions, head %q", len(catalog.All), catalog.Head)
	}
	for _, version := range []string{legacy.GetVersion(), edited.Msg.GetOrkflow().GetVersion()} {
		got, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: name, Version: version}))
		if err != nil || got.Msg.GetOrkflow().GetVersion() != version {
			t.Fatalf("GetOrkflow(%s) = %v, %v", version, got, err)
		}
	}
}

// TestIntegration_AClaimLeftByACrashIsAdoptedByTheNextApply: a writer that claimed
// a version and died before storing it leaves an inert object. The next apply of
// that content, on any replica, stores the version it names.
func TestIntegration_AClaimLeftByACrashIsAdoptedByTheNextApply(t *testing.T) {
	replicas, store, _ := mintedReplicas(t, 1)
	svc := replicas[0]
	ctx := context.Background()
	name, doc := mintedDocument(t, "")
	t.Cleanup(func() { _ = store.Delete(context.Background(), "", name) })

	validated, err := svc.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	hash, err := dsl.ContentHash([]byte(doc), validated.Msg.GetOrkflow())
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	const orphan = "01h455vb4pex5vsknk084sn02q"
	if _, err := store.ClaimDerivedVersion(ctx, "", name, hash, orphan); err != nil {
		t.Fatalf("claim: %v", err)
	}

	applied, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !applied.Msg.GetCreated() || applied.Msg.GetOrkflow().GetVersion() != orphan {
		t.Fatalf("apply = created %v, version %q; want it stored under %q",
			applied.Msg.GetCreated(), applied.Msg.GetOrkflow().GetVersion(), orphan)
	}
}

// TestIntegration_ADamagedClaimDoesNotBlockApply: a claim that no longer decodes
// must not need a person to edit the bucket (AGENTS.md §15). Validation still
// answers, replicas applying together land on one new version, and from then on
// the document is an ordinary no-op again.
func TestIntegration_ADamagedClaimDoesNotBlockApply(t *testing.T) {
	const appliers = 8
	replicas, store, cfg := mintedReplicas(t, appliers)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	raw := auditS3Client(t, cfg)
	name, doc := mintedDocument(t, "")
	t.Cleanup(func() { _ = store.Delete(context.Background(), "", name) })

	first, err := replicas[0].ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	hash, err := dsl.ContentHash([]byte(doc), first.Msg.GetOrkflow())
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	key, err := artifacts.OrkflowDerivedKey("", name, hash)
	if err != nil {
		t.Fatalf("OrkflowDerivedKey: %v", err)
	}
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(`{"version":"../../escape"}`),
	}); err != nil {
		t.Fatalf("damage the claim: %v", err)
	}

	validated, err := replicas[0].ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("validate over a damaged claim: %v", err)
	}
	if got := validated.Msg.GetOrkflow().GetVersion(); got != "" {
		t.Fatalf("validate over a damaged claim reports %q, want none", got)
	}

	start := make(chan struct{})
	responses := make([]*orkstackv1.ApplyOrkflowResponse, appliers)
	var wg sync.WaitGroup
	for i, replica := range replicas {
		wg.Go(func() {
			<-start
			resp, err := replica.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
			if err != nil {
				t.Errorf("replica %d: %v", i, err)
				return
			}
			responses[i] = resp.Msg
		})
	}
	close(start)
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	repaired := responses[0].GetOrkflow().GetVersion()
	if !mintedVersionShape.MatchString(repaired) || repaired == first.Msg.GetOrkflow().GetVersion() {
		t.Fatalf("version after the damage = %q, want a new minted one beside %q", repaired, first.Msg.GetOrkflow().GetVersion())
	}
	creators := 0
	for i, resp := range responses {
		if got := resp.GetOrkflow().GetVersion(); got != repaired {
			t.Fatalf("replica %d landed on %q, replica 0 on %q", i, got, repaired)
		}
		if resp.GetCreated() {
			creators++
		}
	}
	if creators != 1 {
		t.Fatalf("%d replicas reported created=true, want exactly one", creators)
	}
	catalog, err := store.Versions(ctx, "", name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if len(catalog.All) != 2 || catalog.Head != repaired {
		t.Fatalf("catalog = %d versions, head %q; want the old and the new, head %q", len(catalog.All), catalog.Head, repaired)
	}

	before := objectETags(t, ctx, raw, cfg.S3Bucket, name)
	again, err := replicas[1].ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc}))
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if again.Msg.GetCreated() || again.Msg.GetOrkflow().GetVersion() != repaired {
		t.Fatalf("re-apply = created %v, version %q; want no write and %q", again.Msg.GetCreated(), again.Msg.GetOrkflow().GetVersion(), repaired)
	}
	if after := objectETags(t, ctx, raw, cfg.S3Bucket, name); fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("re-apply changed the bucket:\n%v\n%v", before, after)
	}
}
