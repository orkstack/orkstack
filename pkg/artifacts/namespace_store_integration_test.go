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
