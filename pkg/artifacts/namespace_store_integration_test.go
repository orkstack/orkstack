//go:build integration

package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

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
	nsName := "m101-ns-" + uniqueToken(t)
	t.Cleanup(func() {
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, artifacts.NamespacePrefix+"/"+nsName+".json")
		deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, artifacts.OrkflowPrefix+"/"+nsName+"/")
	})

	if err := store.Put(ctx, &orkstackv1.Namespace{
		Name:         nsName,
		Description:  "M10.1 namespace registry",
		DefaultPool:  "main",
		AllowedPools: []string{"main", "gpu"},
	}); err != nil {
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
	if got.GetName() != nsName || got.GetDescription() != "M10.1 namespace registry" {
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

	orkflow := testOrkflow(t, "owned", "1.0.0", time.Now().UTC())
	orkflow.Namespace = nsName
	orkflowKey := fmt.Sprintf("%s/%s/owned/1.0.0.json", artifacts.OrkflowPrefix, nsName)
	orkflowStore, err := artifacts.NewS3OrkflowStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore: %v", err)
	}
	if err := orkflowStore.Put(ctx, orkflow); err != nil {
		t.Fatalf("Put owned orkflow: %v", err)
	}
	if err := store.Delete(ctx, nsName); !errors.Is(err, artifacts.ErrNamespaceNotEmpty) || !strings.Contains(err.Error(), "1") {
		t.Fatalf("Delete(non-empty) = %v, want ErrNamespaceNotEmpty with a count", err)
	}
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(orkflowKey),
	}); err != nil {
		t.Fatalf("delete owned orkflow: %v", err)
	}
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
