//go:build integration

package artifacts_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/testsupport/commitcontract"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestCommitStoreContract_OnTheRealBackend runs the same contract the unit
// test runs over the local store, the memory store and the S3 double, against
// the deployment's real S3-compatible store: one body of assertions, so the
// backend the Orker actually commits to cannot drift from the ones tested
// without a bucket.
func TestCommitStoreContract_OnTheRealBackend(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	store, err := artifacts.NewS3CommitStore(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	root, err := artifacts.CommitRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	key, err := artifacts.CommitKey("default", "ork-contract-"+uniqueToken(t), "contract")
	if err != nil {
		t.Fatal(err)
	}
	commitcontract.Run(t, store, strings.TrimSuffix(root, "/")+"/"+key)
}

// TestS3CommitStore_WireContractOnTheRealBackend pins what the real store
// answers for the object the contract left: the documented content type, an
// ETag, and the bytes MarshalCommit produced — and, after a refused second
// writer, the same bytes.
func TestS3CommitStore_WireContractOnTheRealBackend(t *testing.T) {
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)
	store, err := artifacts.NewS3CommitStore(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	key, err := artifacts.CommitKey("default", "ork-wire-"+uniqueToken(t), "build")
	if err != nil {
		t.Fatal(err)
	}
	root, err := artifacts.CommitRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	uri := strings.TrimSuffix(root, "/") + "/" + key
	winner := commitcontract.Commit("run-wire", "build")
	if _, err := store.Create(context.Background(), uri, winner); err != nil {
		t.Fatalf("Create: %v", err)
	}
	raw := newRawS3Client(t, cfg)
	head, err := raw.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got := aws.ToString(head.ContentType); got != artifacts.CommitContentType {
		t.Fatalf("Content-Type = %q, want %q", got, artifacts.CommitContentType)
	}
	if aws.ToString(head.ETag) == "" {
		t.Fatal("the stored commit has no ETag")
	}
	if _, err := store.Create(context.Background(), uri, commitcontract.Commit("run-other", "build")); err == nil {
		t.Fatal("a second Create succeeded")
	}
	got, err := store.Get(context.Background(), uri)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want, err := artifacts.MarshalCommit(winner)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Raw) != string(want) {
		t.Fatal("the refused writer replaced the stored bytes")
	}
}
