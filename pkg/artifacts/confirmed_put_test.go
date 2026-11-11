package artifacts

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// lostAnswers are the two ways a conditional write's answer is lost once the
// write landed (s3Double.loseAnswer): the SDK resends it, and the resend
// either meets the stored write — If-None-Match finds the object it created,
// If-Match an etag it moved: a 412 — or fails until the SDK gives up. Either
// way the write landed, and the store must say so: never a refusal, never a
// failure, never a second application of what it already applied.
var lostAnswers = []struct {
	name           string
	failingResends int
}{
	{"the resend meets the stored write", 0},
	{"the SDK gives up after the write landed", 2},
}

// failReadsAfterPut makes the read-backs of key fail: once the first PUT of it
// arrives, the next n GETs of it answer 500.
func failReadsAfterPut(d *s3Double, key string, n int) {
	var armed atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, key) && armed.CompareAndSwap(false, true) {
			d.mu.Lock()
			d.failGets[key] = n
			d.mu.Unlock()
		}
	}
}

// requireResent fails a test whose double never saw the SDK resend: without
// a resend the case proves nothing.
func requireResent(t *testing.T, d *s3Double) {
	t.Helper()
	if puts, _, _ := d.counts(); puts < 2 {
		t.Fatalf("%d PUTs: the SDK never resent the write, so nothing here was tested", puts)
	}
}

// The shared blob layer (s3Blobs.create and replace) is every conditional
// write of the audit, delivery, delivery-body, cleanup, snapshot, trigger and
// webhook stores: a create whose answer was lost is not "already there", and
// a replace whose answer was lost is not "changed under you" — which a trigger
// or webhook update answers by applying its change a second time.
func TestConfirmedPut_Blobs(t *testing.T) {
	ctx := context.Background()
	for _, lost := range lostAnswers {
		t.Run("create: "+lost.name, func(t *testing.T) {
			d := newS3Double(t)
			b := &s3Blobs{client: d.fastClient(t), bucket: d.bucket}
			d.loseAnswer("records/one.json", lost.failingResends)
			if err := b.create(ctx, "records/one.json", "application/json", []byte(`{"mine":true}`)); err != nil {
				t.Fatalf("create = %v; the write landed", err)
			}
			requireResent(t, d)
		})
		t.Run("replace: "+lost.name, func(t *testing.T) {
			d := newS3Double(t)
			b := &s3Blobs{client: d.fastClient(t), bucket: d.bucket}
			d.objects["records/one.json"] = []byte(`{"v":1}`)
			version := doubleETag([]byte(`{"v":1}`))
			d.loseAnswer("records/one.json", lost.failingResends)
			if err := b.replace(ctx, "records/one.json", "application/json", []byte(`{"v":2}`), version); err != nil {
				t.Fatalf("replace = %v; the write landed", err)
			}
			requireResent(t, d)
		})
	}
	t.Run("create refused", func(t *testing.T) {
		d := newS3Double(t)
		b := &s3Blobs{client: d.fastClient(t), bucket: d.bucket}
		d.objects["records/one.json"] = []byte(`{"theirs":true}`)
		if err := b.create(ctx, "records/one.json", "application/json", []byte(`{"mine":true}`)); !errors.Is(err, errBlobExists) {
			t.Fatalf("create over another write = %v, want errBlobExists", err)
		}
	})
	t.Run("replace refused", func(t *testing.T) {
		d := newS3Double(t)
		b := &s3Blobs{client: d.fastClient(t), bucket: d.bucket}
		d.objects["records/one.json"] = []byte(`{"v":1}`)
		stale := doubleETag([]byte(`{"v":0}`))
		if err := b.replace(ctx, "records/one.json", "application/json", []byte(`{"v":2}`), stale); !errors.Is(err, errBlobChanged) {
			t.Fatalf("replace of a moved object = %v, want errBlobChanged", err)
		}
		if got, _ := d.object("records/one.json"); string(got) != `{"v":1}` {
			t.Fatalf("a refused replace left %s", got)
		}
	})
}

// A commit whose answer was lost is the commit this attempt made: Create
// reports it created, so the result is this attempt's and its memo is
// written, and a commit the SDK gave up on is not COMMIT_WRITE_FAILED with
// commit.json in the store (§4.1, §15). Another attempt's commit is still
// ErrPreconditionFailed, which the Orker adopts.
func TestConfirmedPut_Commit(t *testing.T) {
	ctx := context.Background()
	key := CommitPrefix + "/default/ork-1/build/" + CommitObjectName
	uri := func(d *s3Double) string { return "s3://" + d.bucket + "/" + key }
	for _, lost := range lostAnswers {
		t.Run(lost.name, func(t *testing.T) {
			d := newS3Double(t)
			store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
			d.loseAnswer(key, lost.failingResends)
			record, err := store.Create(ctx, uri(d), sampleCommit())
			if err != nil {
				t.Fatalf("Create = %v; the commit landed", err)
			}
			if stored, _ := d.object(key); record.ETag != doubleETag(stored) {
				t.Fatalf("Create's etag %s, want the stored commit's %s", record.ETag, doubleETag(stored))
			}
			if !record.confirmedByRead {
				t.Fatal("a Create a read confirmed is not marked confirmedByRead")
			}
			requireResent(t, d)
		})
	}
	t.Run("refused", func(t *testing.T) {
		d := newS3Double(t)
		store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
		winner := sampleCommit()
		winner.Attempt = 2
		document, err := MarshalCommit(winner)
		if err != nil {
			t.Fatalf("MarshalCommit: %v", err)
		}
		d.objects[key] = document
		if _, err := store.Create(ctx, uri(d), sampleCommit()); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("Create over another attempt's commit = %v, want ErrPreconditionFailed", err)
		}
	})
}

// The namespace, image policy and retention policy stores each write one
// document under a PutConditions: a write whose answer was lost is the write
// the caller made, never "somebody else edited it", and a stale condition is
// still refused.
func TestConfirmedPut_Documents(t *testing.T) {
	ctx := context.Background()
	namespaceKey, err := NamespaceKey("payments")
	if err != nil {
		t.Fatalf("NamespaceKey: %v", err)
	}
	for _, doc := range []struct {
		name string
		key  string
		// put writes the document and returns the etag it reports, "" when
		// the store reports none.
		put func(t *testing.T, d *s3Double, cond PutConditions) (string, error)
	}{
		{"namespace", namespaceKey, func(t *testing.T, d *s3Double, cond PutConditions) (string, error) {
			store := &S3NamespaceStore{client: d.fastClient(t), bucket: d.bucket, now: time.Now}
			ns, err := store.Put(ctx, &orkstackv1.Namespace{Name: "payments"}, cond)
			return ns.GetEtag(), err
		}},
		{"image policy", ImagePolicyKey, func(t *testing.T, d *s3Double, cond PutConditions) (string, error) {
			store := &S3ImagePolicyStore{client: d.fastClient(t), bucket: d.bucket, now: time.Now}
			_, err := store.Put(ctx, &orkstackv1.ImagePolicy{RequireDigest: true}, cond)
			return "", err
		}},
		{"retention policy", RetentionPolicyKey, func(t *testing.T, d *s3Double, cond PutConditions) (string, error) {
			store := &S3RetentionPolicyStore{client: d.fastClient(t), bucket: d.bucket, now: time.Now}
			_, err := store.Put(ctx, &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(24 * time.Hour)}, cond)
			return "", err
		}},
	} {
		for _, lost := range lostAnswers {
			t.Run(doc.name+", created: "+lost.name, func(t *testing.T) {
				d := newS3Double(t)
				d.loseAnswer(doc.key, lost.failingResends)
				etag, err := doc.put(t, d, PutConditions{IfAbsent: true})
				if err != nil {
					t.Fatalf("Put(IfAbsent) = %v; the write landed", err)
				}
				if stored, _ := d.object(doc.key); etag != "" && etag != doubleETag(stored) {
					t.Fatalf("Put's etag %s, want the stored document's %s", etag, doubleETag(stored))
				}
				requireResent(t, d)
			})
			t.Run(doc.name+", replaced: "+lost.name, func(t *testing.T) {
				d := newS3Double(t)
				if _, err := doc.put(t, d, PutConditions{IfAbsent: true}); err != nil {
					t.Fatalf("the first Put: %v", err)
				}
				first, _ := d.object(doc.key)
				d.resetCounts()
				d.loseAnswer(doc.key, lost.failingResends)
				if _, err := doc.put(t, d, PutConditions{IfMatch: doubleETag(first)}); err != nil {
					t.Fatalf("Put(IfMatch) = %v; the write landed", err)
				}
				requireResent(t, d)
			})
		}
		t.Run(doc.name+", refused", func(t *testing.T) {
			d := newS3Double(t)
			if _, err := doc.put(t, d, PutConditions{IfAbsent: true}); err != nil {
				t.Fatalf("the first Put: %v", err)
			}
			first, _ := d.object(doc.key)
			if _, err := doc.put(t, d, PutConditions{IfAbsent: true}); !errors.Is(err, ErrPreconditionFailed) {
				t.Fatalf("Put(IfAbsent) over a stored document = %v, want ErrPreconditionFailed", err)
			}
			if _, err := doc.put(t, d, PutConditions{IfMatch: doubleETag([]byte("stale"))}); !errors.Is(err, ErrPreconditionFailed) {
				t.Fatalf("Put(IfMatch) with a stale etag = %v, want ErrPreconditionFailed", err)
			}
			if got, _ := d.object(doc.key); string(got) != string(first) {
				t.Fatal("a refused Put changed the stored document")
			}
		})
	}
}

// orkflowFixture is an S3 double with a store against it and one orkflow
// version, 1.0.0, applied: stored, and the head naming it.
func orkflowFixture(t *testing.T) (*s3Double, *S3OrkflowStore) {
	t.Helper()
	d := newS3Double(t)
	store := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	if _, created, err := store.Put(context.Background(), applyOf("1.0.0", 1), NoHeadExpectation()); err != nil || !created {
		t.Fatalf("Put(1.0.0) = created %v, %v", created, err)
	}
	return d, store
}

// applyOf is one apply of version of the orkflow deploy, stamped at second.
func applyOf(version string, second int64) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:       "deploy",
		Version:    version,
		SourceYaml: "apiVersion: orkstack/v1\nkind: Orkflow\nname: deploy\nversion: \"" + version + "\"\n",
		CreatedAt:  timestamppb.New(time.Unix(1_700_000_000+second, 0)),
	}
}

func headVersion(t *testing.T, store *S3OrkflowStore) string {
	t.Helper()
	pointer, _, err := store.readHead(context.Background(), DefaultNamespace, "deploy")
	if err != nil {
		t.Fatalf("readHead: %v", err)
	}
	return pointer.Version
}

// An apply's two conditional writes — the version object, created only if
// absent, and the head, swapped under the etag the apply read — must not
// mistake their own landed write for another apply's. A version create that
// did ("already stored, identical") reported the apply a repeat and never
// moved the head onto it; a head swap that did reported the head moved by
// somebody else, and Put then deleted the version the head now named.
func TestConfirmedPut_OrkflowApply(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	for _, lost := range lostAnswers {
		for _, tc := range []struct {
			write    string
			key      string
			expected HeadExpectation
		}{
			{"the version create", versionKey, NoHeadExpectation()},
			{"the version create, head expected", versionKey, ExpectHead("1.0.0")},
			{"the head swap", headKey, NoHeadExpectation()},
			{"the head swap, head expected", headKey, ExpectHead("1.0.0")},
		} {
			t.Run(tc.write+": "+lost.name, func(t *testing.T) {
				d, store := orkflowFixture(t)
				d.resetCounts()
				d.loseAnswer(tc.key, lost.failingResends)
				_, created, err := store.Put(ctx, applyOf("2.0.0", 2), tc.expected)
				if err != nil || !created {
					t.Fatalf("Put(2.0.0) = created %v, %v; the apply landed", created, err)
				}
				if _, ok := d.object(versionKey); !ok {
					t.Fatal("version 2.0.0 is not stored")
				}
				if head := headVersion(t, store); head != "2.0.0" {
					t.Fatalf("the head names %q, want 2.0.0", head)
				}
				requireResent(t, d)
			})
		}
	}
	t.Run("the head swap refused", func(t *testing.T) {
		d, store := orkflowFixture(t)
		d.onRequest = func(r *http.Request) {
			// Another apply's head swap lands just before this one's.
			if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, headKey) {
				return
			}
			document, err := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_003, 0)})
			if err != nil {
				t.Errorf("marshalHeadPointer: %v", err)
				return
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			d.objects[headKey] = document
		}
		if _, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0")); !errors.Is(err, ErrHeadMoved) {
			t.Fatalf("Put(2.0.0) after another apply moved the head = %v, want ErrHeadMoved", err)
		}
		if _, ok := d.object(versionKey); ok {
			t.Fatal("a refused apply left version 2.0.0 stored")
		}
	})
}

// A derived claim whose answer was lost names this caller's candidate: the
// claim is the answer, whoever wrote it.
func TestConfirmedPut_DerivedClaim(t *testing.T) {
	ctx := context.Background()
	hash := strings.Repeat("a", 64)
	key, err := OrkflowDerivedKey(DefaultNamespace, "deploy", hash)
	if err != nil {
		t.Fatalf("OrkflowDerivedKey: %v", err)
	}
	for _, lost := range lostAnswers {
		t.Run(lost.name, func(t *testing.T) {
			d := newS3Double(t)
			store := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
			d.loseAnswer(key, lost.failingResends)
			version, err := store.ClaimDerivedVersion(ctx, DefaultNamespace, "deploy", hash, "candidate-1")
			if err != nil || version != "candidate-1" {
				t.Fatalf("ClaimDerivedVersion = %q, %v; the claim landed", version, err)
			}
			requireResent(t, d)
		})
	}
}

// Equal bytes are not this write when the store refused the write's only
// send: the store answered this request, and it was another writer's bytes
// that were there. A re-create of bytes already stored — another attempt's
// identical commit, the boot probe's fixed candidate, a document stamped by a
// still clock — is refused as before, and the boot probe still sees exactly
// one winner of two identical writes.
func TestConfirmedPut_EqualBytesRefusedOnTheOnlySendAreAnotherWrite(t *testing.T) {
	ctx := context.Background()
	t.Run("commit re-create", func(t *testing.T) {
		d := newS3Double(t)
		store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
		uri := "s3://" + d.bucket + "/" + CommitPrefix + "/default/ork-1/build/" + CommitObjectName
		if _, err := store.Create(ctx, uri, sampleCommit()); err != nil {
			t.Fatalf("the first Create: %v", err)
		}
		if _, err := store.Create(ctx, uri, sampleCommit()); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("a re-Create of the same bytes = %v, want ErrPreconditionFailed", err)
		}
	})
	t.Run("commit probe", func(t *testing.T) {
		d := newS3Double(t)
		store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
		if err := ProbeCommitStore(ctx, store); err != nil {
			t.Fatalf("the boot probe of a new store: %v", err)
		}
		if err := ProbeCommitStore(ctx, store); err != nil {
			t.Fatalf("the boot probe against its durable winner: %v", err)
		}
	})
	t.Run("blob create", func(t *testing.T) {
		d := newS3Double(t)
		b := &s3Blobs{client: d.fastClient(t), bucket: d.bucket}
		if err := b.create(ctx, "records/one.json", "application/json", []byte(`{"same":true}`)); err != nil {
			t.Fatalf("the first create: %v", err)
		}
		if err := b.create(ctx, "records/one.json", "application/json", []byte(`{"same":true}`)); !errors.Is(err, errBlobExists) {
			t.Fatalf("a create of the same bytes = %v, want errBlobExists", err)
		}
	})
	t.Run("namespace under a still clock", func(t *testing.T) {
		d := newS3Double(t)
		still := time.Unix(1_700_000_000, 0)
		store := &S3NamespaceStore{client: d.fastClient(t), bucket: d.bucket, now: func() time.Time { return still }}
		if _, err := store.Put(ctx, &orkstackv1.Namespace{Name: "payments"}, PutConditions{IfAbsent: true}); err != nil {
			t.Fatalf("the first Put: %v", err)
		}
		if _, err := store.Put(ctx, &orkstackv1.Namespace{Name: "payments"}, PutConditions{IfAbsent: true}); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("a second Put(IfAbsent) of the same bytes = %v, want ErrPreconditionFailed", err)
		}
	})
}

// The boot probe's two writers send one fixed candidate. On a new key, one
// writer's answer is lost and a read finds the candidate there, and the other
// writer's only send is refused: that refusal proves the store arbitrated,
// whoever won. The probe passes without claiming that the first writer won.
func TestConfirmedPut_ACommitProbeWhoseAnswerWasLostIsProvenByTheOtherRefusal(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
	d.loseAnswer(CommitProbeKey, 0)
	if err := ProbeCommitStore(ctx, store); err != nil {
		t.Fatalf("a probe whose one writer lost its answer and whose other was refused = %v, want it proven", err)
	}
	requireResent(t, d)
	if err := ProbeCommitStore(ctx, store); err != nil {
		t.Fatalf("the next probe, against the durable winner: %v", err)
	}
}

// An apply whose version create lost its answer cannot tell its own write from
// an identical apply's — two applies of one document behind the same head are
// stamped alike — so when its head swap then loses, the version stays:
// removing it could leave the other apply's head naming nothing. The apply is
// still refused, and says why the version stays.
func TestConfirmedPut_AnApplyRemovesOnlyAVersionItKnowsItWrote(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	d.loseAnswer(versionKey, 0)
	d.onRequest = func(r *http.Request) {
		// Another apply of the same document makes 2.0.0 the head just before
		// this apply's swap.
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, headKey) {
			return
		}
		document, err := marshalHeadPointer(headPointer{Version: "2.0.0", AppliedAt: time.Unix(1_700_000_005, 0)})
		if err != nil {
			t.Errorf("marshalHeadPointer: %v", err)
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		d.objects[headKey] = document
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	if !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "left the version it wrote") {
		t.Fatalf("Put(2.0.0) = %v, want the head moved, and the version kept", err)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the apply removed version 2.0.0, which the head names")
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q, want 2.0.0", head)
	}
}

// A version create whose answer was lost, followed by a head swap that lost
// every attempt, leaves the version stored ahead of the head: Put keeps it
// (it cannot tell its write from an identical apply's) and refuses. Applying
// the same document again then completes that commit — the head moves onto
// the version — though it stores no version and reports created=false. A
// re-apply of a version the head is past stays the no-op it is.
func TestConfirmedPut_ARetriedApplyCompletesAStrandedCreate(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	d.loseAnswer(versionKey, 0)
	rewrites := 0
	d.onRequest = func(r *http.Request) {
		// Another writer re-writes the head, still naming 1.0.0, just before
		// each of this apply's swaps.
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, headKey) {
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		rewrites++
		document, err := marshalHeadPointer(headPointer{Version: "1.0.0", AppliedAt: time.Unix(1_700_000_001, int64(rewrites))})
		if err != nil {
			t.Errorf("marshalHeadPointer: %v", err)
			return
		}
		d.objects[headKey] = document
	}
	if _, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation()); !errors.Is(err, ErrPreconditionFailed) ||
		!strings.Contains(err.Error(), "left the version it wrote") {
		t.Fatalf("Put(2.0.0) = %v, want the head lost on every attempt and the version kept", err)
	}
	if head := headVersion(t, store); head != "1.0.0" {
		t.Fatalf("the head names %q after the refused apply, want 1.0.0", head)
	}
	d.mu.Lock()
	d.onRequest = nil
	d.mu.Unlock()

	_, created, err := store.Put(ctx, applyOf("2.0.0", 10), NoHeadExpectation())
	if err != nil || created {
		t.Fatalf("the retried apply = created %v, %v; want the stored version, not created", created, err)
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q after the retried apply, want 2.0.0: the stranded create was never completed", head)
	}

	// A re-apply of 1.0.0, which the head is past, moves nothing.
	if _, created, err := store.Put(ctx, applyOf("1.0.0", 20), NoHeadExpectation()); err != nil || created {
		t.Fatalf("re-applying 1.0.0 = created %v, %v; want the no-op repeat", created, err)
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("re-applying 1.0.0 moved the head to %q", head)
	}
}

// On a later boot the probe key already holds the candidate, so neither
// writer can create it: a writer whose first send failed and whose resend met
// the durable winner was refused, and the probe passes — it never claims that
// writer won, and never fails a store it proved.
func TestConfirmedPut_AProbeOverItsDurableWinnerCountsALostAnswerAsARefusal(t *testing.T) {
	ctx := context.Background()
	d := newS3Double(t)
	store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
	if err := ProbeCommitStore(ctx, store); err != nil {
		t.Fatalf("the first probe: %v", err)
	}
	d.resetCounts()
	d.mu.Lock()
	d.failPuts[CommitProbeKey] = 1 // the first send of the next probe fails, unstored
	d.mu.Unlock()
	if err := ProbeCommitStore(ctx, store); err != nil {
		t.Fatalf("a probe over its durable winner, one send of which failed = %v, want it to pass", err)
	}
	requireResent(t, d)
}

// Only a 4xx is the store's refusal of a write it did not carry out. A 5xx,
// no response, or a 2xx or 3xx the client could not read leaves the outcome
// unknown, and putConfirmed reads the key.
func TestAnsweredByStore(t *testing.T) {
	response := func(status int) error {
		return fmt.Errorf("operation error S3: PutObject: %w", &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      errors.New("the answer"),
		})
	}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{response(http.StatusPreconditionFailed), true},
		{response(http.StatusConflict), true},
		{response(http.StatusNotFound), true},
		{response(http.StatusForbidden), true},
		{response(http.StatusOK), false}, // a 200 whose body or headers did not read
		{response(http.StatusMovedPermanently), false},
		{response(http.StatusInternalServerError), false},
		{response(http.StatusServiceUnavailable), false},
		{errors.New("connection reset by peer"), false},
	} {
		if got := answeredByStore(tc.err); got != tc.want {
			t.Errorf("answeredByStore(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// A first send that fails unstored, another writer's different bytes landing
// before the resend, and a resend the store refuses: the read finds the other
// writer's bytes, so the write is refused — never reported landed.
func TestConfirmedPut_AResendRefusedOverAnotherWritersBytesIsARefusal(t *testing.T) {
	ctx := context.Background()
	theirs := func(t *testing.T, d *s3Double, key string, other []byte) {
		t.Helper()
		sends := 0
		d.failPuts[key] = 1
		d.onRequest = func(r *http.Request) {
			if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, key) {
				return
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if sends++; sends == 2 { // the resend: another writer got there first
				d.objects[key] = other
			}
		}
	}
	t.Run("blob create", func(t *testing.T) {
		d := newS3Double(t)
		b := &s3Blobs{client: d.fastClient(t), bucket: d.bucket}
		theirs(t, d, "records/one.json", []byte(`{"theirs":true}`))
		if err := b.create(ctx, "records/one.json", "application/json", []byte(`{"mine":true}`)); !errors.Is(err, errBlobExists) {
			t.Fatalf("create = %v, want errBlobExists", err)
		}
		requireResent(t, d)
	})
	t.Run("commit", func(t *testing.T) {
		d := newS3Double(t)
		store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
		key := CommitPrefix + "/default/ork-1/build/" + CommitObjectName
		winner := sampleCommit()
		winner.Attempt = 2
		document, err := MarshalCommit(winner)
		if err != nil {
			t.Fatalf("MarshalCommit: %v", err)
		}
		theirs(t, d, key, document)
		if _, err := store.Create(ctx, "s3://"+d.bucket+"/"+key, sampleCommit()); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("Create = %v, want ErrPreconditionFailed", err)
		}
		requireResent(t, d)
	})
}

// v1Retryer is a retryer written before aws.RetryerV2: embedding the
// interface hides every method but aws.Retryer's.
type v1Retryer struct{ aws.Retryer }

// The resend the SDK makes is noted whatever retryer the client was given: one
// that predates aws.RetryerV2, and one the client wraps for RetryMaxAttempts.
func TestConfirmedPut_ResendsAreNotedWhateverTheRetryer(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		opt  func(*s3.Options)
	}{
		{"a retryer that is not a RetryerV2", func(o *s3.Options) { o.Retryer = v1Retryer{o.Retryer} }},
		{"a retryer wrapped for RetryMaxAttempts", func(o *s3.Options) { o.RetryMaxAttempts = 3 }},
	} {
		for _, lost := range lostAnswers {
			t.Run(tc.name+": "+lost.name, func(t *testing.T) {
				d := newS3Double(t)
				b := &s3Blobs{client: d.fastClient(t, tc.opt), bucket: d.bucket}
				if _, isV2 := b.client.Options().Retryer.(aws.RetryerV2); tc.name == "a retryer that is not a RetryerV2" && isV2 {
					t.Fatal("the client's retryer is a RetryerV2, so nothing here was tested")
				}
				d.loseAnswer("records/one.json", lost.failingResends)
				if err := b.create(ctx, "records/one.json", "application/json", []byte(`{"mine":true}`)); err != nil {
					t.Fatalf("create = %v; the write landed", err)
				}
				requireResent(t, d)
			})
		}
	}
}

// Two applies of one document — a client's retry while its first request is
// still running on another replica — both read head 1.0.0. The first creates
// the version cleanly; the second finds it stored ahead of the head and moves
// the head onto it before the first's swap. The first's swap is then refused,
// but the head names exactly this version at this apply's stamp: its commit
// landed, and it must not remove the version the head names.
func TestConfirmedPut_AnIdenticalApplyThatMovesTheHeadFirstIsTheCreatorsCommit(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var raced atomic.Bool
	var raceCreated bool
	var raceErr error
	d.onRequest = func(r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, headKey) || !raced.CompareAndSwap(false, true) {
			return
		}
		// The second apply runs to the end between the first's create and
		// the first's swap.
		_, raceCreated, raceErr = other.Put(ctx, applyOf("2.0.0", 3), NoHeadExpectation())
	}
	_, created, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	if !raced.Load() {
		t.Fatal("the second apply never ran, so nothing here was tested")
	}
	if raceErr != nil || raceCreated {
		t.Fatalf("the second apply = created %v, %v; want the stored version, not created", raceCreated, raceErr)
	}
	if err != nil || !created {
		t.Fatalf("the first apply = created %v, %v; want created: the head names its version at its stamp", created, err)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("version 2.0.0 was removed while the head names it")
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q, want 2.0.0", head)
	}
}

// A clean create, then a head swap whose answer is lost and whose read-back
// fails too: whether the swap landed is unknown — it did here — so the apply
// must not remove the version, which the head may name.
func TestConfirmedPut_AnApplyWhoseSwapOutcomeIsUnknownKeepsTheVersion(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	d.loseAnswer(headKey, 2)         // stored, then every resend fails: the SDK gives up
	failReadsAfterPut(d, headKey, 3) // and every attempt of the read-back fails
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	if err == nil || !strings.Contains(err.Error(), "left the version it wrote") {
		t.Fatalf("Put(2.0.0) = %v, want an unknown outcome that keeps the version", err)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("version 2.0.0 was removed, and the head names it")
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q, want 2.0.0 (the swap landed)", head)
	}
}

// A promotion whose answer was lost and whose read-back failed does not know
// whether it landed: it reports exactly that, not a pointer somebody else
// moved — which a later read of its own write would otherwise claim.
func TestSetCurrentVersion_ALostAnswerWhoseReadBackFailsIsAnUnknownOutcome(t *testing.T) {
	d, store, pointerKey := casFixture(t)
	d.loseAnswer(pointerKey, 0)
	failReadsAfterPut(d, pointerKey, 3)
	_, err := store.SetCurrentVersion(context.Background(), DefaultNamespace, "deploy", "2.0.0", ExpectCurrent("1.0.0"))
	if !errors.Is(err, errOutcomeUnknown) || errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("SetCurrentVersion = %v, want an unknown outcome, never a refusal", err)
	}
}

// A create every send of which failed unstored, and whose read finds nothing
// there, did not land: its error stands, neither a refusal nor unknown.
func TestConfirmedPut_ACreateThatNeverLandedKeepsItsError(t *testing.T) {
	d := newS3Double(t)
	b := &s3Blobs{client: d.fastClient(t), bucket: d.bucket}
	d.failPuts["records/one.json"] = 3
	err := b.create(context.Background(), "records/one.json", "application/json", []byte(`{"mine":true}`))
	if err == nil || errors.Is(err, errBlobExists) || errors.Is(err, errOutcomeUnknown) {
		t.Fatalf("create = %v, want its own 500", err)
	}
}

// scriptedProbeStore answers the boot probe's two Creates as scripted, in
// arrival order, over a key that is new before the race.
type scriptedProbeStore struct {
	mu      sync.Mutex
	answers []func() (CommitRecord, error)
	creates int
}

func (s *scriptedProbeStore) commitProbeURI() string { return "probe" }

func (s *scriptedProbeStore) Create(context.Context, string, *orkstackv1.ActivityCommit) (CommitRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	answer := s.answers[s.creates]
	s.creates++
	return answer()
}

func (s *scriptedProbeStore) Get(context.Context, string) (CommitRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.creates == 0 {
		return CommitRecord{}, ErrNotFound
	}
	return CommitRecord{Commit: commitProbeCandidate()}, nil
}

// The probe's verdict from two writers on a new key: a refusal of one
// writer's only send proves arbitration whether the other won cleanly or only
// a read says so; a writer known only by a read beside a clean winner, or two
// such writers, prove nothing, and the probe says it could not tell.
func TestProbeCommitStore_ArbitrationFromWhatEachWriterKnows(t *testing.T) {
	won := func() (CommitRecord, error) { return CommitRecord{}, nil }
	read := func() (CommitRecord, error) { return CommitRecord{confirmedByRead: true}, nil }
	refused := func() (CommitRecord, error) { return CommitRecord{}, ErrPreconditionFailed }
	for _, tc := range []struct {
		name    string
		answers []func() (CommitRecord, error)
		proven  bool
	}{
		{"a winner and a refusal", []func() (CommitRecord, error){won, refused}, true},
		{"a read-confirmed writer and a refusal", []func() (CommitRecord, error){read, refused}, true},
		{"a winner and a read-confirmed writer", []func() (CommitRecord, error){won, read}, false},
		{"two read-confirmed writers", []func() (CommitRecord, error){read, read}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ProbeCommitStore(context.Background(), &scriptedProbeStore{answers: tc.answers})
			switch {
			case tc.proven && err != nil:
				t.Fatalf("ProbeCommitStore = %v, want proven", err)
			case !tc.proven && !errors.Is(err, ErrCommitStoreUnavailable):
				t.Fatalf("ProbeCommitStore = %v, want an unknown outcome (ErrCommitStoreUnavailable)", err)
			}
		})
	}
}

// Put never removes a version whose head it cannot read: another apply moves
// the head to 3.0.0 before this apply's swap, so the swap is refused, and the
// read of the head just before the rollback fails. The head may name the
// version, so it stays.
func TestConfirmedPut_AnApplyNeverRemovesAVersionWhoseHeadItCannotRead(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	var headGets atomic.Int32
	d.onRequest = func(r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, headKey) {
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		switch {
		case r.Method == http.MethodPut:
			// Another apply moves the head to 3.0.0 just before this swap.
			document, err := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_005, 0)})
			if err != nil {
				t.Errorf("marshalHeadPointer: %v", err)
				return
			}
			d.objects[headKey] = document
		case r.Method == http.MethodGet && headGets.Add(1) == 3:
			// Reads: the apply's first, the refused swap's, then the one
			// before the rollback, which fails.
			d.failGets[headKey] = 3
		}
	}
	// LOW-G: an unreadable pointer is said to be one, with its error, and
	// never as a pointer naming the version.
	if _, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0")); !errors.Is(err, ErrHeadMoved) ||
		!strings.Contains(err.Error(), "the version stays stored") || !strings.Contains(err.Error(), "its pointers could not be read") ||
		!strings.Contains(err.Error(), "InternalError") || strings.Contains(err.Error(), "pointer names it") {
		t.Fatalf("Put(2.0.0) = %v, want the head moved and the version kept, as its pointers could not be read", err)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the apply removed version 2.0.0 without reading the head")
	}
}

// An apply's swap is refused because another apply moved the head to 3.0.0,
// stamped before this version, so the version is still ahead of the head. The
// rollback removes it — a refused apply leaves nothing stored — but an
// identical apply completes it between the rollback's read of the head and its
// removal. The rollback reads the head again, finds it naming the version, and
// puts the version back: the head never stays on a version that is not stored.
func TestConfirmedPut_ARollbackPutsBackAVersionAnIdenticalApplyMadeTheHead(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var moved, completed atomic.Bool
	var otherErr error
	d.onRequest = func(r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && moved.CompareAndSwap(false, true):
			// Another apply moves the head to 3.0.0, stamped before 2.0.0,
			// just before this apply's swap.
			document, err := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_001, 500_000_000)})
			if err != nil {
				t.Errorf("marshalHeadPointer: %v", err)
				return
			}
			d.mu.Lock()
			d.objects[headKey] = document
			d.mu.Unlock()
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, versionKey) && completed.CompareAndSwap(false, true):
			// The identical apply completes 2.0.0 between the rollback's
			// read of the head and its removal.
			_, _, otherErr = other.Put(ctx, applyOf("2.0.0", 3), NoHeadExpectation())
		}
	}
	if _, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0")); !errors.Is(err, ErrHeadMoved) {
		t.Fatalf("Put(2.0.0) = %v, want the head moved", err)
	}
	if completed.CompareAndSwap(false, true) {
		// No removal was attempted; the identical apply completes it now.
		_, _, otherErr = other.Put(ctx, applyOf("2.0.0", 3), NoHeadExpectation())
	}
	if otherErr != nil {
		t.Fatalf("the identical apply: %v", otherErr)
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q, want 2.0.0", head)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("version 2.0.0 was removed, and the head names it")
	}
}

// A refused apply leaves nothing stored even when its version is stamped ahead
// of the head that beat it, and the same document retargeted at the new head
// creates it afresh and makes it the head: the contract
// TestS3OrkflowStore_AnApplyRefusedInsideTheWriteLeavesNothingStored holds
// against the real store.
func TestConfirmedPut_ARefusedApplyLeavesNothingStoredEvenAheadOfTheHead(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	var moved atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, headKey) || !moved.CompareAndSwap(false, true) {
			return
		}
		// Another apply moves the head to 3.0.0, stamped before 2.0.0.
		document, err := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_001, 500_000_000)})
		if err != nil {
			t.Errorf("marshalHeadPointer: %v", err)
			return
		}
		d.mu.Lock()
		d.objects[headKey] = document
		d.mu.Unlock()
	}
	if _, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0")); !errors.Is(err, ErrHeadMoved) {
		t.Fatalf("Put(2.0.0) = %v, want the head moved", err)
	}
	if _, ok := d.object(versionKey); ok {
		t.Fatal("the refused apply left version 2.0.0 stored")
	}
	if _, created, err := store.Put(ctx, applyOf("2.0.0", 4), ExpectHead("3.0.0")); err != nil || !created {
		t.Fatalf("the retargeted apply = created %v, %v; want created", created, err)
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q after the retargeted apply, want 2.0.0", head)
	}
}

// A rollback whose read of the head after the removal fails does not guess:
// putting the version back could undo a Delete of the whole orkflow that ran
// meanwhile, so the version stays removed and the caller is told the head
// could not be read.
func TestConfirmedPut_ARollbackDoesNotGuessAHeadItCannotRead(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	var headGets atomic.Int32
	d.onRequest = func(r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, headKey) {
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		switch {
		case r.Method == http.MethodPut:
			// Another apply moves the head to 3.0.0, past 2.0.0.
			document, err := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_005, 0)})
			if err != nil {
				t.Errorf("marshalHeadPointer: %v", err)
				return
			}
			d.objects[headKey] = document
		case r.Method == http.MethodGet && headGets.Add(1) == 4:
			// Reads: the apply's first, the refused swap's, the one before
			// the removal, then the one after it, which fails.
			d.failGets[headKey] = 3
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	// LOW-6: what it tells the caller to do is true for the same apply too:
	// its stated head has moved, so the head as it is now is what to expect.
	if !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "could not be read afterwards") ||
		!strings.Contains(err.Error(), "against the head as it is now") {
		t.Fatalf("Put(2.0.0) = %v, want the head moved and an unread head reported", err)
	}
	if _, ok := d.object(versionKey); ok {
		t.Fatal("the rollback put version 2.0.0 back on a head it could not read")
	}
}

// The rollback's re-read cannot see a head move that lands after it: an
// identical apply that read the version before the removal, and swaps the head
// onto it only after the refused apply has returned. That apply checks the
// version it moved the head onto is still stored, and puts it back: the head
// never stays on a version that is not stored.
func TestConfirmedPut_AHeadMovedOntoAVersionAfterItsRollbackPutsItBack(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	refusedReturned, otherAtSwap, otherDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var moved, deleting, otherSwapping atomic.Bool
	var otherErr error
	d.onRequest = func(r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && moved.CompareAndSwap(false, true):
			// Another apply moves the head to 3.0.0, stamped before 2.0.0,
			// just before the first apply's swap.
			document, err := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_001, 500_000_000)})
			if err != nil {
				t.Errorf("marshalHeadPointer: %v", err)
				return
			}
			d.mu.Lock()
			d.objects[headKey] = document
			d.mu.Unlock()
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, versionKey) && deleting.CompareAndSwap(false, true):
			// The identical apply starts now and reads 2.0.0, still stored,
			// then is held at its swap until the refused apply has returned.
			go func() {
				defer close(otherDone)
				_, _, otherErr = other.Put(ctx, applyOf("2.0.0", 3), NoHeadExpectation())
			}()
			<-otherAtSwap
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && deleting.Load() &&
			otherSwapping.CompareAndSwap(false, true):
			close(otherAtSwap)
			<-refusedReturned
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	close(refusedReturned)
	if !deleting.Load() {
		t.Fatal("the refused apply never removed its version, so nothing here was tested")
	}
	<-otherDone
	if !errors.Is(err, ErrHeadMoved) {
		t.Fatalf("the refused apply = %v, want the head moved", err)
	}
	if otherErr != nil {
		t.Fatalf("the identical apply = %v", otherErr)
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q, want 2.0.0", head)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the head names 2.0.0, which is not stored")
	}
}

// identicalApply is applyOf for a document two applies stamp alike: its own
// stamp is before the fixture's head, so each gets the head's plus 1 ns.
func identicalApply() *orkstackv1.Orkflow { return applyOf("2.0.0", 0) }

// HIGH-1: an apply whose create's answer was lost, and whose read found the
// identical document an apply that was then refused created, does not know it
// created the version; when it moves the head after the refused apply's
// rollback removed the version, it checks and puts the version back.
func TestConfirmedPut_AnApplyThatOnlyReadItsCreateBackChecksTheHeadsVersion(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	second := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	secondAtCreate, firstCreated := make(chan struct{}), make(chan struct{})
	secondAtSwap, firstDone := make(chan struct{}), make(chan struct{})
	var versionPuts, headPuts atomic.Int32
	d.onRequest = func(r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, versionKey):
			<-secondAtSwap // the second apply has read the version back
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, versionKey) && versionPuts.Add(1) == 1:
			// The second apply's create: held until the first's create is
			// stored, then it fails unstored, and its resend meets that.
			close(secondAtCreate)
			<-firstCreated
			d.mu.Lock()
			d.failPuts[versionKey] = 1
			d.mu.Unlock()
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey):
			switch headPuts.Add(1) {
			case 1: // the first apply's swap, after its create: another apply moved the head first
				d.mu.Lock()
				before, _ := unmarshalHeadPointer(d.objects[headKey])
				document, _ := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_001, 0),
					Since: before.Since}) // as a swap does, it keeps the incarnation
				d.objects[headKey] = document
				d.mu.Unlock()
				close(firstCreated)
			case 2: // the second apply's swap: held until the first has returned
				close(secondAtSwap)
				<-firstDone
			}
		}
	}
	var secondErr error
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		_, _, secondErr = second.Put(ctx, identicalApply(), NoHeadExpectation())
	}()
	<-secondAtCreate
	_, _, err := store.Put(ctx, identicalApply(), ExpectHead("1.0.0"))
	close(firstDone)
	<-secondDone
	if !errors.Is(err, ErrHeadMoved) || secondErr != nil {
		t.Fatalf("first = %v (want the head moved), second = %v (want success)", err, secondErr)
	}
	if head := headVersion(t, store); head != "2.0.0" {
		t.Fatalf("the head names %q, want 2.0.0", head)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the head names 2.0.0, which is not stored")
	}
}

// refusedApplyHooks makes the next apply's head swap lose to another apply
// that moved the head to 3.0.0, and runs during, before the rollback's DELETE.
func refusedApplyHooks(d *s3Double, headKey, versionKey string, during func()) {
	var moved, deleting atomic.Bool
	d.onRequest = func(r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && moved.CompareAndSwap(false, true):
			// Another apply moves the head as a swap does: its incarnation's
			// since is copied.
			d.mu.Lock()
			before, _ := unmarshalHeadPointer(d.objects[headKey])
			document, _ := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_005, 0),
				Since: before.Since})
			d.objects[headKey] = document
			d.mu.Unlock()
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, versionKey) && deleting.CompareAndSwap(false, true):
			during()
		}
	}
}

// HIGH-2: a promotion of the refused apply's listed version that lands between
// the rollback's read of the pointers and its removal: the rollback reads the
// current pointer again and puts the version back.
func TestConfirmedPut_ARollbackPutsBackAVersionAPromotionMadeCurrent(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	promoter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var promoteErr error
	refusedApplyHooks(d, headKey, versionKey, func() {
		_, promoteErr = promoter.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "2.0.0", NoCurrentExpectation())
	})
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	if !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "a promotion made it current") || promoteErr != nil {
		t.Fatalf("Put = %v, promotion = %v; want the head moved with the version put back, and the promotion landed", err, promoteErr)
	}
	if current, _ := store.CurrentVersion(ctx, DefaultNamespace, "deploy"); current != "2.0.0" {
		t.Fatalf("current = %q, want 2.0.0", current)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the current pointer names 2.0.0, which is not stored")
	}
}

// HIGH-A: a promotion that read the version before the rollback removed it and
// swaps the pointer only after the rollback returned: it reads its target
// back, finds it gone while the pointer is still its own, puts it back and
// lands. Every order of the two ends with the pointer on a stored version.
func TestSetCurrentVersion_APromotionWhoseTargetARollbackRemovedPutsItBack(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	pointerKey, _ := OrkflowCurrentKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	promoter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	atSwap, rollbackDone, promoteDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var promoteErr error
	refusedApplyHooks(d, headKey, versionKey, func() {
		go func() {
			defer close(promoteDone)
			_, promoteErr = promoter.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "2.0.0", NoCurrentExpectation())
		}()
		<-atSwap
	})
	hooks := d.onRequest
	var swapping atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, pointerKey) && swapping.CompareAndSwap(false, true) {
			close(atSwap)
			<-rollbackDone
			return
		}
		hooks(r)
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	close(rollbackDone)
	<-promoteDone
	if !errors.Is(err, ErrHeadMoved) {
		t.Fatalf("Put = %v, want the head moved", err)
	}
	if promoteErr != nil {
		t.Fatalf("the promotion = %v, want it landed with its target put back", promoteErr)
	}
	// HIGH-A stays whole under the incarnation rule: the head names one, and
	// the refused apply's version, created within it, was put back.
	if head, _, _ := store.readHead(ctx, DefaultNamespace, "deploy"); head.Since.IsZero() {
		t.Fatal("the head carries no since, so the incarnation rule was not exercised")
	}
	if current, _ := store.CurrentVersion(ctx, DefaultNamespace, "deploy"); current != "2.0.0" {
		t.Fatalf("current = %q, want 2.0.0", current)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the current pointer names 2.0.0, which is not stored")
	}
}

// MEDIUM-1: a Delete took the head pointer while an apply's swap was in
// flight, so its If-Match meets a missing key (404): the head moved to
// nothing. A stated apply is refused naming no head, and its rollback removes
// the version, so the deleted orkflow does not reappear.
func TestConfirmedPut_AHeadSwapOnADeletedPointerIsTheHeadMovedToNothing(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	var deleted atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && deleted.CompareAndSwap(false, true) {
			d.mu.Lock()
			delete(d.objects, headKey)
			d.mu.Unlock()
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	var moved *HeadMovedError
	if !errors.As(err, &moved) || moved.Head != "" {
		t.Fatalf("Put = %v, want the head moved to nothing", err)
	}
	if _, ok := d.object(versionKey); ok {
		t.Fatal("the refused apply left 2.0.0 stored after the head was deleted")
	}
}

// HIGH-3, MEDIUM-2, LOW-3: the helpers a rollback and an ensure use. A restore
// writes the exact bytes it read — fields a decode discards included — and on
// a version already there reads it: the same document is as good, another is
// ErrVersionExists. An ensure finds another document under the version and
// says so; with the version gone and the head moved on it puts nothing back.
func TestRestoreAndEnsure_HoldTheVersionToItsDocument(t *testing.T) {
	ctx := context.Background()
	key, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	mine, theirs := applyOf("2.0.0", 2), applyOf("2.0.0", 2)
	theirs.Description = "another document under the same version"
	raw, _ := marshalOrkflow(mine)
	future := append([]byte(`{"futureField":1,`), raw[1:]...) // a newer release's field
	other, _ := marshalOrkflow(theirs)
	t.Run("restore writes the bytes it read", func(t *testing.T) {
		d, store := orkflowFixture(t)
		if err := store.restoreVersion(ctx, key, future, mine); err != nil {
			t.Fatalf("restoreVersion: %v", err)
		}
		if got, _ := d.object(key); string(got) != string(future) {
			t.Fatalf("restored %s, want the bytes read", got)
		}
	})
	t.Run("restore over the same document", func(t *testing.T) {
		d, store := orkflowFixture(t)
		d.objects[key] = raw
		if err := store.restoreVersion(ctx, key, future, mine); err != nil {
			t.Fatalf("restoreVersion over the same document = %v, want nil", err)
		}
	})
	t.Run("restore over another document", func(t *testing.T) {
		d, store := orkflowFixture(t)
		d.objects[key] = other
		if err := store.restoreVersion(ctx, key, raw, mine); !errors.Is(err, ErrVersionExists) {
			t.Fatalf("restoreVersion over another document = %v, want ErrVersionExists", err)
		}
	})
	t.Run("ensure finds another document", func(t *testing.T) {
		d, store := orkflowFixture(t)
		d.objects[key] = other
		if err := store.ensureHeadVersion(ctx, DefaultNamespace, "deploy", key, mine, raw); !errors.Is(err, ErrVersionExists) {
			t.Fatalf("ensureHeadVersion = %v, want ErrVersionExists", err)
		}
	})
	t.Run("ensure with the head moved on", func(t *testing.T) {
		d, store := orkflowFixture(t) // the head names 1.0.0; 2.0.0 is not stored
		if err := store.ensureHeadVersion(ctx, DefaultNamespace, "deploy", key, mine, raw); err != nil {
			t.Fatalf("ensureHeadVersion = %v, want nil", err)
		}
		if _, ok := d.object(key); ok {
			t.Fatal("ensureHeadVersion put back a version the head does not name")
		}
	})
}

// LOW-3: the rollback's first check keeps a version a pointer names. Here the
// current pointer names the refused apply's version already (a promotion that
// landed before the rollback looked), so the version stays.
func TestConfirmedPut_ARollbackKeepsAVersionThePointersName(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	pointerKey, _ := OrkflowCurrentKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	d.objects[pointerKey] = pointerDocument(t, "2.0.0", time.Unix(1_700_000_003, 0))
	refusedApplyHooks(d, headKey, versionKey, func() { t.Error("the rollback removed a version the current pointer names") })
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	if !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "the head or the current pointer names it") ||
		strings.Contains(err.Error(), "could not be read") {
		t.Fatalf("Put = %v, want the head moved and the version kept, as a pointer names it", err)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("2.0.0 was removed")
	}
}

// MEDIUM-4: a Delete of the orkflow and a fresh create of the refused apply's
// version with other content land between the rollback's read and its
// removal. The removal is held to the etag it read, so the new object is left
// alone, and the caller is told.
func TestConfirmedPut_ARollbackLeavesAnotherObjectAlone(t *testing.T) {
	ctx := context.Background()
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	d, store := orkflowFixture(t)
	theirs := applyOf("2.0.0", 9)
	theirs.Description = "created afresh after a Delete"
	other, _ := marshalOrkflow(theirs)
	refusedApplyHooks(d, headKey, versionKey, func() {
		d.mu.Lock()
		d.objects[versionKey] = other
		d.mu.Unlock()
	})
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	if !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "is left alone") {
		t.Fatalf("Put = %v, want the head moved and the new object left alone", err)
	}
	if got, _ := d.object(versionKey); string(got) != string(other) {
		t.Fatal("the rollback removed or replaced the object a fresh create left")
	}
}

// MEDIUM-C: a promotion whose target is gone puts it back only while the
// pointer is still its own write. A pointer another promotion moved on since,
// or one a Delete took, is left alone and nothing is put back; the promotion
// is refused as not found with what happened, never as a store failure the
// API would answer Unavailable.
func TestConfirmPromoted_LeavesAnotherWritersPointerAlone(t *testing.T) {
	ctx := context.Background()
	pointerKey, _ := OrkflowCurrentKey(DefaultNamespace, "deploy")
	versionKey, _ := OrkflowKey(DefaultNamespace, "deploy", "2.0.0") // not stored: the target is gone
	mine := currentPointer{Version: "2.0.0", UpdatedAt: time.Unix(1_700_000_003, 0).UTC()}
	target := applyOf("2.0.0", 2)
	raw, _ := marshalOrkflow(target)
	for _, tc := range []struct {
		name    string
		pointer []byte // nil: a Delete took it
		want    string
	}{
		{"another promotion moved it on", pointerDocument(t, "1.0.0", time.Unix(1_700_000_004, 0)), "moved since"},
		{"another promotion of the same version", pointerDocument(t, "2.0.0", time.Unix(1_700_000_004, 0)), "moved since"},
		{"a Delete took it", nil, "deleted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, store := orkflowFixture(t)
			if tc.pointer != nil {
				d.objects[pointerKey] = tc.pointer
			}
			err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", versionKey, currentPointer{}, nil, mine, target, raw)
			// LOW-6: the refusal names the namespace, as every promotion error does.
			if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), tc.want) ||
				!strings.Contains(err.Error(), `in namespace "default"`) {
				t.Fatalf("confirmPromoted = %v, want refused as not found, naming the namespace: %s", err, tc.want)
			}
			if got, ok := d.object(pointerKey); string(got) != string(tc.pointer) || ok != (tc.pointer != nil) {
				t.Fatalf("the pointer was changed: %s", got)
			}
			if _, ok := d.object(versionKey); ok {
				t.Fatal("the version was put back under a pointer that is not this promotion's")
			}
		})
	}
}
