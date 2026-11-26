//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	ceilingTasks   = 4000 // engine.orkflow_max_tasks' default
	ceilingPages   = 9    // of 500: each task's record, the await's and, once made, the outputs'
	ceilingSources = 180  // the most rows a record holds
)

// openParent is the starts of a child whose parent is open, as the cluster
// describes them: RecordOutputs asks them, and no run executes here.
type openParent struct {
	identity.StartReader
	records *artifacts.RecordStore
	parent  string
	sha     string
}

func (o openParent) facts(run string) identity.ExecutionFacts {
	if run == o.parent {
		return identity.ExecutionFacts{WorkflowID: run, Open: true, ExecutionID: "p", FirstExecutionID: "p"}
	}
	return identity.ExecutionFacts{WorkflowID: run, Open: true, ExecutionID: "c", FirstExecutionID: "c",
		Parent: &identity.ExecutionRef{WorkflowID: o.parent, ExecutionID: "p"}}
}

func (o openParent) Describe(_ context.Context, run, _ string) (identity.ExecutionFacts, error) {
	return o.facts(run), nil
}

func (o openParent) ReadAdmission(ctx context.Context, ns, run, sha string) (artifacts.RecordRead[artifacts.Admission], error) {
	return o.records.ReadAdmission(ctx, ns, run, sha)
}

func (o openParent) ReadChain(context.Context, string, string) (artifacts.RecordRead[artifacts.Chain], error) {
	return artifacts.RecordRead[artifacts.Chain]{Answer: artifacts.RecordFound, Value: artifacts.Chain{FirstExecutionID: "p", AdmissionSHA256: o.sha}}, nil
}

// TestIntegration_ChildOutputsAtCeiling: a child run of as many tasks as an
// Orkflow may declare, whose outputs read as many as a record may name, has its
// outputs recorded with nine listings (and recorded again, as the first one
// stands, the same way), and by key only what its outputs read and its awaited
// task; a task with nothing on record, or an approval that was
// rejected, is named and nothing is recorded. How long it takes with a slowed
// store is logged, never asserted.
func TestIntegration_ChildOutputsAtCeiling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	raw, err := s3bucket.Client(ctx, cfg)
	if err != nil {
		mintInfraAbsent(t, "object store client: %v", err)
	}
	if _, err := raw.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		mintInfraAbsent(t, "object store bucket %q at %s: %v; run `make infra-up`", cfg.S3Bucket, cfg.S3Endpoint, err)
	}
	proxy := s3scope.Open(t, cfg.S3Endpoint, cfg.S3Bucket)
	through := cfg
	through.S3Endpoint = proxy.URL
	records, err := artifacts.NewRecordStore(ctx, through)
	must.OK(t, err)
	writer, err := artifacts.NewRecordTestWriter(ctx, cfg)
	must.OK(t, err)
	put := func(key string, body []byte) error {
		_, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
		return err
	}
	admit := func(a artifacts.Admission) string {
		t.Helper()
		body, sha, err := artifacts.EncodeAdmission(a)
		must.OK(t, err)
		key, err := artifacts.AdmissionKey(a.Namespace, a.RunID, sha)
		must.OK(t, err)
		must.OK(t, put(key, body))
		return sha
	}
	base := artifacts.Admission{Namespace: "default", Orkflow: "deploy", OrkflowVersion: "1", DefinitionDigest: strings.Repeat("d", 64),
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(), EngineBuild: "b",
		SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, ValueRecords: 1}
	row := artifacts.AdmittedTask{Pool: "main", SpecSHA256: strings.Repeat("a", 64)}
	parent := base
	parent.RunID, parent.Starter, parent.Tasks = engine.NewRunID(), artifacts.Starter{Kind: artifacts.StarterEngine}, map[string]artifacts.AdmittedTask{"build": row}
	parent.Children = map[string]string{}
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1", Outputs: map[string]string{}, Tasks: []*orkstackv1.Task{{Id: "gate", Await: &orkstackv1.AwaitSpec{}}}}
	offered := map[string]*orkstackv1.ArtifactRef{}
	values, err := writer.Values(ctx, []byte(`{"n":1}`))
	must.OK(t, err)
	for i := range ceilingTasks {
		id := fmt.Sprintf("t%04d", i)
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: id})
		if i < ceilingSources {
			wf.Outputs["o"+id] = "{{ .tasks." + id + ".output.n }}"
			offered[id] = values
		}
	}
	// A child of the parent at the ceiling, each of whose tasks but skip has
	// its record, and whose approval is given; its gate gives or rejects it.
	type seeded struct {
		run, sha string
		gate     func(rejected bool)
		commit   func(id string)
	}
	children := []string{engine.NewRunID(), engine.NewRunID()}
	for i, id := range children {
		parent.Children[fmt.Sprintf("sub%d", i)] = id
	}
	parentSHA := admit(parent)
	seed := func(n int, skip string) seeded {
		t.Helper()
		child := base
		child.RunID, child.RootRunID, child.Tasks = children[n], parent.RunID, map[string]artifacts.AdmittedTask{}
		child.Completes = []string{"gate"}
		for i := range ceilingTasks {
			id := fmt.Sprintf("t%04d", i)
			child.Tasks[id], child.Completes = row, append(child.Completes, id)
		}
		child.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: parent.RunID, AdmissionSHA256: parentSHA, TaskID: fmt.Sprintf("sub%d", n)}
		sha := admit(child)
		commit := func(id string) {
			t.Helper()
			key, err := artifacts.CommitKey("default", child.RunID, id)
			must.OK(t, err)
			key, err = artifacts.AttestedCommitKey(key)
			must.OK(t, err)
			doc, err := artifacts.MarshalCommit(&orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: child.RunID,
				TaskId: id, Values: values, Recorded: &orkstackv1.CommitRecording{AdmissionSha256: sha}})
			must.OK(t, err)
			must.OK(t, put(key, doc))
		}
		var group errgroup.Group
		group.SetLimit(32)
		for i := range ceilingTasks {
			if id := fmt.Sprintf("t%04d", i); id != skip {
				group.Go(func() error { commit(id); return nil })
			}
		}
		must.OK(t, group.Wait())
		resolution, err := artifacts.RecordKey(artifacts.RecordResolution, "default", child.RunID, "gate", 0)
		must.OK(t, err)
		gate := func(rejected bool) {
			t.Helper()
			detail := &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "default", RunId: child.RunID, TaskId: "gate",
				Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice"}
			if rejected {
				detail.Reason = "no"
			}
			dsha, err := writer.Detail(ctx, detail)
			must.OK(t, err)
			rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: "default",
				RunId: child.RunID, TaskId: "gate", AdmissionSha256: sha, InputBinding: make([]byte, 32), Rejected: rejected, DetailSha256: dsha}
			if !rejected {
				rec.Values = values
			}
			body, err := artifacts.MarshalRecordedValues(rec)
			must.OK(t, err)
			must.OK(t, put(resolution, body))
		}
		gate(false)
		return seeded{run: child.RunID, sha: sha, gate: gate, commit: commit}
	}
	const missing = "t3210"
	whole := seed(0, "")

	starts := openParent{records: records, parent: parent.RunID, sha: parentSHA}
	recorder := &identity.ValuesRecorder{Records: records, Starts: starts, EngineBuild: "b", Now: time.Now,
		Describe:   func(_ context.Context, run string) (identity.ExecutionFacts, error) { return starts.facts(run), nil },
		Definition: func(context.Context, artifacts.Admission) (*orkstackv1.Orkflow, error) { return wf, nil },
		Render:     func(context.Context, identity.OutputsRender) ([]byte, error) { return []byte(`{"n":1}`), nil },
		Store: func(ctx context.Context, _ artifacts.Admission, body []byte) (*orkstackv1.ArtifactRef, error) {
			sum := sha256.Sum256(body)
			ref, err := writer.Values(ctx, body)
			if err == nil && ref.GetSha256() != hex.EncodeToString(sum[:]) {
				err = errors.New("stored other bytes")
			}
			return ref, err
		}}
	input, err := identity.StartInput(&orkstackv1.Payload{})
	must.OK(t, err)
	askOf := func(c seeded) identity.OutputsAsk {
		return identity.OutputsAsk{Namespace: "default", RunID: c.run, AdmissionSHA256: c.sha, Input: input, Offered: offered, MaxBytes: 1 << 18}
	}
	ask := askOf(whole)
	under, err := artifacts.AttestedRunPrefix("default", whole.run)
	must.OK(t, err)
	// asked is the listings of the child's records and the reads by key of its
	// tasks' ones since the last.
	asked := func() (listings int, byKey map[string]int) {
		byKey = map[string]int{}
		for _, r := range proxy.Requests() {
			task, record, ok := strings.Cut(strings.TrimPrefix(r.Key, under), "/")
			switch {
			case r.Verb == s3scope.List && strings.HasPrefix(r.Key, under):
				listings++
			case r.Verb == s3scope.Get && strings.HasPrefix(r.Key, under) && ok && record != "":
				byKey[task]++
			}
		}
		proxy.Reset()
		return listings, byKey
	}
	for _, slowed := range []time.Duration{0, 50 * time.Millisecond, 250 * time.Millisecond} {
		proxy.Script(func(s3scope.Request) *s3scope.Answer { time.Sleep(slowed); return nil })
		proxy.Reset()
		began := time.Now()
		got, err := recorder.RecordOutputs(ctx, ask)
		must.OKf(t, err, "with every request slowed by %s", slowed)
		took := time.Since(began)
		listings, byKey := asked()
		if listings != ceilingPages || len(byKey) != ceilingSources+1 || byKey["gate"] == 0 || len(got.RecordSHA256) != 64 {
			t.Fatalf("slowed by %s: %d listings, %d tasks read by key (the gate %d times)", slowed, listings, len(byKey), byKey["gate"])
		}
		for i := range ceilingSources {
			if id := fmt.Sprintf("t%04d", i); byKey[id] == 0 {
				t.Fatalf("slowed by %s: the source %s was not read by key: %v", slowed, id, byKey)
			}
		}
		t.Logf("EVIDENCE a child's end at the ceiling: %d tasks, %d sources, every request slowed by %s: %s, %d listings", ceilingTasks, ceilingSources, slowed, took, listings)
	}
	proxy.Script(nil)

	// A second child, whose task missing never had a record: a record deleted
	// would leave its prefix behind in a store that keeps empty directories.
	short := seed(1, missing)
	ask = askOf(short)
	if under, err = artifacts.AttestedRunPrefix("default", short.run); err != nil {
		t.Fatal(err)
	}
	proxy.Reset()
	_, err = recorder.RecordOutputs(ctx, ask)
	var refused *identity.ValuesRefusal
	if !errors.As(err, &refused) || refused.Reason != identity.ReasonOutputsNotRecorded || !strings.Contains(refused.Message, missing) {
		t.Fatalf("with %s's record gone: %v", missing, err)
	}
	if _, byKey := asked(); byKey[missing] != 2 || byKey["gate"] == 0 || len(byKey) != 2 {
		t.Fatalf("with %s's record gone: read by key %v, want it alone, twice, and the awaited one", missing, byKey)
	}
	noOutputs := func(when string) {
		t.Helper()
		key, err := artifacts.OutputsRecordKey("default", short.run)
		must.OK(t, err)
		if _, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)}); err == nil {
			t.Fatalf("%s, the child's outputs were recorded", when)
		}
	}
	noOutputs("with " + missing + "'s record gone")
	short.commit(missing)
	short.gate(true)
	if _, err = recorder.RecordOutputs(ctx, ask); !errors.As(err, &refused) || refused.Reason != identity.ReasonOutputsNotRecorded || !strings.Contains(refused.Message, "gate") {
		t.Fatalf("with a rejected approval no output reads: %v", err)
	}
	noOutputs("with a rejected approval")
}
