package artifacts

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

func testAdmission() Admission {
	return Admission{
		RunID: "ork-1", Namespace: "ns", Orkflow: "deploy", OrkflowVersion: "1.0.0",
		DefinitionDigest: strings.Repeat("d", 64),
		Starter:          Starter{Kind: StarterEngine},
		Origin:           AdmittedOrigin{Kind: "submit", Principal: "alice", Verified: true},
		Nonce:            strings.Repeat("0f", 16),
		AdmittedAt:       time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC),
		EngineBuild:      "b1", Issuer: "https://ork.example", SubjectFormat: SubjectFormatOrk,
		NamespaceID: "ns-id", OrkflowID: "of-id", TokenTTL: 3600, SpecVersion: 1,
		Sets: []IdentitySet{{Identities: []AdmittedIdentity{{Name: "vault", Audience: "https://vault", TTLSeconds: 3600, Refresh: RefreshRequired}}}},
		Tasks: map[string]AdmittedTask{
			"build":  {Pool: "main", SpecSHA256: strings.Repeat("a", 64)},
			"deploy": {Set: 1, Pool: "prod", SpecSHA256: strings.Repeat("b", 64), IdentitySlots: 1, AttemptDeadline: 330},
		},
	}
}

// TestAdmissionKeys_Shape: a run's admission is named by the SHA-256 of its
// bytes under the run's identity prefix, beside its chain record.
func TestAdmissionKeys_Shape(t *testing.T) {
	sha := strings.Repeat("c", 64)
	got, err := AdmissionKey("ns", "ork-1", sha)
	if err != nil || got != "metadata/snapshots/ns/ork-1/identity/admission-"+sha+".json" {
		t.Fatalf("admission key = %q, %v", got, err)
	}
	got, err = ChainKey("ns", "ork-1")
	if err != nil || got != "metadata/snapshots/ns/ork-1/identity/chain.json" {
		t.Fatalf("chain key = %q, %v", got, err)
	}
	for name, f := range map[string]func() (string, error){
		"a short digest":  func() (string, error) { return AdmissionKey("ns", "ork-1", "abc") },
		"an upper digest": func() (string, error) { return AdmissionKey("ns", "ork-1", strings.Repeat("C", 64)) },
		"a namespace":     func() (string, error) { return AdmissionKey("Bad", "ork-1", sha) },
		"no run":          func() (string, error) { return ChainKey("ns", "") },
	} {
		if key, err := f(); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s = %q, %v", name, key, err)
		}
	}
}

// TestAdmission_IsNamedByItsBytesAndReadBack: the record encodes to one
// canonical form whose SHA-256 names it; written twice it is one object;
// read back by run and digest it is the record written.
func TestAdmission_IsNamedByItsBytesAndReadBack(t *testing.T) {
	s := newFastRecordStore(t)
	ctx := context.Background()
	a := testAdmission()
	raw, sha, err := EncodeAdmission(a)
	if err != nil {
		t.Fatal(err)
	}
	if sha != hexDigest(raw) {
		t.Fatalf("digest %s does not name the bytes", sha)
	}
	if again, _, _ := EncodeAdmission(a); !bytes.Equal(again, raw) {
		t.Fatal("the encoding is not deterministic")
	}
	// No children and an empty table of them are one record: a check that
	// reads a parent's list sees nil for both.
	empty := a
	empty.Children = map[string]string{}
	if same, _, _ := EncodeAdmission(empty); !bytes.Equal(same, raw) {
		t.Fatal("an empty children table encodes differently from none")
	}
	// The first write creates it; the second finds the same bytes, which
	// are success and not the caller's own.
	for i, wantCreated := range []bool{true, false} {
		if got, created, err := s.WriteAdmission(ctx, a); err != nil || got != sha || created != wantCreated {
			t.Fatalf("write %d = %s, created %t, %v; want created %t", i+1, got, created, err, wantCreated)
		}
	}
	got, err := s.ReadAdmission(ctx, "ns", "ork-1", sha)
	if err != nil || got.Answer != RecordFound {
		t.Fatalf("read = %+v, %v", got, err)
	}
	if back, _, _ := EncodeAdmission(got.Value); !bytes.Equal(back, raw) {
		t.Fatalf("read back %+v", got.Value)
	}
	if got.Value.SetOf("deploy") == nil || got.Value.SetOf("build") != nil || got.Value.SetOf("absent") != nil {
		t.Fatalf("sets = %+v", got.Value.Tasks)
	}
}

// TestAdmission_ReadAnswers: none is none; bytes that do not hash to the
// name, another run's record, a record that is not canonical, a format
// this binary does not read and a store that does not answer each have
// their own answer, and none of them is none.
func TestAdmission_ReadAnswers(t *testing.T) {
	ctx := context.Background()
	s, b := newFastRecordStoreDown(t)
	sha := strings.Repeat("e", 64)
	if got, err := s.ReadAdmission(ctx, "ns", "ork-1", sha); err != nil || got.Answer != RecordNone {
		t.Fatalf("absent = %+v, %v", got, err)
	}
	raw, real, _ := EncodeAdmission(testAdmission())
	put := func(name string, body []byte) {
		t.Helper()
		if err := b.create(ctx, mustKey(AdmissionKey("ns", "ork-1", name)), "application/json", body); err != nil {
			t.Fatal(err)
		}
	}
	put(sha, raw)
	if got, err := s.ReadAdmission(ctx, "ns", "ork-1", sha); err != nil || got.Answer != RecordInvalid {
		t.Fatalf("bytes under another digest = %+v, %v", got, err)
	}
	other := testAdmission()
	other.RunID = "ork-2"
	otherRaw, otherSHA, _ := EncodeAdmission(other)
	put(otherSHA, otherRaw)
	if got, err := s.ReadAdmission(ctx, "ns", "ork-1", otherSHA); err != nil || got.Answer != RecordInvalid {
		t.Fatalf("another run's record = %+v, %v", got, err)
	}
	spaced := bytes.Replace(raw, []byte(`,"`), []byte(`, "`), 1)
	put(hexDigest(spaced), spaced)
	if got, err := s.ReadAdmission(ctx, "ns", "ork-1", hexDigest(spaced)); err != nil || got.Answer != RecordInvalid {
		t.Fatalf("a record not in its canonical form = %+v, %v", got, err)
	}
	v2 := []byte(`{"v":2,"future":true}`)
	put(hexDigest(v2), v2)
	if got, err := s.ReadAdmission(ctx, "ns", "ork-1", hexDigest(v2)); err != nil || got.Answer != RecordUnsupported || got.Version != 2 {
		t.Fatalf("format 2 = %+v, %v", got, err)
	}
	b.down = true
	var un *RecordStoreUnanswered
	if _, err := s.ReadAdmission(ctx, "ns", "ork-1", real); !errors.As(err, &un) {
		t.Fatalf("a store that does not answer = %v", err)
	}
}

// TestAdmission_OfARunThatRunsNoContainer: a run whose tasks all compose,
// wait or sleep is admitted with no task of its own: its children name its
// record.
func TestAdmission_OfARunThatRunsNoContainer(t *testing.T) {
	ctx := context.Background()
	s, _ := newFastRecordStoreDown(t)
	a := testAdmission()
	a.Tasks, a.Sets, a.Follows, a.Grants = map[string]AdmittedTask{}, nil, nil, nil
	a.Children = map[string]string{"ship": "ork-2"}
	sha, _, err := s.WriteAdmission(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadAdmission(ctx, a.Namespace, a.RunID, sha)
	if err != nil || got.Answer != RecordFound || len(got.Value.Tasks) != 0 || got.Value.Children["ship"] != "ork-2" || got.Value.SetOf("ship") != nil {
		t.Fatalf("read back %+v, %v", got, err)
	}
}

// withGrant admits the named grants for the run, and says task id uses them.
func withGrant(a *Admission, id string, names ...string) {
	a.Grants = map[string]GrantSelection{}
	for _, name := range names {
		a.Grants[name] = GrantSelection{Provider: "aws", Principal: "role/x", Audience: "sts"}
	}
	t := a.Tasks[id]
	t.Grants = append([]string{}, names...)
	a.Tasks[id] = t
}

// TestAdmission_ATaskThatHoldsOnlyAGrant: it needs its slots and its
// deadline as one that holds an identity does, and no set.
func TestAdmission_ATaskThatHoldsOnlyAGrant(t *testing.T) {
	a := testAdmission()
	withGrant(&a, "deploy", "c", "g")
	a.Sets = nil
	row := a.Tasks["deploy"]
	row.Set = 0
	a.Tasks["deploy"] = row
	raw, _, err := EncodeAdmission(a)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"grants":["c","g"]`)) {
		t.Fatalf("the record is %s", raw)
	}
}

// TestAdmission_RefusesWhatItCannotTrust: a record that would mislead the
// mint is refused by the encoder, so it is never written, and read back it
// is invalid.
func TestAdmission_RefusesWhatItCannotTrust(t *testing.T) {
	for name, mutate := range map[string]func(*Admission){
		"no run":                     func(a *Admission) { a.RunID = "" },
		"a namespace":                func(a *Admission) { a.Namespace = "Bad" },
		"an orkflow name":            func(a *Admission) { a.Orkflow = "" },
		"a definition digest":        func(a *Admission) { a.DefinitionDigest = "x" },
		"a starter kind":             func(a *Admission) { a.Starter.Kind = "anyone" },
		"an engine starter with ids": func(a *Admission) { a.Starter.WorkflowID = "w" },
		"a fire with no execution":   func(a *Admission) { a.Starter = Starter{Kind: StarterFire, WorkflowID: "w"} },
		"a parent with no digest":    func(a *Admission) { a.Starter = Starter{Kind: StarterParentRun, RunID: "ork-0", TaskID: "t"} },
		"a short nonce":              func(a *Admission) { a.Nonce = "00" },
		"no admitted_at":             func(a *Admission) { a.AdmittedAt = time.Time{} },
		"no engine build":            func(a *Admission) { a.EngineBuild = "" },
		"a subject format":           func(a *Admission) { a.SubjectFormat = "x509" },
		"spiffe with no domain":      func(a *Admission) { a.SubjectFormat = SubjectFormatSPIFFE },
		"sets with no issuer":        func(a *Admission) { a.Issuer = "" },
		"an empty table of tags":     func(a *Admission) { a.SessionTags = map[string]SessionTagging{} },
		"tags of an unused audience": func(a *Admission) { a.SessionTags = map[string]SessionTagging{"https://other": {}} },
		"a tag outside the set": func(a *Admission) {
			a.SessionTags = map[string]SessionTagging{"https://vault": {Transitive: []string{"team"}}}
		},
		"transitive tags unsorted": func(a *Admission) {
			a.SessionTags = map[string]SessionTagging{"https://vault": {Transitive: []string{"ork_task", "ork_pool"}}}
		},
		"a transitive tag twice": func(a *Admission) {
			a.SessionTags = map[string]SessionTagging{"https://vault": {Transitive: []string{"ork_task", "ork_task"}}}
		},
		"an empty list of transitive tags": func(a *Admission) {
			a.SessionTags = map[string]SessionTagging{"https://vault": {Transitive: []string{}}}
		},
		"no spec version":           func(a *Admission) { a.SpecVersion = 0 },
		"extra over 16":             func(a *Admission) { a.AttemptsExtra = 17 },
		"no tasks":                  func(a *Admission) { a.Tasks = nil },
		"a set no task could use":   func(a *Admission) { a.Tasks, a.Follows = map[string]AdmittedTask{}, nil },
		"a task id":                 func(a *Admission) { a.Tasks["Bad"] = a.Tasks["build"] },
		"a pool":                    func(a *Admission) { t := a.Tasks["build"]; t.Pool = "P"; a.Tasks["build"] = t },
		"a spec digest":             func(a *Admission) { t := a.Tasks["build"]; t.SpecSHA256 = ""; a.Tasks["build"] = t },
		"a set out of range":        func(a *Admission) { t := a.Tasks["deploy"]; t.Set = 2; a.Tasks["deploy"] = t },
		"an identity with no slots": func(a *Admission) { t := a.Tasks["deploy"]; t.IdentitySlots = 0; a.Tasks["deploy"] = t },
		"slots past the most": func(a *Admission) {
			t := a.Tasks["deploy"]
			t.IdentitySlots = MaxAttemptSlots + 1
			a.Tasks["deploy"] = t
		},
		"no deadline":  func(a *Admission) { t := a.Tasks["deploy"]; t.AttemptDeadline = 0; a.Tasks["deploy"] = t },
		"an empty set": func(a *Admission) { a.Sets[0].Identities = nil },
		"two identities of a name": func(a *Admission) {
			a.Sets[0].Identities = append(a.Sets[0].Identities, a.Sets[0].Identities[0])
		},
		"a refresh policy": func(a *Admission) { a.Sets[0].Identities[0].Refresh = "sometimes" },
		"a follow's kind":  func(a *Admission) { a.Follows = map[string][]FollowRef{"deploy": {{TaskID: "build", Kind: "maybe"}}} },
		"a follow's task": func(a *Admission) {
			a.Follows = map[string][]FollowRef{"deploy": {{TaskID: "", Kind: FollowContainer}}}
		},
		"an empty grants": func(a *Admission) { a.Grants = map[string]GrantSelection{} },
		"a task's grant the run has not": func(a *Admission) {
			withGrant(a, "deploy", "c", "g")
			delete(a.Grants, "g")
		},
		"a grant no task uses": func(a *Admission) {
			withGrant(a, "deploy", "c")
			a.Grants["g"] = a.Grants["c"]
		},
		"a task's empty grants": func(a *Admission) {
			withGrant(a, "deploy")
			a.Grants = nil
		},
		"a task's grants out of order": func(a *Admission) { withGrant(a, "deploy", "g", "c") },
		"a task's grant twice":         func(a *Admission) { withGrant(a, "deploy", "c", "c") },
		"a grant with no slots":        func(a *Admission) { withGrant(a, "build", "c") },
		"grants with no issuer": func(a *Admission) {
			withGrant(a, "deploy", "c")
			a.Issuer, a.Sets = "", nil
			d := a.Tasks["deploy"]
			d.Set = 0
			a.Tasks["deploy"] = d
		},
		"a child's task id":             func(a *Admission) { a.Children = map[string]string{"B": "ork-2"} },
		"a root on a root":              func(a *Admission) { a.RootRunID = "ork-0" },
		"a week's commit wait and more": func(a *Admission) { a.CommitWait = 7*24*3600 + 1 },
		"a parent with no run": func(a *Admission) {
			a.Starter = Starter{Kind: StarterParentRun, RunID: "", AdmissionSHA256: strings.Repeat("a", 64), TaskID: "t"}
		},
	} {
		a := testAdmission()
		mutate(&a)
		if _, _, err := EncodeAdmission(a); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s: encode = %v; want ErrInvalidRecordKey", name, err)
		}
	}
}

// TestChain_CreatedOnceAndCompared: a root's chain record is created once;
// the same record again is success; another is ErrChainDiffers, and the
// one there stays; an object that is not a chain record is invalid.
func TestChain_CreatedOnceAndCompared(t *testing.T) {
	s, b := newFastRecordStoreDown(t)
	ctx := context.Background()
	if got, err := s.ReadChain(ctx, "ns", "ork-1"); err != nil || got.Answer != RecordNone {
		t.Fatalf("absent = %+v, %v", got, err)
	}
	c := Chain{FirstExecutionID: "exec-1", AdmissionSHA256: strings.Repeat("a", 64)}
	for range 2 {
		if err := s.CompleteChain(ctx, "ns", "ork-1", c); err != nil {
			t.Fatal(err)
		}
	}
	other := c
	other.FirstExecutionID = "exec-2"
	if err := s.CompleteChain(ctx, "ns", "ork-1", other); !errors.Is(err, ErrChainDiffers) {
		t.Fatalf("another chain = %v", err)
	}
	if got, err := s.ReadChain(ctx, "ns", "ork-1"); err != nil || got.Answer != RecordFound || got.Value != c {
		t.Fatalf("read = %+v, %v", got, err)
	}
	for _, id := range []string{"", "a\x00b", "a\vb", "\xff", "a/b", strings.Repeat("e", 129)} {
		if err := s.CompleteChain(ctx, "ns", "ork-1", Chain{FirstExecutionID: id, AdmissionSHA256: c.AdmissionSHA256}); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("a chain naming execution %q = %v", id, err)
		}
	}
	// The longest id a chain record may name is written and read back.
	long := Chain{FirstExecutionID: strings.Repeat("e", 128), AdmissionSHA256: c.AdmissionSHA256}
	if err := s.CompleteChain(ctx, "ns", "ork-long", long); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ReadChain(ctx, "ns", "ork-long"); err != nil || got.Answer != RecordFound || got.Value != long {
		t.Fatalf("the longest id = %+v, %v", got, err)
	}
	if err := b.create(ctx, mustKey(ChainKey("ns", "ork-2")), "application/json", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ReadChain(ctx, "ns", "ork-2"); err != nil || got.Answer != RecordInvalid {
		t.Fatalf("a malformed chain = %+v, %v", got, err)
	}
	var invalid *ChainInvalidError
	if err := s.CompleteChain(ctx, "ns", "ork-2", c); !errors.As(err, &invalid) {
		t.Fatalf("completing over a malformed chain = %v", err)
	}
	b.down = true
	var un *RecordStoreUnanswered
	if err := s.CompleteChain(ctx, "ns", "ork-3", c); !errors.As(err, &un) {
		t.Fatalf("a store that does not answer = %v", err)
	}
}

// FuzzAdmission: an admission read back either decodes, and then encodes
// to exactly its bytes, or is refused; it never panics.
func FuzzAdmission(f *testing.F) {
	raw, _, _ := EncodeAdmission(testAdmission())
	f.Add(raw)
	fired := testAdmission()
	fired.Starter = Starter{Kind: StarterFire, WorkflowID: "fire-1", ExecutionID: "e"}
	fired.Follows = map[string][]FollowRef{"deploy": {{TaskID: "build", Kind: FollowContainer}}}
	withGrant(&fired, "deploy", "aws")
	raw2, _, err := EncodeAdmission(fired)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw2)
	f.Add([]byte(`{"v":1}`))
	f.Add([]byte(`{"v":2}`))
	f.Add(bytes.Replace(raw, []byte(`"main"`), []byte(`null`), 1))
	f.Fuzz(func(t *testing.T, raw []byte) {
		a, answer, _ := decodeAdmission(raw)
		if answer != RecordFound {
			return
		}
		again, _, err := EncodeAdmission(a)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%q re-encodes as %q, %v", raw, again, err)
		}
	})
}

// FuzzChain: a chain record read back either decodes to one that encodes
// to its bytes, or is refused.
func FuzzChain(f *testing.F) {
	f.Add(encodeChain(Chain{FirstExecutionID: "exec-1", AdmissionSHA256: strings.Repeat("a", 64)}))
	f.Add([]byte(`{"v":1,"first_execution_id":"","admission_sha256":""}`))
	f.Add([]byte("{\"v\":1,\"first_execution_id\":\"\\u0000\",\"admission_sha256\":\"" + strings.Repeat("a", 64) + "\"}\n"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		c, answer, _ := decodeChain(raw)
		if answer == RecordFound && !bytes.Equal(encodeChain(c), raw) {
			t.Fatalf("%q re-encodes differently", raw)
		}
	})
}

// newFastRecordStore is a record store whose reads believe a miss after a
// millisecond.
func newFastRecordStore(t *testing.T) *RecordStore {
	s, _ := newFastRecordStoreDown(t)
	return s
}

func newFastRecordStoreDown(t *testing.T) (*RecordStore, *downBlobs) {
	t.Helper()
	s, b := newTestRecordStore(t)
	s.reader = &recordReader{b: s.reader.b, slots: s.reader.slots, spacing: fast, many: s.reader.many}
	return s, b
}

// TestAdmission_DeleteRemovesOnlyThatRecord: a submit that ends before any
// start was attempted deletes what it created, by its digest, and a delete
// of one already gone is not an error.
func TestAdmission_DeleteRemovesOnlyThatRecord(t *testing.T) {
	s := newFastRecordStore(t)
	ctx := context.Background()
	a := testAdmission()
	other := a
	other.Nonce = "0123456789abcdef0123456789abcdef"
	sha, _, err := s.WriteAdmission(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	otherSHA, _, err := s.WriteAdmission(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.DeleteAdmission(ctx, a.Namespace, a.RunID, sha); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := s.ReadAdmission(ctx, a.Namespace, a.RunID, sha); err != nil || got.Answer != RecordNone {
		t.Fatalf("the deleted record reads %+v, %v", got, err)
	}
	if got, err := s.ReadAdmission(ctx, a.Namespace, a.RunID, otherSHA); err != nil || got.Answer != RecordFound {
		t.Fatalf("the other record reads %+v, %v", got, err)
	}
	if err := s.DeleteAdmission(ctx, a.Namespace, a.RunID, "not-a-digest"); err == nil {
		t.Fatal("a delete by no digest was accepted")
	}
}

// TestAdmission_PinsSessionTagsPerAudience: an audience in the table has its
// tokens tagged, whichever identity or grant asks for it; one that is not has
// none, and a run that tags nothing has the record it always had.
func TestAdmission_PinsSessionTagsPerAudience(t *testing.T) {
	plain, _, err := EncodeAdmission(testAdmission())
	if err != nil || bytes.Contains(plain, []byte("session_tags")) {
		t.Fatalf("a run that tags nothing: %s, %v", plain, err)
	}
	a := testAdmission()
	a.Grants = map[string]GrantSelection{"deployer": {Provider: "aws", Principal: "arn:aws:iam::1:role/r", Audience: "sts.amazonaws.com"}}
	build := a.Tasks["build"]
	build.Grants, build.IdentitySlots, build.AttemptDeadline = []string{"deployer"}, 1, 330
	a.Tasks["build"] = build
	a.SessionTags = map[string]SessionTagging{"https://vault": {}, "sts.amazonaws.com": {Transitive: []string{"ork_namespace", "ork_task"}}}
	raw, _, err := EncodeAdmission(a)
	if err != nil {
		t.Fatal(err)
	}
	want := `"session_tags":{"https://vault":{},"sts.amazonaws.com":{"transitive":["ork_namespace","ork_task"]}}`
	if !bytes.Contains(raw, []byte(want)) {
		t.Errorf("the record %s does not hold %s", raw, want)
	}
}

func TestSessionTagKeys_AreSortedAndUnique(t *testing.T) {
	if !slices.IsSorted(SessionTagKeys) || len(slices.Compact(slices.Clone(SessionTagKeys))) != len(SessionTagKeys) {
		t.Fatalf("SessionTagKeys = %v", SessionTagKeys)
	}
}

// A version is the one value of a session tag an author spells, so its grammar
// must be within AWS's for a tag's value: letters, digits, spaces and
// _ . : / = + - @, at most 256 characters, not beginning "aws:".
func TestOrkflowVersions_AreValuesAWSTakes(t *testing.T) {
	aws := regexp.MustCompile(`^[A-Za-z0-9 _.:/=+@-]{1,256}$`)
	for c := range 256 {
		for _, v := range []string{string([]byte{byte(c)}), "1" + string([]byte{byte(c)})} {
			if validOrkflowComponent("version", v) == nil && (!aws.MatchString(v) || strings.Contains(v, ":")) {
				t.Errorf("a version may be %q, which AWS does not take", v)
			}
		}
	}
	if validOrkflowComponent("version", strings.Repeat("1", 257)) == nil {
		t.Error("a version may be longer than a tag's value")
	}
}
