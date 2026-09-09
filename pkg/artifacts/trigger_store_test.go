package artifacts_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// newTriggerStores returns every backend a unit test can exercise without
// infrastructure. The S3 backend needs a bucket, so it is proven by
// trigger_store_integration_test.go instead; what both share — the record
// format, the id rule, the namespace filter — is exercised here.
func newTriggerStores(t *testing.T) map[string]triggers.Store {
	t.Helper()
	local, err := artifacts.NewLocalTriggerStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	return map[string]triggers.Store{
		"local":  local,
		"memory": triggers.NewMemoryStore(),
	}
}

func mustCreate(t *testing.T, store triggers.Store, reg triggers.Registration) (triggers.Trigger, string) {
	t.Helper()
	trigger, secret, err := store.Create(context.Background(), reg)
	if err != nil {
		t.Fatalf("Create(%+v): %v", reg, err)
	}
	return trigger, secret
}

func TestTriggerStore_CreateGetListDeleteRoundTrip(t *testing.T) {
	for name, store := range newTriggerStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			created, secret, err := store.Create(ctx, triggers.Registration{
				Namespace:      "default",
				OrkflowName:    "hello-ork",
				OrkflowVersion: "0.2.0",
				Description:    "nightly refresh",
			})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			if !strings.HasPrefix(created.ID, "trg-") || len(created.ID) != len("trg-")+16 {
				t.Errorf("id = %q, want trg- followed by 16 hex characters", created.ID)
			}
			if !triggers.ValidID(created.ID) {
				t.Errorf("generated id %q is not accepted by ValidID", created.ID)
			}
			if len(secret) != 64 {
				t.Errorf("secret has %d hex characters, want 64 (32 random bytes)", len(secret))
			}
			if created.CreatedAt.IsZero() {
				t.Error("created record has no creation time")
			}

			got, found, err := store.Get(ctx, created.ID)
			if err != nil || !found {
				t.Fatalf("Get(%s) = %v, %v, %v", created.ID, got, found, err)
			}
			if got.Namespace != "default" || got.OrkflowName != "hello-ork" ||
				got.OrkflowVersion != "0.2.0" || got.Description != "nightly refresh" {
				t.Errorf("Get returned %+v, want the created coordinates", got)
			}
			if !got.CreatedAt.Equal(created.CreatedAt) {
				t.Errorf("createdAt = %s, want %s", got.CreatedAt, created.CreatedAt)
			}
			if !triggers.VerifySecret(got, secret) {
				t.Error("the stored record does not verify the secret it was created with")
			}

			listed, err := store.List(ctx, "default", "")
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(listed) != 1 || listed[0].ID != created.ID {
				t.Fatalf("List = %+v, want the created trigger", listed)
			}
			if len(listed[0].SecretHash) != 0 || len(listed[0].SecretSalt) != 0 {
				t.Error("a listing carried the secret's verification material")
			}

			deleted, err := store.Delete(ctx, created.ID, "default")
			if err != nil || !deleted {
				t.Fatalf("Delete = %v, %v; want true, nil", deleted, err)
			}
			if _, found, err = store.Get(ctx, created.ID); err != nil || found {
				t.Errorf("after delete Get = %v, %v; want absent", found, err)
			}
			if deleted, err = store.Delete(ctx, created.ID, "default"); err != nil || deleted {
				t.Errorf("deleting twice = %v, %v; want false, nil", deleted, err)
			}
		})
	}
}

func TestTriggerStore_ListFiltersByNamespaceAndOrkflow(t *testing.T) {
	for name, store := range newTriggerStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			registrations := []triggers.Registration{
				{Namespace: "default", OrkflowName: "hello-ork"},
				{Namespace: "default", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0"},
				{Namespace: "default", OrkflowName: "other-ork"},
				{Namespace: "payments", OrkflowName: "hello-ork"},
			}
			for _, reg := range registrations {
				mustCreate(t, store, reg)
			}

			cases := []struct {
				label     string
				namespace string
				orkflow   string
				want      int
			}{
				{"every trigger of a namespace", "default", "", 3},
				{"one orkflow of a namespace", "default", "hello-ork", 2},
				{"another namespace", "payments", "", 1},
				{"an orkflow with no triggers", "default", "absent-ork", 0},
				{"a namespace with no triggers", "staging", "", 0},
			}
			for _, tc := range cases {
				got, err := store.List(ctx, tc.namespace, tc.orkflow)
				if err != nil {
					t.Fatalf("%s: List(%q, %q): %v", tc.label, tc.namespace, tc.orkflow, err)
				}
				if len(got) != tc.want {
					t.Errorf("%s: List(%q, %q) returned %d triggers, want %d",
						tc.label, tc.namespace, tc.orkflow, len(got), tc.want)
				}
				for _, trigger := range got {
					if trigger.Namespace != tc.namespace {
						t.Errorf("%s: listing leaked namespace %q", tc.label, trigger.Namespace)
					}
					if tc.orkflow != "" && trigger.OrkflowName != tc.orkflow {
						t.Errorf("%s: listing leaked orkflow %q", tc.label, trigger.OrkflowName)
					}
				}
			}
		})
	}
}

func TestTriggerStore_DeleteFromAnotherNamespaceIsAbsentAndDeletesNothing(t *testing.T) {
	for name, store := range newTriggerStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			created, _ := mustCreate(t, store, triggers.Registration{
				Namespace: "payments", OrkflowName: "hello-ork",
			})
			deleted, err := store.Delete(ctx, created.ID, "default")
			if err != nil || deleted {
				t.Fatalf("Delete from another namespace = %v, %v; want false, nil", deleted, err)
			}
			if _, found, err := store.Get(ctx, created.ID); err != nil || !found {
				t.Errorf("the trigger was removed by another namespace's delete (found=%v, err=%v)", found, err)
			}
		})
	}
}

func TestTriggerStore_VerifySecretAcceptsOnlyTheIssuedToken(t *testing.T) {
	store := triggers.NewMemoryStore()
	record, secret := mustCreate(t, store, triggers.Registration{
		Namespace: "default", OrkflowName: "hello-ork",
	})
	other, otherSecret := mustCreate(t, store, triggers.Registration{
		Namespace: "default", OrkflowName: "hello-ork",
	})

	tampered := record
	tampered.SecretHash = append([]byte(nil), record.SecretHash...)
	tampered.SecretHash[0] ^= 0xff
	resalted := record
	resalted.SecretSalt = append([]byte(nil), record.SecretSalt...)
	resalted.SecretSalt[0] ^= 0xff

	cases := []struct {
		label     string
		record    triggers.Trigger
		presented string
		want      bool
	}{
		{"the issued token", record, secret, true},
		{"another trigger's token", record, otherSecret, false},
		{"an empty token", record, "", false},
		{"a token with trailing text", record, secret + "x", false},
		{"a tampered digest", tampered, secret, false},
		{"a swapped salt", resalted, secret, false},
		{"a record with no material", record.Public(), secret, false},
		{"another trigger's record", other, secret, false},
	}
	for _, tc := range cases {
		if got := triggers.VerifySecret(tc.record, tc.presented); got != tc.want {
			t.Errorf("VerifySecret(%s) = %v, want %v", tc.label, got, tc.want)
		}
	}
}

func TestTriggerStore_RejectsCoordinatesNoSubmissionCouldUse(t *testing.T) {
	for name, store := range newTriggerStores(t) {
		t.Run(name, func(t *testing.T) {
			cases := []struct {
				label string
				reg   triggers.Registration
			}{
				{"no namespace", triggers.Registration{OrkflowName: "hello-ork"}},
				{"no orkflow", triggers.Registration{Namespace: "default"}},
				{"a namespace that is a path", triggers.Registration{
					Namespace: "../secret", OrkflowName: "hello-ork"}},
				{"an upper-case orkflow", triggers.Registration{
					Namespace: "default", OrkflowName: "Hello"}},
				{"a version that is a path", triggers.Registration{
					Namespace: "default", OrkflowName: "hello-ork", OrkflowVersion: "../1.0.0"}},
			}
			for _, tc := range cases {
				if _, _, err := store.Create(context.Background(), tc.reg); err == nil {
					t.Errorf("%s: Create succeeded, want a refusal", tc.label)
				}
			}
			listed, err := store.List(context.Background(), "default", "")
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(listed) != 0 {
				t.Errorf("a refused create was stored: %+v", listed)
			}
		})
	}
}

func TestLocalTriggerStore_WritesOneFlatRecordWithoutThePlaintextSecret(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalTriggerStore(dir)
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	created, secret := mustCreate(t, store, triggers.Registration{
		Namespace: "payments", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0",
	})

	// The key layout is flat: a namespace-scoped one could not answer
	// Get(id) from the invoke route's URL, which carries no namespace.
	path := filepath.Join(dir, artifacts.TriggerPrefix, created.ID+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatal("the stored record contains the plaintext secret")
	}
	var record struct {
		ID             string `json:"id"`
		Namespace      string `json:"namespace"`
		OrkflowName    string `json:"orkflowName"`
		OrkflowVersion string `json:"orkflowVersion"`
		CreatedAt      string `json:"createdAt"`
		SecretSalt     string `json:"secretSalt"`
		SecretHash     string `json:"secretHash"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("decode record: %v (%s)", err, raw)
	}
	if record.ID != created.ID || record.Namespace != "payments" ||
		record.OrkflowName != "hello-ork" || record.OrkflowVersion != "0.2.0" {
		t.Errorf("record = %+v, want the created coordinates", record)
	}
	if record.SecretSalt == "" || record.SecretHash == "" || record.CreatedAt == "" {
		t.Errorf("record = %+v, want a salt, a digest and a creation time", record)
	}
	if record.SecretHash == record.SecretSalt {
		t.Error("the record's digest is its salt")
	}
}

func TestLocalTriggerStore_IgnoresForeignFilesAndMalformedIDs(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalTriggerStore(dir)
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	ctx := context.Background()
	created, _ := mustCreate(t, store, triggers.Registration{
		Namespace: "default", OrkflowName: "hello-ork",
	})

	// A file nothing in this package wrote must not become a trigger, and must
	// not fail the listing either.
	foreign := filepath.Join(dir, artifacts.TriggerPrefix, "notes.txt")
	if err := os.WriteFile(foreign, []byte("scratch"), 0o600); err != nil {
		t.Fatalf("write foreign file: %v", err)
	}
	listed, err := store.List(ctx, "default", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Errorf("List = %+v, want only the created trigger", listed)
	}

	for _, id := range []string{"", "trg-", "hello", "trg-nothex1234567", "../../etc/passwd"} {
		if _, found, err := store.Get(ctx, id); found || err == nil {
			t.Errorf("Get(%q) = found %v, err %v; want an invalid-id refusal", id, found, err)
		}
	}
}

func TestTriggerStore_ACorruptRecordIsAnErrorAndNeverAValidTrigger(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalTriggerStore(dir)
	if err != nil {
		t.Fatalf("NewLocalTriggerStore: %v", err)
	}
	ctx := context.Background()
	// A record whose id is well-formed but whose body is not: it must never
	// read back as a trigger with empty verification material, which would
	// authenticate nobody but would look like an ordinary absent secret.
	const id = "trg-0123456789abcdef"
	for _, body := range []string{
		`not json`,
		`{"id":"` + id + `"}`,
		`{"id":"` + id + `","namespace":"default","orkflowName":"hello-ork",` +
			`"createdAt":"2026-09-08T00:00:00Z","secretSalt":"","secretHash":""}`,
	} {
		path := filepath.Join(dir, artifacts.TriggerPrefix, id+".json")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write record: %v", err)
		}
		if trigger, found, err := store.Get(ctx, id); err == nil || found {
			t.Errorf("Get on a corrupt record = %+v, %v, %v; want an error", trigger, found, err)
		}
		if _, err := store.List(ctx, "default", ""); err == nil {
			t.Error("List over a corrupt record succeeded; a bad record must not be skipped silently")
		}
	}
}

func TestTriggerStore_EveryCreateDrawsItsOwnIDAndNeverReplacesAStoredOne(t *testing.T) {
	// A collision needs two identical 8-byte draws. What the create loop
	// protects is the consequence rather than the odds: a second trigger never
	// replaces a first one, so a token already handed out keeps working. Both
	// durable backends publish exclusively — Link for local, a conditional PUT
	// for S3 — and retry with a fresh id rather than overwriting.
	for name, store := range newTriggerStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			reg := triggers.Registration{Namespace: "default", OrkflowName: "hello-ork"}
			seen := map[string]string{}
			for i := 0; i < 16; i++ {
				trigger, secret, err := store.Create(ctx, reg)
				if err != nil {
					t.Fatalf("Create #%d: %v", i, err)
				}
				if previous, clash := seen[trigger.ID]; clash {
					t.Fatalf("Create #%d reused id %q (first secret %q)", i, trigger.ID, previous)
				}
				seen[trigger.ID] = secret
			}
			for id, secret := range seen {
				record, found, err := store.Get(ctx, id)
				if err != nil || !found {
					t.Fatalf("Get(%s) = %v, %v", id, found, err)
				}
				if !triggers.VerifySecret(record, secret) {
					t.Errorf("trigger %s no longer verifies the token it was issued", id)
				}
			}
		})
	}
}

func TestTriggerStore_UnknownBackendIsRefused(t *testing.T) {
	if _, err := artifacts.NewTriggerStore(context.Background(),
		artifacts.Config{Backend: "azure"}); err == nil {
		t.Fatal("NewTriggerStore accepted an unknown backend")
	}
	store, err := artifacts.NewTriggerStore(context.Background(),
		artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewTriggerStore(local): %v", err)
	}
	if _, _, err := store.Create(context.Background(), triggers.Registration{
		Namespace: "default", OrkflowName: "hello-ork",
	}); err != nil {
		t.Fatalf("Create through the selected backend: %v", err)
	}
}
