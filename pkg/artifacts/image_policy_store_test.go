package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

func TestImagePolicyDocument_RoundTripsThroughProtojson(t *testing.T) {
	policy := &orkstackv1.ImagePolicy{
		RequireDigest:     true,
		AllowedRegistries: []string{"localhost:5001/orkstack/", "ghcr.io/acme/"},
		SigningKeys:       map[string]string{"staging": task.FixedSigningKeyPEM, "release": task.FixedSigningKeyPEM},
		UpdatedAt:         timestamppb.New(time.Date(2026, 9, 14, 11, 2, 44, 0, time.UTC)),
	}
	document, err := marshalImagePolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	text := string(document)
	if !strings.HasPrefix(text, "{\n  \"") || !strings.HasSuffix(text, "}\n") {
		t.Fatalf("document is not indented protojson:\n%s", text)
	}
	// allowed_registries keep the order given; the map is emitted sorted.
	if strings.Index(text, "localhost:5001") > strings.Index(text, "ghcr.io/acme") {
		t.Fatalf("allowed_registries reordered:\n%s", text)
	}
	if strings.Index(text, `"release"`) > strings.Index(text, `"staging"`) {
		t.Fatalf("signing_keys not sorted:\n%s", text)
	}
	back, err := unmarshalImagePolicy(document)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(back, policy) {
		t.Fatalf("round trip differs:\n%v\n%v", back, policy)
	}
	withUnknown := strings.Replace(text, "{\n", "{\n  \"requireSomethingNewer\": true,\n", 1)
	if _, err := unmarshalImagePolicy([]byte(withUnknown)); err != nil {
		t.Fatalf("an unknown field from a newer engine must be discarded: %v", err)
	}
}

func TestS3ImagePolicyStore_PutRefusesABadEntryAndNamesIt(t *testing.T) {
	double := newS3Double(t)
	store, err := NewS3ImagePolicyStore(context.Background(), double.config())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		policy *orkstackv1.ImagePolicy
		names  string
	}{
		{"prefix", &orkstackv1.ImagePolicy{AllowedRegistries: []string{"acme/"}}, `"acme/"`},
		{"key", &orkstackv1.ImagePolicy{SigningKeys: map[string]string{"release": "not a pem"}}, `signing key "release"`},
		{"key-name", &orkstackv1.ImagePolicy{SigningKeys: map[string]string{"Release": task.FixedSigningKeyPEM}}, `"Release"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := store.Put(context.Background(), c.policy, PutConditions{})
			if !errors.Is(err, ErrInvalidNamespace) || !strings.Contains(err.Error(), c.names) {
				t.Fatalf("Put = %v, want ErrInvalidNamespace naming %s", err, c.names)
			}
		})
	}
	if puts, _, _ := double.counts(); puts != 0 {
		t.Fatalf("a refused policy reached the bucket: %d puts", puts)
	}
	if _, err := store.Get(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get before any write = %v, want ErrNotFound", err)
	}
	stored, err := store.Put(context.Background(), &orkstackv1.ImagePolicy{
		AllowedRegistries: []string{"ghcr.io/acme/"},
		SigningKeys:       map[string]string{"release": task.FixedSigningKeyPEM},
	}, PutConditions{})
	if err != nil {
		t.Fatal(err)
	}
	if stored.GetUpdatedAt() == nil {
		t.Fatal("store did not stamp updated_at")
	}
	got, err := store.Get(context.Background())
	if err != nil || !proto.Equal(got, stored) {
		t.Fatalf("Get = %v, %v; want %v", got, err, stored)
	}
	if _, ok := double.object(ImagePolicyKey); !ok {
		t.Fatalf("no object at %s", ImagePolicyKey)
	}
}

func TestValidNamespaceSigner_RefusesABadSignerAndPassesAnUnknownKey(t *testing.T) {
	err := ValidNamespaceSigner(&orkstackv1.Namespace{Name: "prod", RequireSignedBy: "notary:x"})
	if !errors.Is(err, ErrInvalidNamespace) || !strings.Contains(err.Error(), `"notary:x"`) ||
		!strings.Contains(err.Error(), `namespace "prod"`) {
		t.Fatalf("notary:x = %v", err)
	}
	// Key existence is the API's: the document rule only checks the spelling.
	if err := ValidNamespaceSigner(&orkstackv1.Namespace{Name: "prod", RequireSignedBy: "cosign:acme-release"}); err != nil {
		t.Fatalf("cosign:acme-release = %v", err)
	}
	if err := ValidNamespaceSigner(&orkstackv1.Namespace{Name: "prod"}); err != nil {
		t.Fatalf("unset = %v", err)
	}
}
