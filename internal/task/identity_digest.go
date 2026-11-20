package task

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// IdentitySpecVersion is the newest projection this build computes, the
// version an admission pins for a run's tasks.
const IdentitySpecVersion uint32 = 1

// IdentitySpecMembers are version 1's eleven members of a task's spec
// digest, in the order the task's digest hashes them. devices, the device
// profiles the task names, is the eleventh (the user's decision A1 of
// 2026-11-20): appended after the ten, so their order is unchanged.
var IdentitySpecMembers = [11]string{
	"image", "command", "env", "with", "secrets", "network", "run_as_orker_user", "identities", "credentials", "grants",
	"devices",
}

// IdentitySpec is the projection of a task that its identity's mint binds
// to: only fields every release copies verbatim from the definition and the
// admission into the task's input, and nothing computed, defaulted or
// rendered. A field not here is not hashed, so a field a later release adds
// changes no version-1 digest.
type IdentitySpec struct {
	ImagePinned    string
	Command        []string
	Env            map[string]string
	With           *structpb.Struct
	Secrets        []*orkstackv1.SecretRef
	Network        orkstackv1.NetworkMode
	RunAsOrkerUser bool
	Identities     []*orkstackv1.IdentityRef
	Credentials    []*orkstackv1.CredentialRef
	// Devices are the device profiles the task names, in its order: the
	// definition's names, and the names of the profiles the input carries.
	Devices []string
}

// IdentitySpecOfInput is the projection of the input an Orker received.
func IdentitySpecOfInput(in *orkstackv1.ExecuteTaskInput) IdentitySpec {
	return IdentitySpec{
		ImagePinned: in.GetImage().GetPinned(), Command: in.GetCommand(), Env: in.GetEnv(), With: in.GetWith(),
		Secrets: in.GetSecrets(), Network: in.GetNetwork(), RunAsOrkerUser: in.GetRunAsOrkerUser(),
		Identities: in.GetIdentity().GetIdentities(), Credentials: in.GetIdentity().GetCredentials(),
		Devices: profileNames(in.GetDevices()),
	}
}

// IdentitySpecOfTask is the projection of a task as its definition holds
// it: what the engine hashes at submit into the task's admission, and what
// IdentitySpecOfInput reproduces from the input the interpreter builds.
func IdentitySpecOfTask(t *orkstackv1.Task) IdentitySpec {
	return IdentitySpec{
		ImagePinned: t.GetImage().GetPinned(), Command: t.GetCommand(), Env: t.GetEnv(), With: t.GetWith(),
		Secrets: t.GetSecrets(), Network: t.GetNetwork(), RunAsOrkerUser: t.GetRunAsOrkerUser(),
		Identities: t.GetIdentities(), Credentials: t.GetCredentials(), Devices: t.GetDevices(),
	}
}

// IdentitySpecDigest is the task's spec digest at version: each member is
// the SHA-256 of its canonical JSON (internal/stablejson), and the task's
// digest is the SHA-256 of the canonical JSON of the version followed by the
// ten member digests in order. members maps each member to its digest, which
// the mint names when they differ. An unknown version is refused: an Orker
// that does not compute it says so rather than guessing.
func IdentitySpecDigest(version uint32, s IdentitySpec) (digest string, members map[string]string, err error) {
	if version != 1 {
		return "", nil, fmt.Errorf("task spec digest version %d is not one this build computes (it computes 1)", version)
	}
	values, err := identitySpecV1(s)
	if err != nil {
		return "", nil, err
	}
	members = make(map[string]string, len(IdentitySpecMembers))
	ordered := []any{version}
	for i, name := range IdentitySpecMembers {
		d, err := digestOf(values[i])
		if err != nil {
			return "", nil, fmt.Errorf("task spec digest member %s: %w", name, err)
		}
		members[name] = d
		ordered = append(ordered, d)
	}
	digest, err = digestOf(ordered)
	if err != nil {
		return "", nil, err
	}
	return digest, members, nil
}

func digestOf(v any) (string, error) {
	b, err := stablejson.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// identitySpecV1 is version 1's ten member values, in member order. An
// absent list is an empty one; an unset network is null; enums are their
// numbers, which the proto never reuses.
func identitySpecV1(s IdentitySpec) ([11]any, error) {
	env := make([]map[string]string, 0, len(s.Env))
	for _, k := range sortedKeys(s.Env) {
		env = append(env, map[string]string{"name": k, "value": s.Env[k]})
	}
	var with any
	if s.With != nil {
		with = s.With.AsMap()
	}
	secrets := make([]map[string]any, 0, len(s.Secrets))
	for _, sec := range s.Secrets {
		secrets = append(secrets, map[string]any{"name": sec.GetName(), "source": sec.GetSource(),
			"grant": sec.GetGrant(), "ciphertext": sec.GetCiphertext(), "unmasked": sec.GetUnmasked()})
	}
	var network any
	if s.Network != orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED {
		network = int32(s.Network)
	}
	identities := make([]map[string]any, 0, len(s.Identities))
	for _, id := range s.Identities {
		identities = append(identities, map[string]any{"name": id.GetName(), "audience": id.GetAudience(),
			"ttl_seconds": id.GetTtlSeconds(), "refresh": int32(id.GetRefresh())})
	}
	credentials := make([]map[string]any, 0, len(s.Credentials))
	for _, c := range s.Credentials {
		params := map[string]string{}
		for k, v := range c.GetParams() {
			params[k] = v
		}
		credentials = append(credentials, map[string]any{"name": c.GetName(), "grant": c.GetGrant(),
			"path": c.GetPath(), "params": params, "refresh": int32(c.GetRefresh())})
	}
	command := s.Command
	if command == nil {
		command = []string{}
	}
	devices := s.Devices
	if devices == nil {
		devices = []string{}
	}
	return [11]any{
		map[string]string{"pinned": s.ImagePinned}, command, env, with, secrets, network, s.RunAsOrkerUser,
		identities, credentials, []any{}, devices,
	}, nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// profileNames is the names of the profiles an input carries, in order.
func profileNames(refs []*orkstackv1.ProfileRef) []string {
	if len(refs) == 0 {
		return nil
	}
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.GetName())
	}
	return out
}
