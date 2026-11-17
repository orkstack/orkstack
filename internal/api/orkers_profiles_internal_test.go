package api

import (
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestOrkerProfileProjection_EveryFieldIsClassified fails when a field is
// added to OrkerProfile or ProfileCordon without deciding whether the
// inventory may carry it: every field is public or control channel only,
// never neither and never both.
func TestOrkerProfileProjection_EveryFieldIsClassified(t *testing.T) {
	for _, tc := range []struct {
		message       protoreflect.MessageDescriptor
		public, local map[string]bool
	}{
		{(&orkstackv1.OrkerProfile{}).ProtoReflect().Descriptor(), orkerProfilePublic, orkerProfileControlOnly},
		{(&orkstackv1.ProfileCordon{}).ProtoReflect().Descriptor(), profileCordonPublic, profileCordonControlOnly},
	} {
		fields := tc.message.Fields()
		for i := 0; i < fields.Len(); i++ {
			name := string(fields.Get(i).Name())
			if tc.public[name] == tc.local[name] {
				t.Errorf("%s.%s is classified %v public and %v control-channel-only; classify it once",
					tc.message.Name(), name, tc.public[name], tc.local[name])
			}
		}
		for name := range tc.public {
			if fields.ByName(protoreflect.Name(name)) == nil {
				t.Errorf("%s has no field %s", tc.message.Name(), name)
			}
		}
		for name := range tc.local {
			if fields.ByName(protoreflect.Name(name)) == nil {
				t.Errorf("%s has no field %s", tc.message.Name(), name)
			}
		}
	}
}

func TestOrkerRow_ProjectsProfilesWithoutControlChannelFields(t *testing.T) {
	since := timestamppb.New(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
	described := &orkstackv1.DescribeOrkerResult{
		OrkerId:  "o1",
		Cordoned: true,
		Profiles: []*orkstackv1.OrkerProfile{{
			Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU, Units: 2, UsableUnits: 1, BoundUnits: 1,
			Revision: "mesa-25.2", IdentityDigest: "abc",
		}},
		ProfileCordons: []*orkstackv1.ProfileCordon{{
			Cause: orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE, Profile: "amd",
			Reason: orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD,
			Since:  since, Detail: "held by pid 4242 (orker)",
		}},
	}
	row := orkerRow(engine.OrkerPresence{Identity: "o1"}, described, nil)

	wantProfile := &orkstackv1.OrkerProfile{
		Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU, Units: 2, UsableUnits: 1, BoundUnits: 1,
	}
	if len(row.GetProfiles()) != 1 || !proto.Equal(row.GetProfiles()[0], wantProfile) {
		t.Errorf("profiles = %v, want %v", row.GetProfiles(), wantProfile)
	}
	wantCordon := &orkstackv1.ProfileCordon{
		Cause: orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE, Profile: "amd",
		Reason: orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD, Since: since,
	}
	if len(row.GetProfileCordons()) != 1 || !proto.Equal(row.GetProfileCordons()[0], wantCordon) {
		t.Errorf("profile cordons = %v, want %v", row.GetProfileCordons(), wantCordon)
	}
	// The projection copies: the Orker's answer is untouched.
	if described.GetProfiles()[0].GetRevision() != "mesa-25.2" || described.GetProfileCordons()[0].GetDetail() == "" {
		t.Error("projection changed the described answer")
	}
}

// A field a newer Orker adds to OrkerProfile reaches this engine as an
// unknown field; the inventory must not carry it either.
func TestOrkerRow_DropsUnknownProfileFields(t *testing.T) {
	profile := &orkstackv1.OrkerProfile{Name: "amd"}
	// Field 99, varint 1: what a newer Orker's addition looks like here.
	profile.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	cause := &orkstackv1.ProfileCordon{Profile: "amd"}
	cause.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	row := orkerRow(engine.OrkerPresence{Identity: "o1"}, &orkstackv1.DescribeOrkerResult{
		OrkerId: "o1", Profiles: []*orkstackv1.OrkerProfile{profile}, ProfileCordons: []*orkstackv1.ProfileCordon{cause},
	}, nil)
	if len(row.GetProfiles()[0].ProtoReflect().GetUnknown()) != 0 || len(row.GetProfileCordons()[0].ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("the inventory carries a field this engine never classified")
	}
}
