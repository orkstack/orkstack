package api_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fakeRetentionAdmin stands in for engine.TemporalRunReader's admin seam —
// the cluster's own backstop TTL, with no cluster in this process. It
// records what it was asked so a test can prove the controller asked at all
// (or, for the refused-before-any-call case, that it did NOT).
type fakeRetentionAdmin struct {
	current  time.Duration
	readErr  error
	confirm  time.Duration
	writeErr error

	readNamespaces  []string
	writeNamespaces []string
	writeRequests   []time.Duration
}

func (f *fakeRetentionAdmin) NamespaceRetention(_ context.Context, namespace string) (time.Duration, error) {
	f.readNamespaces = append(f.readNamespaces, namespace)
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.current, nil
}

func (f *fakeRetentionAdmin) SetNamespaceRetention(
	_ context.Context,
	namespace string,
	retention time.Duration,
) (time.Duration, error) {
	f.writeNamespaces = append(f.writeNamespaces, namespace)
	f.writeRequests = append(f.writeRequests, retention)
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	f.current = f.confirm
	return f.confirm, nil
}

var _ archive.RetentionAdmin = (*fakeRetentionAdmin)(nil)

// fakePolicyStore is an in-memory artifacts.RetentionPolicyStore.
type fakePolicyStore struct {
	policy *orkstackv1.RetentionPolicy
	getErr error
}

func (f *fakePolicyStore) Get(context.Context) (*orkstackv1.RetentionPolicy, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.policy == nil {
		return nil, artifacts.ErrNotFound
	}
	return f.policy, nil
}

func (f *fakePolicyStore) Put(_ context.Context, p *orkstackv1.RetentionPolicy, _ artifacts.PutConditions) (*orkstackv1.RetentionPolicy, error) {
	f.policy = p
	return p, nil
}

// retentionFixture wires a Service with a fake reader (satisfying the router
// binding's other seams) and a RetentionController built over admin and a
// policy store pre-set to maxRetention — exactly how `ork` wires
// Config.Retention (cmd/ork/main.go).
func retentionFixture(t *testing.T, admin *fakeRetentionAdmin, maxRetention time.Duration) *api.Service {
	t.Helper()
	admin.current = maxRetention + 24*time.Hour // backstop already reconciled; Reconcile no-ops
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(maxRetention)}}
	ctrl := archive.NewRetentionController(policies, admin, "ork-main", 24*time.Hour, nil)
	return api.NewService(api.Config{
		Reader:    &fakeReader{},
		Retention: ctrl,
	})
}

// TestGetRetentionPolicy_ReadsTheStoredMaximum is the successful-read case.
func TestGetRetentionPolicy_ReadsTheStoredMaximum(t *testing.T) {
	svc := retentionFixture(t, &fakeRetentionAdmin{}, 72*time.Hour)

	resp, err := svc.GetRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.GetRetentionPolicyRequest{}))
	if err != nil {
		t.Fatalf("GetRetentionPolicy: %v", err)
	}
	if got := resp.Msg.GetPolicy().GetMaxRunRetention().AsDuration(); got != 72*time.Hour {
		t.Errorf("max_run_retention = %s, want 72h", got)
	}
}

// TestGetRetentionPolicy_ReportsWhetherNamespaceRetentionIsEnforced proves
// RetentionEnforced flows straight through from Config into the response.
func TestGetRetentionPolicy_ReportsWhetherNamespaceRetentionIsEnforced(t *testing.T) {
	admin := &fakeRetentionAdmin{current: 96 * time.Hour}
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}}
	ctrl := archive.NewRetentionController(policies, admin, "ork-main", 24*time.Hour, nil)
	svc := api.NewService(api.Config{Reader: &fakeReader{}, Retention: ctrl, RetentionEnforced: true})

	resp, err := svc.GetRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.GetRetentionPolicyRequest{}))
	if err != nil {
		t.Fatalf("GetRetentionPolicy: %v", err)
	}
	if !resp.Msg.GetNamespaceRetentionEnforced() {
		t.Error("namespace_retention_enforced = false, want true")
	}
}

// TestGetRetentionPolicy_NoStoredPolicyIsFailedPrecondition proves an engine
// that never ran Ensure (or whose store is empty) answers honestly rather
// than fabricating a default.
func TestGetRetentionPolicy_NoStoredPolicyIsFailedPrecondition(t *testing.T) {
	admin := &fakeRetentionAdmin{current: 96 * time.Hour}
	ctrl := archive.NewRetentionController(&fakePolicyStore{}, admin, "ork-main", 24*time.Hour, nil)
	svc := api.NewService(api.Config{Reader: &fakeReader{}, Retention: ctrl})

	_, err := svc.GetRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.GetRetentionPolicyRequest{}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", connect.CodeOf(err))
	}
}

// TestGetRetentionPolicy_UnavailableStoreIsNotSwallowed proves a store
// failure surfaces rather than answering a fabricated zero value.
func TestGetRetentionPolicy_UnavailableStoreIsNotSwallowed(t *testing.T) {
	boom := errors.New("store unreachable")
	admin := &fakeRetentionAdmin{current: 96 * time.Hour}
	ctrl := archive.NewRetentionController(&fakePolicyStore{getErr: boom}, admin, "ork-main", 24*time.Hour, nil)
	svc := api.NewService(api.Config{Reader: &fakeReader{}, Retention: ctrl})

	_, err := svc.GetRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.GetRetentionPolicyRequest{}))
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want it to wrap %v", err, boom)
	}
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("code = %v, want Unavailable", connect.CodeOf(err))
	}
}

// TestUpdateRetentionPolicy_WritesThroughAndReconcilesTheBackstop is the
// successful-write case: the stored maximum changes, and the underlying
// cluster's own TTL is kept at max+margin.
func TestUpdateRetentionPolicy_WritesThroughAndReconcilesTheBackstop(t *testing.T) {
	admin := &fakeRetentionAdmin{}
	svc := retentionFixture(t, admin, 72*time.Hour)

	resp, err := svc.UpdateRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{
			MaxRunRetention: durationpb.New(168 * time.Hour),
		}))
	if err != nil {
		t.Fatalf("UpdateRetentionPolicy: %v", err)
	}
	if got := resp.Msg.GetPolicy().GetMaxRunRetention().AsDuration(); got != 168*time.Hour {
		t.Errorf("max_run_retention = %s, want 168h", got)
	}
	if len(admin.writeRequests) != 1 || admin.writeRequests[0] != 192*time.Hour {
		t.Errorf("backstop reconcile calls = %v, want [192h] (168h + 24h margin)", admin.writeRequests)
	}
}

// TestUpdateRetentionPolicy_RefusesANonPositiveDurationBeforeAnyCall proves a
// non-positive duration never reaches the store or the cluster.
func TestUpdateRetentionPolicy_RefusesANonPositiveDurationBeforeAnyCall(t *testing.T) {
	for name, retention := range map[string]*durationpb.Duration{
		"zero":     durationpb.New(0),
		"negative": durationpb.New(-time.Hour),
		"unset":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			admin := &fakeRetentionAdmin{}
			svc := retentionFixture(t, admin, 72*time.Hour)

			_, err := svc.UpdateRetentionPolicy(context.Background(),
				connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{MaxRunRetention: retention}))
			if err == nil {
				t.Fatal("UpdateRetentionPolicy succeeded, want a refusal")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", connect.CodeOf(err))
			}
			if len(admin.writeRequests) != 0 {
				t.Errorf("SetNamespaceRetention was called with %v; a non-positive duration must never leave the process",
					admin.writeRequests)
			}
		})
	}
}

// TestUpdateRetentionPolicy_RefusesAMalformedOrOverflowingDurationBeforeAnyCall
// is the round-trip-safety half of validation, distinct from the plain
// non-positive check above.
//
// durationpb.Duration.AsDuration performs SATURATING conversion on overflow
// rather than erroring: a duration of 10,000 years (315,576,000,000
// seconds) is a perfectly VALID protobuf Duration — CheckValid accepts it —
// but silently clamps to math.MaxInt64 nanoseconds (~292 years) once
// converted to a Go time.Duration. This test proves that specific case is
// refused, alongside protobuf-shape violations AsDuration would otherwise
// silently misinterpret rather than reject: an out-of-range nanos field, and
// a seconds/nanos sign mismatch.
func TestUpdateRetentionPolicy_RefusesAMalformedOrOverflowingDurationBeforeAnyCall(t *testing.T) {
	for name, retention := range map[string]*durationpb.Duration{
		"overflows time.Duration but is a valid protobuf Duration (10,000 years)": {
			Seconds: 315576000000, // 10,000yr * 365.25d/yr * 24h/d * 60m/h * 60s/m — durationpb's own max.
		},
		"nanos out of range": {
			Seconds: 1,
			Nanos:   1_000_000_000, // must be strictly less than 1e9.
		},
		"seconds and nanos have different signs": {
			Seconds: 1,
			Nanos:   -1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			admin := &fakeRetentionAdmin{}
			svc := retentionFixture(t, admin, 72*time.Hour)

			_, err := svc.UpdateRetentionPolicy(context.Background(),
				connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{MaxRunRetention: retention}))
			if err == nil {
				t.Fatal("UpdateRetentionPolicy succeeded, want a refusal")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", connect.CodeOf(err))
			}
			if len(admin.writeRequests) != 0 {
				t.Errorf(
					"SetNamespaceRetention was called with %v; a malformed or overflowing duration must never leave the process",
					admin.writeRequests)
			}
		})
	}
}

// TestUpdateRetentionPolicy_OverflowNeverReachesTheClusterWithADifferentValue
// is the precise regression this guards: without the round-trip check, the
// 10,000-year request above would NOT be refused, and the fake would
// receive a clamped ~292-year value instead of the ~10,000 years the caller
// asked for — a silent value substitution, not a rejection.
func TestUpdateRetentionPolicy_OverflowNeverReachesTheClusterWithADifferentValue(t *testing.T) {
	admin := &fakeRetentionAdmin{}
	svc := retentionFixture(t, admin, 72*time.Hour)
	requested := &durationpb.Duration{Seconds: 315576000000}
	clampedIfUnguarded := requested.AsDuration()
	if clampedIfUnguarded != time.Duration(math.MaxInt64) {
		t.Fatalf("test assumption broken: AsDuration(10,000yr) = %s, want the saturated math.MaxInt64 (%s); "+
			"round-trip guard may be moot", clampedIfUnguarded, time.Duration(math.MaxInt64))
	}

	_, err := svc.UpdateRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{MaxRunRetention: requested}))
	if err == nil {
		t.Fatal("UpdateRetentionPolicy succeeded, want a refusal")
	}
	if len(admin.writeRequests) != 0 {
		t.Fatalf("SetNamespaceRetention was called with %v, want no call at all — "+
			"a lossy check would instead have called it with the clamped %s, not the requested 10,000 years",
			admin.writeRequests, clampedIfUnguarded)
	}
}

// TestUpdateRetentionPolicy_LoweringBelowAnExplicitNamespaceValueIsRefused is
// the validation the whole feature exists to guarantee: the ceiling can never
// drop below a namespace's own explicit run_retention while that namespace
// still asks for more.
func TestUpdateRetentionPolicy_LoweringBelowAnExplicitNamespaceValueIsRefused(t *testing.T) {
	admin := &fakeRetentionAdmin{}
	admin.current = 96 * time.Hour
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}}
	ctrl := archive.NewRetentionController(policies, admin, "ork-main", 24*time.Hour, nil)
	registry := newFakeRegistry(&orkstackv1.Namespace{Name: "payments", RunRetention: durationpb.New(48 * time.Hour)})
	svc := api.NewService(api.Config{Reader: &fakeReader{}, Retention: ctrl, Namespaces: registry})

	_, err := svc.UpdateRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{
			MaxRunRetention: durationpb.New(24 * time.Hour),
		}))
	if err == nil {
		t.Fatal("UpdateRetentionPolicy succeeded, want a refusal naming the payments namespace")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", connect.CodeOf(err))
	}
	if len(admin.writeRequests) != 0 {
		t.Errorf("the backstop was reconciled before the namespace check refused the write: %v", admin.writeRequests)
	}
}

// TestUpdateRetentionPolicy_LoweringToExactlyAnInheritedNamespacesCeilingSucceeds
// proves a namespace that inherits (sets no run_retention of its own) never
// blocks a lower ceiling — only an EXPLICIT value above the new maximum does.
func TestUpdateRetentionPolicy_LoweringWithOnlyInheritingNamespacesSucceeds(t *testing.T) {
	admin := &fakeRetentionAdmin{}
	admin.current = 96 * time.Hour
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}}
	ctrl := archive.NewRetentionController(policies, admin, "ork-main", 24*time.Hour, nil)
	registry := newFakeRegistry(&orkstackv1.Namespace{Name: "payments"}) // inherits, sets nothing
	svc := api.NewService(api.Config{Reader: &fakeReader{}, Retention: ctrl, Namespaces: registry})

	resp, err := svc.UpdateRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{
			MaxRunRetention: durationpb.New(24 * time.Hour),
		}))
	if err != nil {
		t.Fatalf("UpdateRetentionPolicy: %v", err)
	}
	if got := resp.Msg.GetPolicy().GetMaxRunRetention().AsDuration(); got != 24*time.Hour {
		t.Errorf("max_run_retention = %s, want 24h", got)
	}
}

// TestUpdateRetentionPolicy_ARejectedBackstopWriteIsSurfaced proves a
// rejected backstop reconcile surfaces rather than a fabricated success.
func TestUpdateRetentionPolicy_ARejectedBackstopWriteIsSurfaced(t *testing.T) {
	rejection := serviceerror.NewInvalidArgument("retention period is below the cluster's configured minimum")
	admin := &fakeRetentionAdmin{writeErr: rejection}
	svc := retentionFixture(t, admin, 72*time.Hour)

	_, err := svc.UpdateRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{
			MaxRunRetention: durationpb.New(168 * time.Hour),
		}))
	if err == nil {
		t.Fatal("UpdateRetentionPolicy succeeded, want the cluster's rejection surfaced")
	}
	if !errors.Is(err, rejection) {
		t.Errorf("err = %v, want it to wrap %v", err, rejection)
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (the cluster's own classification)", connect.CodeOf(err))
	}
}

// TestRetentionPolicy_WithNoControllerAnswersUnimplemented is what an engine
// that never wired Config.Retention honestly knows: neither RPC fabricates a
// value.
func TestRetentionPolicy_WithNoControllerAnswersUnimplemented(t *testing.T) {
	svc := api.NewService(api.Config{Reader: &fakeReader{}})

	if _, err := svc.GetRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.GetRetentionPolicyRequest{})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("GetRetentionPolicy code = %v, want Unimplemented", connect.CodeOf(err))
	}
	if _, err := svc.UpdateRetentionPolicy(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateRetentionPolicyRequest{
			MaxRunRetention: durationpb.New(time.Hour),
		})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("UpdateRetentionPolicy code = %v, want Unimplemented", connect.CodeOf(err))
	}
}
