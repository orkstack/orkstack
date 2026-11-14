package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// FleetCheckInterval is how often an enforcing engine looks for an engine
// that does not enforce.
const FleetCheckInterval = time.Minute

// FleetReport is the fleet check of an enforcing engine: each minute it
// asks the cluster for a fresh engine that does not advertise
// authn-enforce, and logs at error, counts and shows on the Access page
// every one it sees. It refuses nothing: a poller stays listed for minutes
// after its process is gone, so a refusal would turn every deploy into an
// outage (docs/ACCESS.md, "Every engine enforces").
type FleetReport struct {
	Checker engine.CapabilityChecker
	Logger  *slog.Logger
	Now     func() time.Time
	// Unenforced is told how many engines do not enforce, at each check.
	Unenforced func(ctx context.Context, n int)

	mu      sync.Mutex
	engines []*orkstackv1.EngineSeen
}

// Check runs one fleet check.
func (f *FleetReport) Check(ctx context.Context) {
	if f == nil || f.Checker == nil {
		return
	}
	now := time.Now
	if f.Now != nil {
		now = f.Now
	}
	logger := f.Logger
	if logger == nil {
		logger = slog.Default()
	}
	missing, err := f.Checker.MissingCapability(ctx, engine.CapabilityAuthnEnforce, engine.ScopeWorkflow)
	if err != nil {
		logger.Warn("fleet check: the engines could not be listed", "error", err)
		return
	}
	seen := make([]*orkstackv1.EngineSeen, 0, len(missing))
	at := timestamppb.New(now())
	for _, identity := range missing {
		if identity == "" {
			identity = "(an engine that sent no identity)"
		}
		seen = append(seen, &orkstackv1.EngineSeen{Id: identity, Mode: "not enforce", LastSeen: at})
	}
	if len(missing) > 0 {
		logger.Error("fleet check: engines of this deployment do not enforce authentication; set auth.mode: enforce on every replica",
			"engines", missing)
	}
	if f.Unenforced != nil {
		f.Unenforced(ctx, len(missing))
	}
	f.mu.Lock()
	f.engines = seen
	f.mu.Unlock()
}

// Run checks every FleetCheckInterval until ctx ends.
func (f *FleetReport) Run(ctx context.Context) {
	t := time.NewTicker(FleetCheckInterval)
	defer t.Stop()
	for {
		f.Check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Engines are the engines the last check saw that do not enforce.
func (f *FleetReport) Engines() []*orkstackv1.EngineSeen {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*orkstackv1.EngineSeen(nil), f.engines...)
}
