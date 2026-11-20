package orker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
)

// identityMintFunc asks the engine for an attempt's identity tokens.
type identityMintFunc func(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error)

// NewIdentityMinter is the mint an Orker's attempts share: the engine
// client's, with at most maxConcurrent mints in flight at once
// (orker.identity.max_concurrent). A mint that waits for its turn ends with
// its context.
func NewIdentityMinter(engine *EngineClient, maxConcurrent int) identityMintFunc {
	slots := make(chan struct{}, max(maxConcurrent, 1))
	return func(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		defer func() { <-slots }()
		return engine.Mint(ctx, req)
	}
}

// SetIdentityMint gives the dispatcher the mint its attempts ask; an Orker
// that reaches no engine has none, and refuses a task that holds an
// identity (IDENTITY_NOT_CONFIGURED).
func (d *OCIDispatcher) SetIdentityMint(mint identityMintFunc) { d.identityMint = mint }

// identityRequest is the mint request of one attempt: the attempt, its item,
// the task's spec digest and its members as this Orker computes them from its
// input, the admission and both scopes, and each identity's name and
// audience. The request names no Orker: the engine reads it from the
// credential.
func identityRequest(in *orkstackv1.ExecuteTaskInput, attempt int32) (*orkstackv1.MintAttemptTokensRequest, *Failure) {
	idc := in.GetIdentity()
	digest, members, err := task.IdentitySpecDigest(idc.GetDigestVersion(), task.IdentitySpecOfInput(in))
	if err != nil {
		return nil, FailCause(identity.ReasonUnavailable, err, "task %q: %v", in.GetTaskId(), err)
	}
	req := &orkstackv1.MintAttemptTokensRequest{
		Namespace: in.GetNamespace(), RunId: in.GetRunId(), TaskId: in.GetTaskId(), Attempt: attempt,
		TaskSpecSha256: digest, SpecMembers: members, AdmissionSha256: idc.GetAdmissionSha256(),
		ResumeScope: idc.GetResumeScope(), RootScope: idc.GetRootScope(),
	}
	if each := in.GetEach(); each != nil {
		item := each.GetIndex()
		req.Item = &item
	}
	for _, id := range idc.GetIdentities() {
		req.Tokens = append(req.Tokens, &orkstackv1.TokenRequest{Name: id.GetName(), Audience: id.GetAudience()})
	}
	return req, nil
}

// identityAttempt is one attempt's identities: the directory bound at
// /orkstack/identity, the attempt's own directory that holds it, and the
// refresher that keeps them fresh.
type identityAttempt struct {
	// base is the attempt's own directory in the identity storage, which
	// ownBase says is the attempt's to remove; otherwise it is the
	// workspace root, which the workspace removes.
	base      string
	ownBase   bool
	dir       string
	refresher *identityRefresher

	stopOnce sync.Once
	cancel   context.CancelFunc
	done     chan struct{}
	failure  *Failure
}

// setupIdentities makes the attempt's identity directory under the storage
// the boot chose (the attempt's own root when it chose the work root), and
// writes its first tokens: step 6a, after the image and before the render,
// so a memo hit never mints. A failure leaves nothing.
func (d *OCIDispatcher) setupIdentities(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32, wsRoot string, mask *attemptMask) (*identityAttempt, *Failure) {
	if d.identityMint == nil || d.identityConfig == nil {
		return nil, Fail(ReasonIdentityNotConfigured, "task %q holds an identity and this Orker reaches no engine (orker.engine.url)", in.GetTaskId())
	}
	req, f := identityRequest(in, attempt)
	if f != nil {
		return nil, f
	}
	ids := &identityAttempt{base: wsRoot}
	choice := d.identityStorage.Load()
	if choice == nil {
		d.logger().Warn("no identity storage was chosen at boot; this attempt's identity files go under its work root",
			"run", in.GetRunId(), "task", in.GetTaskId())
	}
	if choice != nil && choice.Dir != "" {
		d.identityDirMu.Lock()
		own, made, err := ensureOwnIdentityDir(choice.Dir, d.OrkerID, false)
		d.identityDirMu.Unlock()
		if made {
			d.logger().Warn("this Orker's identity directory was gone and was made again; something on the host cleared it",
				"dir", own)
		}
		if err != nil {
			d.logger().Warn("this Orker's identity directory is not usable", "run", in.GetRunId(), "task", in.GetTaskId(), "error", err)
			return nil, FailCause(ReasonWorkdirFailed, err, "task %q: this Orker's identity directory is not usable: %s", in.GetTaskId(), errorKind(err))
		}
		ids.base, ids.ownBase = filepath.Join(own, filepath.Base(wsRoot)), true
		if err := os.Mkdir(ids.base, 0o700); err != nil {
			return nil, FailCause(ReasonWorkdirFailed, err, "task %q: the attempt's identity directory could not be made: %s", in.GetTaskId(), errorKind(err))
		}
	}
	ids.dir = filepath.Join(ids.base, "identity")
	if f := d.writeFirstIdentities(ctx, in, req, ids, mask); f != nil {
		if err := ids.remove(); err != nil {
			d.logger().Warn("an attempt's identity directory could not be removed after its setup failed",
				"run", in.GetRunId(), "task", in.GetTaskId(), "path", ids.base, "error", err)
		}
		return nil, f
	}
	return ids, nil
}

func (d *OCIDispatcher) writeFirstIdentities(ctx context.Context, in *orkstackv1.ExecuteTaskInput, req *orkstackv1.MintAttemptTokensRequest,
	ids *identityAttempt, mask *attemptMask) *Failure {
	if err := os.Mkdir(ids.dir, 0o700); err != nil {
		return FailCause(ReasonWorkdirFailed, err, "task %q: the identity directory could not be made: %s", in.GetTaskId(), errorKind(err))
	}
	files := make(map[string]*identityFiles, len(in.GetIdentity().GetIdentities()))
	required := make(map[string]bool, len(files))
	for _, id := range in.GetIdentity().GetIdentities() {
		f, err := newIdentityFiles(ids.dir, id.GetName())
		if err != nil {
			return FailCause(ReasonWorkdirFailed, err, "task %q: identity %q's directory could not be made: %s", in.GetTaskId(), id.GetName(), errorKind(err))
		}
		files[id.GetName()] = f
		required[id.GetName()] = id.GetRefresh() != orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT
	}
	if err := os.Chmod(ids.dir, 0o555); err != nil {
		return FailCause(ReasonWorkdirFailed, err, "task %q: the identity directory could not be sealed: %s", in.GetTaskId(), errorKind(err))
	}
	cfg := d.identityConfig
	logger := d.logger().With("run", in.GetRunId(), "task", in.GetTaskId())
	ids.refresher = &identityRefresher{
		files: files,
		mint: func(ctx context.Context) (*orkstackv1.MintAttemptTokensResponse, error) {
			return d.identityMint(ctx, req)
		},
		masker:      mask,
		clock:       systemIdentityClock{},
		draw:        rand.Float64,
		retryMin:    cfg.RetryMin,
		setupBudget: cfg.SetupBudget,
		maxSkew:     cfg.MaxClockSkew,
		maskGrace:   cfg.MaskGrace,
		orkerBuild:  d.BuildID,
		required:    required,
		onSkew: func(skew time.Duration) {
			logger.Warn("the engine's clock and this Orker's are apart", "skew", skew)
		},
		logger: logger,
		onClock: func(agreed bool) {
			if support := d.identity.Load(); support != nil {
				if agreed {
					support.ClockAgreed()
				} else {
					support.ClockSkewed()
				}
			}
		},
		onRevoked: func(reason string) {
			if mask != nil && mask.note != nil {
				mask.note(fmt.Sprintf("[ork] a best_effort identity of this task was refused (%s) and is no longer refreshed; the task runs on", reason))
			}
		},
		onExpired: func(name string) {
			if mask != nil && mask.note != nil {
				mask.note(fmt.Sprintf("[ork] identity %q's token expired unrefreshed; it is refresh: best_effort, so the task runs on", name))
			}
		},
	}
	return ids.refresher.Setup(ctx)
}

// start runs the refresher beside the container until stop, and calls
// onStop once with why the attempt must stop, if it must.
func (ids *identityAttempt) start(ctx context.Context, onStop func(*Failure)) {
	ctx, ids.cancel = context.WithCancel(ctx)
	ids.done = make(chan struct{})
	go func() {
		defer close(ids.done)
		if f := ids.refresher.Run(ctx); f != nil {
			ids.failure = f
			onStop(f)
		}
	}()
}

// stop ends the refresher and waits for it, and says why it stopped the
// attempt, if it did.
func (ids *identityAttempt) stop() *Failure {
	if ids.cancel == nil {
		return nil
	}
	ids.stopOnce.Do(func() {
		ids.cancel()
		<-ids.done
	})
	return ids.failure
}

// remove removes the attempt's identity files: the attempt's own directory
// in the identity storage, or the identity directory under its root.
func (ids *identityAttempt) remove() error {
	ids.stop()
	target := ids.dir
	if ids.ownBase {
		target = ids.base
	}
	if err := removeSealedTree(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// errIdentityStopped is the cause an attempt is stopped with when its
// identity tokens cannot be kept.
type errIdentityStopped struct{ f *Failure }

func (e errIdentityStopped) Error() string { return e.f.Reason + ": " + e.f.Message }

// identitiesOf is what task.json names: the identities of an attempt that
// holds them, and nil for one that does not.
func identitiesOf(ids *identityAttempt, in *orkstackv1.ExecuteTaskInput) []*orkstackv1.IdentityRef {
	if ids == nil {
		return nil
	}
	return in.GetIdentity().GetIdentities()
}

// errorKind is what went wrong, without the host path an os error carries:
// "no space left on device", "permission denied". The whole error, path
// included, is the failure's cause and is logged.
func errorKind(err error) string {
	var pathErr *os.PathError
	var linkErr *os.LinkError
	switch {
	case errors.As(err, &pathErr):
		return pathErr.Err.Error()
	case errors.As(err, &linkErr):
		return linkErr.Err.Error()
	case err == nil:
		return "no error"
	}
	return "an error of the host's filesystem"
}
