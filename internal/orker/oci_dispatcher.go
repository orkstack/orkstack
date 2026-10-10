package orker

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"go.temporal.io/sdk/activity"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/logstream"
)

// System artifact names. They live in ExecuteTaskResult.system_artifacts,
// which is a SEPARATE field from artifacts, so a workload artifact named
// stdout.log cannot collide with the platform's (§3.6).
const (
	StdoutLogName = "stdout.log"
	StderrLogName = "stderr.log"
)

// undecodableRereadDelay is the backoff before the single in-attempt reread of
// an undecodable commit (§5.4).
//
// The reread needs no cross-attempt state, which is the whole point: the Orker
// is stateless and disposable, a retry may land on a different host, and
// Temporal's attempt counter counts every failure cause rather than
// consecutive undecodable reads. A truncated read is a read-time artifact, so
// one immediate reread separates it from genuine corruption without any
// carrier at all.
const undecodableRereadDelay = 250 * time.Millisecond

// OCIDispatcher runs every activity: there is one implementation, and an
// activity is an image.
type OCIDispatcher struct {
	// Runtime is the only Docker-aware seam in the repository.
	Runtime ContainerRuntime
	// Store holds artifacts, values.json and the captured streams.
	Store artifacts.Store
	// Commits holds the durable completion records. Its Create is
	// create-if-absent, and that conditional is the arbitration point of the
	// whole protocol.
	Commits artifacts.CommitStore
	// Refs predicts the ArtifactRef a Put would return, with no I/O, so step 4
	// can bound the projected commit BEFORE any byte is uploaded (§5.4).
	Refs artifacts.RefPredictor
	// Logs is where a task attempt's live output is streamed while it runs.
	Logs artifacts.LogStore
	// Attempts is where the immutable per-attempt record goes at the end of
	// every attempt — the durable answer to "what did attempt 1 do", which
	// Temporal discards when it schedules attempt 2. Nil writes nothing,
	// exactly like Logs and Commits, and the write is best-effort on every
	// path: a record that cannot be stored is logged, never a task failure.
	Attempts artifacts.AttemptStore
	// Memos is where cached tasks' memos live. Nil means this Orker reuses
	// nothing and writes no memo: every cached attempt is a MISS
	// (MEMO_STORE_ABSENT) and runs, exactly like Commits being nil disables
	// adoption.
	Memos artifacts.MemoStore
	// LogOpts bounds that stream.
	LogOpts LogOptions
	// Policy is the Orker-local half of §9.
	Policy SecurityPolicy
	// WorkRoot is the parent of every per-attempt directory.
	WorkRoot string
	// OrkerID identifies this Orker. It labels every container, and it is the
	// startup reaper's key: it is how an Orker recognizes its own previous
	// life's containers (§5.5).
	OrkerID string
	// BuildID is ORK_BUILD_ID, recorded on every commit.
	BuildID string
	// EnvAllowlist limits which variables the `env` template helper reads.
	EnvAllowlist map[string]struct{}
	// LayoutRoot is where embedded OCI image layouts live (§4.6). Empty
	// disables the embedded-layout fallback; cmd/orker fills it from
	// ORK_ORKER_IMAGE_LAYOUT.
	LayoutRoot string
	// HeartbeatInterval is how often an attempt's heartbeat loop ticks. Zero
	// means DefaultHeartbeatInterval.
	//
	// Whoever sets this MUST cap the task worker's
	// worker.Options.MaxHeartbeatThrottleInterval to the same value, or the
	// SDK batches these ticks into 0.8 × HeartbeatTimeout windows and a
	// running container waits that long to learn it was cancelled — see the
	// pairing rule in heartbeat.go.
	HeartbeatInterval time.Duration
	// LogSinkObserver is an optional in-process observer of streamed lines.
	LogSinkObserver func(in *orkstackv1.ExecuteTaskInput, line string)
	// TaskLogs exports every framed line of every attempt, beside the durable
	// chunk sink. Nil exports nothing; the chunk store is unaffected either
	// way.
	TaskLogs TaskLogExporter
	// Phases observes one attempt's progress through the §7.1 sequence. Nil
	// observes nothing: the dispatcher never learns what a span or a metric
	// is, only that something wants to be told when a phase starts and how
	// the attempt ended.
	Phases PhaseObserver
	// TraceContext extracts the W3C trace carrier from ctx, for the optional
	// `trace` key in task.json (§3.2). Nil means task.json never carries one
	// — an image that ignores the key loses nothing either way.
	TraceContext func(ctx context.Context) map[string]string
	// Signatures verifies cosign signatures for a task whose image_policy
	// names a signer, with the key that selection carries. Nil means this
	// Orker cannot verify: such a task fails IMAGE_SIGNATURE_UNAVAILABLE
	// (an Orker with no registry client is an infrastructure fact, and
	// another Orker of the pool may have one), never passes.
	Signatures *dsl.SignatureVerifier
	// Logger records lifecycle decisions. Nil uses slog.Default.
	Logger *slog.Logger

	// now is the memo step's clock (a memo's age, a hit's finished_at). Nil
	// means time.Now; tests inject one.
	now func() time.Time
}

// PhaseObserver watches one attempt's progress through the §7.1 sequence: it
// is told when the attempt begins, each time it enters a phase, and how it
// ended. It is the seam spans and phase timings hang on; the Orker never
// learns what is done with them. Implementations must not block.
type PhaseObserver interface {
	Begin(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32) (context.Context, AttemptObserver)
}

// AttemptObserver is one attempt's observation. Phase is called from the
// dispatcher's own sequence; End exactly once, with the failure reason (empty
// on success or cancellation), whether the result was an adopted commit, and
// the error the activity returns.
type AttemptObserver interface {
	Phase(name string)
	End(outcome AttemptOutcome)
}

// AttemptOutcome is what one attempt ended with, as reported to
// AttemptObserver.End.
type AttemptOutcome struct {
	Reason        string // Failure.Reason, or "" when none
	Canceled      bool
	CommitAdopted bool
	ExitCode      int32 // when known
	Err           error
	// Cache is the attempt's memo decision (key_components included), for an
	// attempt of a cached task that reached the memo step; nil otherwise.
	Cache *orkstackv1.CacheResult
}

// TaskLogExporter receives every framed line of one attempt, beside the
// durable chunk sink. It is the seam an OpenTelemetry (or any other) log
// forwarder plugs into; the Orker never learns where the lines go. It must
// never block the container's streams and must never fail a task.
type TaskLogExporter interface {
	// Open begins one attempt's export stream.
	Open(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32) TaskLogWriter
}

// TaskLogWriter is one attempt's export stream. Close is called after the
// last line, including the Orker's own [ork] teardown lines.
type TaskLogWriter interface {
	Write(line string)
	Close()
}

// TaskLogExporterFunc adapts a function to TaskLogExporter.
type TaskLogExporterFunc func(ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32) TaskLogWriter

// Open calls f.
func (f TaskLogExporterFunc) Open(
	ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32,
) TaskLogWriter {
	return f(ctx, in, attempt)
}

// Execute runs one activity end to end, in the §7.1 order.
//
// The sequence is not a suggestion. Two orderings are load-bearing:
//
//   - The LOG SINK opens before anything else and closes before return, so
//     even a workdir failure is visible to somebody watching the task, and
//     every chunk is durable before Temporal records the terminal event.
//   - IMAGE comes before RENDER. task.json carries image.platformDigest,
//     image.platform and image.delivery, and none of those exists until the
//     image is resolved — under the opposite order all three keys were always
//     absent. Rendering first and rewriting task.json afterwards is worse: it
//     makes an input file mutable during setup, and §3.1's permission model
//     depends on input/ being sealed before the container starts.
func (d *OCIDispatcher) Execute(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
) (*orkstackv1.ExecuteTaskResult, error) {
	startedAt := timestamppb.New(time.Now().UTC())
	attempt := attemptOf(ctx)

	// STEP 1. The live log sink, first and always. Its Close is deferred
	// IMMEDIATELY, before anything that could return or panic between here
	// and the matching defer further down used to sit — a workdir failure,
	// a panic out of d.execute — used to leave the sink open. sink is the
	// durability barrier §7.1 STEP 1 promises, so it closing is not
	// optional on any exit path.
	sink := d.openLogSink(ctx, in, attempt)
	defer sink.Close()
	var export TaskLogWriter
	if d.TaskLogs != nil {
		export = d.TaskLogs.Open(ctx, in, attempt)
		// export's Close is deferred separately, right after Open succeeds,
		// for the same reason: it must run on every exit path, panic
		// included. Defer's LIFO stack then closes export BEFORE sink, the
		// opposite of what an earlier version of this comment claimed — and
		// that is fine. The exporter is a best-effort MIRROR of the chunk
		// sink, not a second durability barrier: every line, including the
		// [ork] teardown lines, is written to both by emit() before any
		// defer runs, so by the time either Close runs there is nothing left
		// for either one to lose. Closing export first only stops it from
		// accepting lines sooner; it is never seen to outlive what it
		// mirrors, because nothing more is written to either after this
		// point.
		if export != nil {
			defer export.Close()
		}
	}
	// emit fans one framed line out to three places: the durable chunk sink,
	// the optional export seam, and the in-process observer. The observer is
	// what lets a caller inside this process watch a container's output while
	// it runs — `orkctl activity test` uses it to cancel exactly when the
	// workload is live, rather than sleeping and hoping. It fires whether or
	// not a LogStore is configured, because openLogSink returns nil without
	// one and an observer that went silent in that case would be a seam only
	// production could use.
	emit := func(line string) {
		sink.Write(line)
		if export != nil {
			export.Write(line)
		}
		if d.LogSinkObserver != nil {
			d.LogSinkObserver(in, line)
		}
	}
	stdoutLine := logstream.NewLineWriter(func(format string, args ...any) {
		emit(fmt.Sprintf(format, args...))
	}, "")
	stderrLine := logstream.NewLineWriter(func(format string, args ...any) {
		emit(fmt.Sprintf(format, args...))
	}, logstream.StderrMarker)

	// STEP 1.5. The phase observer, before the heartbeater — its returned ctx
	// (which may carry a span) is what the rest of Execute uses, so child
	// work and the trace-context file see it.
	var attemptObs AttemptObserver
	if d.Phases != nil {
		ctx, attemptObs = d.Phases.Begin(ctx, in, attempt)
	}

	// beat, result and failure are DECLARED here and assigned below, so end can
	// read all three. end is defined before them because the panic recovery
	// that calls it must be the outermost thing in this function; nothing
	// between this declaration and the assignments can panic.
	var (
		beat    *Heartbeater
		result  *orkstackv1.ExecuteTaskResult
		failure *Failure
		// trace is the memo step's decision, for a cached task. Declared here
		// so the attempt record and the observer see it on every exit path —
		// a miss whose container then failed still records its miss.
		trace = &memoTrace{emit: emit}
	)

	// end is the ONLY thing in Execute allowed to call attemptObs.End, and it
	// enforces the AttemptObserver contract's "exactly once": the ended guard
	// makes every call after the first a no-op, so the three ordinary exit
	// sites below and the panic recovery immediately after this can each call
	// it unconditionally without coordinating who got there first.
	//
	// It has a SECOND consumer: the durable attempt record. Both hang off this
	// one seam because it is the exactly-once-per-attempt point — the observer
	// is the optional, in-process one (a span, a metric) and the record is the
	// always-on, durable one. The record is written INSIDE the guard, so a
	// panic path writes exactly one record and the three ordinary exits write
	// exactly one each.
	ended := false
	end := func(outcome AttemptOutcome) {
		if ended {
			return
		}
		ended = true
		outcome.Cache = trace.result
		if attemptObs != nil {
			attemptObs.End(outcome)
		}
		d.recordAttempt(ctx, in, attempt, startedAt, beat, result, failure, outcome)
	}
	// A panic out of d.execute — a runtime library panicking instead of
	// returning an error, say — must still produce exactly one End call
	// before it propagates, or an observer wired to metrics/tracing loses the
	// attempt with no terminal event at all. Re-panicking after end leaves
	// Temporal's own activity panic handling exactly as it was; this defer
	// only ensures the observer hears about it first.
	defer func() {
		if r := recover(); r != nil {
			end(AttemptOutcome{Reason: ReasonPanic, Err: fmt.Errorf("panic: %v", r)})
			panic(r)
		}
	}()

	// STEP 2. The heartbeat loop, for the ENTIRE attempt.
	//
	// Temporal delivers cancellation to an activity only through a heartbeat
	// response: an activity that never calls RecordHeartbeat never observes
	// ctx.Done(). A loop that only beat between phases could not deliver
	// cancellation to a container that is running, which is exactly when
	// somebody presses cancel.
	beat = NewHeartbeater(ctx)
	beat.interval = d.HeartbeatInterval
	beat.orkerID = d.OrkerID
	if attemptObs != nil {
		beat.onPhase = attemptObs.Phase
	}
	beat.Start(ctx)
	defer beat.Stop()
	beat.Phase("start")

	result, failure = d.execute(ctx, in, attempt, startedAt, beat, stdoutLine, stderrLine, sink, trace)

	// The line writers are flushed before the sink closes, so a workload's
	// last unterminated line is not lost.
	stdoutLine.Close()
	stderrLine.Close()

	// CANCELLATION IS CHECKED FIRST, before any failure this attempt
	// collected.
	//
	// A cancelled attempt is Cancelled, whatever else was in flight. Checking
	// the failure first reports the WRONG terminal reason: a container killed
	// mid-run has necessarily written no values.json, so the attempt would
	// fail with ContractViolation/VALUES_MISSING — blaming the image for not
	// finishing work that was deliberately taken away from it, and doing so
	// NON-RETRYABLY. Found by TestIntegration_CancellationSignalsThenKills.
	//
	// Returning nil here would be worse still: it would report success for
	// work that was torn down, which §6.3 forbids outright.
	if err := ctx.Err(); err != nil {
		end(AttemptOutcome{Canceled: true, Err: err})
		return nil, err
	}
	if failure != nil {
		// Every failure carries the attempt's start, detail or not, so the
		// engine projects TaskRun.started_at for a failed task exactly as for
		// a succeeded one (ExecuteTaskResult.started_at).
		failure.StampStart(startedAt)
		outcome := AttemptOutcome{Reason: failure.Reason, Err: failure.TemporalError()}
		if failure.Detail != nil {
			outcome.ExitCode = failure.Detail.GetExitCode()
		}
		end(outcome)
		return nil, failure.TemporalError()
	}
	end(AttemptOutcome{
		CommitAdopted: result.GetCommitAdopted(),
		ExitCode:      result.GetExitCode(),
	})
	return result, nil
}

// Validate is the pure, I/O-free half of the contract check. It is separate so
// the engine's own tests and the activity test suite can assert the refusals
// without a daemon.
func (d *OCIDispatcher) Validate(in *orkstackv1.ExecuteTaskInput) *Failure {
	if in == nil {
		return Fail(ReasonInputTooLarge, "the activity received no input")
	}
	pinned := in.GetImage().GetPinned()
	switch {
	case pinned == "":
		return Fail(ReasonImageNotPinned,
			"task %q: image.pinned is empty; the engine resolves a reference to a digest at apply "+
				"time and a run executes what that digest names", in.GetTaskId())
	case !strings.Contains(pinned, "@sha256:"):
		return Fail(ReasonImageNotPinned,
			"task %q: image.pinned %q is not {registry}/{repository}@sha256:<64 hex>; a running "+
				"execution must never silently switch images because a tag moved", in.GetTaskId(), pinned)
	}
	if _, failure := d.Policy.ResolveNetwork(in.GetNetwork()); failure != nil {
		return failure
	}
	if _, _, failure := d.Policy.ResolveResources(in.GetResources()); failure != nil {
		return failure
	}
	for _, secret := range in.GetSecrets() {
		if !ValidSecretName(secret.GetName()) {
			return Fail(ReasonSecretSchemeForbid,
				"secret name %q must match ^[A-Za-z0-9._-]{1,128}$", secret.GetName())
		}
		scheme, _ := SplitSecretSource(secret.GetSource())
		if !d.Policy.AllowsSecretScheme(scheme) {
			return Fail(ReasonSecretSchemeForbid,
				"secret %q uses source scheme %q, which this orker does not resolve (allowed: %s); "+
					"an unknown scheme is refused rather than ignored",
				secret.GetName(), scheme, strings.Join(d.Policy.SecretSchemes, ", "))
		}
		if failure := d.secretFloorFailure(secret); failure != nil {
			return failure
		}
	}
	return nil
}

// checkImagePolicy is execute's STEP 3b: the pure rule, then the signature,
// against the selection the run pinned at submit. A task with no selection
// walks exactly today's sequence — no phase, no request.
func (d *OCIDispatcher) checkImagePolicy(ctx context.Context, in *orkstackv1.ExecuteTaskInput, beat *Heartbeater) *Failure {
	sel := in.GetImagePolicy()
	if sel == nil {
		return nil
	}
	beat.Phase("image-policy")
	if refusal := task.CheckImage(sel, 0, in.GetTaskId(), in.GetImage()); refusal != nil {
		return Fail(refusal.Code, "%v; the run pinned this policy at submit and an image "+
			"outside it is refused before anything is pulled", refusal)
	}
	signer := sel.GetRequireSignedBy()
	if signer == "" {
		return nil
	}
	if d.Signatures == nil {
		return Fail(ReasonImageSignatureUnavailable,
			"task %q: namespace policy requires %s but this orker has no registry client "+
				"to verify signatures with (set %s)", in.GetTaskId(), signer, RegistryAuthFileVar)
	}
	if err := d.Signatures.Verify(ctx, in.GetImage(), signer, sel.GetSigningKeyPem()); err != nil {
		var sig *dsl.SignatureError
		if errors.As(err, &sig) {
			return FailCause(sig.Code, err, "task %q: %v; namespace %q requires %s",
				in.GetTaskId(), err, in.GetNamespace(), signer)
		}
		return FailCause(ReasonImageSignatureUnavailable, err, "task %q: %v", in.GetTaskId(), err)
	}
	return nil
}

// secretFloorFailure holds one secret's source against this Orker's
// ORK_ORKER_SECRET_ALLOW floor. It names the source and the floor — never
// what the source resolves to, which nothing on this path has read.
func (d *OCIDispatcher) secretFloorFailure(secret *orkstackv1.SecretRef) *Failure {
	if err := d.Policy.AllowsSecretSource(secret.GetSource()); err != nil {
		return FailCause(ReasonSecretNotPermitted, err,
			"secret %q: %v (this orker's %s); a source outside that floor is refused before anything runs",
			secret.GetName(), err, SecretAllowVar)
	}
	return nil
}

// execute is Execute's body, split out so Execute owns only the two lifecycles
// that must wrap everything (§7.1 steps 1-2 and 15).
func (d *OCIDispatcher) execute(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	attempt int32,
	startedAt *timestamppb.Timestamp,
	beat *Heartbeater,
	stdoutLine, stderrLine io.Writer,
	sink *logChunkSink,
	trace *memoTrace,
) (*orkstackv1.ExecuteTaskResult, *Failure) {
	if failure := d.Validate(in); failure != nil {
		return nil, failure
	}

	// STEP 3. The retry-start commit check, before anything expensive and
	// before any container starts.
	beat.Phase("commit-check")
	if adopted, failure := d.adopt(ctx, in, attempt); adopted != nil || failure != nil {
		return adopted, failure
	}

	// STEP 3b. The image policy the run pinned at submit.
	//
	// AFTER the commit check: an attempt whose commit.json exists returned
	// above and is never judged again, by the rule or by the registry — a
	// signature removed or rotated after the commit must not turn a durably
	// committed result back into work (AGENTS.md §15). BEFORE the workspace,
	// the pin directory, the staged secrets and the pull: a refused image
	// leaves nothing on disk and is never fetched.
	//
	// NOT in Validate, which runs before the commit check.
	if failure := d.checkImagePolicy(ctx, in, beat); failure != nil {
		return nil, failure
	}

	// STEP 4. The workspace.
	beat.Phase("workspace")
	root, err := d.makeAttemptDir(in, attempt)
	if err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err,
			"task %q: create the per-attempt workspace: %v", in.GetTaskId(), err)
	}
	ws, err := NewWorkspace(root)
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, FailCause(ReasonWorkdirFailed, err,
			"task %q: build the workspace tree: %v", in.GetTaskId(), err)
	}
	// Scratch space for exactly this attempt: removed on success, failure,
	// panic and cancellation alike.
	//
	// Workspace.Remove, not os.RemoveAll: Seal leaves input/ at 0555 and
	// unlinking needs write on the DIRECTORY, so a plain RemoveAll would fail
	// and leave the attempt's tree — staged secrets included — on disk.
	defer func() { _ = ws.Remove() }()
	// Secret bytes must not outlive the attempt that needed them, on ANY exit
	// path (§9.4).
	defer func() { _ = ws.RemoveSecrets() }()

	// STEP 4b. The pin's shared directory, for a pinned attempt only (§3.1).
	//
	// There is NO defer beside the two above, and that is the whole difference
	// between this directory and the attempt's own: it must outlive this
	// attempt, because the next task of the pin — and the next attempt of this
	// one — is entitled to find what this container wrote. It ends by
	// ReleasePin, a boot reap or its lease, never by an attempt (§6.4).
	//
	// A failure here fails the attempt exactly as a workspace failure does,
	// because it is the same class of failure: the Orker could not build the
	// filesystem the contract promises the workload.
	if pin := in.GetPin(); pin != nil {
		pinWS, err := AcquirePin(
			d.WorkRoot, in.GetRunId(), pin.GetName(), pin.GetInstanceId(), pin.GetTimeoutSeconds())
		if err != nil {
			return nil, FailCause(ReasonWorkdirFailed, err,
				"task %q: acquire the shared directory of pin %q: %v", in.GetTaskId(), pin.GetName(), err)
		}
		// Set BEFORE containerSpec builds the mount set, which is the only
		// thing that reads it. Mounts() appends /orkstack/shared exactly when
		// this field is non-empty.
		ws.Shared = pinWS.Shared
	}

	// STEP 5. Input staging. A cached task stages only what its memo key
	// and its render need — the upstream values.json files, verified, and the
	// fan-in item commits — and RECORDS the artifact fetches and secrets
	// (deferBytes): a hit needs neither.
	beat.Phase("stage-inputs")
	cached := cachedInput(in)
	staged, failure := d.stageInputs(ctx, in, ws, cached)
	if failure != nil {
		return nil, failure
	}

	// STEP 5b. The memo step, for a cached task only (docs/OCI_ACTIVITY_
	// CONTRACT.md §5.4 "Memoized completion"). The render happens here, with
	// the renderer STEP 7 uses, so the key hashes the exact config.json the
	// container would read; STEP 7 then writes that value and does not
	// render twice.
	var (
		eachItem    json.RawMessage
		rendered    map[string]any
		preRendered bool
	)
	if cached {
		beat.Phase("memo")
		var item *resolvedEachItem
		if each := in.GetEach(); each != nil {
			eachItem, failure = d.resolveEachItem(in, ws)
			if failure != nil {
				return nil, failure
			}
			item = &resolvedEachItem{Bytes: eachItem, As: each.GetAs(), Index: each.GetIndex(), Count: each.GetCount()}
		}
		rendered, failure = d.render(ctx, in, staged, eachItem)
		if failure != nil {
			return nil, failure
		}
		preRendered = true
		config, err := encodeInputFile(configValue(rendered))
		if err != nil {
			return nil, FailCause(ReasonWorkdirFailed, err, "encode config.json: %v", err)
		}
		runJSON, err := encodeInputFile(payloadMapOf(in.GetRunInput()))
		if err != nil {
			return nil, FailCause(ReasonWorkdirFailed, err, "encode run.json: %v", err)
		}
		key, ok := computeMemoKey(in, config, runJSON, staged, item)
		switch {
		case !ok:
			// Even under refresh: nothing can be keyed, and no memo is written.
			trace.miss(memoKey{}, MemoMissInputUnhashed,
				"an input reference of task %q carries no sha256, so no memo key can be computed", in.GetTaskId())
			trace.log("[ork] cache miss (%s): %s; running the container", MemoMissInputUnhashed, trace.result.GetMissDetail())
		case in.GetCache().GetRefresh():
			trace.result = &orkstackv1.CacheResult{
				Outcome:       orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED,
				Key:           key.Key,
				KeyComponents: key.Components,
			}
			trace.log("[ork] cache bypassed: the run was submitted with no_cache; running the container")
		default:
			if hit := d.lookupMemo(ctx, in, attempt, startedAt, key, trace); hit != nil {
				return d.commitHit(ctx, in, attempt, startedAt, key, hit, trace)
			}
			trace.log("[ork] cache miss (%s): %s; running the container",
				trace.result.GetMissReason(), trace.result.GetMissDetail())
		}

		// STEP 5c. What a hit did not need: the recorded artifact fetches,
		// then the secrets.
		beat.Phase("stage-artifacts")
		if failure := d.completeStaging(ctx, in, ws, staged); failure != nil {
			return nil, failure
		}
	}

	// STEP 6. The image — BEFORE the render, so task.json can carry what the
	// pull resolved.
	beat.Phase("image")
	image, failure := d.resolveImage(ctx, in)
	if failure != nil {
		return nil, failure
	}

	// STEP 7. Render, then compose and SEAL the input tree.
	beat.Phase("render")
	effectiveCPU, effectiveMem, resFailure := d.Policy.ResolveResources(in.GetResources())
	if resFailure != nil {
		return nil, resFailure
	}
	network, netFailure := d.Policy.ResolveNetwork(in.GetNetwork())
	if netFailure != nil {
		return nil, netFailure
	}
	runAsOrkerUser := in.GetRunAsOrkerUser() || d.Policy.ForceUser

	// An item of a fan-out resolves its own element of the array the engine
	// counted, after staging (a task source is read from the staged
	// values.json) and before render (.each reads it).
	if !preRendered {
		if in.GetEach() != nil {
			eachItem, failure = d.resolveEachItem(in, ws)
			if failure != nil {
				return nil, failure
			}
		}
		rendered, failure = d.render(ctx, in, staged, eachItem)
		if failure != nil {
			return nil, failure
		}
	}
	if err := ws.WriteConfigJSON(rendered); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "write config.json: %v", err)
	}
	if err := ws.WriteRunJSON(payloadMapOf(in.GetRunInput())); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "write run.json: %v", err)
	}
	// The trace key is optional and non-ABI (§3.2): d.TraceContext is nil in
	// every deployment that does not trace, and ctx here is the one Phases.Begin
	// returned, which is what may carry a span.
	var traceCarrier map[string]string
	if d.TraceContext != nil {
		traceCarrier = d.TraceContext(ctx)
	}
	if err := ws.WriteTaskJSON(in, TaskContext{
		Attempt:        attempt,
		Pool:           resolvedPool(in.GetPool()),
		Namespace:      artifacts.ResolveNamespace(in.GetNamespace()),
		RunAsOrkerUser: runAsOrkerUser,
		EffectiveCPU:   effectiveCPU,
		EffectiveMem:   effectiveMem,
		Network:        network,
		Image:          image,
		StagedInputs:   staged.artifacts,
		StagedUpstream: staged.upstream,
		StagedSecrets:  staged.secrets,
		Trace:          traceCarrier,
		// Nil for an unpinned task, which is what keeps paths.shared and
		// task.pin out of its task.json entirely (§3.2).
		Pin: in.GetPin(),
		// Nil for an attempt that is not an item of a fan-out, which keeps
		// the `each` key out of its task.json entirely.
		Each:               in.GetEach(),
		EachItem:           eachItem,
		StagedEachUpstream: staged.eachUpstream,
	}); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "write task.json: %v", err)
	}
	if err := ws.Seal(); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "seal the input tree: %v", err)
	}
	if err := ws.SealSecrets(); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "seal the secrets directory: %v", err)
	}

	// STEP 8. Create.
	beat.Phase("create")
	spec, failure := d.containerSpec(in, ws, image, attempt, network, effectiveCPU, effectiveMem, runAsOrkerUser)
	if failure != nil {
		return nil, failure
	}
	id, err := d.Runtime.Create(ctx, spec)
	if err != nil {
		return nil, d.runtimeFailure(err, "create the container")
	}
	beat.Container(id)
	// Remove runs on a DETACHED context: cleanup must survive the
	// cancellation that caused it (§6.3).
	defer func() {
		removeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controlTimeout)
		defer cancel()
		if err := d.Runtime.Remove(removeCtx, id); err != nil {
			sink.Write(fmt.Sprintf("[ork] container %s could not be removed: %v", id, err))
		}
	}()

	// STEP 9-10. Run, stream, and wait for exit, cancellation or the task
	// timeout.
	beat.Phase("run")
	capture, failure := d.runContainer(ctx, in, ws, id, beat, stdoutLine, stderrLine)
	if failure != nil {
		// Even a failure publishes what it produced.
		return nil, d.attachFailureDetail(ctx, in, ws, capture, image, attempt, failure)
	}

	// A cancelled attempt stops here. runContainer has already signalled,
	// waited out the grace period, killed and let the deferred Remove run;
	// continuing into validation would only produce a misleading
	// VALUES_MISSING for a container that was never allowed to finish.
	if err := ctx.Err(); err != nil {
		return nil, nil
	}

	// STEP 11. Exit capture.
	beat.Phase("exit")
	if capture.status.Code != 0 {
		failure := Fail(ReasonContainerExitNonzero,
			"task %q: the container exited %d", in.GetTaskId(), capture.status.Code)
		if capture.status.OOMKilled {
			failure = Fail(ReasonContainerExitNonzero,
				"task %q: the container exited %d after the kernel killed it for exceeding its "+
					"memory limit of %s", in.GetTaskId(), capture.status.Code, effectiveMem)
		}
		return nil, d.attachFailureDetail(ctx, in, ws, capture, image, attempt, failure)
	}

	// STEP 12. Validate — INCLUDING readability — and bound the projected
	// commit before any byte is uploaded.
	beat.Phase("validate")
	values, failure := ReadValues(filepath.Join(ws.Output, "values.json"), in.GetLimits())
	if failure != nil {
		return nil, d.attachFailureDetail(ctx, in, ws, capture, image, attempt, failure)
	}
	scanned, failure := ScanOutputArtifacts(ws.Artifacts, in.GetLimits())
	if failure != nil {
		return nil, d.attachFailureDetail(ctx, in, ws, capture, image, attempt, failure)
	}
	for _, artifact := range scanned {
		if failure := CheckReadable(artifact.Path, ContainerArtifacts+"/"+artifact.Name); failure != nil {
			return nil, d.attachFailureDetail(ctx, in, ws, capture, image, attempt, failure)
		}
	}

	// STEP 13. Hash, bound, upload, commit.
	beat.Phase("commit")
	return d.commit(ctx, in, ws, capture, image, scanned, values, attempt, startedAt, trace)
}

// ---------------------------------------------------------------- step 0

// adopt implements the §5.4 retry-start check.
//
// A commit that exists and matches means this activity already finished
// durably. Adopting it is what makes "Orker died after commit.json, before
// Temporal completion" a container that ran ONCE rather than twice.
func (d *OCIDispatcher) adopt(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	attempt int32,
) (*orkstackv1.ExecuteTaskResult, *Failure) {
	uri := in.GetCommitUri()
	if uri == "" || d.Commits == nil {
		return nil, nil
	}
	record, err := d.Commits.Get(ctx, uri)
	switch {
	case err == nil:
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, nil
	case errors.Is(err, artifacts.ErrCommitUndecodable):
		// One immediate reread, within this attempt. If it decodes, continue
		// at the matching branch; if it does not, the stored bytes are
		// corrupt rather than in flight.
		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(undecodableRereadDelay):
		}
		record, err = d.Commits.Get(ctx, uri)
		if err != nil {
			if errors.Is(err, artifacts.ErrNotFound) {
				return nil, nil
			}
			if errors.Is(err, artifacts.ErrCommitUndecodable) {
				return nil, FailCause(ReasonCommitUndecodable, err,
					"an object exists at %s but is still not a valid ActivityCommit after an "+
						"in-attempt reread (%d bytes); the stored bytes are corrupt, not in flight",
					uri, record.Bytes)
			}
			return nil, FailCause(ReasonCommitReadFailed, err, "read the commit at %s: %v", uri, err)
		}
	default:
		// Execution NEVER proceeds on an unknown commit state: running the
		// container blind could duplicate a durably committed result.
		return nil, FailCause(ReasonCommitReadFailed, err,
			"the commit store at %s could not be read: %v", uri, err)
	}

	commit := record.Commit
	if commit.GetSchemaVersion() != artifacts.CommitSchemaVersion {
		return nil, Fail(ReasonCommitSchema,
			"the commit at %s has schema_version %d; this orker understands %d, and an orker that "+
				"cannot read a commit must neither overwrite it nor re-run past it",
			uri, commit.GetSchemaVersion(), artifacts.CommitSchemaVersion)
	}
	if mismatch := commitIdentityMismatch(commit, in); mismatch != "" {
		return nil, Fail(ReasonCommitIdentity,
			"the commit at %s is for a different activity: %s", uri, mismatch)
	}

	d.logger().Info("adopted durable activity commit",
		"commit_uri", uri,
		"run_id", in.GetRunId(),
		"task_id", in.GetTaskId(),
		"committing_attempt", commit.GetAttempt(),
		"retry_attempt", attempt)

	result := &orkstackv1.ExecuteTaskResult{
		TaskId:          in.GetTaskId(),
		ExitCode:        commit.GetExitCode(),
		Commit:          &orkstackv1.CommitRef{Uri: uri, Sha256: record.SHA256, Etag: record.ETag, CreatedAt: commit.GetFinishedAt()},
		Artifacts:       commit.GetArtifacts(),
		SystemArtifacts: commit.GetSystemArtifacts(),
		ValuesRef:       commit.GetValues(),
		Image:           commit.GetImage(),
		CommitAdopted:   true,
		StartedAt:       commit.GetStartedAt(),
		FinishedAt:      commit.GetFinishedAt(),
		OrkerId:         d.OrkerID,
		// The attempt is the COMMITTING attempt's, not this one's. Without it
		// the engine could not populate TaskRun.attempt identically for fresh
		// and adopted results, and a read-model difference would be decided by
		// nothing but which Orker happened to crash.
		Attempt: commit.GetAttempt(),
	}
	_ = attempt
	// A cached task's adopted result carries the committing attempt's memo
	// decision: a retry after a hit's commit returns the hit, never a fresh
	// run. memo_written is never reconstructed (the commit cannot know it).
	if cachedInput(in) {
		result.Cache = commit.GetCache()
	}

	// Adoption reconstructs the FULL result, including the inline copy.
	// Otherwise the same task would produce a populated TaskRun.output on a
	// fresh attempt and an empty one on an adopted attempt.
	values := commit.GetValues()
	if values.GetSizeBytes() > 0 && values.GetSizeBytes() <= InlineValuesLimit {
		raw, failure := d.refetchValues(ctx, values)
		if failure != nil {
			return nil, failure
		}
		result.ValuesJson = raw
		result.ValuesInlined = true
	}
	return result, nil
}

func (d *OCIDispatcher) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

// refetchValues re-reads a small committed values.json so an adopted result
// carries the same inline bytes a fresh one would.
//
// Adoption does NOT re-verify artifact bytes — they may have been expired by a
// lifecycle rule, and a commit is a record of what happened, not a live
// guarantee that every byte still exists. values is the one exception, because
// it is being COPIED into the result rather than merely referenced.
func (d *OCIDispatcher) refetchValues(ctx context.Context, ref *orkstackv1.ArtifactRef) ([]byte, *Failure) {
	return d.refetchCommittedValues(ctx, ref, InlineValuesLimit)
}

// commitIdentityMismatch compares exactly the seven identity fields of §5.4.
//
// image.platform_digest and image.platform are deliberately NOT compared. They
// are audit data recorded by the winning attempt: the engine leaves both empty
// in ExecuteTaskInput, so comparing them would reject EVERY adoption, and on a
// mixed-architecture pool it would additionally reject two well-formed
// attempts of the same activity for differing on a fact that is allowed to
// differ. The index digest is the identity because it is what was pinned, what
// was pulled, and what does not vary by host.
func commitIdentityMismatch(commit *orkstackv1.ActivityCommit, in *orkstackv1.ExecuteTaskInput) string {
	checks := []struct {
		field, stored, wanted string
	}{
		{"namespace", commit.GetNamespace(), artifacts.ResolveNamespace(in.GetNamespace())},
		{"run_id", commit.GetRunId(), in.GetRunId()},
		{"task_id", commit.GetTaskId(), in.GetTaskId()},
		{"orkflow_definition_digest", commit.GetOrkflowDefinitionDigest(), in.GetOrkflowDefinitionDigest()},
		{"idempotency_key", commit.GetIdempotencyKey(), in.GetIdempotencyKey()},
		{"image.index_digest", commit.GetImage().GetIndexDigest(), in.GetImage().GetIndexDigest()},
	}
	for _, check := range checks {
		if check.stored != check.wanted {
			return fmt.Sprintf("%s is %q, this activity's is %q", check.field, check.stored, check.wanted)
		}
	}
	return ""
}

// ---------------------------------------------------------------- steps 5-8

// stagedInputs is what input staging produced, in the shape task.json needs.
type stagedInputs struct {
	artifacts []taskJSONInputArtifact
	upstream  []taskJSONUpstream
	secrets   []taskJSONSecret
	// paths maps "<producer>/<name>" to the CONTAINER path, for the render's
	// `artifact` helper. The helper returns a container path because that is
	// the only path the workload can open.
	paths map[string]string
	// upstreamValues maps a task id to its decoded values.json, for the
	// render's .tasks.<id>.output.
	upstreamValues map[string]map[string]any
	// upstreamArtifacts maps a task id to its refs, for .tasks.<id>.artifacts.
	upstreamArtifacts map[string][]*orkstackv1.ArtifactRef
	// eachUpstream lists every fan-out upstream staged for a direct
	// dependent, for task.json inputs.eachUpstream — and for a cached task's
	// memo key, because a zero-item fan-in stages no file.
	eachUpstream []taskJSONEachUpstream

	// inputFiles is every staged-or-deferred input file, by container path,
	// task.json name and reference digest: what a cached task's memo key
	// covers. Built in both modes.
	inputFiles []memoInputFile
	// inputUnhashed is set when any staged reference carried no sha256: no
	// memo key can be computed (INPUT_UNHASHED).
	inputUnhashed bool
	// deferred lists the artifact fetches stageInputs recorded instead of
	// performing (deferBytes), in order; completeStaging performs them.
	deferred []deferredFetch
	// secretsDeferred is set when stageInputs skipped secret resolution
	// (deferBytes); completeStaging resolves them.
	secretsDeferred bool
}

// noteInputFile records one input file for the memo key.
func (s *stagedInputs) noteInputFile(containerPath, name, sha string) {
	if sha == "" {
		s.inputUnhashed = true
	}
	s.inputFiles = append(s.inputFiles, memoInputFile{
		Path:    containerPath,
		NameB64: EncodeArtifactName(name),
		SHA256:  sha,
	})
}

// fetch downloads ref into hostPath now, or — with deferBytes — records the
// download for completeStaging.
func (d *OCIDispatcher) fetchOrDefer(ctx context.Context, staged *stagedInputs, deferBytes bool,
	ref *orkstackv1.ArtifactRef, hostPath string) *Failure {
	if deferBytes {
		staged.deferred = append(staged.deferred, deferredFetch{ref: ref, hostPath: hostPath})
		return nil
	}
	return d.fetchInto(ctx, ref, hostPath)
}

// stageInputs stages every upstream contribution and the secrets. With
// deferBytes false it is exactly today's staging, in today's order. With
// deferBytes true — a cached task's attempt — everything is built the same
// way, upstream values.json objects are still fetched and verified (the
// render reads them), but every artifact download, and the secrets, are
// recorded for completeStaging instead of performed.
func (d *OCIDispatcher) stageInputs(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	ws *Workspace,
	deferBytes bool,
) (*stagedInputs, *Failure) {
	staged := &stagedInputs{
		artifacts:         []taskJSONInputArtifact{},
		upstream:          []taskJSONUpstream{},
		secrets:           []taskJSONSecret{},
		paths:             map[string]string{},
		upstreamValues:    map[string]map[string]any{},
		upstreamArtifacts: map[string][]*orkstackv1.ArtifactRef{},
	}

	for _, up := range in.GetUpstream() {
		producer := up.GetTaskId()
		// A fan-out upstream carries no commit of its own: its items do, and
		// they are staged one directory per item (§3.1).
		if up.GetEach() != nil {
			if failure := d.stageEachUpstream(ctx, in, ws, up, staged, deferBytes); failure != nil {
				return nil, failure
			}
			continue
		}
		staged.upstreamArtifacts[producer] = up.GetArtifacts()

		for _, ref := range up.GetArtifacts() {
			hostDir := filepath.Join(ws.Input, "artifacts", safeComponent(producer))
			if err := os.MkdirAll(hostDir, 0o755); err != nil {
				return nil, FailCause(ReasonWorkdirFailed, err,
					"stage input artifacts for %q: %v", producer, err)
			}
			safe := safeComponent(ref.GetName())
			hostPath := filepath.Join(hostDir, safe)
			containerPath := ContainerInputArtifacts + "/" + safeComponent(producer) + "/" + safe

			if failure := d.fetchOrDefer(ctx, staged, deferBytes, ref, hostPath); failure != nil {
				return nil, failure
			}
			staged.noteInputFile(containerPath, ref.GetName(), ref.GetSha256())
			entry := taskJSONInputArtifact{
				Producer:    producer,
				NameB64:     EncodeArtifactName(ref.GetName()),
				Path:        containerPath,
				SizeBytes:   ref.GetSizeBytes(),
				SHA256:      ref.GetSha256(),
				ContentType: ref.GetContentType(),
			}
			// name is present only when the exact name is valid UTF-8;
			// nameB64 is always present and authoritative (§3.2).
			if utf8.ValidString(ref.GetName()) {
				entry.Name = ref.GetName()
			}
			staged.artifacts = append(staged.artifacts, entry)
			staged.paths[producer+"/"+ref.GetName()] = containerPath
		}

		// The ancestor's committed values.json, fetched by reference. Fan-in
		// is O(ancestors) small records, never a transitive copy of every
		// ancestor's payload (OQ-2).
		if values := up.GetValues(); values.GetUri() != "" {
			hostDir := filepath.Join(ws.Input, "upstream", safeComponent(producer))
			if err := os.MkdirAll(hostDir, 0o755); err != nil {
				return nil, FailCause(ReasonWorkdirFailed, err,
					"stage upstream values for %q: %v", producer, err)
			}
			hostPath := filepath.Join(hostDir, "values.json")
			if failure := d.fetchInto(ctx, values, hostPath); failure != nil {
				return nil, failure
			}
			valuesPath := ContainerUpstream + "/" + safeComponent(producer) + "/values.json"
			staged.noteInputFile(valuesPath, "values.json", values.GetSha256())
			staged.upstream = append(staged.upstream, taskJSONUpstream{
				TaskID:     producer,
				ValuesPath: valuesPath,
			})
			if raw, err := os.ReadFile(hostPath); err == nil {
				decoded := map[string]any{}
				if json.Unmarshal(raw, &decoded) == nil {
					staged.upstreamValues[producer] = decoded
				}
			}
		}
	}

	// A cached task's secrets wait for its memo step: a hit reads none.
	if deferBytes {
		staged.secretsDeferred = len(in.GetSecrets()) > 0
		return staged, nil
	}
	if failure := d.stageSecrets(in, ws, staged); failure != nil {
		return nil, failure
	}
	return staged, nil
}

// stageSecrets resolves and writes every secret. The value never appears in
// a proto message, in Temporal history, in the archive, in a log chunk, or in
// task.json — task.json carries only names and paths (§9.4).
func (d *OCIDispatcher) stageSecrets(in *orkstackv1.ExecuteTaskInput, ws *Workspace, staged *stagedInputs) *Failure {
	for _, secret := range in.GetSecrets() {
		value, failure := d.resolveSecret(secret)
		if failure != nil {
			return failure
		}
		hostPath := filepath.Join(ws.Secrets, secret.GetName())
		if err := os.WriteFile(hostPath, value, 0o600); err != nil {
			return FailCause(ReasonSecretUnresolved, err,
				"write secret %q: %v", secret.GetName(), err)
		}
		staged.secrets = append(staged.secrets, taskJSONSecret{
			Name: secret.GetName(),
			Path: ContainerSecrets + "/" + secret.GetName(),
		})
	}
	return nil
}

// fetchInto downloads one referenced artifact and verifies its digest.
func (d *OCIDispatcher) fetchInto(ctx context.Context, ref *orkstackv1.ArtifactRef, dest string) *Failure {
	if d.Store == nil {
		return Fail(ReasonInputUnavailable, "this orker has no artifact store")
	}
	reader, err := d.Store.Get(ctx, ref.GetUri())
	if err != nil {
		return FailCause(ReasonInputUnavailable, err,
			"fetch input artifact %q from %s: %v", ref.GetName(), ref.GetUri(), err)
	}
	defer func() { _ = reader.Close() }()

	file, err := os.Create(dest)
	if err != nil {
		return FailCause(ReasonWorkdirFailed, err, "stage input artifact %q: %v", ref.GetName(), err)
	}
	if _, err := io.Copy(file, reader); err != nil {
		_ = file.Close()
		return FailCause(ReasonInputUnavailable, err,
			"fetch input artifact %q from %s: %v", ref.GetName(), ref.GetUri(), err)
	}
	if err := file.Close(); err != nil {
		return FailCause(ReasonWorkdirFailed, err, "stage input artifact %q: %v", ref.GetName(), err)
	}

	// A recorded digest that does not match the fetched bytes is the store
	// contradicting the commit that referenced it. It is terminal: no retry
	// makes corrupt bytes correct.
	if want := ref.GetSha256(); want != "" {
		got, _, err := HashFile(dest)
		if err != nil {
			return FailCause(ReasonInputUnavailable, err,
				"hash the staged input artifact %q: %v", ref.GetName(), err)
		}
		if got != want {
			return Fail(ReasonInputCorrupt,
				"input artifact %q from %s hashes to %s but its reference records %s",
				ref.GetName(), ref.GetUri(), got, want)
		}
	}
	return nil
}

func (d *OCIDispatcher) resolveSecret(ref *orkstackv1.SecretRef) ([]byte, *Failure) {
	scheme, locator := SplitSecretSource(ref.GetSource())
	if !d.Policy.AllowsSecretScheme(scheme) {
		return nil, Fail(ReasonSecretSchemeForbid,
			"secret %q uses source scheme %q, which this orker does not resolve (allowed: %s)",
			ref.GetName(), scheme, strings.Join(d.Policy.SecretSchemes, ", "))
	}
	// The floor again, beside the read: this is the function that reads the
	// value, and the check that protects the value lives here whatever a
	// caller checked first.
	if failure := d.secretFloorFailure(ref); failure != nil {
		return nil, failure
	}
	switch scheme {
	case "env":
		value, ok := os.LookupEnv(locator)
		if !ok {
			return nil, Fail(ReasonSecretUnresolved,
				"secret %q: environment variable %q is not set on this orker", ref.GetName(), locator)
		}
		return []byte(value), nil
	case "file":
		value, err := os.ReadFile(locator)
		if err != nil {
			return nil, FailCause(ReasonSecretUnresolved, err,
				"secret %q: read %s: %v", ref.GetName(), locator, err)
		}
		return value, nil
	default:
		return nil, Fail(ReasonSecretSchemeForbid,
			"secret %q: source scheme %q is unknown", ref.GetName(), scheme)
	}
}

// resolveImage pulls the pinned image, or loads it from an embedded OCI layout.
func (d *OCIDispatcher) resolveImage(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
) (*orkstackv1.ImageRef, *Failure) {
	pinned := in.GetImage().GetPinned()
	info, err := d.Runtime.Pull(ctx, pinned)
	if err != nil && d.LayoutRoot != "" {
		// Docker load trusts blob filenames. Verify the descriptor and the exact
		// root bytes before allowing the runtime to import anything (§14.15).
		layout := filepath.Join(d.LayoutRoot, layoutFileName(in.GetImage().GetIndexDigest()))
		verified, verifyErr := verifyLayoutRoot(layout, in.GetImage().GetIndexDigest())
		switch {
		case errors.Is(verifyErr, errLayoutDigestMismatch):
			return nil, FailCause(ReasonImageDigestMismatch, verifyErr,
				"embedded image layout %s does not match %s: %v", layout, in.GetImage().GetIndexDigest(), verifyErr)
		case verifyErr == nil && verified:
			if loaded, loadErr := d.Runtime.LoadLayout(ctx, layout, in.GetImage().GetIndexDigest()); loadErr == nil {
				info = loaded
				err = nil
			}
		}
	}
	if err != nil {
		return nil, FailCause(ReasonImagePullFailed, err,
			"task %q: pull %s: %v", in.GetTaskId(), pinned, err)
	}

	// Clone rather than mutate: the input belongs to Temporal's decoded
	// payload, and an activity that edited its own argument would make a
	// retry's input differ from the recorded one.
	image := &orkstackv1.ImageRef{
		Reference:      in.GetImage().GetReference(),
		Registry:       in.GetImage().GetRegistry(),
		Repository:     in.GetImage().GetRepository(),
		IndexDigest:    in.GetImage().GetIndexDigest(),
		Pinned:         pinned,
		MediaType:      in.GetImage().GetMediaType(),
		Platforms:      in.GetImage().GetPlatforms(),
		PlatformDigest: info.PlatformDigest,
		Platform:       info.Platform,
		Delivery:       info.Delivery,
	}
	return image, nil
}

// layoutFileName renders the embedded layout tarball's name from a digest.
func layoutFileName(digest string) string {
	return strings.ReplaceAll(digest, ":", "-") + ".tar"
}

var errLayoutDigestMismatch = errors.New("embedded layout digest mismatch")

// verifyLayoutRoot checks both links the runtime does not: index.json must
// name digest, and blobs/sha256/<hex> must hash to that digest. It intentionally
// runs before LoadLayout.
func verifyLayoutRoot(layout, digest string) (bool, error) {
	const prefix = "sha256:"
	if !strings.HasPrefix(digest, prefix) || len(digest) != len(prefix)+64 {
		return false, nil
	}
	wantedBlob := "blobs/sha256/" + strings.TrimPrefix(digest, prefix)
	f, err := os.Open(layout)
	if err != nil {
		return false, nil
	}
	defer f.Close()
	tr := tar.NewReader(f)
	foundDescriptor := false
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false, fmt.Errorf("read embedded layout %s: %w", layout, err)
		}
		if filepath.ToSlash(h.Name) != "index.json" {
			continue
		}
		var index struct {
			Manifests []struct {
				Digest string `json:"digest"`
			} `json:"manifests"`
		}
		if err := json.NewDecoder(io.LimitReader(tr, 4<<20)).Decode(&index); err != nil {
			return false, fmt.Errorf("decode embedded layout index: %w", err)
		}
		for _, descriptor := range index.Manifests {
			if descriptor.Digest == digest {
				foundDescriptor = true
			}
		}
		break
	}
	if !foundDescriptor {
		return false, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	tr = tar.NewReader(f)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if filepath.ToSlash(h.Name) != wantedBlob {
			continue
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, tr); err != nil {
			return false, err
		}
		got := hex.EncodeToString(hash.Sum(nil))
		if got != strings.TrimPrefix(digest, prefix) {
			return false, fmt.Errorf("%w: blob claims %s, bytes hash to sha256:%s", errLayoutDigestMismatch, digest, got)
		}
		return true, nil
	}
}

func (d *OCIDispatcher) containerSpec(
	in *orkstackv1.ExecuteTaskInput,
	ws *Workspace,
	image *orkstackv1.ImageRef,
	attempt int32,
	network, cpu, memory string,
	runAsOrkerUser bool,
) (ContainerSpec, *Failure) {
	spec := ContainerSpec{
		Image:   image.GetPinned(),
		Command: in.GetCommand(),
		Env:     in.GetEnv(),
		WorkDir: ContainerRoot,
		Mounts:  ws.Mounts(),

		TmpfsPath:  ContainerScratchTmp,
		TmpfsBytes: d.Policy.TmpfsBytes,

		Network:   network,
		CPUs:      cpu,
		Memory:    memory,
		PidsLimit: d.Policy.PidsLimit,

		Labels: map[string]string{
			"ork.run":     in.GetRunId(),
			"ork.task":    in.GetTaskId(),
			"ork.attempt": fmt.Sprintf("%d", attempt),
			// ork.orker is how an Orker recognizes its own previous life's
			// containers, which is what the startup reaper keys on (§5.5).
			"ork.orker": d.OrkerID,
		},

		ReadOnlyRootfs:      true,
		DropAllCapabilities: true,
		NoNewPrivileges:     true,
		SeccompProfile:      SeccompBuiltin,
		Init:                true,

		// The runtime-owned lifetime bound of §5.5 measure 3: a reaper cannot
		// reach the containers of an Orker that never comes back, so the
		// container's own budget has to be enforced by something inside its
		// own lifetime.
		StopTimeoutSeconds: int(graceSecondsOf(in)),
	}
	// ork.pin names the opening of the pin this container belongs to, for a
	// pinned attempt only. It is not a reaper key — ork.orker already is — but
	// it is what lets an operator ask the runtime which containers are sharing
	// one directory, which is exactly the question a pin raises.
	if pin := in.GetPin(); pin != nil {
		spec.Labels["ork.pin"] = pin.GetInstanceId()
	}
	// ork.item names the item of a fan-out this container runs, beside
	// ork.task, so an operator can ask the runtime for one item's container.
	if each := in.GetEach(); each != nil {
		spec.Labels["ork.item"] = fmt.Sprintf("%d", each.GetIndex())
	}
	if runAsOrkerUser {
		spec.User = fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	}
	return spec, nil
}

// ---------------------------------------------------------------- steps 9-11

// runCapture is what one container run produced.
type runCapture struct {
	status     ExitStatus
	stdoutPath string
	stderrPath string
	finishedAt *timestamppb.Timestamp
}

func (d *OCIDispatcher) runContainer(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	ws *Workspace,
	id ContainerID,
	beat *Heartbeater,
	stdoutLine, stderrLine io.Writer,
) (*runCapture, *Failure) {
	logDir := filepath.Join(ws.Root, "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "create the log capture directory: %v", err)
	}
	capture := &runCapture{
		stdoutPath: filepath.Join(logDir, StdoutLogName),
		stderrPath: filepath.Join(logDir, StderrLogName),
	}

	// The two streams go to two places each: live to the sink with the
	// existing framing, and byte-exact to their own file for the durable
	// system artifact. They are never merged on disk (§3.6).
	stdoutFile, err := os.Create(capture.stdoutPath)
	if err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "create %s: %v", StdoutLogName, err)
	}
	defer func() { _ = stdoutFile.Close() }()
	stderrFile, err := os.Create(capture.stderrPath)
	if err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "create %s: %v", StderrLogName, err)
	}
	defer func() { _ = stderrFile.Close() }()

	logMax := in.GetLimits().GetLogMaxBytes()
	stdout := io.MultiWriter(stdoutLine, newCappedWriter(stdoutFile, logMax, beat))
	stderr := io.MultiWriter(stderrLine, newCappedWriter(stderrFile, logMax, beat))

	if err := d.Runtime.Start(ctx, id, stdout, stderr); err != nil {
		return capture, d.runtimeFailure(err, "start the container")
	}

	// Wait runs on a context of its OWN, not the activity's, so cancellation
	// does not abort the wait before the kill sequence has finished. The
	// select below is what observes cancellation.
	waitCtx, cancelWait := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWait()

	exited := make(chan struct{})
	var status ExitStatus
	var waitErr error
	go func() {
		defer close(exited)
		status, waitErr = d.Runtime.Wait(waitCtx, id)
	}()

	// timeout_seconds is the TASK's own wall clock and fires first. It is not
	// StartToCloseTimeout and not HeartbeatTimeout; confusing the three is how
	// a system ends up with a container nobody kills.
	var timeout <-chan time.Time
	if seconds := in.GetTimeoutSeconds(); seconds > 0 {
		timer := time.NewTimer(time.Duration(seconds) * time.Second)
		defer timer.Stop()
		timeout = timer.C
	}
	grace := time.Duration(graceSecondsOf(in)) * time.Second

	select {
	case <-exited:
		capture.finishedAt = timestamppb.New(time.Now().UTC())
		if waitErr != nil {
			return capture, FailCause(ReasonRuntimeRefused, waitErr,
				"wait for the container: %v", waitErr)
		}
		capture.status = status
		return capture, nil

	case <-timeout:
		beat.Phase("timeout")
		TerminateContainer(ctx, d.Runtime, id, grace, exited)
		<-exited
		capture.finishedAt = timestamppb.New(time.Now().UTC())
		return capture, Fail(ReasonContainerTimeout,
			"task %q: the container exceeded its timeout of %d seconds and was terminated "+
				"(SIGTERM, then SIGKILL after a %s grace period)",
			in.GetTaskId(), in.GetTimeoutSeconds(), grace)

	case <-ctx.Done():
		// Cancellation arrived through a heartbeat response. The sequence is
		// signal, wait the grace, kill, then let the deferred Remove run on a
		// detached context.
		beat.Phase("cancel")
		TerminateContainer(ctx, d.Runtime, id, grace, exited)
		<-exited
		capture.finishedAt = timestamppb.New(time.Now().UTC())
		// A canceled activity produces neither an ExecuteTaskResult nor an
		// ActivityCommit — Temporal permits only CanceledError details on
		// that path. What survives is the live log stream, which the sink has
		// been writing as numbered chunks throughout the run and which the
		// CLI and UI already read by (run, task, attempt) with no dependence
		// on a result (§3.6).
		return capture, nil
	}
}

// runtimeFailure separates "the runtime refused" from "the host cannot bind
// the workspace". The distinction is drawn from the runtime error, never from
// an exit code.
func (d *OCIDispatcher) runtimeFailure(err error, what string) *Failure {
	if errors.Is(err, ErrNotBindable) {
		return FailCause(ReasonWorkspaceNotBindable, err,
			"%s: the container daemon cannot bind-mount this orker's workspace root %q. "+
				"On macOS with a VM-hosted daemon $HOME is shared and /var/folders is not; point "+
				"%s at a shared path. Retrying will not fix a host misconfiguration: %v",
			what, d.WorkRoot, WorkRootVar, err)
	}
	return FailCause(ReasonRuntimeRefused, err, "%s: %v", what, err)
}

// ---------------------------------------------------------------- steps 12-13

func (d *OCIDispatcher) commit(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	ws *Workspace,
	capture *runCapture,
	image *orkstackv1.ImageRef,
	scanned []ScannedArtifact,
	values []byte,
	attempt int32,
	startedAt *timestamppb.Timestamp,
	trace *memoTrace,
) (*orkstackv1.ExecuteTaskResult, *Failure) {
	provenance := artifacts.Provenance{
		Orkflow: in.GetOrkflowName(),
		TaskID:  in.GetTaskId(),
		RunID:   in.GetRunId(),
	}

	// Step 3: hash everything, streaming.
	type pending struct {
		name        string
		path        string
		sha         string
		size        int64
		contentType string
	}
	items := make([]pending, 0, len(scanned))
	for _, artifact := range scanned {
		sha, size, err := HashFile(artifact.Path)
		if err != nil {
			return nil, FailCause(ReasonOutputNotReadable, err,
				"hash output artifact %q: %v", artifact.Name, err)
		}
		// Resolve the content type HERE, from the same bytes and the same rule
		// the store will use, so the step-4 projection and the step-5 upload
		// agree on the field. Leaving it empty made RefFor project "" while
		// Put sniffed a real type, so the bound understated the manifest it
		// claimed to measure byte-for-byte.
		contentType, err := sniffFileContentType(artifact.Path)
		if err != nil {
			return nil, FailCause(ReasonOutputNotReadable, err,
				"read output artifact %q to resolve its content type: %v", artifact.Name, err)
		}
		items = append(items, pending{
			name: artifact.Name, path: artifact.Path,
			sha: sha, size: size, contentType: contentType,
		})
	}
	valuesSHA := HashBytes(values)

	// Step 4: BOUND the commit before uploading a byte.
	//
	// The commit is the only thing a conforming workload can overflow without
	// breaking any workload-facing limit. Measuring it here, with refs that
	// are byte-identical to the ones step 8 will commit, means a violation
	// costs no bandwidth and produces no orphans — and the only code available
	// at step 8 would be retryable, so discovering it there would loop.
	projected := &orkstackv1.ActivityCommit{
		SchemaVersion:           artifacts.CommitSchemaVersion,
		Namespace:               artifacts.ResolveNamespace(in.GetNamespace()),
		RunId:                   in.GetRunId(),
		TaskId:                  in.GetTaskId(),
		OrkflowName:             in.GetOrkflowName(),
		OrkflowVersion:          in.GetOrkflowVersion(),
		OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
		IdempotencyKey:          in.GetIdempotencyKey(),
		Image:                   image,
		ExitCode:                capture.status.Code,
		StartedAt:               startedAt,
		FinishedAt:              capture.finishedAt,
		Attempt:                 attempt,
		OrkerId:                 d.OrkerID,
		BuildId:                 d.BuildID,
		// A cached task's commit records its memo decision — set here, so the
		// step-4 projection measures it. Nil for every other task, which
		// keeps its commit byte-identical.
		Cache: commitCache(trace.decision()),
	}
	if d.Refs != nil {
		projected.Values = refPtr(d.Refs.RefFor(provenance, "values.json", valuesSHA, int64(len(values)), "application/json"))
		for _, item := range items {
			projected.Artifacts = append(projected.Artifacts,
				refPtr(d.Refs.RefFor(provenance, item.name, item.sha, item.size, item.contentType)))
		}
		for _, name := range []string{StdoutLogName, StderrLogName} {
			projected.SystemArtifacts = append(projected.SystemArtifacts,
				refPtr(d.Refs.RefFor(provenance, name, strings.Repeat("0", 64), 0, "text/plain")))
		}
		document, err := artifacts.MarshalCommit(projected)
		if err != nil {
			return nil, FailCause(ReasonCommitWriteFailed, err, "project the commit: %v", err)
		}
		if max := in.GetLimits().GetCommitMaxBytes(); max > 0 && int64(len(document)) > max {
			return nil, Fail(ReasonCommitTooLarge,
				"the completion manifest for task %q would be %d bytes, over the %d-byte limit; "+
					"measured before any upload, so no bandwidth was spent and no orphan objects exist",
				in.GetTaskId(), len(document), max)
		}
	}

	// Steps 5-7: upload artifacts, then values.json, then the streams.
	uploaded := make([]*orkstackv1.ArtifactRef, 0, len(items))
	for _, item := range items {
		ref, failure := d.upload(ctx, provenance, item.name, item.path, item.contentType, item.sha, item.size)
		if failure != nil {
			return nil, failure
		}
		uploaded = append(uploaded, ref)
	}
	valuesRef, failure := d.uploadBytes(ctx, provenance, "values.json", values, "application/json")
	if failure != nil {
		return nil, failure
	}
	systemRefs, failure := d.uploadStreams(ctx, provenance, capture)
	if failure != nil {
		return nil, failure
	}

	// Step 8: the conditional create. This is the completion boundary;
	// nothing after it can change the outcome.
	commit := projected
	commit.Values = valuesRef
	commit.Artifacts = uploaded
	commit.SystemArtifacts = systemRefs

	result := &orkstackv1.ExecuteTaskResult{
		TaskId:          in.GetTaskId(),
		ExitCode:        capture.status.Code,
		Artifacts:       uploaded,
		SystemArtifacts: systemRefs,
		ValuesRef:       valuesRef,
		Image:           image,
		StartedAt:       startedAt,
		FinishedAt:      capture.finishedAt,
		OrkerId:         d.OrkerID,
		Attempt:         attempt,
	}
	if len(values) <= InlineValuesLimit {
		result.ValuesJson = values
		result.ValuesInlined = true
	}

	if d.Commits == nil || in.GetCommitUri() == "" {
		result.Cache = resultCache(trace.decision())
		return result, nil
	}
	record, err := d.Commits.Create(ctx, in.GetCommitUri(), commit)
	switch {
	case err == nil:
		result.Commit = &orkstackv1.CommitRef{
			Uri:       in.GetCommitUri(),
			Sha256:    record.SHA256,
			Etag:      record.ETag,
			CreatedAt: capture.finishedAt,
		}
		// The memo is written only here, after THIS attempt's commit was
		// created — never after the conflict branch's adoption below — so a
		// memo always points at a commit that already exists.
		if decision := trace.decision(); shouldWriteMemo(decision) {
			d.writeMemo(ctx, in, memoKey{Key: decision.GetKey(), Components: decision.GetKeyComponents()},
				commit, record, trace)
		}
		result.Cache = resultCache(trace.decision())
		return result, nil

	case errors.Is(err, artifacts.ErrPreconditionFailed):
		// Another Orker committed the same logical activity while this one was
		// working — possible whenever Temporal delivers a retry to a second
		// worker before the first finished. Read the winner, apply the same
		// identity checks as step 0, and return THAT commit's result.
		//
		// The loser's uploaded artifacts are inert: with content-addressed
		// keys they are either byte-identical to the winner's (same key, same
		// bytes) or unreferenced by any commit, and in neither case can they
		// corrupt what the winner recorded.
		adopted, failure := d.adopt(ctx, in, attempt)
		if failure != nil {
			return nil, failure
		}
		if adopted == nil {
			return nil, FailCause(ReasonCommitWriteFailed, err,
				"the commit at %s was refused as already present, but reading it found nothing",
				in.GetCommitUri())
		}
		return adopted, nil

	default:
		// A storage failure can never look like success (AGENTS.md §15).
		return nil, FailCause(ReasonCommitWriteFailed, err,
			"write the commit at %s: %v", in.GetCommitUri(), err)
	}
}

// upload sends the file at path. sha and size are the digest step 3 already
// computed with HashFile, or "" when nothing has hashed it yet: a known digest
// lets the store upload straight from the file, once, with the digest as a
// checksum the server verifies, instead of spooling and hashing it again.
func (d *OCIDispatcher) upload(
	ctx context.Context,
	prov artifacts.Provenance,
	name, path, contentType, sha string,
	size int64,
) (*orkstackv1.ArtifactRef, *Failure) {
	file, err := os.Open(path)
	if err != nil {
		return nil, FailCause(ReasonOutputNotReadable, err, "open output artifact %q: %v", name, err)
	}
	defer func() { _ = file.Close() }()
	return d.put(ctx, prov, name, file, contentType, sha, size)
}

func (d *OCIDispatcher) uploadBytes(
	ctx context.Context,
	prov artifacts.Provenance,
	name string,
	data []byte,
	contentType string,
) (*orkstackv1.ArtifactRef, *Failure) {
	return d.put(ctx, prov, name, strings.NewReader(string(data)), contentType, HashBytes(data), int64(len(data)))
}

func (d *OCIDispatcher) put(
	ctx context.Context,
	prov artifacts.Provenance,
	name string,
	r io.Reader,
	contentType, sha string,
	size int64,
) (*orkstackv1.ArtifactRef, *Failure) {
	if d.Store == nil {
		return nil, Fail(ReasonArtifactUploadFailed, "this orker has no artifact store")
	}
	metadata := prov.Metadata(nil)
	metadata["artifact"] = name
	ref, err := d.Store.Put(ctx, name, r, artifacts.PutOptions{
		ContentType:    contentType,
		Metadata:       metadata,
		ExpectedSHA256: sha,
		ExpectedSize:   size,
		SpoolDir:       d.spoolDir(),
	})
	if err != nil {
		return nil, FailCause(ReasonArtifactUploadFailed, err, "upload %q: %v", name, err)
	}
	return refPtr(ref), nil
}

// SpoolDirName is the directory under the work root where a store spools a
// reader it cannot seek. Boot empties it: a spool file outlives its upload only
// when the process was killed mid-upload, and nothing will ever read it.
const SpoolDirName = "ork-spool"

func (d *OCIDispatcher) spoolDir() string {
	root := d.WorkRoot
	if root == "" {
		root = os.TempDir()
	}
	return filepath.Join(root, SpoolDirName)
}

// uploadStreams publishes stdout.log and stderr.log as system artifacts.
func (d *OCIDispatcher) uploadStreams(
	ctx context.Context,
	prov artifacts.Provenance,
	capture *runCapture,
) ([]*orkstackv1.ArtifactRef, *Failure) {
	refs := make([]*orkstackv1.ArtifactRef, 0, 2)
	for _, stream := range []struct{ name, path string }{
		{StdoutLogName, capture.stdoutPath},
		{StderrLogName, capture.stderrPath},
	} {
		if stream.path == "" {
			continue
		}
		if _, err := os.Stat(stream.path); err != nil {
			continue
		}
		// No digest yet: a stream capture is hashed by the store in one pass
		// and uploaded from the same file.
		ref, failure := d.upload(ctx, prov, stream.name, stream.path, "text/plain; charset=utf-8", "", 0)
		if failure != nil {
			return nil, failure
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// ---------------------------------------------------------------- step 14

// attachFailureDetail is the §7.1 step-14 failure path.
//
// A FAILED TASK STILL PUBLISHES WHAT IT PRODUCED. The compiler errors, test
// reports and core dumps a failing task produces are exactly what somebody
// opens the run to read, and they live in a workdir that is about to be
// deleted.
//
// The failure path SCANS TOO, under the same §3.5 rules — an earlier draft
// left them unenforced on exactly the path a broken or hostile image is most
// likely to take. What differs is the consequence: a violation here is a NOTE
// attached to the workload's error, never a replacement for it. A task that
// exited 1 and wrote a symlink into output/artifacts/ is reported as exit 1,
// with the symlink named in the details and not uploaded.
//
// An upload failure on this path is swallowed, deliberately: the workload's
// error is WHY the task failed, and replacing it with a storage error would
// tell the author their build failed because of the bucket.
func (d *OCIDispatcher) attachFailureDetail(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	ws *Workspace,
	capture *runCapture,
	image *orkstackv1.ImageRef,
	attempt int32,
	failure *Failure,
) *Failure {
	detail := &orkstackv1.ActivityFailureDetail{
		ExitCode: capture.exitCode(),
		Image:    image,
		Attempt:  attempt,
		OrkerId:  d.OrkerID,
	}
	prov := artifacts.Provenance{
		Orkflow: in.GetOrkflowName(),
		TaskID:  in.GetTaskId(),
		RunID:   in.GetRunId(),
	}
	// Uploads on the failure path run on a detached context: the activity may
	// already be past its deadline, and losing the evidence is the outcome
	// this path exists to prevent.
	upCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()

	if ws != nil {
		scanned, scanFailure := ScanOutputArtifacts(ws.Artifacts, in.GetLimits())
		if scanFailure != nil {
			detail.ScanNotes = append(detail.ScanNotes, scanFailure.Error())
		}
		for _, artifact := range scanned {
			if readFailure := CheckReadable(artifact.Path, ContainerArtifacts+"/"+artifact.Name); readFailure != nil {
				detail.ScanNotes = append(detail.ScanNotes, readFailure.Error())
				continue
			}
			ref, upFailure := d.upload(upCtx, prov, artifact.Name, artifact.Path, "", "", 0)
			if upFailure != nil {
				detail.ScanNotes = append(detail.ScanNotes,
					fmt.Sprintf("artifact %q could not be stored: %s", artifact.Name, upFailure.Message))
				continue
			}
			detail.Artifacts = append(detail.Artifacts, ref)
		}
	}
	if capture != nil {
		if refs, upFailure := d.uploadStreams(upCtx, prov, capture); upFailure == nil {
			detail.SystemArtifacts = refs
		} else {
			detail.ScanNotes = append(detail.ScanNotes,
				"the captured streams could not be stored: "+upFailure.Message)
		}
	}
	return failure.WithDetail(detail)
}

func (c *runCapture) exitCode() int32 {
	if c == nil {
		return 0
	}
	return c.status.Code
}

// ---------------------------------------------------------------- step 16

// attemptRecordTimeout bounds the one small PUT the attempt record costs.
//
// It is short on purpose. The record is written after the attempt's outcome is
// already decided, on a context that has usually just been cancelled or has
// just passed its deadline — so the only question is whether the store answers
// promptly, and a slow store must not hold an activity's worker slot open. A
// write that does not finish in this window is logged and dropped, which is
// the same trade every other best-effort path in this file makes.
const attemptRecordTimeout = 5 * time.Second

// recordAttempt writes the immutable record of ONE attempt (§7.1 step 16).
//
// WHY IT EXISTS. Temporal owns retries, so the workflow only ever sees a task's
// final outcome: when attempt 1 fails, its ApplicationError is consumed by the
// scheduler for attempt 2 and nothing durable is left behind. The Orker is the
// only process that observes every attempt, and end() is its exactly-once
// per-attempt seam, so this is the one place a complete retry timeline can be
// written from. SUCCEEDED and adopted attempts get a record too: a timeline
// with no final row would force every reader to synthesize one from TaskRun and
// commit.json, and two producers of one row is how they come to disagree.
//
// IT IS BEST-EFFORT, ALWAYS. A record that cannot be written is a Warn and
// nothing else. This function returns nothing and touches neither the result
// nor the failure, because an observability write that could fail a task would
// make retry visibility a new way for work to fail — the same projection-not-
// orchestration rule the engine's emit() already applies to PublishEvent.
//
// THE CONTEXT IS DETACHED. A CANCELED attempt is exactly the attempt somebody
// most wants the record of, and its ctx is already done; attachFailureDetail
// detaches for the same reason one step earlier.
func (d *OCIDispatcher) recordAttempt(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	attempt int32,
	startedAt *timestamppb.Timestamp,
	beat *Heartbeater,
	result *orkstackv1.ExecuteTaskResult,
	failure *Failure,
	outcome AttemptOutcome,
) {
	if d.Attempts == nil || in == nil {
		return
	}
	record := d.attemptRecord(in, attempt, startedAt, beat, result, failure, outcome)
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), attemptRecordTimeout)
	defer cancel()
	if err := d.Attempts.PutAttempt(writeCtx, record); err != nil {
		d.logger().Warn("task attempt record not written",
			"run_id", in.GetRunId(),
			"task_id", in.GetTaskId(),
			"attempt", attempt,
			"outcome", record.GetOutcome().String(),
			"error", err)
	}
}

// attemptRecord projects one attempt onto the TaskAttempt message. It is split
// from recordAttempt so a test can assert the projection without a store.
func (d *OCIDispatcher) attemptRecord(
	in *orkstackv1.ExecuteTaskInput,
	attempt int32,
	startedAt *timestamppb.Timestamp,
	beat *Heartbeater,
	result *orkstackv1.ExecuteTaskResult,
	failure *Failure,
	outcome AttemptOutcome,
) *orkstackv1.TaskAttempt {
	record := &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion,
		RunId:         in.GetRunId(),
		TaskId:        in.GetTaskId(),
		Attempt:       attempt,
		ExitCode:      outcome.ExitCode,
		StartedAt:     startedAt,
		FinishedAt:    timestamppb.New(time.Now().UTC()),
		OrkerId:       d.OrkerID,
		BuildId:       d.BuildID,
		// The routing REQUEST, as TaskRun.pool spells it — resolvedPool is the
		// same helper task.json is composed with, so the record names the pool
		// the workload was told it ran in.
		Pool:          resolvedPool(in.GetPool()),
		CommitAdopted: outcome.CommitAdopted,
	}
	// An item's record carries its index and is keyed by it
	// (metadata/attempts/{run}/{task}/{item}/{attempt}.json).
	if each := in.GetEach(); each != nil {
		index := each.GetIndex()
		record.Item = &index
	}
	// What this Orker reserved for the attempt — the same effective values
	// execute() gave the container and task.json — so usage can be derived
	// from the record alone (docs/STATS.md, "Usage").
	record.Resources = d.Policy.ReservedResources(in.GetResources())
	if beat != nil {
		// The phase the attempt ENDED in, straight off the heartbeater rather
		// than a second copy of the sequence: "run" and "commit" are different
		// stories about the same non-zero exit code.
		record.Phase = beat.progress().Phase
		// Every phase it passed through, closed at the record's own
		// finished_at: the same sequence, so the last span's name IS Phase.
		record.Phases = beat.PhaseSpans(record.FinishedAt.AsTime())
	}

	// The branch order is Execute's own, and it is load-bearing there for the
	// reason §6.3 gives: a cancelled attempt is CANCELED whatever failure was
	// in flight when the teardown reached it.
	switch {
	case outcome.Canceled:
		record.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED
	case failure != nil:
		record.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
		record.Error = failure.ErrorProto(in.GetTaskId())
	case outcome.Err != nil:
		// The panic path: Execute recovers, calls end with ReasonPanic and
		// re-panics, so there is no *Failure to read. A record with no error at
		// all would report the one exit path that is not a normal return as
		// indistinguishable from a clean one.
		record.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED
		record.Error = (&Failure{
			Reason:  outcome.Reason,
			Message: outcome.Err.Error(),
		}).ErrorProto(in.GetTaskId())
	case result != nil:
		record.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
	}

	// The image AS EXECUTED and the streams this attempt managed to upload,
	// from whichever of the two carriers exists. Neither is reachable from
	// Execute's own scope: the image is resolved inside execute(), and a
	// failure's stream refs are attached by attachFailureDetail.
	switch {
	case result != nil:
		record.Image = result.GetImage()
		record.SystemArtifacts = result.GetSystemArtifacts()
		if record.ExitCode == 0 {
			record.ExitCode = result.GetExitCode()
		}
		record.CommitAdopted = record.CommitAdopted || result.GetCommitAdopted()
	case failure.GetDetail() != nil:
		record.Image = failure.Detail.GetImage()
		record.SystemArtifacts = failure.Detail.GetSystemArtifacts()
		if record.ExitCode == 0 {
			record.ExitCode = failure.Detail.GetExitCode()
		}
	}

	// A cached task's memo decision, key_components included: how an
	// operator sees which input changed between a miss and the run whose memo
	// it no longer matches. A HIT pulled nothing and uploaded nothing, so its
	// image is the input's pinned reference (no platform) and it carries no
	// system artifacts — the result's are the producer's.
	if outcome.Cache != nil {
		record.Cache = proto.Clone(outcome.Cache).(*orkstackv1.CacheResult)
		if result != nil && !result.GetCommitAdopted() &&
			outcome.Cache.GetOutcome() == orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
			record.Image = in.GetImage()
			record.SystemArtifacts = nil
		}
	}
	return record
}

// ---------------------------------------------------------------- helpers

func (d *OCIDispatcher) makeAttemptDir(in *orkstackv1.ExecuteTaskInput, attempt int32) (string, error) {
	root := d.WorkRoot
	if root == "" {
		root = os.TempDir()
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", fmt.Errorf("create workdir root %s: %w", root, err)
	}
	pattern := fmt.Sprintf("ork-%s-%s-%d-*",
		safeComponent(in.GetRunId()), safeComponent(in.GetTaskId()), attempt)
	// An item's directory names it, for an operator reading the work root:
	// ork-{run}-{task}-i00097-{attempt}-*.
	if each := in.GetEach(); each != nil {
		pattern = fmt.Sprintf("ork-%s-%s-i%s-%d-*",
			safeComponent(in.GetRunId()), safeComponent(in.GetTaskId()), itemSegmentOf(each.GetIndex()), attempt)
	}
	dir, err := os.MkdirTemp(root, pattern)
	if err != nil {
		return "", err
	}
	// 0700 is what makes the 1777 children safe: no other host user can
	// traverse into them (§3.1).
	if err := os.Chmod(dir, ModeAttempt); err != nil {
		return "", err
	}
	return dir, nil
}

func (d *OCIDispatcher) openLogSink(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	attempt int32,
) *logChunkSink {
	item := artifacts.NoItem
	if each := in.GetEach(); each != nil {
		item = artifacts.ItemAt(each.GetIndex())
	}
	return newLogChunkSink(ctx, d.Logs, artifacts.LogRef{
		RunID:   in.GetRunId(),
		TaskID:  in.GetTaskId(),
		Item:    item,
		Attempt: attempt,
	}, d.LogOpts).withErrorReporter(func(err error) {
		if activity.IsActivity(ctx) {
			activity.GetLogger(ctx).Warn("task log chunk lost",
				"run_id", in.GetRunId(), "task_id", in.GetTaskId(), "error", err)
		}
	})
}

// cappedWriter bounds one captured stream at limits.logMaxBytes.
type cappedWriter struct {
	w       io.Writer
	limit   int64
	written int64
	beat    *Heartbeater
}

func newCappedWriter(w io.Writer, limit int64, beat *Heartbeater) io.Writer {
	return &cappedWriter{w: w, limit: limit, beat: beat}
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	if c.beat != nil {
		c.beat.Logged(int64(len(p)))
	}
	if c.limit > 0 && c.written >= c.limit {
		// The bytes are dropped rather than the write failing: a workload
		// that logs too much has still done its work, and the live stream
		// already carries a truncation notice.
		return len(p), nil
	}
	allowed := p
	if c.limit > 0 && c.written+int64(len(p)) > c.limit {
		allowed = p[:c.limit-c.written]
	}
	n, err := c.w.Write(allowed)
	c.written += int64(n)
	return len(p), err
}

func attemptOf(ctx context.Context) int32 {
	if activity.IsActivity(ctx) {
		return activity.GetInfo(ctx).Attempt
	}
	return 1
}

func graceSecondsOf(in *orkstackv1.ExecuteTaskInput) int32 {
	if seconds := in.GetCancellationGracePeriodSeconds(); seconds > 0 {
		return seconds
	}
	return int32(DefaultCancellationGrace / time.Second)
}

func resolvedPool(pool string) string {
	if pool == "" {
		return "main"
	}
	return pool
}

// render turns the RAW `with:` block into config.json.
//
// A render failure is ContractViolation/RENDER_FAILED and is never retried: a
// missing key, a template parse error and a rendered "<no value>" are all
// facts about the definition, and the definition does not change between
// attempts (§3.3).
func (d *OCIDispatcher) render(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
	staged *stagedInputs,
	eachItem json.RawMessage,
) (map[string]any, *Failure) {
	with := map[string]any{}
	if in.GetWith() != nil {
		with = in.GetWith().AsMap()
	}
	eachData, err := eachRenderData(in.GetEach(), eachItem)
	if err != nil {
		return nil, FailCause(ReasonEachItemUnavailable, err, "task %q item %d: %v",
			in.GetTaskId(), in.GetEach().GetIndex(), err)
	}
	rendered, err := newRenderer(
		in.GetTaskId(),
		payloadMapOf(in.GetRunInput()),
		staged.upstreamValues,
		staged.upstreamArtifacts,
		staged.paths,
		d.EnvAllowlist,
		eachData,
	).render(ctx, with)
	if err != nil {
		return nil, FailCause(ReasonRenderFailed, err, "%v", err)
	}
	return rendered, nil
}
func refPtr(ref artifacts.ArtifactRef) *orkstackv1.ArtifactRef {
	return &orkstackv1.ArtifactRef{
		Name:        ref.Name,
		Uri:         ref.URI,
		SizeBytes:   ref.SizeBytes,
		Sha256:      ref.SHA256,
		ContentType: ref.ContentType,
	}
}

// payloadMapOf decodes a Payload into the plain map run.json is written from
// and templates address as .inputs.
func payloadMapOf(p *orkstackv1.Payload) map[string]any {
	if p.GetData() == nil {
		return map[string]any{}
	}
	return p.GetData().AsMap()
}
