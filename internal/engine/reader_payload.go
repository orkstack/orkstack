package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// RunKeySource gives a reader the key of a run it was asked for (RunKeys).
type RunKeySource interface {
	RunKeyFor(ctx context.Context, c payloadcrypt.Context) (RunKeyHeld, error)
}

// WithPayloadKeys opens a sealed run's payloads with keys
// (payload-encryption.md §7). Without it a sealed run's history does not
// read.
func (r *TemporalRunReader) WithPayloadKeys(keys RunKeySource) *TemporalRunReader {
	r.payloadKeys = keys
	return r
}

// errPayloadKeyUnavailable is a key that could not be had: the cluster, or an
// engine's key file. It says nothing of the run, so a read that meets it is
// an error, never an empty or a lesser answer. A run that is keyless, or a
// payload that does not open, is its own.
var errPayloadKeyUnavailable = errors.New("a key of the run could not be had")

// runKeyWait is how long one read waits for a run's key.
const runKeyWait = 30 * time.Second

// runNamespaceKeyWait is how long runNamespace waits for a run's key before it
// takes the memo's word: well under one attempt of a force cancel's step.
const runNamespaceKeyWait = 3 * time.Second

// runPayloads reads the payloads of one run's history, for one read of it. It
// opens what is sealed for that run alone: a payload sealed for another run,
// copied into this history, is ErrUnreadable and its run's key is not asked
// for, because whoever may read this run may not read that one through it.
// The key is asked for once, when first needed, and its answer, or why there
// is none, is the whole read's.
type runPayloads struct {
	converter.DataConverter
	own payloadcrypt.Context
	key func() (RunKeyHeld, error) // nil for a reader with no source
	// keyless is set once the read has found the run keyless: nothing more
	// of it is opened. A read is one goroutine's.
	keyless error
}

// holdStart holds an execution's start, the one this read is of, to rule 6
// (payload-encryption.md section 4.1) before anything of it is used, its
// carry included: at every read, whether or not the run's key is already
// held, because a start can be copied under another memo. A start that fails
// it is a keyless run's, whatever its key opens, and nothing of the run is
// opened from there on. A key that could not be had is an error.
func (p *runPayloads) holdStart(started *historypb.WorkflowExecutionStartedEventAttributes) error {
	field, keyed := started.GetMemo().GetFields()[payloadcrypt.MemoField]
	if p.key == nil || !sealedStart(started) {
		return nil // nothing is opened, or nothing is sealed
	}
	if !keyed {
		p.keyless = fmt.Errorf("%w: %s's start is sealed and its memo holds no key", payloadcrypt.ErrKeyless, p.own)
		logNotOpened("the start of an encrypted run is not the run's; its memo alone is shown", p.own, p.keyless)
		return nil
	}
	key, err := p.key()
	if errors.Is(err, errPayloadKeyUnavailable) {
		return fmt.Errorf("engine: read the start of %s: %w", p.own, err)
	}
	if err == nil {
		err = p.sameTree(key.Field, field)
		if err == nil {
			err = startIsTheRuns(payloadcrypt.NewScope(key.Key, p.own), key.Field.Namespace, started.GetMemo(), started)
		}
	}
	if p.keyless = err; err != nil {
		logNotOpened("the start of an encrypted run does not open; its memo alone is shown", p.own, err)
	}
	return nil
}

// logNotOpened says what of a run does not open, and why. A keyless run's is
// expected, and every read of it meets it.
func logNotOpened(what string, own payloadcrypt.Context, err error) {
	level := slog.LevelWarn
	if errors.Is(err, payloadcrypt.ErrKeyless) {
		level = slog.LevelDebug
	}
	slog.Log(context.Background(), level, what, "run", own.ID, "error", err)
}

// sameTree holds the key field a start carries to the one the run's key was
// opened from, verified: the same run tree's key, of the same namespace. A
// wrap under another engines' key is another wrap of the same key.
func (p *runPayloads) sameTree(verified payloadcrypt.KeyMemo, field *commonpb.Payload) error {
	own, err := payloadcrypt.ParseKeyMemo(field.GetData())
	if err != nil {
		return err
	}
	own.EngineKeyID, own.Wrap = verified.EngineKeyID, nil
	verified.Wrap = nil
	if !reflect.DeepEqual(own, verified) {
		return fmt.Errorf("%w: %s's start carries another key field than the run", payloadcrypt.ErrKeyless, p.own)
	}
	return nil
}

// memoField is a memo's plaintext string field.
func memoField(memo *commonpb.Memo, name string) (string, bool) {
	var v string
	f, ok := memo.GetFields()[name]
	ok = ok && DataConverter().FromPayload(f, &v) == nil
	return v, ok
}

// runConverter is the payloads of runID's history, read under ctx.
func (r *TemporalRunReader) runConverter(ctx context.Context, runID string) *runPayloads {
	p := &runPayloads{DataConverter: r.converter, own: payloadcrypt.RunContext(runID)}
	if r.payloadKeys == nil {
		return p
	}
	held := sync.OnceValues(func() (RunKeyHeld, error) {
		bounded, cancel := context.WithTimeout(ctx, runKeyWait)
		defer cancel()
		key, err := r.payloadKeys.RunKeyFor(bounded, p.own)
		// Retired since the read began: what the API answers from the archive.
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			err = fmt.Errorf("%w (%w)", ErrRunNotFound, err)
		}
		if err != nil && !errors.Is(err, payloadcrypt.ErrKeyless) && !errors.Is(err, payloadcrypt.ErrUnreadable) {
			err = fmt.Errorf("%w: %w", errPayloadKeyUnavailable, err)
		}
		return key, err
	})
	p.key = func() (RunKeyHeld, error) {
		if p.keyless != nil {
			return RunKeyHeld{}, p.keyless
		}
		return held()
	}
	p.DataConverter = payloadcrypt.NewConverter(r.converter, nil).WithReaderKeys(func(_ context.Context, c payloadcrypt.Context) (*payloadcrypt.Key, error) {
		if c != p.own {
			return nil, fmt.Errorf("%w: a payload of %s in the history of %s", payloadcrypt.ErrUnreadable, c, p.own)
		}
		key, err := p.key()
		return key.Key, err
	})
	return p
}

// runNamespace is the namespace of the run whose start attrs is: its
// definition's, or, where a sealed start does not open now or ever, its
// memo's, which the engine that started it wrote. For a decision that must be
// possible of any run, a force cancel of one whose key cannot be had above
// all: whoever could forge a memo may end the run outright.
func (r *TemporalRunReader) runNamespace(ctx context.Context, runID string, attrs *historypb.WorkflowExecutionStartedEventAttributes) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, runNamespaceKeyWait)
	defer cancel()
	dc := r.runConverter(ctx, runID)
	err := holdOrKeyless(dc, attrs)
	if err == nil {
		namespace, startErr := startedNamespace(dc, attrs)
		if startErr == nil {
			return namespace, nil
		}
		err = startErr
	}
	if namespace, ok := memoField(attrs.GetMemo(), MemoNamespace); ok && namespace != "" {
		return namespace, nil
	}
	return "", fmt.Errorf("engine: run %s: its namespace: %w", runID, err)
}

// startedFromMemo names run's namespace and Orkflow from the memo it was
// started with, for a sealed start that does not open. A memo that names no
// namespace is an error.
func startedFromMemo(run *orkstackv1.Run, memo *commonpb.Memo) error {
	namespace, ok := memoField(memo, MemoNamespace)
	if !ok || namespace == "" {
		return errors.New("its start does not open, and its memo names no namespace")
	}
	run.Namespace = namespace
	run.OrkflowName, _ = memoField(memo, MemoOrkflowName)
	run.OrkflowVersion, _ = memoField(memo, MemoOrkflowVersion)
	run.OrkflowNamespace, _ = memoField(memo, MemoOrkflowOwner)
	return nil
}

// openedClose is a run's closing event with its failure, or the details of
// its cancel, opened where they are sealed for the run. One that does not
// open with the run's key, or whose run is keyless, stays as the cluster
// holds it: its reason code is all of it that reads. A key that could not be
// had is an error: the close says why the run ended, and is not guessed.
func (p *runPayloads) openedClose(e *historypb.HistoryEvent) (*historypb.HistoryEvent, error) {
	failure := e.GetWorkflowExecutionFailedEventAttributes().GetFailure()
	details := e.GetWorkflowExecutionCanceledEventAttributes().GetDetails().GetPayloads()
	// Nothing is asked for a close that holds nothing sealed.
	if p.key == nil || !payloadcrypt.FailureSealed(failure) && !slices.ContainsFunc(details, payloadcrypt.IsSealed) {
		return e, nil
	}
	opened, err := p.openClose(e)
	if errors.Is(err, errPayloadKeyUnavailable) {
		return nil, fmt.Errorf("engine: read how %s ended: %w", p.own, err)
	}
	if err != nil {
		logNotOpened("how an encrypted run ended does not open; its reason code alone is shown", p.own, err)
		return e, nil
	}
	return opened, nil
}

func (p *runPayloads) openClose(e *historypb.HistoryEvent) (*historypb.HistoryEvent, error) {
	key, err := p.key()
	if err != nil {
		return nil, err
	}
	scope := payloadcrypt.NewScope(key.Key, p.own)
	opened := proto.Clone(e).(*historypb.HistoryEvent)
	if attrs := opened.GetWorkflowExecutionFailedEventAttributes(); attrs.GetFailure() != nil {
		if attrs.Failure, err = payloadcrypt.OpenFailureProto(scope, attrs.GetFailure()); err != nil {
			return nil, err
		}
	}
	payloads := opened.GetWorkflowExecutionCanceledEventAttributes().GetDetails().GetPayloads()
	for i, d := range payloads {
		if !payloadcrypt.IsSealed(d) {
			continue
		}
		if payloads[i], err = scope.Open(d); err != nil {
			return nil, err
		}
	}
	return opened, nil
}

// holdOrKeyless is holdStart for a read that has no lesser answer: a keyless
// run is an error of it.
func holdOrKeyless(p *runPayloads, started *historypb.WorkflowExecutionStartedEventAttributes) error {
	if err := p.holdStart(started); err != nil {
		return err
	}
	return p.keyless
}
