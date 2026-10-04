package artifacts

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
)

// A task's output is bytes, and bytes travel as stored objects — never inside a
// Temporal payload and never on the Event stream (AGENTS.md §2.2). So the live
// log of a running task is a sequence of small objects in the artifact bucket:
//
//	metadata/logs/{run-id}/{task-id}/{attempt}/{seq}          one chunk
//	metadata/logs/{run-id}/{task-id}/{attempt}/{seq}.trunc    the truncation notice
//
// ORDER LIVES IN THE KEY. Both counters are rendered zero-padded to a fixed
// width, so a lexicographic listing IS numeric order and a reader never sorts
// by LastModified — two clocks, two replicas and a retry make modification
// time a lie, while the key is assigned by the single Orker executing that
// attempt and is therefore dense, total and gap-free without any coordination.
//
// The `.trunc` suffix is a sidecar spelling rather than a flag inside the
// object, so a reader learns a stream was cut from the key alone: the notice
// survives a body that fails to download, and it sorts after every ordinary
// chunk of the same sequence, so a resume can never hand it out twice.
const (
	// LogPrefix roots every task log chunk in the bucket. It sits beside the
	// run archive rather than inside it, so an operator lifecycle rule can
	// expire logs without touching run history (docs/DEPLOY.md).
	LogPrefix = "metadata/logs"

	// LogChunkContentType is what a chunk object is served as.
	LogChunkContentType = "text/plain; charset=utf-8"

	// logSeqWidth is the fixed width of the sequence segment. int64 tops out
	// at 19 digits, so 20 renders every sequence there can be without ever
	// growing the segment — a widening segment would reorder the keys already
	// written.
	logSeqWidth = 20

	// logAttemptWidth is the fixed width of the attempt segment, for the same
	// reason at int32's 10 digits: `LatestLogAttempt` compares attempts by
	// listing, and unpadded "10" sorts before "2".
	logAttemptWidth = 10

	// logTruncSuffix marks the truncation notice.
	logTruncSuffix = ".trunc"
)

// ErrInvalidLogKey reports a log coordinate that cannot be addressed.
var ErrInvalidLogKey = errors.New("invalid task log key")

// LogRef addresses one attempt of one task within one run: the unit a log
// stream is ordered over. A retry writes a fresh attempt rather than appending
// to the last, so a resumed reader never sees two tries interleaved.
type LogRef struct {
	RunID  string
	TaskID string
	// Item names one item of a fan-out task; NoItem for a task's own log.
	// An item's chunks live one segment below its task's:
	// metadata/logs/{run}/{task}/{item:05}/{attempt}/{seq}.
	Item    Item
	Attempt int32
}

// LogChunk is one durable slice of a task attempt's output.
type LogChunk struct {
	LogRef
	// Seq orders the chunk within its attempt: dense, 1-based.
	Seq int64
	// Data is raw log bytes, whole lines except where one line exceeded the
	// chunk limit and was split.
	Data []byte
	// Truncated marks the notice chunk written when an attempt hit its byte
	// cap. Nothing was stored after it.
	Truncated bool
}

// LogStore reads and writes task log chunks. It is a separate interface from
// Store because the two have opposite shapes: an artifact is one immutable
// blob named by its content, while a log is an append-only sequence read by
// range while it is still being written.
//
// Implementations hold no cache and no per-run state: the Orker that writes a
// chunk and the engine replica that serves it are different processes, and any
// replica must answer for any run (AGENTS.md §7).
type LogStore interface {
	// PutLogChunk writes one chunk. Chunks are immutable and written once;
	// rewriting the same coordinates is allowed but pointless.
	PutLogChunk(ctx context.Context, chunk LogChunk) error
	// ReadLogChunks returns the attempt's chunks with Seq strictly greater
	// than afterSeq, in sequence order, at most limit of them (0 = no cap)
	// and at most maxBytes of chunk data (0 = no cap).
	//
	// BOTH BOUNDS ARE NEEDED, because a count is not a size. One chunk is
	// whatever the Orker flushed — up to ORK_ORKER_LOG_FLUSH_BYTES plus one
	// over-long line (internal/orker/logsink.go) — so a page of 32 chunks is
	// a page of anywhere between a few hundred bytes and tens of megabytes,
	// and a reader that bounded only the count would size its memory on a
	// number the writer's configuration decides. maxBytes is the bound that
	// holds whatever those flush bounds are set to.
	//
	// The FIRST chunk is always returned whole, even when it alone passes
	// maxBytes: a page that could come back empty would stall a reader on a
	// chunk it can never receive. A short page therefore means "no more at
	// this instant, OR the budget is spent", and a caller that must drain
	// everything reads again from the last sequence it received.
	//
	// An attempt that wrote nothing reads empty, never an error.
	ReadLogChunks(
		ctx context.Context, ref LogRef, afterSeq int64, limit int, maxBytes int64,
	) ([]LogChunk, error)
	// LatestLogAttempt returns the highest attempt of a task — or, with item
	// set, of one item of a fan-out task — that wrote any chunk. NoItem never
	// counts an item's chunks. A scope with no logs at all returns an error
	// matching ErrNotFound.
	LatestLogAttempt(ctx context.Context, runID, taskID string, item Item) (int32, error)
}

// LogScopePrefix renders the prefix holding every attempt of one scope: the
// task prefix for NoItem, and metadata/logs/{run-id}/{task-id}/{item:05}/ for
// an item.
func LogScopePrefix(runID, taskID string, item Item) (string, error) {
	prefix, err := LogTaskPrefix(runID, taskID)
	if err != nil {
		return "", err
	}
	if !item.Set {
		return prefix, nil
	}
	seg, err := ItemSegment(item.Index)
	if err != nil {
		return "", fmt.Errorf("artifacts: task log key: item: %w", errors.Join(ErrInvalidLogKey, err))
	}
	return prefix + seg + "/", nil
}

// LogTaskPrefix renders the prefix holding every attempt of one task:
// metadata/logs/{run-id}/{task-id}/.
func LogTaskPrefix(runID, taskID string) (string, error) {
	run, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: task log key: run id: %w", errors.Join(ErrInvalidLogKey, err))
	}
	taskSegment, err := encodeArchiveSegment(taskID)
	if err != nil {
		return "", fmt.Errorf("artifacts: task log key: task id: %w", errors.Join(ErrInvalidLogKey, err))
	}
	return path.Join(LogPrefix, run, taskSegment) + "/", nil
}

// LogAttemptPrefix renders the prefix holding one attempt's chunks:
// metadata/logs/{run-id}/{task-id}/{attempt}/, or
// metadata/logs/{run-id}/{task-id}/{item:05}/{attempt}/ for an item.
func LogAttemptPrefix(ref LogRef) (string, error) {
	taskPrefix, err := LogScopePrefix(ref.RunID, ref.TaskID, ref.Item)
	if err != nil {
		return "", err
	}
	if ref.Attempt < 1 {
		// 0 is the request-level spelling of "the latest attempt", resolved
		// by LatestLogAttempt before anything is addressed. It is never a
		// stored coordinate, so building a key from it is a bug, not a read
		// of attempt zero.
		return "", fmt.Errorf("artifacts: task log key: attempt %d is not a stored attempt: %w",
			ref.Attempt, ErrInvalidLogKey)
	}
	return taskPrefix + fmt.Sprintf("%0*d", logAttemptWidth, ref.Attempt) + "/", nil
}

// LogChunkKey renders the object key for one chunk.
func LogChunkKey(ref LogRef, seq int64, truncated bool) (string, error) {
	prefix, err := LogAttemptPrefix(ref)
	if err != nil {
		return "", err
	}
	if seq < 1 {
		return "", fmt.Errorf("artifacts: task log key: sequence %d is not 1-based: %w",
			seq, ErrInvalidLogKey)
	}
	key := prefix + fmt.Sprintf("%0*d", logSeqWidth, seq)
	if truncated {
		key += logTruncSuffix
	}
	return key, nil
}

// logChunkCeilingKey is the highest key any chunk of seq can occupy. A listing
// that starts after it starts at the next sequence, whichever spelling seq
// used — which is what keeps a resume from replaying a truncation notice.
func logChunkCeilingKey(ref LogRef, seq int64) (string, error) {
	return LogChunkKey(ref, seq, true)
}

// ParseLogChunkKey reverses LogChunkKey. A key this package did not generate
// reports ok=false rather than an error, so a scan skips foreign objects
// instead of failing on them.
func ParseLogChunkKey(key string) (ref LogRef, seq int64, truncated bool, ok bool) {
	rest, found := strings.CutPrefix(key, LogPrefix+"/")
	if !found {
		return LogRef{}, 0, false, false
	}
	parts := strings.Split(rest, "/")
	var item Item
	switch len(parts) {
	case 4:
	case 5:
		index, isItem := parseItemSegment(parts[2])
		if !isItem {
			return LogRef{}, 0, false, false
		}
		item = ItemAt(index)
		parts = []string{parts[0], parts[1], parts[3], parts[4]}
	default:
		return LogRef{}, 0, false, false
	}
	runID, ok := decodeArchiveSegment(parts[0])
	if !ok {
		return LogRef{}, 0, false, false
	}
	taskID, ok := decodeArchiveSegment(parts[1])
	if !ok {
		return LogRef{}, 0, false, false
	}
	attempt, ok := parseFixedWidth(parts[2], logAttemptWidth)
	if !ok || attempt < 1 || attempt > (1<<31-1) {
		return LogRef{}, 0, false, false
	}
	seqSegment := parts[3]
	if trimmed, cut := strings.CutSuffix(seqSegment, logTruncSuffix); cut {
		seqSegment, truncated = trimmed, true
	}
	seq, ok = parseFixedWidth(seqSegment, logSeqWidth)
	if !ok || seq < 1 {
		return LogRef{}, 0, false, false
	}
	return LogRef{RunID: runID, TaskID: taskID, Item: item, Attempt: int32(attempt)}, seq, truncated, true
}

// parseFixedWidth decodes one zero-padded counter segment. The width is
// checked, not just the digits: a segment of another width was written by
// something else and its ordering guarantee is not this one's.
func parseFixedWidth(segment string, width int) (int64, bool) {
	if len(segment) != width {
		return 0, false
	}
	value, err := strconv.ParseInt(segment, 10, 64)
	if err != nil || value < 0 {
		return 0, false
	}
	return value, true
}

// LogStoreFactory builds a LogStore from a Config. A backend that can hold
// task logs registers one of these, mirroring Factory and ArchiveFactory.
type LogStoreFactory func(cfg Config) (LogStore, error)

var logStoreRegistry = map[string]LogStoreFactory{}

// RegisterLogStore adds a log-store factory under name.
func RegisterLogStore(name string, f LogStoreFactory) error {
	if name == "" {
		return fmt.Errorf("artifacts: register log store: empty name")
	}
	if f == nil {
		return fmt.Errorf("artifacts: register log store %q: nil factory", name)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := logStoreRegistry[name]; exists {
		return fmt.Errorf("artifacts: register log store %q: already registered", name)
	}
	logStoreRegistry[name] = f
	return nil
}

// LogStoreBackends lists the backends that can hold task logs, sorted.
func LogStoreBackends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(logStoreRegistry))
	for name := range logStoreRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// NewLogStore builds the LogStore named by cfg.Backend, so one
// ORK_ARTIFACT_BACKEND selects where artifacts, run history and task logs all
// live.
func NewLogStore(cfg Config) (LogStore, error) {
	name := cfg.Backend
	if name == "" {
		name = DefaultBackend
		cfg.Backend = name
	}
	registryMu.RLock()
	f, ok := logStoreRegistry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: log store backend %q: %w (registered: %v)",
			name, ErrUnknownBackend, LogStoreBackends())
	}
	store, err := f(cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: log store backend %q: %w", name, err)
	}
	return store, nil
}

// NewLogStoreFromEnv builds the LogStore described by the ORK_* environment.
func NewLogStoreFromEnv() (LogStore, error) { return NewLogStore(ConfigFromEnv()) }

func init() {
	mustRegisterLogStore(BackendLocal, func(cfg Config) (LogStore, error) {
		return NewLocalLogStore(localDir(cfg))
	})
	mustRegisterLogStore(BackendS3, func(cfg Config) (LogStore, error) {
		return NewS3LogStore(context.Background(), cfg)
	})
}

func mustRegisterLogStore(name string, f LogStoreFactory) {
	if err := RegisterLogStore(name, f); err != nil {
		panic(err)
	}
}
