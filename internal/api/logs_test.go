package api_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// memoryLogStore is the shared bucket, as far as these tests are concerned:
// the Orker writes into it and the engine reads out of it, and neither knows
// anything about the other. Every test below runs against a Service that
// started nothing and executed nothing.
type memoryLogStore struct {
	mu     sync.Mutex
	chunks []artifacts.LogChunk
	// grow, when set, appends one more chunk to that attempt after every read
	// this store serves: the task never goes quiet. It is what tells a
	// BOUNDED replay from a follow — against a store that is never silent, a
	// stream that ends only on a sweep finding nothing never ends at all.
	grow *artifacts.LogRef
}

// quietGap is a silence long enough to outlast many poll cycles of the
// fixture's log poller (5–20 ms below). Every "it is still following" claim in
// this file is measured across one, because without it the test races the
// server and a stream that quietly ended passes whenever the next write lands
// inside a single poll interval.
const quietGap = 300 * time.Millisecond

func (s *memoryLogStore) PutLogChunk(_ context.Context, chunk artifacts.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := chunk
	stored.Data = append([]byte(nil), chunk.Data...)
	s.chunks = append(s.chunks, stored)
	return nil
}

func (s *memoryLogStore) ReadLogChunks(
	_ context.Context, ref artifacts.LogRef, afterSeq int64, limit int,
) ([]artifacts.LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []artifacts.LogChunk
	for _, chunk := range s.chunks {
		if chunk.LogRef != ref || chunk.Seq <= afterSeq {
			continue
		}
		out = append(out, chunk)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	if s.grow != nil {
		s.appendLocked(*s.grow)
	}
	return out, nil
}

func (s *memoryLogStore) LatestLogAttempt(_ context.Context, runID, taskID string) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	latest := int32(0)
	for _, chunk := range s.chunks {
		if chunk.RunID == runID && chunk.TaskID == taskID && chunk.Attempt > latest {
			latest = chunk.Attempt
		}
	}
	if latest == 0 {
		return 0, artifacts.ErrNotFound
	}
	return latest, nil
}

func (s *memoryLogStore) write(ref artifacts.LogRef, seq int64, body string) {
	if err := s.PutLogChunk(context.Background(), artifacts.LogChunk{
		LogRef: ref, Seq: seq, Data: []byte(body),
	}); err != nil {
		panic(err)
	}
}

// keepWriting makes the task chatty for as long as anyone reads it.
func (s *memoryLogStore) keepWriting(ref artifacts.LogRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := ref
	s.grow = &copied
}

func (s *memoryLogStore) appendLocked(ref artifacts.LogRef) {
	next := int64(0)
	for _, chunk := range s.chunks {
		if chunk.LogRef == ref && chunk.Seq > next {
			next = chunk.Seq
		}
	}
	next++
	s.chunks = append(s.chunks, artifacts.LogChunk{
		LogRef: ref, Seq: next, Data: []byte(fmt.Sprintf("still writing %d\n", next)),
	})
}

// logFixture is a Service reading a run out of the record and its logs out of
// the store. Nothing is in process memory, which is the deployment this
// milestone has to work in.
type logFixture struct {
	*readerFixture
	logs   *memoryLogStore
	reader *fakeReader
}

func newLogFixture(t *testing.T, run *orkstackv1.Run) *logFixture {
	t.Helper()
	logs := &memoryLogStore{}
	reader := &fakeReader{
		runs:   map[string]*orkstackv1.Run{run.GetId(): run},
		events: map[string][]*orkstackv1.Event{run.GetId(): {}},
	}
	fixture := newLogReaderFixture(t, reader, logs)
	return &logFixture{readerFixture: fixture, logs: logs, reader: reader}
}

// runningRun is a run whose one task is still executing.
func runningRun(runID, taskID string) *orkstackv1.Run {
	at := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	return &orkstackv1.Run{
		Id:             runID,
		OrkflowName:    "chatty",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_RUNNING,
		CreatedAt:      timestamppb.New(at),
		StartedAt:      timestamppb.New(at),
		TaskRuns: []*orkstackv1.TaskRun{{
			TaskId:  taskID,
			Plugin:  "shell",
			Status:  orkstackv1.Status_STATUS_RUNNING,
			Attempt: 1,
		}},
	}
}

func finish(run *orkstackv1.Run, status orkstackv1.Status) *orkstackv1.Run {
	run.Status = status
	for _, taskRun := range run.GetTaskRuns() {
		taskRun.Status = status
	}
	return run
}

// The post-mortem read: everything the task wrote, in order, once.
func TestStreamTaskLogs_ReplaysAFinishedTasksWholeLogInOrder(t *testing.T) {
	run := finish(runningRun("ork-logs-1", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}
	for seq := int64(1); seq <= 5; seq++ {
		fixture.logs.write(ref, seq, fmt.Sprintf("line %d\n", seq))
	}

	chunks := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "build",
	})
	if body := chunkBody(chunks); body != "line 1\nline 2\nline 3\nline 4\nline 5\n" {
		t.Fatalf("the replayed log reads %q", body)
	}
	for i, chunk := range chunks {
		if i < len(chunks)-1 && chunk.GetSeq() != int64(i+1) {
			t.Fatalf("chunk %d carries seq %d; the stream is not in order", i, chunk.GetSeq())
		}
	}
	last := chunks[len(chunks)-1]
	if !last.GetLast() {
		t.Error("the final chunk of a finished task is not marked last")
	}
	if len(last.GetData()) != 0 {
		t.Errorf("the terminator carries %q; it must carry no data", last.GetData())
	}
}

// The milestone's claim, at the API: chunks written while the reader is
// attached arrive while the reader is attached. The task is left RUNNING until
// the test has seen them, so an implementation that only replayed what existed
// at connect time cannot pass.
func TestStreamTaskLogs_FollowsChunksWrittenAfterTheReaderConnected(t *testing.T) {
	run := runningRun("ork-logs-live", "build")
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}
	fixture.logs.write(ref, 1, "before\n")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "build", Follow: true}))
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	defer stream.Close()

	if !stream.Receive() {
		t.Fatalf("no first chunk: %v", stream.Err())
	}
	if got := string(stream.Msg().GetData()); got != "before\n" {
		t.Fatalf("first chunk = %q, want %q", got, "before\n")
	}

	// A QUIET GAP, longer than many poll cycles. It is the load-bearing part
	// of this test: a follower that ends at the first sweep that finds nothing
	// is dead by now, so the chunk written after it could never arrive. Without
	// the gap the two race, and a non-following implementation passes whenever
	// the write happens to land inside one poll interval.
	time.Sleep(quietGap)
	fixture.logs.write(ref, 2, "after\n")
	if !stream.Receive() {
		t.Fatalf("the chunk written after a %s silence never arrived: %v", quietGap, stream.Err())
	}
	if got := string(stream.Msg().GetData()); got != "after\n" {
		t.Fatalf("second chunk = %q, want %q", got, "after\n")
	}
	if stream.Msg().GetLast() {
		t.Fatal("the stream ended while the task was still RUNNING")
	}

	// Now the task finishes: the stream must end, and it must end after one
	// more sweep that picks up whatever landed with the terminal state. The
	// same quiet gap first, for the same reason.
	time.Sleep(quietGap)
	fixture.logs.write(ref, 3, "final\n")
	fixture.reader.setRun(finish(runningRun(run.GetId(), "build"), orkstackv1.Status_STATUS_SUCCEEDED))

	var tail []string
	for stream.Receive() {
		msg := stream.Msg()
		if len(msg.GetData()) > 0 {
			tail = append(tail, string(msg.GetData()))
		}
		if msg.GetLast() {
			break
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream ended with %v", err)
	}
	if strings.Join(tail, "") != "final\n" {
		t.Fatalf("the chunks after the terminal state were %q, want the final line", tail)
	}
}

// The other half of the same contract: WITHOUT follow the stream is a replay.
// It delivers what the store holds and ends there, whether or not the task is
// still writing — `orkctl run logs` with no -f must return a transcript, not
// become a follower nobody asked for.
//
// The store below never goes quiet: it appends another chunk after every read
// it serves, which is exactly what a chatty running task does. So a replay
// that ended on the first sweep finding nothing would not end at all here,
// and the deadline names that failure rather than reporting a bare timeout.
func TestStreamTaskLogs_WithoutFollowReplaysWhatIsStoredWhileTheTaskKeepsWriting(t *testing.T) {
	run := runningRun("ork-logs-replay", "build") // still RUNNING throughout
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}
	for seq := int64(1); seq <= 3; seq++ {
		fixture.logs.write(ref, seq, fmt.Sprintf("line %d\n", seq))
	}
	fixture.logs.keepWriting(ref)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "build"}))
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	defer stream.Close()

	var chunks []*orkstackv1.TaskLogChunk
	for stream.Receive() {
		chunks = append(chunks, stream.Msg())
	}
	if err := stream.Err(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("the replay never ended (%v); without follow it is still tailing a task that keeps writing", err)
	}
	if body := chunkBody(chunks); body != "line 1\nline 2\nline 3\n" {
		t.Fatalf("the replay read %q, want the three chunks stored when it opened", body)
	}
	last := chunks[len(chunks)-1]
	if !last.GetLast() {
		t.Fatal("the replay's final chunk is not marked last")
	}
	if last.GetSeq() != 3 {
		t.Errorf("the terminator carries seq %d, want 3 — a client resumes the follow from there",
			last.GetSeq())
	}

	// And follow=true over the same store does NOT end: the difference is the
	// flag, not the store.
	followCtx, cancelFollow := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFollow()
	following, err := fixture.runClient().StreamTaskLogs(followCtx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "build", Follow: true}))
	if err != nil {
		t.Fatalf("StreamTaskLogs(follow): %v", err)
	}
	defer following.Close()
	for following.Receive() {
		if following.Msg().GetLast() {
			t.Fatal("the follower ended while the task was still RUNNING and still writing")
		}
	}
}

// Resume is the same contract StreamEvents has: hand back the last sequence
// seen and the stream continues from the next one, replaying nothing.
func TestStreamTaskLogs_ResumesFromSeqWithoutRepeatingOrSkipping(t *testing.T) {
	run := finish(runningRun("ork-logs-resume", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}
	for seq := int64(1); seq <= 6; seq++ {
		fixture.logs.write(ref, seq, fmt.Sprintf("line %d\n", seq))
	}

	chunks := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "build", FromSeq: 4,
	})
	if body := chunkBody(chunks); body != "line 5\nline 6\n" {
		t.Fatalf("resume from_seq=4 read %q, want lines 5 and 6", body)
	}

	// A resume past the end returns only the terminator, never an error: the
	// caller has everything.
	tail := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "build", FromSeq: 6,
	})
	if body := chunkBody(tail); body != "" {
		t.Fatalf("resume from the end replayed %q", body)
	}
	if len(tail) != 1 || !tail[0].GetLast() {
		t.Fatalf("resume from the end returned %d chunks, want just the terminator", len(tail))
	}
}

// Attempt 0 means "the latest attempt that wrote anything", so a caller who
// never saw the retry reads the try that matters.
func TestStreamTaskLogs_AttemptZeroSelectsTheLatestAttempt(t *testing.T) {
	run := finish(runningRun("ork-logs-retry", "flaky"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	fixture.logs.write(artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 1}, 1, "first try\n")
	fixture.logs.write(artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 2}, 1, "second try\n")

	latest := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "flaky",
	})
	if body := chunkBody(latest); body != "second try\n" {
		t.Fatalf("attempt 0 read %q, want the latest attempt", body)
	}
	for _, chunk := range latest {
		if chunk.GetAttempt() != 2 {
			t.Errorf("a chunk reports attempt %d, want 2 — the resolved attempt must be on the wire",
				chunk.GetAttempt())
		}
	}

	// And an explicit attempt reads exactly that one.
	first := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "flaky", Attempt: 1,
	})
	if body := chunkBody(first); body != "first try\n" {
		t.Fatalf("attempt 1 read %q, want the first try", body)
	}
}

// A RETRY is the case a follower is most needed for and the one that used to
// break it. The Orker keys chunks by the Temporal activity attempt
// (internal/orker/activity.go), and a retry changes nothing a task's STATUS
// shows: it stays RUNNING while try 2 starts under a new prefix. A stream that
// resolved "the latest attempt" once therefore followed a dead attempt —
// delivering nothing, and reporting itself live — for the rest of the task's
// life.
func TestStreamTaskLogs_FollowsTheTaskOntoItsNextAttempt(t *testing.T) {
	run := runningRun("ork-logs-retry-live", "flaky") // RUNNING throughout
	fixture := newLogFixture(t, run)
	first := artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 1}
	second := artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 2}
	fixture.logs.write(first, 1, "try one\n")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "flaky", Follow: true}))
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	defer stream.Close()

	if !stream.Receive() {
		t.Fatalf("no first chunk: %v", stream.Err())
	}
	if got := string(stream.Msg().GetData()); got != "try one\n" {
		t.Fatalf("first chunk = %q, want the first attempt's line", got)
	}

	// Attempt 1 dies. The task does not: Temporal retries the activity, and
	// the run's state does not move at all. The quiet gap is what makes this
	// the real shape — the follower has settled onto attempt 1 and backed off
	// before attempt 2 exists.
	time.Sleep(quietGap)
	fixture.logs.write(second, 1, "try two\n")

	var got []*orkstackv1.TaskLogChunk
	for stream.Receive() {
		msg := stream.Msg()
		if len(msg.GetData()) == 0 {
			continue
		}
		got = append(got, msg)
		break
	}
	if len(got) == 0 {
		t.Fatalf("the retry's output never arrived (%v): the stream is still following "+
			"an attempt that is over, and telling the reader it is live", stream.Err())
	}
	if body := string(got[0].GetData()); body != "try two\n" {
		t.Fatalf("after the retry the stream delivered %q, want the second attempt's line", body)
	}
	if got[0].GetAttempt() != 2 {
		t.Errorf("the retry's chunk reports attempt %d, want 2 — a client cannot tell a "+
			"new attempt from more of the old one without it", got[0].GetAttempt())
	}
	if got[0].GetSeq() != 1 {
		t.Errorf("the retry's first chunk carries seq %d, want 1: seq is dense per "+
			"attempt and the new attempt is delivered from its start", got[0].GetSeq())
	}

	// And the stream still ends where it always did: on the task's terminal
	// state, having swept the attempt it is now on.
	fixture.logs.write(second, 2, "try two done\n")
	fixture.reader.setRun(finish(runningRun(run.GetId(), "flaky"),
		orkstackv1.Status_STATUS_SUCCEEDED))
	var tail []string
	for stream.Receive() {
		msg := stream.Msg()
		if len(msg.GetData()) > 0 {
			tail = append(tail, string(msg.GetData()))
		}
		if msg.GetLast() {
			if msg.GetAttempt() != 2 {
				t.Errorf("the terminator names attempt %d, want the attempt the stream "+
					"ended on", msg.GetAttempt())
			}
			break
		}
	}
	if strings.Join(tail, "") != "try two done\n" {
		t.Fatalf("the chunks after the terminal state were %q, want the retry's last line", tail)
	}
}

// A PINNED attempt never moves. `orkctl run logs --attempt 1` is a request for
// one try's transcript, and a stream that wandered onto attempt 2 would answer
// a question nobody asked.
func TestStreamTaskLogs_APinnedAttemptDoesNotFollowARetry(t *testing.T) {
	run := runningRun("ork-logs-retry-pinned", "flaky")
	fixture := newLogFixture(t, run)
	first := artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 1}
	second := artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 2}
	fixture.logs.write(first, 1, "try one\n")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{
			RunId: run.GetId(), TaskId: "flaky", Attempt: 1, Follow: true,
		}))
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	defer stream.Close()
	if !stream.Receive() {
		t.Fatalf("no first chunk: %v", stream.Err())
	}

	time.Sleep(quietGap)
	fixture.logs.write(second, 1, "try two\n")
	time.Sleep(quietGap)
	// Attempt 1 writes one more line, and that is what a pinned stream owes.
	fixture.logs.write(first, 2, "try one again\n")
	for stream.Receive() {
		msg := stream.Msg()
		if len(msg.GetData()) == 0 {
			continue
		}
		if msg.GetAttempt() != 1 {
			t.Fatalf("a stream pinned to attempt 1 delivered attempt %d (%q)",
				msg.GetAttempt(), msg.GetData())
		}
		if got := string(msg.GetData()); got == "try one again\n" {
			return
		}
	}
	t.Fatalf("the pinned stream ended before its own attempt's next line: %v", stream.Err())
}

// A RESUME ACROSS A RETRY. seq is dense per attempt, so a cursor handed back
// from attempt 1 says nothing about attempt 2 — and a browser that reconnects
// after a wifi handover, mid-retry, hands back exactly that. from_attempt is
// what tells the engine which attempt the cursor counted.
func TestStreamTaskLogs_ResumeFromAnotherAttemptStartsTheNewOneAtItsFirstChunk(t *testing.T) {
	run := finish(runningRun("ork-logs-retry-resume", "flaky"),
		orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	first := artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 1}
	second := artifacts.LogRef{RunID: run.GetId(), TaskID: "flaky", Attempt: 2}
	// A long first try and a short retry: the shape that loses everything.
	for seq := int64(1); seq <= 20; seq++ {
		fixture.logs.write(first, seq, fmt.Sprintf("try one line %d\n", seq))
	}
	fixture.logs.write(second, 1, "try two line 1\n")
	fixture.logs.write(second, 2, "try two line 2\n")

	resumed := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "flaky", FromSeq: 20, FromAttempt: 1,
	})
	if body := chunkBody(resumed); body != "try two line 1\ntry two line 2\n" {
		t.Fatalf("resuming at seq 20 of attempt 1 read %q; the cursor of a dead attempt "+
			"was applied to the live one, which reads as a silent task", body)
	}

	// Without from_attempt the cursor is taken at face value, which is what a
	// caller who typed --from-seq means: it is the attempt this request
	// resolves to.
	same := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "flaky", FromSeq: 1, FromAttempt: 2,
	})
	if body := chunkBody(same); body != "try two line 2\n" {
		t.Fatalf("resuming at seq 1 of attempt 2 read %q, want its second line", body)
	}
}

// The COST of following, measured. Each poll of a live task lists the store;
// asking the CLUSTER as well — which means replaying the run's whole history
// (engine.TemporalRunReader.GetRun) — is what a chatty task must not pay for on
// every tick, per pane, per tab.
//
// The bound below is deliberately loose: it is the shape that is wrong, not a
// number. A per-tick read of a store that never goes quiet grows without limit
// with the length of the follow; a read taken only when a sweep finds nothing
// does not grow at all.
func TestStreamTaskLogs_AChattyTaskDoesNotReplayItsRunsHistoryPerPoll(t *testing.T) {
	run := runningRun("ork-logs-cost", "chatter")
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "chatter", Attempt: 1}
	fixture.logs.write(ref, 1, "line 1\n")
	// The task never goes quiet: the store appends another chunk after every
	// read it serves.
	fixture.logs.keepWriting(ref)

	// The fixture polls every 5ms (newLogReaderFixture), so a follow held for
	// this long is ~60 ticks. Reading the run on each of them is the defect.
	const follow = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), follow)
	defer cancel()
	stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "chatter", Follow: true}))
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	defer stream.Close()

	chunks := 0
	for stream.Receive() {
		chunks++
	}
	if chunks < 10 {
		t.Fatalf("the follow delivered %d chunks in %s; it was not busy enough to "+
			"measure anything", chunks, follow)
	}
	// One read to resolve the run before the stream opened (LookupRunIn), one
	// for the first pass's state, and nothing per tick after that.
	if reads := fixture.reader.runReads(); reads > 4 {
		t.Errorf("a %s follow of a task that never stopped writing replayed the run's "+
			"history %d times (%d chunks delivered); a talking task is a task that is "+
			"neither finished nor retried, so no tick of it needs the cluster",
			follow, reads, chunks)
	}
}

// A task that wrote nothing is normal — the pane is opened unconditionally —
// so it reads empty and ends, not an error.
func TestStreamTaskLogs_ATaskThatWroteNothingEndsCleanly(t *testing.T) {
	run := finish(runningRun("ork-logs-silent", "quiet"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)

	chunks := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "quiet",
	})
	if len(chunks) != 1 || !chunks[0].GetLast() {
		t.Fatalf("a silent task streamed %d chunks, want just the terminator", len(chunks))
	}
}

// Truncation must survive the wire: the flag the sink set is the flag the
// client sees, or a cut log looks complete.
func TestStreamTaskLogs_CarriesTheTruncationFlag(t *testing.T) {
	run := finish(runningRun("ork-logs-trunc", "chatty"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "chatty", Attempt: 1}
	fixture.logs.write(ref, 1, "kept\n")
	if err := fixture.logs.PutLogChunk(context.Background(), artifacts.LogChunk{
		LogRef: ref, Seq: 2, Data: []byte("[ork] log truncated\n"), Truncated: true,
	}); err != nil {
		t.Fatalf("PutLogChunk: %v", err)
	}

	chunks := collectLogChunks(t, fixture, &orkstackv1.StreamTaskLogsRequest{
		RunId: run.GetId(), TaskId: "chatty",
	})
	var sawNotice bool
	for _, chunk := range chunks {
		if chunk.GetTruncated() {
			sawNotice = true
			if !strings.Contains(string(chunk.GetData()), "truncated") {
				t.Errorf("the notice reads %q", chunk.GetData())
			}
		}
	}
	if !sawNotice {
		t.Fatal("the truncation notice reached the client unmarked; a cut log would look complete")
	}
}

// A run of another namespace is NotFound, on exactly the rule GetRun and the
// event stream follow: a run id is an address inside a namespace, not a
// capability over the deployment.
func TestStreamTaskLogs_RefusesARunOfAnotherNamespace(t *testing.T) {
	run := finish(runningRun("ork-logs-ns", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	run.Namespace = "payments"
	fixture := newLogFixture(t, run)
	fixture.logs.write(artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}, 1, "secret\n")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "build"}))
	if err == nil {
		defer stream.Close()
		if stream.Receive() {
			t.Fatalf("the default namespace read %q out of a payments run",
				stream.Msg().GetData())
		}
		err = stream.Err()
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("StreamTaskLogs across namespaces = %v (code %v), want NotFound",
			err, connect.CodeOf(err))
	}
}

func TestStreamTaskLogs_RequiresARunAndATask(t *testing.T) {
	run := finish(runningRun("ork-logs-args", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		req  *orkstackv1.StreamTaskLogsRequest
	}{
		{"no run", &orkstackv1.StreamTaskLogsRequest{TaskId: "build"}},
		{"no task", &orkstackv1.StreamTaskLogsRequest{RunId: run.GetId()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(tc.req))
			if err == nil {
				defer stream.Close()
				stream.Receive()
				err = stream.Err()
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("err = %v (code %v), want InvalidArgument", err, connect.CodeOf(err))
			}
		})
	}
}

// A task the run does not have is NotFound rather than an empty stream: a
// typo must not look like a quiet task.
func TestStreamTaskLogs_UnknownTaskIsNotFound(t *testing.T) {
	run := finish(runningRun("ork-logs-unknown", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)

	stream, err := fixture.runClient().StreamTaskLogs(context.Background(), connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "nope"}))
	if err == nil {
		defer stream.Close()
		stream.Receive()
		err = stream.Err()
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("err = %v (code %v), want NotFound", err, connect.CodeOf(err))
	}
}

// With no log store wired, the RPC says so instead of pretending the task was
// quiet. An operator who forgot the configuration must be told.
func TestStreamTaskLogs_WithoutALogStoreSaysSo(t *testing.T) {
	run := finish(runningRun("ork-logs-none", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	reader := &fakeReader{
		runs:   map[string]*orkstackv1.Run{run.GetId(): run},
		events: map[string][]*orkstackv1.Event{run.GetId(): {}},
	}
	fixture := newLogReaderFixture(t, reader, nil)

	stream, err := fixture.runClient().StreamTaskLogs(context.Background(), connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "build"}))
	if err == nil {
		defer stream.Close()
		stream.Receive()
		err = stream.Err()
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("err = %v (code %v), want FailedPrecondition", err, connect.CodeOf(err))
	}
}

// The WebSocket route is the browser's spelling of the same stream. It goes
// through the same service method, so the two cannot drift.
func TestWebSocketTaskLogs_ReplaysAndFollowsTheSameStream(t *testing.T) {
	run := runningRun("ork-logs-ws", "build")
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}
	fixture.logs.write(ref, 1, "socket line 1\n")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := fmt.Sprintf("%s/ws/runs/%s/tasks/%s/logs?follow=true",
		strings.Replace(fixture.baseURL, "http://", "ws://", 1), run.GetId(), "build")
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	defer conn.CloseNow()

	first := readLogFrame(t, ctx, conn)
	if got := string(first.GetData()); got != "socket line 1\n" {
		t.Fatalf("first frame = %q", got)
	}

	// The same quiet gap the RPC test uses, for the same reason: a socket that
	// ended at the first empty sweep is gone by now, and the frame below could
	// not arrive.
	time.Sleep(quietGap)
	fixture.logs.write(ref, 2, "socket line 2\n")
	second := readLogFrame(t, ctx, conn)
	if got := string(second.GetData()); got != "socket line 2\n" {
		t.Fatalf("second frame = %q, want the line written after a %s silence", got, quietGap)
	}

	fixture.reader.setRun(finish(runningRun(run.GetId(), "build"), orkstackv1.Status_STATUS_SUCCEEDED))
	for {
		frame := readLogFrame(t, ctx, conn)
		if frame.GetLast() {
			break
		}
	}
}

// The socket resumes on the same token the RPC does, which is what a browser
// reconnecting after a dropped connection has to hand back.
func TestWebSocketTaskLogs_ResumesFromSeq(t *testing.T) {
	run := finish(runningRun("ork-logs-ws-resume", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)
	ref := artifacts.LogRef{RunID: run.GetId(), TaskID: "build", Attempt: 1}
	for seq := int64(1); seq <= 3; seq++ {
		fixture.logs.write(ref, seq, fmt.Sprintf("line %d\n", seq))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := fmt.Sprintf("%s/ws/runs/%s/tasks/%s/logs?from_seq=2",
		strings.Replace(fixture.baseURL, "http://", "ws://", 1), run.GetId(), "build")
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	frame := readLogFrame(t, ctx, conn)
	if got := string(frame.GetData()); got != "line 3\n" {
		t.Fatalf("resumed frame = %q, want line 3", got)
	}
}

func TestWebSocketTaskLogs_UnknownRunIsHTTPNotFound(t *testing.T) {
	run := finish(runningRun("ork-logs-ws-404", "build"), orkstackv1.Status_STATUS_SUCCEEDED)
	fixture := newLogFixture(t, run)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := strings.Replace(fixture.baseURL, "http://", "ws://", 1) + "/ws/runs/nope/tasks/build/logs"
	conn, resp, err := websocket.Dial(ctx, url, nil)
	if err == nil {
		conn.CloseNow()
		t.Fatal("the socket opened for a run that does not exist")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("dial failed with %v and response %v, want HTTP 404", err, resp)
	}
	_ = resp.Body.Close()
}

// ---- helpers ----------------------------------------------------------------

func collectLogChunks(
	t *testing.T,
	fixture *logFixture,
	req *orkstackv1.StreamTaskLogsRequest,
) []*orkstackv1.TaskLogChunk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := fixture.runClient().StreamTaskLogs(ctx, connect.NewRequest(req))
	if err != nil {
		t.Fatalf("StreamTaskLogs: %v", err)
	}
	defer stream.Close()
	var chunks []*orkstackv1.TaskLogChunk
	for stream.Receive() {
		chunks = append(chunks, stream.Msg())
	}
	if err := stream.Err(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("StreamTaskLogs ended with %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("the stream delivered nothing at all, not even a terminator")
	}
	return chunks
}

func chunkBody(chunks []*orkstackv1.TaskLogChunk) string {
	var b strings.Builder
	for _, chunk := range chunks {
		b.Write(chunk.GetData())
	}
	return b.String()
}

func readLogFrame(
	t *testing.T,
	ctx context.Context,
	conn *websocket.Conn,
) *orkstackv1.TaskLogChunk {
	t.Helper()
	_, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read log frame: %v", err)
	}
	chunk := &orkstackv1.TaskLogChunk{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload, chunk); err != nil {
		t.Fatalf("decode log frame %s: %v", payload, err)
	}
	return chunk
}

// newLogReaderFixture is newReaderFixture with a log store attached.
func newLogReaderFixture(t *testing.T, reader *fakeReader, logs artifacts.LogStore) *readerFixture {
	t.Helper()
	return newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Logs = logs
		cfg.LogPoll = api.LogPollOptions{Min: 5 * time.Millisecond, Max: 20 * time.Millisecond}
	})
}
