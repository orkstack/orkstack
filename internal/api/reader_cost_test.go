package api_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func (f *fakeReader) costs() (runStarts, runReads, streams int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runStartCount, f.runReadCount, f.streamCount
}

// Opening a run socket reads the run's start event once — to answer a 404
// before the upgrade — and its history once, as the stream. It never replays
// the history to learn the run's namespace.
func TestWebSocketBridge_ASocketOpenReadsTheStartEventOnceAndTheHistoryOnce(t *testing.T) {
	f := newForeignFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, strings.Replace(f.baseURL, "http://", "ws://", 1)+"/ws/runs/ork-foreign-1", nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.CloseNow()
	frames := 0
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			break
		}
		frames++
	}
	if frames != 4 {
		t.Fatalf("the socket delivered %d events, want 4", frames)
	}
	starts, reads, streams := f.reader.costs()
	if starts != 1 || reads != 0 || streams != 1 {
		t.Errorf("a socket open cost %d start reads, %d whole-run reads and %d streams; want 1, 0 and 1", starts, reads, streams)
	}
}

// The cheap check is still a namespace check: a run of another namespace is
// a 404 on the socket, and NotFound on the Connect stream, before anything is
// sent.
func TestRunNamespaceCheck_ARunOfAnotherNamespaceIsStillNotFound(t *testing.T) {
	f := newForeignFixture(t)
	f.reader.mu.Lock()
	f.reader.runs["ork-foreign-1"].Namespace = "beta"
	f.reader.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, resp, err := websocket.Dial(ctx, strings.Replace(f.baseURL, "http://", "ws://", 1)+"/ws/runs/ork-foreign-1", nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("a run of namespace beta, asked for in default: dial err %v, response %v; want 404", err, resp)
	}
	err = f.svc.StreamRunEvents(ctx, "ork-foreign-1", "", 0, func(*orkstackv1.Event) error {
		t.Fatal("an event of another namespace's run was sent")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), `run "ork-foreign-1" not found in namespace "default"`) {
		t.Fatalf("StreamRunEvents answered %v, want the NotFound LookupRunIn words", err)
	}
	if _, _, streams := f.reader.costs(); streams != 0 {
		t.Errorf("the history was streamed %d times for a refused run", streams)
	}
}

// Ten quiet log followers of one run asking at once share one run read.
func TestTaskIsTerminal_ConcurrentQuietFollowersShareOneRunRead(t *testing.T) {
	f := newForeignFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.svc.TaskIsTerminalForTest(ctx, "ork-foreign-1", "", "hello")
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("taskIsTerminal: %v", err)
		}
	}
	if _, reads, _ := f.reader.costs(); reads != 1 {
		t.Fatalf("ten concurrent followers made %d whole-run reads, want 1", reads)
	}

	// A failed read is not remembered: the next asker reads again.
	f.reader.mu.Lock()
	f.reader.getErr = context.DeadlineExceeded
	f.reader.mu.Unlock()
	if _, err := f.svc.TaskIsTerminalForTest(ctx, "ork-missing-1", "", "hello"); err == nil {
		t.Fatal("a failing read answered")
	}
	f.reader.mu.Lock()
	f.reader.getErr = nil
	f.reader.mu.Unlock()
	if _, err := f.svc.TaskIsTerminalForTest(ctx, "ork-foreign-1", "beta", "hello"); err == nil {
		t.Fatal("a run of default answered for namespace beta through the memo")
	}
}
