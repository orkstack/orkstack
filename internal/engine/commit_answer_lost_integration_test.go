//go:build integration

package engine_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// commitAnswerProxy stands between one Orker and the object store and loses
// the answer of the first write of a task's commit.json: it forwards that PUT,
// so the commit is stored, and answers it 500; then it answers the S3
// client's resends of it 500 without forwarding them, so the client gives up
// with the commit in the store. Every other request passes through, its Host
// header (which the request is signed over) unchanged.
type commitAnswerProxy struct {
	t      *testing.T
	server *httptest.Server
	proxy  *httputil.ReverseProxy

	mu sync.Mutex
	// lost is the path of the commit whose answer was lost, failing the
	// resends of it still to fail, and puts every PUT of a commit path.
	lost    string
	failing int
	puts    map[string]int
}

func newCommitAnswerProxy(t *testing.T, endpoint string, failingResends int) *commitAnswerProxy {
	t.Helper()
	target, err := url.Parse(endpoint)
	if err != nil || target.Host == "" {
		t.Fatalf("the store endpoint %q cannot be proxied: %v", endpoint, err)
	}
	p := &commitAnswerProxy{t: t, proxy: httputil.NewSingleHostReverseProxy(target),
		failing: failingResends, puts: map[string]int{}}
	p.server = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.server.Close)
	return p
}

func (p *commitAnswerProxy) serve(w http.ResponseWriter, r *http.Request) {
	commit := r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/"+artifacts.CommitPrefix+"/") &&
		strings.HasSuffix(r.URL.Path, "/"+artifacts.CommitObjectName)
	if !commit {
		p.proxy.ServeHTTP(w, r)
		return
	}
	p.mu.Lock()
	p.puts[r.URL.Path]++
	switch {
	case p.lost == "":
		p.lost = r.URL.Path
		p.mu.Unlock()
		stored := httptest.NewRecorder()
		p.proxy.ServeHTTP(stored, r)
		if stored.Code != http.StatusOK {
			p.t.Errorf("the commit write the proxy forwarded was answered %d: %s", stored.Code, stored.Body)
		}
		lostAnswer(w)
		return
	case r.URL.Path == p.lost && p.failing > 0:
		p.failing--
		p.mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		lostAnswer(w)
		return
	}
	p.mu.Unlock()
	p.proxy.ServeHTTP(w, r)
}

// lostAnswer is the 500 an S3 server answers when it fails a request.
func lostAnswer(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error><Code>InternalError</Code><Message>answer lost</Message></Error>`)
}

func (p *commitAnswerProxy) commitPuts() (string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lost, p.puts[p.lost]
}

// A task's commit that landed but whose answer was lost, and every resend of
// which failed until the S3 client gave up, is still the task's result: the
// run succeeds on the task's one attempt (the Orkflow declares no retry:) with
// that commit, never COMMIT_WRITE_FAILED with commit.json in the store — a
// durably committed result is never reported as a failure (AGENTS.md §4.1,
// §15). Real Temporal, a real Orker running a real container, and the real
// object store, reached through a proxy that loses the answer.
func TestIntegration_ACommitWhoseAnswerWasLostIsTheTasksResult(t *testing.T) {
	ctx := cordonContext(t, 6*time.Minute)
	pool := fmt.Sprintf("lost-answer-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "lostanswer", pool)

	proxy := newCommitAnswerProxy(t, d.cfg.S3Endpoint, 2) // the S3 client makes three attempts
	cfg := d.cfg
	cfg.S3Endpoint = proxy.server.URL
	store, err := artifacts.New(cfg)
	must.OKf(t, err, "open the Orker's artifact store")
	commits, err := artifacts.NewCommitStore(ctx, cfg)
	must.OKf(t, err, "open the Orker's commit store")
	logs, err := artifacts.NewLogStore(cfg)
	must.OKf(t, err, "open the Orker's log store")
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict content-addressed references", store)
	}
	must.OKf(t, artifacts.ProbeCommitStore(ctx, commits), "the boot probe through the proxy")
	o := newPinOrker(t, ctx, pinOrkerConfig{
		id: "orker-lost-" + d.token, pool: pool, queue: mustQueueForPool(t, pool),
		store: store, commits: commits, logs: logs, refs: refs,
		temporalNamespace: d.namespace, hostsNoPins: true,
	})
	awaitPoolServedBy(t, ctx, engine.NewTemporalPoolProbe(d.temporal), pool, o.queue, 1)

	d.storeOrkflow("echo-"+d.token, pool, cordonEchoYAML, 0)
	runID := d.submit("echo-" + d.token)
	run := d.await(runID)

	path, puts := proxy.commitPuts()
	if path == "" {
		t.Fatal("no commit was written through the proxy, so nothing here was tested")
	}
	if puts != 3 {
		t.Fatalf("the commit at %s was sent %d times, want 3: the first stored, two resends failed, then the client gave up", path, puts)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the run ended %v: %v; its commit is in the store", run.GetStatus(), run.GetError())
	}
	echo := pinTaskRun(t, run, "echo")
	if echo.GetAttempt() != 1 || len(o.ranFor(runID)) != 1 {
		t.Fatalf("attempt=%d handed=%v; the task should have finished on its one attempt", echo.GetAttempt(), o.ranFor(runID))
	}
	record, err := d.commits.Get(ctx, echo.GetCommit().GetUri())
	must.OKf(t, err, "read the task's commit %s", echo.GetCommit().GetUri())
	if record.Commit.GetAttempt() != 1 || record.Commit.GetOrkerId() != o.id || record.ETag != echo.GetCommit().GetEtag() {
		t.Fatalf("the stored commit (attempt %d by %q, etag %s) is not the one the run recorded (etag %s)",
			record.Commit.GetAttempt(), record.Commit.GetOrkerId(), record.ETag, echo.GetCommit().GetEtag())
	}
	t.Logf("EVIDENCE lost commit answer: %s sent %d times (stored, then two resends failed and the client gave up); "+
		"the run succeeded on attempt 1 with that commit", path, puts)
}
