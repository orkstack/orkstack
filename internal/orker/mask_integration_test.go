//go:build integration

package orker_test

import (
	"context"
	"encoding/base64"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// chunkRecorder keeps the live log chunks an attempt writes; the Orker only
// ever writes them.
type chunkRecorder struct {
	artifacts.LogStore
	mu   sync.Mutex
	data strings.Builder
}

func (r *chunkRecorder) PutLogChunk(_ context.Context, chunk artifacts.LogChunk) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.Write(chunk.Data)
	return nil
}

func (r *chunkRecorder) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.data.String()
}

// maskRun is one attempt of the masking task and everything it let out.
type maskRun struct {
	chunks, live string
	result       *orkstackv1.ExecuteTaskResult
	err          error
	dispatcher   *orker.OCIDispatcher
	storeDir     string
}

// TestIntegration_MaskingAndTheValuesCheck: a real container prints its
// resolved secret plainly, in base64 and split across two writes, on both
// streams, then writes it to values.json. The live chunks, the in-process
// stream and the stored stdout.log and stderr.log hold `***` and never the
// value, and the attempt fails VALUES_CONTAIN_SECRET with no object holding
// the value. (control: the same task returning the secret's name succeeds.)
func TestIntegration_MaskingAndTheValuesCheck(t *testing.T) {
	requireDocker(t)
	const value = "masked-in-a-real-container-4f1d9c"
	t.Setenv("MASK_INTEGRATION_SECRET", value)
	script := func(result string) string {
		return `s=/orkstack/input/secrets/db; ` +
			`printf 'plain %s\n' "$(cat $s)"; ` +
			`printf 'b64 %s\n' "$(base64 < $s | tr -d '\n')"; ` +
			`printf 'split '; head -c 10 $s; sleep 0.2; tail -c +11 $s; printf '\n'; ` +
			`printf 'err %s\n' "$(cat $s)" >&2; ` +
			`printf 'not a secret: public-text\n'; ` +
			result
	}
	run := func(t *testing.T, taskID, result string) maskRun {
		t.Helper()
		dispatcher, _ := newDispatcher(t)
		storeDir := t.TempDir()
		store, err := artifacts.NewLocalStore(storeDir)
		must.OK(t, err)
		dispatcher.Store = store
		chunks := &chunkRecorder{}
		dispatcher.Logs = chunks
		var mu sync.Mutex
		var live strings.Builder
		dispatcher.LogSinkObserver = func(_ *orkstackv1.ExecuteTaskInput, line string) {
			mu.Lock()
			defer mu.Unlock()
			live.WriteString(line + "\n")
		}
		in := baseInput(t, taskID, "/bin/sh", "-c", script(result))
		in.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:MASK_INTEGRATION_SECRET"}}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		res, err := dispatcher.Execute(ctx, in)
		mu.Lock()
		defer mu.Unlock()
		return maskRun{chunks: chunks.text(), live: live.String(), result: res, err: err, dispatcher: dispatcher, storeDir: storeDir}
	}

	t.Run("refused", func(t *testing.T) {
		got := run(t, "mask-refused", `printf '{"token":"%s"}' "$(cat $s)" > /orkstack/output/values.json`)
		if got.err == nil || !strings.Contains(got.err.Error(), orker.ReasonValuesContainSecret) {
			t.Fatalf("err = %v; want %s", got.err, orker.ReasonValuesContainSecret)
		}
		detail := failureDetail(t, got.err)
		ctx := context.Background()
		stdout := readArtifact(t, ctx, got.dispatcher, detail.GetSystemArtifacts(), orker.StdoutLogName)
		stderr := readArtifact(t, ctx, got.dispatcher, detail.GetSystemArtifacts(), orker.StderrLogName)
		b64 := base64.StdEncoding.EncodeToString([]byte(value))
		for name, text := range map[string]string{"live chunks": got.chunks, "live stream": got.live,
			"stdout.log": stdout, "stderr.log": stderr, "failure": got.err.Error()} {
			if strings.Contains(text, value) || strings.Contains(text, b64) || strings.Contains(text, value[:10]) {
				t.Fatalf("%s holds the secret:\n%s", name, text)
			}
		}
		for _, want := range []string{"plain ***", "b64 ***", "split ***", "not a secret: public-text"} {
			if !strings.Contains(stdout, want) || !strings.Contains(got.chunks, want) || !strings.Contains(got.live, want) {
				t.Fatalf("want %q in stdout.log, the live chunks and the live stream:\n%s\n---\n%s", want, stdout, got.chunks)
			}
		}
		if !strings.Contains(stderr, "err ***") {
			t.Fatalf("stderr.log = %q; want the secret masked", stderr)
		}
		must.OK(t, filepath.WalkDir(got.storeDir, func(path string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() {
				return err
			}
			raw, err := os.ReadFile(path)
			if err == nil && strings.Contains(string(raw), value) {
				t.Errorf("object %s holds the secret", path)
			}
			return err
		}))
	})

	t.Run("control", func(t *testing.T) {
		got := run(t, "mask-control", `printf '{"secret":"db"}' > /orkstack/output/values.json`)
		must.OKf(t, got.err, "a result naming the secret")
		if got := string(got.result.GetValuesJson()); got != `{"secret":"db"}` {
			t.Fatalf("values = %q; want the result as the task wrote it", got)
		}
	})
}
