package orker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/logstream"
)

// TestInstanceLog: an instance's output is stored in order past what would end a
// task's log, its streams told apart, and what its budget dropped is said at its
// end.
func TestInstanceLog(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalLogStore(dir)
	must.OK(t, err)
	attempt := artifacts.InstanceAttempt{SourceID: "evs-0123456789abcdef", Index: 1, Epoch: 7, Attempt: 2}
	l := newInstanceLog(t.Context(), store, attempt, LogOptions{MaxBytes: 10}, 100, nil)
	out, errs := logstream.NewLineWriter(l.Line, ""), logstream.NewLineWriter(l.Line, logstream.StderrMarker)
	fmt.Fprintln(out, "first")
	fmt.Fprintln(errs, "100% second")
	_, err = out.Write([]byte(strings.Repeat(strings.Repeat("x", 1023)+"\n", 1100)))
	must.OK(t, err)
	out.Close()
	errs.Close()
	l.Orker("the container ended")
	l.Close()
	var all string
	for seq := int64(1); ; seq++ {
		key, err := artifacts.SourceLogKey(attempt, seq)
		must.OK(t, err)
		raw, err := os.ReadFile(filepath.Join(dir, key))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		must.OK(t, err)
		all += string(raw)
	}
	lines := strings.Split(strings.TrimSuffix(all, "\n"), "\n")
	if lines[0] != "first" || lines[1] != "[stderr] 100% second" || len(all) < 1<<20-1024 || len(all) > 1<<20+400 {
		t.Fatalf("%d bytes, beginning %q", len(all), lines[:2])
	}
	tail := lines[len(lines)-3:]
	if !strings.HasPrefix(tail[0], "[ork] stdout passed its 100 bytes an hour ("+SourceHostLogBytesPerHourVar+")") || tail[1] != "[ork] the container ended" ||
		!strings.HasSuffix(tail[2], "bytes of stdout were dropped") {
		t.Fatalf("the log ends %q", tail)
	}
}

// TestInstanceLog_AStoreThatFails: a chunk that is not stored is told, and holds
// nothing up.
func TestInstanceLog_AStoreThatFails(t *testing.T) {
	store, err := artifacts.NewLocalLogStore(t.TempDir())
	must.OK(t, err)
	told := 0
	l := newInstanceLog(t.Context(), store, artifacts.InstanceAttempt{SourceID: "no key holds this"}, LogOptions{}, 100, func(error) { told++ })
	l.Line("%s", "x")
	l.Close()
	if told == 0 {
		t.Fatal("a chunk was lost and nobody was told")
	}
}
