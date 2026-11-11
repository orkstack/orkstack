package orker

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// lockedBuffer is a bytes.Buffer a logger may write while a test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// jsonLogTo is a logger that writes one JSON record a line to w, so a test
// reads a record's level and attributes and never the words around them.
func jsonLogTo(w *lockedBuffer) *slog.Logger { return slog.New(slog.NewJSONHandler(w, nil)) }

// logRecord is one structured log line: its level, its message and its
// attributes, by key.
type logRecord map[string]any

func (r logRecord) level() string { s, _ := r[slog.LevelKey].(string); return s }

// mentions reports whether any string of the record, its message or an
// attribute's value, contains text.
func (r logRecord) mentions(text string) bool {
	for _, value := range r {
		if s, ok := value.(string); ok && strings.Contains(s, text) {
			return true
		}
	}
	return false
}

// logRecords is every record jsonLogTo wrote to logs.
func logRecords(t *testing.T, logs *lockedBuffer) []logRecord {
	t.Helper()
	var records []logRecord
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var record logRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q is not a JSON record: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

// withAttr is the first record that carries the attribute key, or nil.
func withAttr(records []logRecord, key string) logRecord {
	for _, record := range records {
		if _, ok := record[key]; ok {
			return record
		}
	}
	return nil
}

// TestBoot_SaysWhetherACapacityBoundsAdmission: an Orker that declares no
// capacity admits by max_concurrent alone, a count of tasks, and says at boot
// that its admission is best effort, with the count and the per-task cap that
// bound it; one that declares a capacity says what it declared instead
// (docs/DEPLOY.md, "Declaring an Orker's capacity"). The two are told apart by
// what the record carries, not by its wording.
func TestBoot_SaysWhetherACapacityBoundsAdmission(t *testing.T) {
	ceiling, err := DefaultSecurityPolicy().ceiling()
	must.OK(t, err)
	declaredCPU, _, err := parseCapacity("16", "")
	must.OK(t, err)
	for _, tc := range []struct {
		name        string
		capacityCPU string
		// carries is the attribute that marks the record, and what it holds;
		// absent is the one the other case's record carries and this one's
		// must not.
		carries string
		holds   any
		absent  string
	}{
		{
			name:    "no capacity",
			carries: "max_concurrent", holds: float64(DefaultMaxConcurrent),
			absent: "capacity",
		},
		{
			name:        "a capacity",
			capacityCPU: "16",
			carries:     "capacity", holds: declaredCPU.String(),
			absent: "max_concurrent",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedBuffer{}
			activities := NewActivitiesWithOptions(nil, Options{
				Runtime:     &reapRecorder{labelled: map[string]int{}},
				WorkRoot:    t.TempDir(),
				CapacityCPU: tc.capacityCPU,
				TaskQueue:   "q",
				Logger:      jsonLogTo(logs),
			})
			t.Cleanup(activities.Close)
			if err := activities.Boot(context.Background()); err != nil {
				t.Fatalf("Boot = %v", err)
			}
			records := logRecords(t, logs)
			record := withAttr(records, tc.carries)
			if record == nil {
				t.Fatalf("boot log has no record carrying %s:\n%s", tc.carries, logs.String())
			}
			if record.level() != "INFO" {
				t.Errorf("the record carrying %s is %s, want INFO", tc.carries, record.level())
			}
			if record[tc.carries] != tc.holds {
				t.Errorf("%s = %v, want %v", tc.carries, record[tc.carries], tc.holds)
			}
			if got, want := fmt.Sprint(record["per_task_cap"]), ceiling.String(); got != want {
				t.Errorf("per_task_cap = %s, want the policy's cap, %s", got, want)
			}
			if other := withAttr(records, tc.absent); other != nil {
				t.Errorf("boot log also carries %s, which belongs to the other case: %v", tc.absent, other)
			}
		})
	}
}
