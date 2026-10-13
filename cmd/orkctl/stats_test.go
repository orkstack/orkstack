package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// statsReader is the cluster half of the read model for these tests: the live
// runs and the finished-run durations an engine would read from the execution
// index, with nothing of either in this process.
type statsReader struct {
	executions map[string]engine.Executions
}

func (s statsReader) GetRun(context.Context, string) (*orkstackv1.Run, error) {
	return nil, engine.ErrRunNotFound
}

func (s statsReader) ListRuns(context.Context, engine.RunQuery) ([]*orkstackv1.Run, error) {
	return nil, nil
}

func (s statsReader) StreamEvents(
	context.Context, string, int64, func(*orkstackv1.Event) error,
) error {
	return engine.ErrRunNotFound
}

func (s statsReader) Executions(
	_ context.Context,
	w engine.StatsWindow,
) (engine.Executions, error) {
	return s.executions[w.Namespace], nil
}

// RunningCount answers admission's question from the same executions map.
func (s statsReader) RunStart(context.Context, string) (*engine.RunStartRecord, error) {
	return nil, engine.ErrRunNotFound
}

func (s statsReader) RunningCount(_ context.Context, namespace string, _ int64) (int64, error) {
	return int64(len(s.executions[namespace].Running)), nil
}

// seedStats wires a fixture whose stats surface has something to report, and
// returns it. The archive is a real one on a temp directory, so the counts
// below come out of real index keys.
func seedStats(t *testing.T, executions map[string]engine.Executions) *fixture {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(store)

	archive := func(id, namespace, orkflow string, status orkstackv1.Status, at time.Time) {
		t.Helper()
		if err := archiver.Archive(context.Background(), &orkstackv1.Run{
			Id:          id,
			Namespace:   namespace,
			OrkflowName: orkflow,
			Status:      status,
			CreatedAt:   timestamppb.New(at),
			FinishedAt:  timestamppb.New(at.Add(time.Minute)),
		}, nil); err != nil {
			t.Fatalf("archive %s: %v", id, err)
		}
	}
	inWindow := fixedNow.Add(-2 * time.Hour)
	archive("ork-a", "default", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, inWindow)
	archive("ork-b", "default", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, inWindow)
	archive("ork-c", "default", "deploy", orkstackv1.Status_STATUS_FAILED, inWindow)
	archive("ork-d", "default", "nightly", orkstackv1.Status_STATUS_CANCELED, inWindow)
	// A second namespace, so --namespace has something to be wrong about.
	archive("ork-e", "payments", "charge", orkstackv1.Status_STATUS_FAILED, inWindow)
	archive("ork-f", "payments", "charge", orkstackv1.Status_STATUS_FAILED, inWindow)

	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = statsReader{executions: executions}
		cfg.Archive = archiver
	})
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	return f
}

// TestStats_GoldenHumanAndJSON pins both renderings.
//
// Human output and --json are two contracts: a person reads the first and a
// script parses the second, and either changing silently breaks somebody.
func TestStats_GoldenHumanAndJSON(t *testing.T) {
	executions := map[string]engine.Executions{
		"default": {
			Running: []*orkstackv1.Run{{
				Id:          "ork-live-1",
				Namespace:   "default",
				OrkflowName: "deploy",
				Status:      orkstackv1.Status_STATUS_RUNNING,
				CreatedAt:   timestamppb.New(fixedNow.Add(-10 * time.Minute)),
			}},
			Durations: []time.Duration{
				2 * time.Second, 4 * time.Second, 6 * time.Second, 8 * time.Second,
			},
		},
	}
	f := seedStats(t, executions)

	human := f.run("stats").requireExit(t, exitOK)
	requireGolden(t, "stats.golden", human.stdout)

	machine := f.run("--json", "stats").requireExit(t, exitOK)
	requireGolden(t, "stats.json.golden", machine.stdout)
}

// TestStats_SinceNarrowsTheWindow. The flag has to actually reach the server,
// which a golden of the default window would never show.
func TestStats_SinceNarrowsTheWindow(t *testing.T) {
	f := seedStats(t, map[string]engine.Executions{})

	day := f.run("stats").requireExit(t, exitOK)
	if !strings.Contains(day.stdout, "succeeded  2") {
		t.Fatalf("the default window did not count the seeded runs:\n%s", day.stdout)
	}
	// The seeded runs are two hours old, so an hour-long window holds none.
	hour := f.run("stats", "--since", "1h").requireExit(t, exitOK)
	if !strings.Contains(hour.stdout, "succeeded  0") {
		t.Errorf("--since 1h still counts runs from two hours ago:\n%s", hour.stdout)
	}
	if !strings.Contains(hour.stdout, unavailable) {
		t.Errorf("a window with no finished runs reports a success rate:\n%s", hour.stdout)
	}
}

// TestStats_NamespaceScopesTheAnswer. --namespace is half of what makes these
// numbers mean anything: a rate that quietly mixed two tenants would be a
// number about nobody.
func TestStats_NamespaceScopesTheAnswer(t *testing.T) {
	f := seedStats(t, map[string]engine.Executions{})

	fallback := f.run("stats").requireExit(t, exitOK)
	if !strings.Contains(fallback.stdout, "Namespace:     default") {
		t.Errorf("with no --namespace, stats did not act in the default namespace:\n%s",
			fallback.stdout)
	}
	if !strings.Contains(fallback.stdout, "succeeded  2") ||
		!strings.Contains(fallback.stdout, "failed     1") {
		t.Errorf("the default namespace does not report its own runs:\n%s", fallback.stdout)
	}

	scoped := f.run("-n", "payments", "stats").requireExit(t, exitOK)
	if !strings.Contains(scoped.stdout, "Namespace:     payments") {
		t.Errorf("--namespace did not reach the report header:\n%s", scoped.stdout)
	}
	if !strings.Contains(scoped.stdout, "succeeded  0") ||
		!strings.Contains(scoped.stdout, "failed     2") {
		t.Errorf("payments was answered with the default namespace's runs:\n%s", scoped.stdout)
	}
	if !strings.Contains(scoped.stdout, "charge        2       2") {
		t.Errorf("payments' most-failing table is not its own:\n%s", scoped.stdout)
	}
	if strings.Contains(scoped.stdout, "deploy") || strings.Contains(scoped.stdout, "nightly") {
		t.Errorf("payments' report names the default namespace's orkflows:\n%s", scoped.stdout)
	}
}

// TestStats_RejectsAnUnreadableSince, naming both spellings it accepts. A
// silent fall back to the default window would answer a question nobody asked
// and look exactly right.
func TestStats_RejectsAnUnreadableSince(t *testing.T) {
	f := seedStats(t, map[string]engine.Executions{})
	got := f.run("stats", "--since", "last tuesday").requireExit(t, exitError)
	for _, want := range []string{"--since", "6h", "2026-05-01"} {
		if !strings.Contains(got.stderr, want) {
			t.Errorf("error %q does not name %q", got.stderr, want)
		}
	}
}

// TestParseSince_ReadsALookbackOrAnInstant.
//
// A duration stays a duration all the way to the server: it is half a window,
// and the other half is the server's clock.
func TestParseSince_ReadsALookbackOrAnInstant(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		raw          string
		wantLookback time.Duration
		wantAt       time.Time
		wantErr      bool
	}{
		{name: "hours", raw: "6h", wantLookback: 6 * time.Hour},
		{name: "minutes", raw: "90m", wantLookback: 90 * time.Minute},
		{name: "an instant", raw: "2026-05-01T09:00:00Z",
			wantAt: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)},
		{name: "a date", raw: "2026-05-01",
			wantAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		{name: "forwards is not a window", raw: "-6h", wantErr: true},
		{name: "zero is not a window", raw: "0s", wantErr: true},
		{name: "prose", raw: "last tuesday", wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			lookback, at, err := parseSince(testCase.raw)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("parseSince(%q) = %v/%v, want an error", testCase.raw, lookback, at)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSince(%q): %v", testCase.raw, err)
			}
			if lookback != testCase.wantLookback {
				t.Errorf("parseSince(%q) lookback = %v, want %v",
					testCase.raw, lookback, testCase.wantLookback)
			}
			if !at.Equal(testCase.wantAt) {
				t.Errorf("parseSince(%q) instant = %s, want %s",
					testCase.raw, at, testCase.wantAt)
			}
		})
	}
}
