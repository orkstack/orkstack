package orker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
)

// listingRecorder is reapRecorder that can also say what other identities hold.
type listingRecorder struct {
	reapRecorder
	others []OtherOrkerContainer
	err    error
	asked  int
}

func (r *listingRecorder) OtherOrkersContainers(context.Context, string) ([]OtherOrkerContainer, error) {
	r.asked++
	return r.others, r.err
}

func TestBoot_WarnsAboutContainersOfOtherIdentities(t *testing.T) {
	runtime := &listingRecorder{
		reapRecorder: reapRecorder{labelled: map[string]int{}},
		others: []OtherOrkerContainer{
			{ID: "c0a1", State: "running", Orker: "orker-gone"},
			{ID: "c0a2", State: "exited", Orker: "orker-gone"},
			{ID: "c0a3", State: "running", Orker: "orker-other"},
		},
	}
	_, logged, err := bootWithLog(t, t.TempDir(), "other-orkers-"+t.Name(), runtime)
	if err != nil {
		t.Fatalf("Boot: %v", err)
	}
	text := logged.String()
	if runtime.asked != 1 {
		t.Errorf("listed %d times, want once", runtime.asked)
	}
	for _, want := range []string{"level=WARN", "orker-gone", "orker-other", "c0a1", "c0a2", "c0a3", "count=3", "running=2", "orkctl orker list"} {
		if !strings.Contains(text, want) {
			t.Errorf("the warning does not say %q:\n%s", want, text)
		}
	}
}

func TestBoot_SaysNothingWhenNoOtherIdentityHoldsAContainer(t *testing.T) {
	runtime := &listingRecorder{reapRecorder: reapRecorder{labelled: map[string]int{}}}
	_, logged, err := bootWithLog(t, t.TempDir(), "other-orkers-"+t.Name(), runtime)
	if err != nil {
		t.Fatalf("Boot: %v", err)
	}
	if runtime.asked != 1 {
		t.Errorf("listed %d times, want once", runtime.asked)
	}
	if strings.Contains(logged.String(), "other orker") {
		t.Errorf("warned with nothing to say:\n%s", logged)
	}
}

// TestBoot_AListingThatFailsDoesNotFailBoot: the warning is a courtesy to an
// operator, and an Orker that could not give it can still work.
func TestBoot_AListingThatFailsDoesNotFailBoot(t *testing.T) {
	runtime := &listingRecorder{
		reapRecorder: reapRecorder{labelled: map[string]int{}},
		err:          errors.New("the daemon said no"),
	}
	_, logged, err := bootWithLog(t, t.TempDir(), "other-orkers-"+t.Name(), runtime)
	if err != nil {
		t.Fatalf("a failed listing failed Boot: %v", err)
	}
	if !strings.Contains(logged.String(), "the daemon said no") {
		t.Errorf("the failure was not said:\n%s", logged)
	}
}

// TestWarnAboutOtherOrkers_ACancelledBootBlamesNothing: an Orker told to stop while
// it lists found nothing wrong with its runtime.
func TestWarnAboutOtherOrkers_ACancelledBootBlamesNothing(t *testing.T) {
	var logged bytes.Buffer
	activities := NewActivitiesWithOptions(nil, Options{
		Runtime:  &listingRecorder{reapRecorder: reapRecorder{labelled: map[string]int{}}, err: context.Canceled},
		Logger:   slog.New(slog.NewTextHandler(&logged, nil)),
		WorkRoot: t.TempDir(), OrkerID: "other-orkers",
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activities.warnAboutOtherOrkers(ctx)
	if logged.Len() != 0 {
		t.Errorf("a cancelled listing logged:\n%s", logged.String())
	}
}

// TestBoot_TheWarningIsBounded: a shared daemon with a thousand containers must
// not write a thousand-word line at every boot; the count is the whole.
func TestBoot_TheWarningIsBounded(t *testing.T) {
	var others []OtherOrkerContainer
	for range 1000 {
		others = append(others, OtherOrkerContainer{ID: "c0a1", State: "running", Orker: "orker-gone"})
	}
	runtime := &listingRecorder{reapRecorder: reapRecorder{labelled: map[string]int{}}, others: others}
	_, logged, err := bootWithLog(t, t.TempDir(), "other-orkers-"+t.Name(), runtime)
	if err != nil {
		t.Fatalf("Boot: %v", err)
	}
	if got := strings.Count(logged.String(), "c0a1"); got != reapLogLimit {
		t.Errorf("the line names %d containers, want %d", got, reapLogLimit)
	}
	if !strings.Contains(logged.String(), "count=1000") {
		t.Errorf("the count is not the whole:\n%s", logged)
	}
}

// TestDockerRuntime_OtherOrkersContainers_OnlyLooks is what makes the warning
// safe where another Orker shares the daemon: a list, and nothing else asked.
func TestDockerRuntime_OtherOrkersContainers_OnlyLooks(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("others", "c1\trunning\torker-gone\nc2\texited\torker-me\nc3\tcreated\torker-other\n")
	runtime := &DockerRuntime{Bin: fake.bin, CLI: CLIDocker}
	got, err := runtime.OtherOrkersContainers(context.Background(), "orker-me")
	if err != nil {
		t.Fatalf("OtherOrkersContainers: %v", err)
	}
	want := []OtherOrkerContainer{
		{ID: "c1", State: "running", Orker: "orker-gone"},
		{ID: "c3", State: "created", Orker: "orker-other"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("listed %+v, want %+v", got, want)
	}
	calls := fake.calls()
	if len(calls) != 1 || !strings.HasPrefix(calls[0], "ps --all --filter label=ork.orker ") {
		t.Errorf("asked %q, want one list", calls)
	}
}

func TestDockerRuntime_OtherOrkersContainers_AFailureIsAnError(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("otherpsfail", "")
	runtime := &DockerRuntime{Bin: fake.bin, CLI: CLIDocker}
	if _, err := runtime.OtherOrkersContainers(context.Background(), "orker-me"); err == nil {
		t.Fatal("a list the CLI refused reads as a list with nothing in it")
	}
}

func TestParseOtherContainers(t *testing.T) {
	for _, tt := range []struct {
		name, answer string
		want         []OtherOrkerContainer
	}{
		{name: "empty", answer: ""},
		{name: "blank lines", answer: "\n\n"},
		{name: "only this identity", answer: "c1\trunning\tme\n"},
		{name: "another", answer: "c1\trunning\tother\n", want: []OtherOrkerContainer{{ID: "c1", State: "running", Orker: "other"}}},
		// A label anyone can set, on a container nobody here made: it stays one
		// field, and a line with no such field is no container of an Orker's.
		{name: "a label holding a tab", answer: "c1\trunning\to\tther\n", want: []OtherOrkerContainer{{ID: "c1", State: "running", Orker: "o\tther"}}},
		{name: "an empty label", answer: "c1\trunning\t\n"},
		{name: "a CLI's own line", answer: "Emulate Docker CLI using podman.\nc1\trunning\tother\n", want: []OtherOrkerContainer{{ID: "c1", State: "running", Orker: "other"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseOtherContainers(tt.answer, "me"); !slices.Equal(got, tt.want) {
				t.Errorf("parsed %+v, want %+v", got, tt.want)
			}
		})
	}
}
