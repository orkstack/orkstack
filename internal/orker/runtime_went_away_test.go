package orker

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestDockerRuntime_Wait_TellsARuntimeThatWentAwayFromAnOperatorsStop: a
// container stopped from outside whose event stream then ends by itself was
// stopped by its runtime going away, as a daemon that shuts down closes the
// stream right after its last word; one whose stream stays open was stopped
// through a runtime that stayed up, an operator's stop. A container that ended
// by itself, or a stream that ended before it said anything, is no evidence
// either way.
func TestDockerRuntime_Wait_TellsARuntimeThatWentAwayFromAnOperatorsStop(t *testing.T) {
	for _, tt := range []struct {
		name   string
		events string
		// ends ends the stream right after its words, with the daemon still
		// answering, as a CLI that crashed does; endsAfter after that long;
		// gone ends it after that long and the daemon answers nothing after,
		// as a daemon that shuts down does. None keeps it open.
		ends      bool
		endsAfter string
		gone      string
		// settle is the settle bound, for a stream that hears no stop.
		settle      time.Duration
		wantStopped bool
		wantGone    bool
	}{
		{name: "a daemon's clean shutdown", events: "start \nkill 15\nstop \ndie \n", gone: "0", wantStopped: true, wantGone: true},
		{name: "the daemon goes a moment after its last word", events: "start \nkill 15\nstop \ndie \n", gone: "0.1", wantStopped: true, wantGone: true},
		{name: "the daemon goes as soon as it said so", events: "start \nkill 15\n", gone: "0", wantStopped: true, wantGone: true},
		{name: "an operator's stop through a daemon that stays up", events: "start \nkill 15\nstop \ndie \ndestroy \n", wantStopped: true},
		{name: "an operator's stop, the stream's CLI crashing while the daemon answers", events: "start \nkill 15\nstop \ndie \n", ends: true, wantStopped: true},
		{name: "an operator's stop, the stream ending only after the window", events: "start \nkill 15\nstop \ndie \n", endsAfter: "4", wantStopped: true},
		{name: "exits by itself, then the stream ends", events: "start \ndie \n", ends: true},
		{name: "the stream ends before it says", events: "start \n", ends: true, settle: 200 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("events", tt.events)
			if tt.ends {
				fake.set("eventsend", "")
			}
			if tt.endsAfter != "" {
				fake.set("eventsendafter", tt.endsAfter)
			}
			if tt.gone != "" {
				fake.set("eventsendgone", tt.gone)
			}
			fake.set("startquick", "")
			runtime := &DockerRuntime{Bin: fake.bin, CLI: CLIDocker, stopSettle: tt.settle, goneWindow: time.Second}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var stdout, stderr bytes.Buffer
			must.OKf(t, runtime.Start(ctx, "cid-0123", &stdout, &stderr), "Start")
			status, err := runtime.Wait(ctx, "cid-0123")
			must.OKf(t, err, "Wait")
			if status.StoppedFromOutside != tt.wantStopped || status.RuntimeWentAway != tt.wantGone {
				t.Errorf("stopped = %t, runtime went away = %t; want %t, %t",
					status.StoppedFromOutside, status.RuntimeWentAway, tt.wantStopped, tt.wantGone)
			}
		})
	}
}
