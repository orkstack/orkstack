package servicehost

import (
	"fmt"

	"github.com/orkstack/orkstack/pkg/eventsources"
)

// ReasonStoppedByRuntime is the activity contract's reason for a container
// its runtime stopped from outside (AGENTS.md §3.2).
const ReasonStoppedByRuntime = "CONTAINER_STOPPED_BY_RUNTIME"

// Exit is what the runtime and the Orker saw of a standing service's
// container when it ended.
type Exit struct {
	// Code is the exit code.
	Code int
	// AskedToStop: the Orker had begun the stop sequence (§6.2).
	AskedToStop bool
	// OOMKilled: the runtime killed it for memory.
	OOMKilled bool
	// StoppedByRuntime: the runtime stopped it from outside (its daemon
	// shutting down, an operator's stop), told from what the runtime
	// observed, never from the number.
	StoppedByRuntime bool
	// BeforeFirstStatus: it ended before it wrote any status.json.
	BeforeFirstStatus bool
	// ImageUserRoot: the image's config declares the root user (or none),
	// which the Orker overrides with its own non-root uid.
	ImageUserRoot bool
}

// Ending is an attempt's end: its reason ("" for a stop asked for), whether
// its owner retries it, and a sentence.
type Ending struct {
	Reason  string
	Retried bool
	Detail  string
}

// Stopped reports an ending that is no failure of the image.
func (e Ending) Stopped() bool { return e.Reason == "" }

// Meaning is the table of 14 §6.4 for a standing service, with the
// non-root rule beside it: an image that declares root and ends before its
// first status is named for that, never left to look like any failure.
func Meaning(x Exit) Ending {
	switch {
	case x.AskedToStop:
		return Ending{Detail: fmt.Sprintf("stopped; exited %d", x.Code)}
	case x.StoppedByRuntime:
		return Ending{Reason: ReasonStoppedByRuntime, Retried: true, Detail: "its runtime stopped the container from outside"}
	case x.OOMKilled:
		return Ending{Reason: eventsources.ReasonServiceOOMKilled, Retried: true, Detail: "killed for memory"}
	case x.ImageUserRoot && x.BeforeFirstStatus:
		return Ending{Reason: eventsources.ReasonServiceRunsAsRoot, Retried: true,
			Detail: fmt.Sprintf("the image declares the root user and exited %d before its first status; a service image runs as an arbitrary non-root uid", x.Code)}
	case x.Code == 0:
		return Ending{Reason: eventsources.ReasonServiceExited, Retried: true, Detail: "exited 0 when nobody asked it to stop"}
	}
	return Ending{Reason: eventsources.ReasonServiceFailed, Retried: true, Detail: fmt.Sprintf("exited %d", x.Code)}
}
