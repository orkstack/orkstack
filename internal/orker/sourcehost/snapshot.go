package sourcehost

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The states an instance shows of itself.
const (
	StateStarting    = "starting"
	StateActive      = "active"
	StateWaiting     = "waiting"
	StateInvalid     = "invalid"
	StateUnreachable = "unreachable"
	StateStopping    = "stopping"
)

// Snapshot is what an instance is doing, as its Orker sees it. Broker, Warnings,
// Provider and Dropped are the image's word.
type Snapshot struct {
	State  string
	Reason string
	// Since is when it began to show that, and ReadySince when it became ready
	// and has been since, none while it is not.
	Since      time.Time
	ReadySince time.Time
	// Reported: it has written a status, whatever it says. Checked is when the
	// instance's loop last looked at every lane.
	Reported bool
	Checked  time.Time

	Lanes          eventsources.LaneDetails
	Broker         *eventsources.BrokerDetails
	Warnings       []string
	LastAcceptedAt time.Time
	LastPollOkAt   time.Time
	// Provider is its broker's kind, and Dropped how many messages an
	// at-most-once image received and never handed over.
	Provider string
	Dropped  int64
}

// shown is a state and its reason, and since when.
type shown struct {
	state, reason string
	since         time.Time
}

// brokerStatus is the broker block of a status as it is shown.
type brokerStatus struct {
	Object           string `json:"object"`
	MaxDeliveries    int    `json:"maxDeliveries"`
	LeaseSeconds     int64  `json:"leaseSeconds"`
	RetentionSeconds int64  `json:"retentionSeconds"`
	Pending          int64  `json:"pending"`
	Unacknowledged   int64  `json:"unacknowledged"`
	Redelivered      int64  `json:"redelivered"`
	DeadLetter       struct {
		Depth int64 `json:"depth"`
	} `json:"deadLetter"`
	Dropped int64 `json:"dropped"`

	seen bool
}

// readBroker reads a status's broker block, nothing of one that is not of its
// shape.
func readBroker(block jsontext.Value) brokerStatus {
	b := brokerStatus{seen: true}
	if len(block) == 0 || json.Unmarshal(block, &b) != nil {
		return brokerStatus{}
	}
	return b
}

// Snapshot is the instance as it is now.
func (in *Instance) Snapshot() Snapshot {
	hold := in.heldBack()
	in.mu.Lock()
	defer in.mu.Unlock()
	now := in.Now()
	out := Snapshot{ReadySince: in.readySince, LastAcceptedAt: in.acceptedAt, LastPollOkAt: in.pollOkAt, Checked: in.looked}
	in.show(now, hold)
	out.State, out.Reason, out.Since = in.shown.state, in.shown.reason, in.shown.since
	out.Lanes.Parked = len(in.held)
	for _, d := range in.Drivers {
		open, owed := d.Lane.Open(now)
		if open {
			out.Lanes.Busy++
		}
		if wedged, _ := d.Lane.Wedged(); wedged {
			out.Lanes.Wedged++
		}
		out.Lanes.OldestStepSeconds = max(out.Lanes.OldestStepSeconds, int(owed/time.Second))
	}
	out.Lanes.Busy = max(out.Lanes.Busy-out.Lanes.Parked, 0)
	if in.status != nil {
		last, reported := in.status.Last()
		out.Reported = reported
		out.Warnings = append([]string(nil), last.Warnings...)
	}
	if b := in.block; b.seen {
		out.Broker = &eventsources.BrokerDetails{CheckedAt: in.checkedAt, Object: b.Object, MaxDeliveries: b.MaxDeliveries, LeaseSeconds: b.LeaseSeconds,
			RetentionSeconds: b.RetentionSeconds, Pending: b.Pending, Unacknowledged: b.Unacknowledged, Redelivered: b.Redelivered, DeadLetterDepth: b.DeadLetter.Depth}
		kind, _, _ := strings.Cut(b.Object, ":")
		out.Provider, out.Dropped = SafeWord(kind, 32), b.Dropped
	}
	return out
}

// heldBack is what the Orker holds the instance back for. It is asked without
// the instance's lock: what it reads is the Orker's, under locks of its own.
func (in *Instance) heldBack() string {
	if in.Hold == nil {
		return ""
	}
	return in.Hold()
}

// show keeps what the instance shows and since when. Locked.
func (in *Instance) show(now time.Time, hold string) {
	if state, reason := in.state(now, hold); in.shown.state != state || in.shown.reason != reason {
		in.shown = shown{state, reason, now}
	}
}

// state is what the instance shows. Locked.
func (in *Instance) state(now time.Time, hold string) (state, reason string) {
	if in.stopping != nil && in.isStopping() {
		return StateStopping, ""
	}
	if in.status == nil {
		return StateStarting, ""
	}
	last, _ := in.status.Last()
	switch in.status.Health(now) {
	case servicehost.HealthReady:
	case servicehost.HealthUnhealthy:
		if last.Reason == servicehost.CheckUnreachable {
			return StateUnreachable, last.Reason
		}
		return StateInvalid, last.Reason
	case servicehost.HealthNoVerdict:
		return StateUnreachable, ""
	default:
		return StateStarting, ""
	}
	if hold != "" {
		return StateWaiting, hold
	}
	if in.waiter.Waiting() {
		return StateWaiting, in.waitCause
	}
	return StateActive, ""
}
