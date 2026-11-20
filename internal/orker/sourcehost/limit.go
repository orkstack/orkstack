package sourcehost

import "time"

// The delivery limits the Orker accepts from a container's status (06 §5):
// anything else is OBJECT_INVALID, so an image cannot switch the
// dead-letter receipt off by reporting a limit nobody reaches.
const (
	MinDeliveryLimit = 2
	MaxDeliveryLimit = 100
)

// ValidDeliveryLimit reports a broker's delivery limit the Orker accepts.
func ValidDeliveryLimit(n int) bool { return n >= MinDeliveryLimit && n <= MaxDeliveryLimit }

// DeliveryLimit is the limit a message's delivery count is compared with:
// the smaller of the last two limits the container reported, and when the
// one it is was read.
type DeliveryLimit struct {
	last, prev     int
	lastAt, prevAt time.Time
}

// Observe takes a limit a status reported. One the Orker does not accept
// changes nothing; the caller treats that status as OBJECT_INVALID.
func (d *DeliveryLimit) Observe(n int, at time.Time) bool {
	if !ValidDeliveryLimit(n) {
		return false
	}
	d.prev, d.prevAt, d.last, d.lastAt = d.last, d.lastAt, n, at
	return true
}

// Limit is the smaller of the last two limits reported and when it was
// read; 0 before any.
func (d *DeliveryLimit) Limit() (int, time.Time) {
	if d.prev == 0 || (d.last != 0 && d.last <= d.prev) {
		return d.last, d.lastAt
	}
	return d.prev, d.prevAt
}
