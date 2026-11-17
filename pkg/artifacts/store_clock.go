package artifacts

import (
	"context"
	"fmt"
	"time"
)

// StoreClockKey is the one object the store-clock probe writes. It is
// overwritten by every engine, read back only by the engine that just wrote
// it, and decides nothing but that engine's reading of its own clock.
const StoreClockKey = "metadata/cleanup/gc-clock"

// StoreClock reads the object store's clock: there is no clock API, but
// every write is stamped with the store's time (LastModified).
type StoreClock struct {
	blobBase
	now func() time.Time
}

// NewStoreClock opens the probe on the backend cfg selects.
func NewStoreClock(ctx context.Context, cfg Config) (*StoreClock, error) {
	b, err := openBlobs(ctx, cfg, "store clock", "metadata/cleanup")
	if err != nil {
		return nil, err
	}
	return &StoreClock{blobBase: blobBase{b}, now: time.Now}, nil
}

// StoreReading is one probe: the store's stamp of the write, and this
// process's clock just before the write and just after the read.
type StoreReading struct {
	Stamp  time.Time
	Before time.Time
	After  time.Time
}

// Read writes the probe and reads the store's stamp of it: the one probe
// Skew and the artifact GC are both built on.
func (c *StoreClock) Read(ctx context.Context) (StoreReading, error) {
	before := c.now()
	if err := c.b.put(ctx, StoreClockKey, "text/plain", []byte(before.UTC().Format(time.RFC3339Nano))); err != nil {
		return StoreReading{}, fmt.Errorf("artifacts: store clock: write %s: %w", c.b.uri(StoreClockKey), err)
	}
	stamped, err := c.b.modified(ctx, StoreClockKey)
	if err != nil {
		return StoreReading{}, fmt.Errorf("artifacts: store clock: read %s: %w", c.b.uri(StoreClockKey), err)
	}
	return StoreReading{Stamp: stamped.UTC(), Before: before, After: c.now()}, nil
}

// Skew writes the probe, reads the store's stamp of it, and returns this
// process's clock, at the middle of that write and read, minus the store's.
// Positive means this process runs ahead of the store. Two engines writing
// at once can each read the other's stamp, which is seconds apart at most.
func (c *StoreClock) Skew(ctx context.Context) (time.Duration, error) {
	r, err := c.Read(ctx)
	if err != nil {
		return 0, err
	}
	local := r.Before.Add(r.After.Sub(r.Before) / 2)
	return local.Sub(storeNow(r.Stamp)), nil
}

// EarliestStoreNow is a lower bound of the store's clock at this process's
// now: a stamp is no later than a second past the store's time of a write
// (a store that truncates is exact; one that rounds up is at most a second
// ahead), that write (this engine's or another's) was done before After, and
// the store's clock has run since then as long as this process's has. The
// artifact GC reads ages from it, so an age can only read late, never early.
func (r StoreReading) EarliestStoreNow(now time.Time) time.Time {
	return r.Stamp.Add(-time.Second).Add(max(now.Sub(r.After), 0))
}

// LatestInstant is the latest store time a stamp can stand for: a store
// that stamps whole seconds (S3) truncates, so a whole-second stamp may be
// any instant of its second.
func LatestInstant(stamp time.Time) time.Time {
	if stamp.Nanosecond() == 0 {
		return stamp.Add(time.Second - time.Nanosecond)
	}
	return stamp
}

// storeNow is the store's time a stamp stands for. S3 stamps whole seconds,
// truncated, so a whole-second stamp is read at the middle of its second.
func storeNow(stamp time.Time) time.Time {
	if stamp.Nanosecond() == 0 {
		return stamp.Add(500 * time.Millisecond)
	}
	return stamp
}
