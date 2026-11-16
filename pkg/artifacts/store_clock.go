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

// Skew writes the probe, reads the store's stamp of it, and returns this
// process's clock, at the middle of that write and read, minus the store's.
// Positive means this process runs ahead of the store. Two engines writing
// at once can each read the other's stamp, which is seconds apart at most.
func (c *StoreClock) Skew(ctx context.Context) (time.Duration, error) {
	before := c.now()
	if err := c.b.put(ctx, StoreClockKey, "text/plain", []byte(before.UTC().Format(time.RFC3339Nano))); err != nil {
		return 0, fmt.Errorf("artifacts: store clock: write %s: %w", c.b.uri(StoreClockKey), err)
	}
	stamped, err := c.b.modified(ctx, StoreClockKey)
	if err != nil {
		return 0, fmt.Errorf("artifacts: store clock: read %s: %w", c.b.uri(StoreClockKey), err)
	}
	after := c.now()
	local := before.Add(after.Sub(before) / 2)
	return local.Sub(storeNow(stamped)), nil
}

// storeNow is the store's time a stamp stands for. S3 stamps whole seconds,
// truncated, so a whole-second stamp is read at the middle of its second.
func storeNow(stamp time.Time) time.Time {
	if stamp.Nanosecond() == 0 {
		return stamp.Add(500 * time.Millisecond)
	}
	return stamp
}
