package artifacts

import (
	"context"
	"testing"
	"time"
)

// TestStoreClock_SkewIsLocalMinusStore: the probe writes its one key, reads
// the store's stamp of it, and reports this process's clock minus the
// store's, to within the store's one-second resolution.
func TestStoreClock_SkewIsLocalMinusStore(t *testing.T) {
	for _, ahead := range []time.Duration{0, 10 * time.Second, -7 * time.Second} {
		clock, err := NewStoreClock(context.Background(), Config{Backend: BackendLocal, LocalDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		clock.now = func() time.Time { return time.Now().Add(ahead) }
		skew, err := clock.Skew(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if diff := skew - ahead; diff > time.Second || diff < -time.Second {
			t.Errorf("skew with the local clock %v ahead = %v; want within 1s of it", ahead, skew)
		}
		if _, err := clock.b.head(context.Background(), StoreClockKey); err != nil {
			t.Errorf("the probe key %s was not written: %v", StoreClockKey, err)
		}
	}
}

// TestStoreClock_WholeSecondStampIsCentred: a store that stamps whole
// seconds (S3) is read at the middle of its second, so a reading is never
// biased by up to a second in one direction.
func TestStoreClock_WholeSecondStampIsCentred(t *testing.T) {
	stamp := time.Unix(1000, 0)
	if got := storeNow(stamp); !got.Equal(stamp.Add(500 * time.Millisecond)) {
		t.Fatalf("storeNow(%v) = %v; want the middle of that second", stamp, got)
	}
	fine := time.Unix(1000, 123)
	if got := storeNow(fine); !got.Equal(fine) {
		t.Fatalf("storeNow(%v) = %v; want it unchanged", fine, got)
	}
}
