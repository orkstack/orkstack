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

// TestStoreClock_GCAgesReadLateNeverEarly: on a store that stamps whole
// seconds, truncated, the artifact GC's age of a retirement — its earliest
// store-now minus the latest instant a sidecar stamp stands for — is never
// more than the true age, at every sub-second boundary.
func TestStoreClock_GCAgesReadLateNeverEarly(t *testing.T) {
	truncate := func(t time.Time) time.Time { return t.Truncate(time.Second) }
	base := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	offsets := []time.Duration{0, time.Nanosecond, 100 * time.Millisecond, 499 * time.Millisecond,
		500 * time.Millisecond, 900 * time.Millisecond, time.Second - time.Nanosecond}
	for _, retireOff := range offsets {
		for _, probeOff := range offsets {
			for _, roundTrip := range []time.Duration{0, 50 * time.Millisecond, 3 * time.Second} {
				retired := base.Add(retireOff)
				written := base.Add(time.Minute + probeOff)
				after := written.Add(roundTrip)
				now := after.Add(time.Second)
				// A store that truncates its stamps, and one that rounds the
				// probe's up (a sidecar's is read as its second's end anyway).
				for _, stamp := range []time.Time{truncate(written), written.Add(time.Second - time.Nanosecond).Truncate(time.Second)} {
					reading := StoreReading{Stamp: stamp, Before: written, After: after}
					gcAge := reading.EarliestStoreNow(now).Sub(LatestInstant(truncate(retired)))
					if trueAge := now.Sub(retired); gcAge > trueAge {
						t.Fatalf("retired %v, probe at %v stamped %v (round trip %v): the GC's age %v exceeds the true age %v",
							retireOff, probeOff, stamp, roundTrip, gcAge, trueAge)
					}
				}
			}
		}
	}
}

// TestLatestInstant: a whole-second stamp stands for any instant of its
// second, so its latest is just before the next; a finer stamp is exact.
func TestLatestInstant(t *testing.T) {
	whole := time.Unix(1000, 0)
	if got := LatestInstant(whole); !got.Equal(time.Unix(1001, 0).Add(-time.Nanosecond)) {
		t.Fatalf("LatestInstant(%v) = %v", whole, got)
	}
	fine := time.Unix(1000, 123)
	if got := LatestInstant(fine); !got.Equal(fine) {
		t.Fatalf("LatestInstant(%v) = %v; want it unchanged", fine, got)
	}
}

// TestStoreClock_ReadIsWhatSkewIsBuiltOn: one probe, and its earliest
// store-now is never after this process's own clock when the two agree.
func TestStoreClock_ReadIsWhatSkewIsBuiltOn(t *testing.T) {
	clock, err := NewStoreClock(context.Background(), Config{Backend: BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	r, err := clock.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Stamp.IsZero() || r.After.Before(r.Before) {
		t.Fatalf("reading = %+v", r)
	}
	if earliest := r.EarliestStoreNow(r.After); earliest.After(r.After) {
		t.Fatalf("one clock: the earliest store-now %v is after the local now %v", earliest, r.After)
	}
}
