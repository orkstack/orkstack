package identity

import (
	"context"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// countingStores counts admission reads of the real store under it.
type countingStores struct {
	MintStores
	reads int
}

func (c *countingStores) ReadAdmission(ctx context.Context, ns, run, sha string) (artifacts.RecordRead[artifacts.Admission], error) {
	c.reads++
	return c.MintStores.ReadAdmission(ctx, ns, run, sha)
}

// TestAdmissionCache: a found record is read once and answered from memory
// after; an absent one is asked again each time; past the bound the least
// recently used is dropped.
func TestAdmissionCache(t *testing.T) {
	w := newMintWorld(t)
	counting := &countingStores{MintStores: w.store}
	cache := NewAdmissionCache(counting, MaxAdmissionCacheBytes)
	ctx := context.Background()
	for range 3 {
		if r, err := cache.ReadAdmission(ctx, "ns", "ork-1", w.sha); err != nil || r.Answer != artifacts.RecordFound {
			t.Fatalf("read = %v, %v", r.Answer, err)
		}
	}
	if counting.reads != 1 {
		t.Fatalf("a found record was read %d times", counting.reads)
	}
	for range 2 {
		if r, _ := cache.ReadAdmission(ctx, "ns", "ork-1", otherSHA); r.Answer == artifacts.RecordFound {
			t.Fatal("an absent record was found")
		}
	}
	if counting.reads != 3 {
		t.Fatalf("an absent record was kept: %d reads", counting.reads)
	}
	tiny := NewAdmissionCache(counting, 1)
	before := counting.reads
	tiny.ReadAdmission(ctx, "ns", "ork-1", w.sha)
	tiny.ReadAdmission(ctx, "ns", "ork-1", w.sha)
	if counting.reads != before+2 {
		t.Fatal("a record larger than the bound was kept")
	}
}
