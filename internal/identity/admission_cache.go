package identity

import (
	"container/list"
	"context"
	"sync"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The bounds of identity.admission_cache_bytes: the replica's cache of the
// admission records its mints have read (workload-identity §3, step 2).
const (
	DefaultAdmissionCacheBytes = 128 << 20
	MinAdmissionCacheBytes     = 1 << 20
	MaxAdmissionCacheBytes     = 4 << 30
)

// AdmissionCache keeps the admission records a replica's mints read, the
// least recently used dropped first once their encoded bytes pass the
// bound. A record never changes once written and is named by its digest,
// so a found one is kept; an absent or untrusted answer never is, and is
// asked again of the store next time.
type AdmissionCache struct {
	MintStores
	limit int64

	mu    sync.Mutex
	used  int64
	order *list.List
	byKey map[string]*list.Element
}

type cachedAdmission struct {
	key  string
	adm  artifacts.Admission
	size int64
}

// NewAdmissionCache caches stores' admission reads within limit bytes.
func NewAdmissionCache(stores MintStores, limit int64) *AdmissionCache {
	return &AdmissionCache{MintStores: stores, limit: limit, order: list.New(), byKey: map[string]*list.Element{}}
}

// ReadAdmission answers a kept record, or reads it and keeps it when found.
// The record's maps and slices are shared with the cache: a caller treats
// it as read-only, as the Minter does.
func (c *AdmissionCache) ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error) {
	key := namespace + "\x00" + runID + "\x00" + sha256Hex
	c.mu.Lock()
	if e, ok := c.byKey[key]; ok {
		c.order.MoveToFront(e)
		adm := e.Value.(*cachedAdmission).adm
		c.mu.Unlock()
		return artifacts.RecordRead[artifacts.Admission]{Answer: artifacts.RecordFound, Value: adm}, nil
	}
	c.mu.Unlock()
	read, err := c.MintStores.ReadAdmission(ctx, namespace, runID, sha256Hex)
	if err != nil || read.Answer != artifacts.RecordFound {
		return read, err
	}
	raw, _, encErr := artifacts.EncodeAdmission(read.Value)
	if encErr != nil {
		return read, nil // a record that does not encode again is answered, not kept
	}
	c.keep(key, read.Value, int64(len(raw)))
	return read, nil
}

func (c *AdmissionCache) keep(key string, adm artifacts.Admission, size int64) {
	if size > c.limit {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.byKey[key]; ok {
		return
	}
	c.byKey[key] = c.order.PushFront(&cachedAdmission{key: key, adm: adm, size: size})
	c.used += size
	for c.used > c.limit {
		last := c.order.Back()
		v := last.Value.(*cachedAdmission)
		c.order.Remove(last)
		delete(c.byKey, v.key)
		c.used -= v.size
	}
}
