package api

import (
	"container/list"
	"sync"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// pinnedActivityCache answers InspectActivity for a reference that pins a
// digest, once this process has read that digest from the registry.
//
// A run page asks for the icon of every distinct image its tasks ran, each
// by its pinned reference; without this, every page view would cost three
// registry GETs per image. What it keeps is a function of the digest alone —
// an image's index, config and labels are content-addressed and never change
// — so any replica computes the same answer, and nothing here is execution
// state or a read model behind a wired reader (AGENTS.md §7, §13). It is
// process-local, bounded, never shared, never stored, and gone when the
// process exits. A tag or a catalog name that resolves to a tag is never
// answered from it, because a tag moves; a resolution that failed is never
// kept, because the registry may serve it on the next read.
type pinnedActivityCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int
	bytes      int
	order      *list.List // most recently used at the front
	entries    map[string]*list.Element
}

// pinnedEntry is one cached answer and the bytes it counts against maxBytes.
type pinnedEntry struct {
	key  string
	info *orkstackv1.ActivityInfo
	size int
}

// Bounds of the cache. An entry holds at most three bounded labels' worth
// of parsed content, so the byte bound, not the count, is what a hostile
// stream of distinct digests meets first. It bounds memory, not wire bytes:
// a parsed message holds about eight to nine bytes of heap for each byte of
// its encoding when it is maps and strings, and up to about forty when it is
// structured defaults ([{},{},…], measured on interface labels at their
// bound), so an entry is charged heapPerWireByte times its encoded size.
const (
	pinnedCacheEntries = 512
	pinnedCacheBytes   = 16 << 20
	heapPerWireByte    = 40
)

func newPinnedActivityCache() *pinnedActivityCache {
	return &pinnedActivityCache{
		maxEntries: pinnedCacheEntries,
		maxBytes:   pinnedCacheBytes,
		order:      list.New(),
		entries:    map[string]*list.Element{},
	}
}

// pinnedKey is the cache key of a reference: registry/repository@digest when
// it pins a digest, "" when it does not (a tag, or a reference that does not
// parse) and so must never be answered from the cache.
func pinnedKey(reference string) string {
	parsed, err := dsl.ParseReference(reference)
	if err != nil || parsed.Digest == "" {
		return ""
	}
	return parsed.Pinned(parsed.Digest)
}

// get returns a copy of the answer cached for key, or nil. The copy carries
// no name and no reference: those are the request's, not the digest's.
func (c *pinnedActivityCache) get(key string) *orkstackv1.ActivityInfo {
	c.mu.Lock()
	element, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	c.order.MoveToFront(element)
	// An entry's message is never changed once kept, so it is copied
	// outside the lock: many readers of one large entry do not queue.
	info := element.Value.(*pinnedEntry).info
	c.mu.Unlock()
	return proto.Clone(info).(*orkstackv1.ActivityInfo)
}

// put keeps what info says about the digest key names — everything but its
// name and reference — evicting the least recently used entries past either
// bound. An answer larger than the whole byte bound is not kept.
func (c *pinnedActivityCache) put(key string, info *orkstackv1.ActivityInfo) {
	kept := proto.Clone(info).(*orkstackv1.ActivityInfo)
	kept.Name, kept.Reference = "", ""
	size := pinnedCost(key, kept)
	if size > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		c.remove(element)
	}
	c.entries[key] = c.order.PushFront(&pinnedEntry{key: key, info: kept, size: size})
	c.bytes += size
	for c.order.Len() > c.maxEntries || c.bytes > c.maxBytes {
		c.remove(c.order.Back())
	}
}

// pinnedCost is the memory an entry is charged.
func pinnedCost(key string, info *orkstackv1.ActivityInfo) int {
	return heapPerWireByte*proto.Size(info) + len(key)
}

// remove drops one entry; the caller holds mu.
func (c *pinnedActivityCache) remove(element *list.Element) {
	entry := c.order.Remove(element).(*pinnedEntry)
	delete(c.entries, entry.key)
	c.bytes -= entry.size
}
