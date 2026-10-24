package dsl

import (
	"crypto/sha256"
	"encoding/hex"
)

// sourceHash derives the version of a document that pins none: the sha256 of
// its source as written. The source is the whole document — header, comments
// and every byte — because a version names ONE immutable document, and the
// store decides an apply is a repeat by comparing the raw source bytes
// (pkg/artifacts.SameStoredOrkflow). A derivation covering less than that (a
// semantic hash that ignores comments, say) would let one version name two
// different documents, and the store would then refuse the second as an
// attempt to rewrite an immutable version — with advice, "publish a new
// version", that the author of an unpinned document cannot follow, because a
// derived version is not theirs to name. Two notions of "the same document"
// in one system is one too many, so the hash covers exactly what the store
// compares: both read the raw source, so they can never disagree.
//
// The cost is that a cosmetic edit is a version too. For a store that keeps
// the source that is the honest answer: the file a reader opens changed, so
// what it is stored as changed with it. An author who wants to decide when a
// version happens pins `version:` and gets exactly that.
func sourceHash(source []byte) string {
	sum := sha256.Sum256(source)
	return hex.EncodeToString(sum[:])
}
