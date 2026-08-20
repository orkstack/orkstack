package dsl

import (
	"crypto/sha256"
	"encoding/hex"
)

// sourceHash derives the version of a document that pins none: the sha256 of
// the source BYTES, exactly the bytes the compiled orkflow carries in
// source_yaml and the store writes into that version's object.
//
// That identity is the contract, not an implementation detail. A version names
// ONE immutable document, and the store decides an apply is a repeat by
// comparing what it holds — source included (pkg/artifacts.sameStoredOrkflow).
// A derivation covering less than those bytes lets one version name two
// different documents, and the store then refuses the second as an attempt to
// rewrite an immutable version. This hash used to normalize the YAML and hash
// only its semantic content, so a comment-only edit of an unpinned document
// compiled to the stored version with different source and could not be
// applied at all — and the refusal's advice, "publish a new version", was
// unfollowable, because a derived version is not the author's to name. Two
// notions of "the same document" in one system is one too many.
//
// The cost is that a cosmetic edit is a version too. For a store that keeps
// the source that is the honest answer: the file a reader opens changed, so
// what it is stored as changed with it. An author who wants to decide when a
// version happens pins `version:` and gets exactly that.
func sourceHash(source []byte) string {
	sum := sha256.Sum256(source)
	return hex.EncodeToString(sum[:])
}
