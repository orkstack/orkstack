package dsl

import (
	"crypto/sha256"
	"encoding/hex"
)

// sourceHash derives the version of a document that pins none: the sha256 of
// its VERSIONED source (VersionedSource) — the bytes the compiled orkflow
// carries in source_yaml and the store writes into that version's object,
// less the two header lines when they are the document's first YAML lines.
//
// That identity is the contract, not an implementation detail. A version names
// ONE immutable document, and the store decides an apply is a repeat by
// comparing what it holds — the versioned source of each side included
// (pkg/artifacts.SameStoredOrkflow). A derivation covering less than that (a
// semantic hash that ignores comments, say) would let one version name two
// different documents, and the store would then refuse the second as an
// attempt to rewrite an immutable version — with advice, "publish a new
// version", that the author of an unpinned document cannot follow, because a
// derived version is not theirs to name. Two notions of "the same document"
// in one system is one too many, so the hash covers exactly what the store
// compares: both read VersionedSource, so they can never disagree.
//
// What equal versioned sources mean, so a reader can check this loosens
// nothing that matters. Among sources this release accepts and versions
// stored before it, two different sources have the same versioned source in
// exactly two cases: they are one text with the two header lines placed at
// different points among its leading comment lines; or one is a version
// stored without the header, and the other is that text with the two lines
// added among those leading lines. The proof: if one side is stripped and the
// other not, the unstripped side is the stripped side's text, which has no
// header (a second one would be a duplicate key the compiler refuses), so it
// is a header-less source, which only a stored version can be. If both are
// stripped, removing the header gives both the same text S; VersionedSource
// crosses only preamble lines, so each side is S with the header inserted at
// a line boundary inside S's leading preamble lines — the first case. A byte
// order mark changes neither case: a leading one stays in front on both
// sides, and one anywhere else makes the line it opens unreadable (before a
// key yaml.v3 reads it as part of the key, which KnownFields refuses; before
// a comment the line no longer parses), so no accepted or stored source has
// one there.
//
// The cost is that a cosmetic edit is a version too. For a store that keeps
// the source that is the honest answer: the file a reader opens changed, so
// what it is stored as changed with it. An author who wants to decide when a
// version happens pins `version:` and gets exactly that.
func sourceHash(source []byte) string {
	sum := sha256.Sum256(VersionedSource(source))
	return hex.EncodeToString(sum[:])
}
