// Package docscheck holds the documentation drift checks (AGENTS.md §11.1,
// docs/PROOFS.md §11): tests that fail when a reference doc describes
// something that does not exist, or stops describing something that does.
//
// The checks that read every document read the markdown a commit of the work
// tree would hold: tracked files, and untracked ones git does not ignore
// (tree_test.go). Nothing generated, ignored or in another checkout below the
// tree is judged, and the checks need git on PATH and a git checkout.
//
// It is a package of tests only. Nothing imports it, and it has no non-test
// code, so it adds no dependency to a binary.
package docscheck
