// Package docscheck holds the documentation drift checks (AGENTS.md §11.1,
// docs/PROOFS.md §11): tests that fail when a reference doc describes
// something that does not exist, or stops describing something that does.
//
// It is a package of tests only. Nothing imports it, and it has no non-test
// code, so it adds no dependency to a binary.
package docscheck
