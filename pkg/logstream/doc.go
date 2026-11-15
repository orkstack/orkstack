// Package logstream holds the wire shape of a task's live output: one stream
// per attempt, with stdout and stderr told apart in band, per line.
//
// Four components speak the format: the Orker writes it from a container's two
// file descriptors, the artifact store carries it as numbered chunks,
// RunService.StreamTaskLogs serves it, and the UI decodes it.
//
// It also holds Masker, which replaces the secrets an Orker resolved for an
// attempt in that output before anything reads it: a byte-stream transformer
// in front of the line framing, one per stream, sharing one set of forms.
//
// This package is public and MUST stay free of engine, Temporal and generated
// proto imports: it is a byte format, and everything that speaks it should be
// able to depend on it without inheriting anything else.
package logstream
