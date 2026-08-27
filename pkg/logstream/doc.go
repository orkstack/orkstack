// Package logstream holds the wire shape of a task's live output: one stream
// per attempt, with stdout and stderr told apart in band, per line.
//
// It was pkg/plugin/linewriter.go until the OCI Activity Contract retired the
// in-process plugin registry. The framing outlived the package that happened to
// host it, because it is not a plugin concern at all: the Orker writes it from
// a container's two file descriptors, the artifact store carries it as numbered
// chunks, RunService.StreamTaskLogs serves it, and the UI decodes it. Deleting
// pkg/plugin would have deleted a format four components already speak, so the
// file moved rather than died (design §2.4).
//
// This package is public and MUST stay free of engine, Temporal and generated
// proto imports: it is a byte format, and everything that speaks it should be
// able to depend on it without inheriting anything else.
package logstream
