package main

import (
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// temporalLogMaxBytes bounds the end of the Temporal server's log a proof
// keeps, before compression. The proof's Temporal logs at `warn`
// (deploy/docker-compose.yml), so a part's whole log is far below it.
const temporalLogMaxBytes = 16 << 20

// temporalLogTimeout bounds `docker logs`. A part's cleanup and a local
// proof's exit run temporal-log with TERM, INT and HUP ignored, before they
// stop the container, so a docker that does not answer must not hold that
// stop: a cancel would leave the Temporal running. A part's log, at `warn`,
// takes well under a second.
const temporalLogTimeout = 20 * time.Second

// containerNameRE is a Docker container name, which never starts with `-`.
var containerNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// tailBuffer keeps the last max bytes written to it, and counts them all.
type tailBuffer struct {
	max   int
	buf   []byte
	total int
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.total += len(p)
	b.buf = append(b.buf, p...)
	if len(b.buf) > 2*b.max {
		b.buf = append(b.buf[:0], b.buf[len(b.buf)-b.max:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) tail() []byte {
	if len(b.buf) > b.max {
		return b.buf[len(b.buf)-b.max:]
	}
	return b.buf
}

// cmdTemporalLog is `proofrun temporal-log`: it keeps the log of a proof's own
// Temporal container — `docker logs`, both of its streams — gzip-compressed in
// one file, before the container is stopped and, started with --rm, removed
// with its log. A log longer than -max-bytes keeps its end, after one line
// saying how much of its start was left out. Exit 1 when docker cannot give
// the log within -timeout (nothing is written), 2 for an invalid invocation.
func cmdTemporalLog(args []string) int {
	fs := flag.NewFlagSet("temporal-log", flag.ContinueOnError)
	container := fs.String("container", "", "the proof's Temporal container (required)")
	out := fs.String("out", "", "the gzip file to write (required)")
	maxBytes := fs.Int("max-bytes", temporalLogMaxBytes, "keep at most the log's last this many bytes, before compression")
	timeout := fs.Duration("timeout", temporalLogTimeout, "give up on docker after this long")
	if err := fs.Parse(args); err != nil || fs.NArg() > 0 || *out == "" || *maxBytes < 1 || *timeout <= 0 || !containerNameRE.MatchString(*container) {
		fmt.Fprintln(os.Stderr, "proofrun temporal-log -container NAME -out FILE.gz [-max-bytes N] [-timeout D]")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "logs", *container)
	// A child of docker that outlives it holds the pipes open; Wait stops
	// waiting for them this long after docker is killed.
	cmd.WaitDelay = 2 * time.Second
	log := &tailBuffer{max: *maxBytes}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(string(log.tail()))
		if len(msg) > 300 {
			msg = msg[len(msg)-300:]
		}
		if ctx.Err() != nil {
			err = fmt.Errorf("no answer within %v", *timeout)
		}
		fmt.Fprintf(os.Stderr, "proofrun temporal-log: docker logs %s: %v: %s\n", *container, err, msg)
		return 1
	}
	kept := log.tail()
	tmp, err := os.CreateTemp(filepath.Dir(*out), ".temporal-log-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun temporal-log:", err)
		return 1
	}
	defer os.Remove(tmp.Name())
	zw := gzip.NewWriter(tmp)
	if left := log.total - len(kept); left > 0 {
		fmt.Fprintf(zw, "[proofrun temporal-log: the log's first %d bytes left out]\n", left)
	}
	_, err = zw.Write(kept)
	if cerr := zw.Close(); err == nil {
		err = cerr
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(tmp.Name(), 0o644)
	}
	if err == nil {
		err = os.Rename(tmp.Name(), *out)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun temporal-log:", err)
		return 1
	}
	fmt.Fprintf(stdout, "temporal-log: kept %d of the %d bytes %s logged -> %s\n", len(kept), log.total, *container, *out)
	return 0
}
