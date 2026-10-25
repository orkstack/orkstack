package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// replayStore serves the upstream bytes this invocation fetched, under the
// URI the source run recorded, and keeps everything else in a local store in
// the work directory. There is no path from it to the deployment's bucket.
//
// It is the ONLY artifacts.Store the local dispatcher holds. The recorded
// URIs stay unchanged in the UpstreamRefs the dispatcher stages from, so
// `.tasks.<id>.artifacts[].uri` renders exactly as it rendered on the Orker —
// an s3:// URI or a file:// one, whichever backend the run used — while the
// bytes behind it are the ones ReadArtifact verified a moment earlier. The
// dispatcher re-verifies each staged byte against the reference's sha256
// either way.
type replayStore struct {
	fetched map[string]string     // recorded ArtifactRef.uri -> verified local file
	local   *artifacts.LocalStore // session/.store
}

var _ artifacts.Store = (*replayStore)(nil)

func newReplayStore(dir string) (*replayStore, error) {
	local, err := artifacts.NewLocalStore(dir)
	if err != nil {
		return nil, err
	}
	return &replayStore{fetched: map[string]string{}, local: local}, nil
}

// serve records that uri is answered by the verified file at path.
func (s *replayStore) serve(uri, path string) { s.fetched[uri] = path }

// Get serves a fetched URI first, then the local store.
func (s *replayStore) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	if path, ok := s.fetched[uri]; ok {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open fetched %s: %w", uri, err)
		}
		return file, nil
	}
	return s.local.Get(ctx, uri)
}

// Put writes into the session's local store: everything the dispatcher
// uploads — outputs, values.json, the captured streams — stays on this
// machine.
func (s *replayStore) Put(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	return s.local.Put(ctx, name, r, opts)
}

func (s *replayStore) Stat(ctx context.Context, uri string) (artifacts.ArtifactRef, error) {
	return s.local.Stat(ctx, uri)
}

func (s *replayStore) List(ctx context.Context, prefix string) ([]artifacts.ArtifactRef, error) {
	return s.local.List(ctx, prefix)
}

// Presign is unsupported: nothing about a local run is shared by URL.
func (s *replayStore) Presign(context.Context, string, time.Duration) (string, error) {
	return "", fmt.Errorf("task run: presign: %w", artifacts.ErrUnsupported)
}

func (s *replayStore) Delete(ctx context.Context, uri string) error {
	return s.local.Delete(ctx, uri)
}

func (s *replayStore) Close() error { return s.local.Close() }
