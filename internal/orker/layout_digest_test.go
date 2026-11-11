package orker

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

type layoutProbeRuntime struct{ loads int }

func (*layoutProbeRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{}, errors.New("registry unavailable")
}
func (r *layoutProbeRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	r.loads++
	return PullInfo{}, nil
}
func (*layoutProbeRuntime) Create(context.Context, ContainerSpec) (ContainerID, error) {
	return "", nil
}
func (*layoutProbeRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	return nil
}
func (*layoutProbeRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }
func (*layoutProbeRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{}, nil
}
func (*layoutProbeRuntime) Remove(context.Context, ContainerID) error     { return nil }
func (*layoutProbeRuntime) Reap(context.Context, string) (int, error)     { return 0, nil }
func (*layoutProbeRuntime) Describe(context.Context) (RuntimeInfo, error) { return RuntimeInfo{}, nil }

func TestTamperedEmbeddedLayoutIsRejectedBeforeRuntimeLoad(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	root := t.TempDir()
	layout := filepath.Join(root, layoutFileName(digest))
	f, err := os.Create(layout)
	must.OK(t, err)
	tw := tar.NewWriter(f)
	index := []byte(`{"schemaVersion":2,"manifests":[{"digest":"` + digest + `"}]}`)
	for name, body := range map[string][]byte{"index.json": index, "blobs/sha256/" + strings.Repeat("a", 64): []byte("tampered bytes")} {
		must.OK(t, tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}))
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	must.OK(t, tw.Close())
	must.OK(t, f.Close())
	runtime := &layoutProbeRuntime{}
	d := &OCIDispatcher{Runtime: runtime, LayoutRoot: root}
	_, failure := d.resolveImage(context.Background(), &orkstackv1.ExecuteTaskInput{TaskId: "build", Image: &orkstackv1.ImageRef{Pinned: "registry/image@" + digest, IndexDigest: digest}})
	if failure == nil || failure.Reason != ReasonImageDigestMismatch {
		t.Fatalf("failure = %#v, want IMAGE_DIGEST_MISMATCH", failure)
	}
	if runtime.loads != 0 {
		t.Fatalf("LoadLayout called %d times; tampered bytes must be rejected first", runtime.loads)
	}
}
