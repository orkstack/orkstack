package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// materializedDir is the workdir subdirectory holding upstream artifacts the
// `artifact "<task>/<name>"` helper downloads. Plugins see the files by the
// absolute path the helper renders.
const materializedDir = ".artifacts"

// upstreamResolver materializes upstream artifacts into the task workdir. It
// caches by reference so repeated references download once and always render
// the same path.
type upstreamResolver struct {
	store    artifacts.Store
	upstream map[string]task.UpstreamResult
	root     string

	mu    sync.Mutex
	cache map[string]string
}

func newUpstreamResolver(
	store artifacts.Store,
	upstream map[string]task.UpstreamResult,
	workDir string,
) *upstreamResolver {
	return &upstreamResolver{
		store:    store,
		upstream: upstream,
		root:     filepath.Join(workDir, materializedDir),
		cache:    map[string]string{},
	}
}

// Resolve downloads the referenced artifact and returns its local path.
func (r *upstreamResolver) Resolve(ctx context.Context, ref string) (string, error) {
	producer, name, found := strings.Cut(ref, "/")
	if !found || producer == "" || name == "" {
		return "", plugin.Permanent(fmt.Errorf(
			"artifact reference %q must be \"<task>/<name>\"", ref))
	}

	upstream, ok := r.upstream[producer]
	if !ok {
		return "", plugin.Permanent(fmt.Errorf(
			"artifact %q: task %q is not an upstream of this task (upstream tasks: %s)",
			ref, producer, joinOrNone(upstreamIDs(r.upstream))))
	}

	var target *plugin.ArtifactRef
	names := make([]string, 0, len(upstream.Artifacts))
	for i := range upstream.Artifacts {
		names = append(names, upstream.Artifacts[i].Name)
		if upstream.Artifacts[i].Name == name {
			target = &upstream.Artifacts[i]
			break
		}
	}
	if target == nil {
		return "", plugin.Permanent(fmt.Errorf(
			"artifact %q: task %q produced no artifact named %q (produced: %s)",
			ref, producer, name, joinOrNone(names)))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if path, cached := r.cache[ref]; cached {
		return path, nil
	}
	if r.store == nil {
		return "", plugin.Permanent(fmt.Errorf("artifact %q: no artifact store is configured", ref))
	}

	path, err := r.download(ctx, producer, name, target.URI)
	if err != nil {
		return "", err
	}
	r.cache[ref] = path
	return path, nil
}

func (r *upstreamResolver) download(ctx context.Context, producer, name, uri string) (string, error) {
	dir := filepath.Join(r.root, safeComponent(producer))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("materialize %s/%s: %w", producer, name, err)
	}
	path := filepath.Join(dir, safeComponent(name))

	reader, err := r.store.Get(ctx, uri)
	if err != nil {
		return "", fmt.Errorf("materialize %s/%s from %s: %w", producer, name, uri, err)
	}
	defer func() { _ = reader.Close() }()

	file, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("materialize %s/%s: %w", producer, name, err)
	}
	if _, err := io.Copy(file, reader); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("materialize %s/%s from %s: %w", producer, name, uri, err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("materialize %s/%s: %w", producer, name, err)
	}
	return path, nil
}

// safeComponent turns an opaque logical artifact name into a single safe path
// component. Logical names may contain separators, dot segments, or invalid
// UTF-8, so any rewritten name carries a digest suffix to stay unique.
func safeComponent(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.' || r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	cleaned := b.String()
	if len(cleaned) > 64 {
		cleaned = cleaned[:64]
	}
	if cleaned == name && cleaned != "." && cleaned != ".." && cleaned != "" {
		return cleaned
	}
	sum := sha256.Sum256([]byte(name))
	if cleaned == "" || cleaned == "." || cleaned == ".." {
		cleaned = "artifact"
	}
	return cleaned + "-" + hex.EncodeToString(sum[:])[:8]
}

func upstreamIDs(upstream map[string]task.UpstreamResult) []string {
	ids := make([]string, 0, len(upstream))
	for id := range upstream {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func joinOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}
