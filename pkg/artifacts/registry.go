package artifacts

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Factory builds a Store from a Config. A new provider registers one of these.
type Factory func(cfg Config) (Store, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register adds a backend factory under name. It returns an error (rather than
// panicking) on an empty name, a nil factory, or a duplicate registration, so a
// library caller can recover.
func Register(name string, f Factory) error {
	if name == "" {
		return fmt.Errorf("artifacts: register backend: empty name")
	}
	if f == nil {
		return fmt.Errorf("artifacts: register backend %q: nil factory", name)
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[name]; exists {
		return fmt.Errorf("artifacts: register backend %q: already registered", name)
	}
	registry[name] = f
	return nil
}

// Backends lists the registered backend names, sorted.
func Backends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// New builds the Store named by cfg.Backend. An empty backend means
// DefaultBackend. An unregistered backend returns an error matching
// ErrUnknownBackend.
func New(cfg Config) (Store, error) {
	name := cfg.Backend
	if name == "" {
		name = DefaultBackend
		cfg.Backend = name
	}

	registryMu.RLock()
	f, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: backend %q: %w (registered: %v)", name, ErrUnknownBackend, Backends())
	}

	st, err := f(cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: backend %q: %w", name, err)
	}
	return st, nil
}

// NewFromEnv builds the Store described by the ORK_* environment.
func NewFromEnv() (Store, error) { return New(ConfigFromEnv()) }

// NewCommitStore builds the durable completion-record store selected by cfg.
func NewCommitStore(ctx context.Context, cfg Config) (CommitStore, error) {
	switch name := cfg.Backend; name {
	case "", BackendLocal:
		return NewLocalCommitStore(localDir(cfg))
	case BackendS3:
		return NewS3CommitStore(ctx, cfg)
	default:
		return nil, fmt.Errorf("artifacts: commit store backend %q: %w", name, ErrUnknownBackend)
	}
}

// NewCommitStoreFromEnv builds the completion store from ORK_* configuration.
func NewCommitStoreFromEnv(ctx context.Context) (CommitStore, error) {
	return NewCommitStore(ctx, ConfigFromEnv())
}

// ArchiveFactory builds a RunArchive from a Config. A backend that can persist
// finished runs registers one of these.
type ArchiveFactory func(cfg Config) (RunArchive, error)

var archiveRegistry = map[string]ArchiveFactory{}

// RegisterRunArchive adds a run-archive factory under name, mirroring Register.
func RegisterRunArchive(name string, f ArchiveFactory) error {
	if name == "" {
		return fmt.Errorf("artifacts: register run archive: empty name")
	}
	if f == nil {
		return fmt.Errorf("artifacts: register run archive %q: nil factory", name)
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := archiveRegistry[name]; exists {
		return fmt.Errorf("artifacts: register run archive %q: already registered", name)
	}
	archiveRegistry[name] = f
	return nil
}

// RunArchiveBackends lists the backends that can archive runs, sorted.
func RunArchiveBackends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	names := make([]string, 0, len(archiveRegistry))
	for name := range archiveRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// NewRunArchive builds the RunArchive named by cfg.Backend, so one
// ORK_ARTIFACT_BACKEND selects where artifacts and run history both live.
func NewRunArchive(cfg Config) (RunArchive, error) {
	name := cfg.Backend
	if name == "" {
		name = DefaultBackend
		cfg.Backend = name
	}

	registryMu.RLock()
	f, ok := archiveRegistry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: run archive backend %q: %w (registered: %v)",
			name, ErrUnknownBackend, RunArchiveBackends())
	}

	archive, err := f(cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: run archive backend %q: %w", name, err)
	}
	return archive, nil
}

// NewRunArchiveFromEnv builds the RunArchive described by the ORK_* environment.
func NewRunArchiveFromEnv() (RunArchive, error) { return NewRunArchive(ConfigFromEnv()) }

func init() {
	mustRegister(BackendLocal, func(cfg Config) (Store, error) {
		return NewLocalStore(localDir(cfg))
	})
	mustRegister(BackendS3, func(cfg Config) (Store, error) {
		return NewS3Store(context.Background(), cfg)
	})
	mustRegisterArchive(BackendLocal, func(cfg Config) (RunArchive, error) {
		return NewLocalRunArchive(localDir(cfg))
	})
	mustRegisterArchive(BackendS3, func(cfg Config) (RunArchive, error) {
		return NewS3RunArchive(context.Background(), cfg)
	})
}

func localDir(cfg Config) string {
	if cfg.LocalDir == "" {
		return DefaultLocalDir
	}
	return cfg.LocalDir
}

// mustRegister is only used for this package's own builtin backends, where a
// duplicate would be an unrecoverable init-time programming error.
func mustRegister(name string, f Factory) {
	if err := Register(name, f); err != nil {
		panic(err)
	}
}

func mustRegisterArchive(name string, f ArchiveFactory) {
	if err := RegisterRunArchive(name, f); err != nil {
		panic(err)
	}
}
