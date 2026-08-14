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

func init() {
	mustRegister(BackendLocal, func(cfg Config) (Store, error) {
		dir := cfg.LocalDir
		if dir == "" {
			dir = DefaultLocalDir
		}
		return NewLocalStore(dir)
	})
	mustRegister(BackendS3, func(cfg Config) (Store, error) {
		return NewS3Store(context.Background(), cfg)
	})
}

// mustRegister is only used for this package's own builtin backends, where a
// duplicate would be an unrecoverable init-time programming error.
func mustRegister(name string, f Factory) {
	if err := Register(name, f); err != nil {
		panic(err)
	}
}
