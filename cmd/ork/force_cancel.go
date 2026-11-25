package main

import (
	"context"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
)

// forceCancelKeys is what this engine signs and verifies force cancels with:
// its issuer, or, where it holds no key, what an engine without keys may say.
func forceCancelKeys(issuer *identity.Issuer, published engine.PublishedKeys, mode func() string) engine.ForceCancelKeys {
	if issuer != nil {
		return issuer
	}
	return engine.NewKeylessEngine(published, mode)
}

// deployedMode is the authentication mode the deployment runs: not known until
// the mode record is read, beneath which no engine starts; from then on the
// higher of this engine's and the record's.
type deployedMode struct {
	known chan struct{}
	mode  func() string // set before known is closed
}

func newDeployedMode() *deployedMode { return &deployedMode{known: make(chan struct{})} }

// follow hands it the guard's answer, once the record is read.
func (m *deployedMode) follow(mode func() string) {
	m.mode = mode
	close(m.known)
}

// Mode is the mode now, "" while it is not known.
func (m *deployedMode) Mode() string {
	select {
	case <-m.known:
		return m.mode()
	default:
		return ""
	}
}

// wait reports whether the mode became known before ctx ended.
func (m *deployedMode) wait(ctx context.Context) bool {
	select {
	case <-m.known:
		return true
	case <-ctx.Done():
		return false
	}
}
