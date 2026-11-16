package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// maxKeyFileBytes bounds one signing key file: a PEM RSA key of 8192 bits
// is under 7 KiB.
const maxKeyFileBytes = 64 << 10

// identityConfig is the identity.* settings (docs/TASK_IDENTITY.md).
type identityConfig struct {
	issuer          string
	serveDiscovery  bool
	keyFiles        []string
	algorithm       jwt.Algorithm
	activationDelay time.Duration
	verifyPublished bool
	subjectFormat   identity.Format
	trustDomain     string
	// copyFile and copyMaxAge are identity.keys_copy_file and
	// identity.keys_copy_max_age: the published keys kept on disk, and how
	// old a copy may be and still decide.
	copyFile   string
	copyMaxAge time.Duration
}

// The bounds of identity.keys_copy_max_age.
const (
	minKeysCopyMaxAge = 5 * time.Minute
	maxKeysCopyMaxAge = 24 * time.Hour
)

// enabled says task identity is on: identity.issuer is set.
func (c identityConfig) enabled() bool { return c.issuer != "" }

// hasKeys says the engine holds signing keys: with no issuer they sign no
// token and nothing is served, and still sign what engines hand each other.
func (c identityConfig) hasKeys() bool { return len(c.keyFiles) > 0 }

// identityFromEnv reads the identity.* settings. Every error refuses the
// boot, naming its setting.
func identityFromEnv(lookup func(string) (string, bool)) (identityConfig, error) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}
	c := identityConfig{
		serveDiscovery:  identity.DefaultServeDiscovery,
		algorithm:       identity.DefaultAlgorithm,
		activationDelay: identity.DefaultActivationDelay,
		verifyPublished: identity.DefaultVerifyPublished,
		subjectFormat:   identity.DefaultSubjectFormat,
		trustDomain:     get("ORK_IDENTITY_SPIFFE_TRUST_DOMAIN"),
		copyFile:        get("ORK_IDENTITY_KEYS_COPY_FILE"),
		copyMaxAge:      identity.DefaultKeysCopyMaxAge,
	}
	var err error
	if raw := get("ORK_IDENTITY_ISSUER"); raw != "" {
		if c.issuer, err = identity.CheckIssuerURL(raw); err != nil {
			return c, fmt.Errorf("identity.issuer (ORK_IDENTITY_ISSUER): %w", err)
		}
	}
	for _, b := range []struct {
		env, setting string
		into         *bool
	}{
		{"ORK_IDENTITY_SERVE_DISCOVERY", "identity.serve_discovery", &c.serveDiscovery},
		{"ORK_IDENTITY_SIGNING_VERIFY_PUBLISHED", "identity.signing.verify_published", &c.verifyPublished},
	} {
		if raw := get(b.env); raw != "" {
			if *b.into, err = strconv.ParseBool(raw); err != nil {
				return c, fmt.Errorf("%s (%s) %q is not true or false", b.setting, b.env, raw)
			}
		}
	}
	for _, f := range strings.Split(get("ORK_IDENTITY_SIGNING_KEY_FILES"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			c.keyFiles = append(c.keyFiles, f)
		}
	}
	if raw := get("ORK_IDENTITY_SIGNING_ALGORITHM"); raw != "" {
		if c.algorithm, err = jwt.ParseAlgorithm(raw); err != nil {
			return c, fmt.Errorf("identity.signing.algorithm (ORK_IDENTITY_SIGNING_ALGORITHM): %w", err)
		}
	}
	if raw := get("ORK_IDENTITY_SIGNING_ACTIVATION_DELAY"); raw != "" {
		if c.activationDelay, err = time.ParseDuration(raw); err != nil || c.activationDelay < 0 {
			return c, fmt.Errorf("identity.signing.activation_delay (ORK_IDENTITY_SIGNING_ACTIVATION_DELAY) %q is not a duration of zero or more", raw)
		}
	}
	if c.copyFile != "" && !filepath.IsAbs(c.copyFile) {
		return c, fmt.Errorf("identity.keys_copy_file (ORK_IDENTITY_KEYS_COPY_FILE) %q is not an absolute path", c.copyFile)
	}
	if raw := get("ORK_IDENTITY_KEYS_COPY_MAX_AGE"); raw != "" {
		if c.copyMaxAge, err = time.ParseDuration(raw); err != nil || c.copyMaxAge < minKeysCopyMaxAge || c.copyMaxAge > maxKeysCopyMaxAge {
			return c, fmt.Errorf("identity.keys_copy_max_age (ORK_IDENTITY_KEYS_COPY_MAX_AGE) %q is not a duration from %s to %s", raw, minKeysCopyMaxAge, maxKeysCopyMaxAge)
		}
	}
	if c.subjectFormat, err = identity.ParseFormat(get("ORK_IDENTITY_SUBJECT_FORMAT")); err != nil {
		return c, fmt.Errorf("identity.subject_format (ORK_IDENTITY_SUBJECT_FORMAT): %w", err)
	}
	if c.trustDomain != "" {
		if err := identity.ValidTrustDomain(c.trustDomain); err != nil {
			return c, fmt.Errorf("identity.spiffe.trust_domain (ORK_IDENTITY_SPIFFE_TRUST_DOMAIN): %w", err)
		}
	}
	if !c.enabled() {
		return c, nil
	}
	if c.subjectFormat == identity.FormatSPIFFE && c.trustDomain == "" {
		return c, errors.New("identity.subject_format (ORK_IDENTITY_SUBJECT_FORMAT) is spiffe, and identity.spiffe.trust_domain (ORK_IDENTITY_SPIFFE_TRUST_DOMAIN) is not set")
	}
	if len(c.keyFiles) == 0 {
		return c, errors.New("identity.issuer (ORK_IDENTITY_ISSUER) is set, and identity.signing.key_files (ORK_IDENTITY_SIGNING_KEY_FILES) lists no key")
	}
	return c, nil
}

// checkBoot refuses an issuer the deployment's store cannot carry: on the
// local backend no other engine reads the keys a replica published. It
// refuses an engine whose auth.mode is observe or enforce with no signing
// key, issuer or not: in those modes a record can say "verified", and one
// that nothing can check afterwards is worth less than "asserted".
func (c identityConfig) checkBoot(backend, mode string) error {
	if c.enabled() && backend == artifacts.BackendLocal {
		return errors.New("identity.issuer (ORK_IDENTITY_ISSUER) needs shared object storage: on the local backend no other engine reads the keys one publishes")
	}
	if !c.hasKeys() && (mode == authn.ModeObserve || mode == authn.ModeEnforce) {
		return fmt.Errorf("auth.mode (ORK_AUTH_MODE) is %s, and identity.signing.key_files (ORK_IDENTITY_SIGNING_KEY_FILES) lists no key: in observe and enforce an engine signs what it hands other engines, also with identity.issuer unset (docs/TASK_IDENTITY.md)", mode)
	}
	return nil
}

// checkCopyFile proves the engine can keep its copy of the published keys:
// it writes and removes a file beside it, and refuses a copy already there
// that is not a regular file only its owner can write, which the engine
// would never load.
func (c identityConfig) checkCopyFile() error {
	if c.copyFile == "" {
		return nil
	}
	if info, err := os.Lstat(c.copyFile); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("identity.keys_copy_file (ORK_IDENTITY_KEYS_COPY_FILE) %s is not a regular file only its owner can write (%s); remove it, and the engine writes it 0600", c.copyFile, info.Mode())
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("identity.keys_copy_file (ORK_IDENTITY_KEYS_COPY_FILE): %w", err)
	}
	probe, err := os.CreateTemp(filepath.Dir(c.copyFile), ".ork-keys-copy-probe-*")
	if err != nil {
		return fmt.Errorf("identity.keys_copy_file (ORK_IDENTITY_KEYS_COPY_FILE): the engine cannot write beside %s: %w", c.copyFile, err)
	}
	name := probe.Name()
	cerr := probe.Close()
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("identity.keys_copy_file (ORK_IDENTITY_KEYS_COPY_FILE): remove the probe %s: %w", name, err)
	}
	if cerr != nil {
		return fmt.Errorf("identity.keys_copy_file (ORK_IDENTITY_KEYS_COPY_FILE): the engine cannot write beside %s: %w", c.copyFile, cerr)
	}
	return nil
}

// loadKeys reads identity.signing.key_files in order.
func (c identityConfig) loadKeys() ([]*jwt.Signer, error) {
	keys := make([]*jwt.Signer, 0, len(c.keyFiles))
	for _, path := range c.keyFiles {
		raw, err := readKeyFile(path)
		if err != nil {
			return nil, fmt.Errorf("identity.signing.key_files: %s: %w", path, err)
		}
		k, err := jwt.ParsePrivateKeyPEM(raw)
		if err != nil {
			return nil, fmt.Errorf("identity.signing.key_files: %s: %w", path, err)
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// readKeyFile reads one key file: a regular file, which a FIFO or a device
// is not, so a mistyped path cannot hang the boot.
func readKeyFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file (%s)", info.Mode().Type())
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxKeyFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxKeyFileBytes {
		return nil, fmt.Errorf("more than %d bytes; a key file holds one PEM key", maxKeyFileBytes)
	}
	return raw, nil
}

// newIssuer loads the keys and builds the issuer; it reads nothing yet.
func (c identityConfig) newIssuer(store identity.KeyStore, engineName string, clockSkew time.Duration, observer identity.CopyObserver, logger *slog.Logger) (*identity.Issuer, error) {
	keys, err := c.loadKeys()
	if err != nil {
		return nil, err
	}
	for _, path := range c.keyFiles {
		if info, err := os.Stat(path); err == nil && info.Mode().Perm()&0o077 != 0 && logger != nil {
			logger.Warn("identity: a signing key file can be read by users other than its owner; make it 0600", "setting", "identity.signing.key_files", "path", path, "mode", info.Mode().Perm().String())
		}
	}
	return identity.New(identity.Config{
		URL: c.issuer, Keys: keys, Algorithm: c.algorithm, ActivationDelay: c.activationDelay,
		VerifyPublished: c.verifyPublished, SubjectFormat: c.subjectFormat, TrustDomain: c.trustDomain,
		Engine: engineName, ClockSkew: clockSkew, CopyFile: c.copyFile, CopyMaxAge: c.copyMaxAge, Observer: observer,
	}, store, identity.NewHTTPLiveIssuer(c.issuer), logger)
}

// setupIssuer builds the issuer, publishes this replica's keys, and keeps
// the document read each minute until ctx ends. A store that does not
// answer at boot is not fatal: the issuer signs nothing until it has read
// the document, or loaded its copy on disk, and its loop publishes once the
// store answers. Nil when the engine holds no signing key; with keys and no
// issuer it signs no token and serves nothing.
func setupIssuer(ctx context.Context, c identityConfig, storeCfg artifacts.Config, engineName string, clockSkew time.Duration, observer identity.CopyObserver, logger *slog.Logger) (*identity.Issuer, error) {
	if !c.hasKeys() {
		return nil, nil
	}
	store, err := artifacts.NewPublishedKeysStore(ctx, storeCfg)
	if err != nil {
		return nil, fmt.Errorf("open the published signing keys: %w", err)
	}
	issuer, err := c.newIssuer(store, engineName, clockSkew, observer, logger)
	if err != nil {
		return nil, err
	}
	if err := issuer.Publish(ctx); err != nil {
		// A document this release cannot read, or a change its rules
		// refuse, never clears by waiting: the engine does not start as if
		// it could sign. A store that does not answer is retried.
		if errors.Is(err, artifacts.ErrPublishedKeysUnreadable) || errors.Is(err, artifacts.ErrPublishedKeysRefused) {
			return nil, fmt.Errorf("identity: %w", err)
		}
		logger.Error("identity: the signing keys could not be published at boot; retrying each minute", "err", err)
	}
	go issuer.Run(ctx)
	logger.Info("identity signing keys", "issuer", c.issuer, "keys", len(c.keyFiles), "algorithm", c.algorithm,
		"subject_format", c.subjectFormat, "trust_domain", c.trustDomain, "serve_discovery", c.serveDiscovery,
		"keys_copy_file", c.copyFile, "keys_copy_max_age", c.copyMaxAge)
	return issuer, nil
}
