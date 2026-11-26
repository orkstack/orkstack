package orker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// serviceWorkspace is one service container's files on its host: all of it in
// memory, the Orker's own, and gone with the container.
type serviceWorkspace struct {
	root  string
	input string
	ask   string
	// secrets are the values its container was given, for its log's mask.
	secrets []secretValue
}

func (w *serviceWorkspace) mounts() []Mount {
	return []Mount{{Source: w.input, Destination: "/orkstack/input", ReadOnly: true}, {Source: w.ask, Destination: servicehost.PathAsk, ReadOnly: true}}
}

func (w *serviceWorkspace) remove() error {
	if w == nil {
		return nil
	}
	return removeSealedTree(w.root)
}

// newServiceWorkspace makes the tree of one start of an instance, with its
// context, its configuration and its secrets, sealed. Nothing of it is left when
// it fails.
func (a *Activities) newServiceWorkspace(name string, serviceContext servicehost.Context, spec *eventsources.ServiceSpec) (_ *serviceWorkspace, failure *instanceFailure) {
	d := a.dispatcher
	if !singlePathElement(name) {
		return nil, instanceFail(eventsources.ReasonServiceHostStalled, "a start cannot be named %q", sourcehost.SafeWord(name, 64))
	}
	storage := a.services.storageDir()
	if storage == "" {
		return nil, instanceFail(eventsources.ReasonServiceHostUnfit, "%s", unfitNoStorage)
	}
	d.identityDirMu.Lock()
	own, made, err := ensureOwnIdentityDir(storage, d.OrkerID, false)
	d.identityDirMu.Unlock()
	if err != nil {
		return nil, instanceFail(eventsources.ReasonServiceHostStalled, "this Orker's own directory is not usable: %s", errorKind(err))
	}
	if made {
		d.logger().Warn("this Orker's own directory of identity and service files was gone and was made again; something on the host cleared it", "dir", own)
	}
	w := &serviceWorkspace{root: filepath.Join(own, name)}
	w.input, w.ask = filepath.Join(w.root, "input"), filepath.Join(w.root, "ask")
	defer func() {
		if failure != nil {
			if err := w.remove(); err != nil {
				d.logger().Warn("a service's workspace could not be removed after its setup failed", "path", w.root, "error", err)
			}
		}
	}()
	stalled := func(what string, err error) *instanceFailure {
		return instanceFail(eventsources.ReasonServiceHostStalled, "%s: %s", what, errorKind(err))
	}
	if err := os.Mkdir(w.root, 0o700); err != nil {
		failure = stalled("its workspace could not be made", err)
		// What is there is not this start's.
		w = nil
		return nil, failure
	}
	for _, dir := range []string{w.input, filepath.Join(w.input, "secrets"), w.ask} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			return nil, stalled("its workspace could not be made", err)
		}
	}
	raw, err := stablejson.Marshal(serviceContext)
	if err != nil {
		return nil, stalled("its context could not be encoded", err)
	}
	config := []byte(spec.With)
	if len(config) == 0 {
		config = []byte("{}")
	}
	for name, body := range map[string][]byte{"service.json": raw, "config.json": config} {
		if err := writeIdentityFile(filepath.Join(w.input, name), body); err != nil {
			return nil, stalled("its "+name+" could not be written", err)
		}
	}
	for _, s := range spec.Secrets {
		if f := d.checkInstanceSecret(s); f != nil {
			return nil, f
		}
		value, f := d.resolveSecret(&orkstackv1.SecretRef{Name: s.Name, Source: s.Source})
		if f != nil {
			return nil, secretFailure(f)
		}
		if err := writeIdentityFile(filepath.Join(w.input, "secrets", s.Name), value); err != nil {
			return nil, stalled("its secrets could not be written", err)
		}
		w.secrets = append(w.secrets, secretValue{name: s.Name, value: value})
	}
	for _, dir := range []string{filepath.Join(w.input, "secrets"), w.input} {
		if err := os.Chmod(dir, 0o555); err != nil {
			return nil, stalled("its workspace could not be sealed", err)
		}
	}
	return w, nil
}

// singlePathElement reports a name that joins to a path inside a directory, and
// names no directory of its own.
func singlePathElement(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\x00")
}

// secretFailure is a secret's failure as an instance's: one this Orker may not
// read is the pool's to say, and the same on every host; one that does not resolve
// is a variable or a file this host lacks.
func secretFailure(f *Failure) *instanceFailure {
	switch f.Reason {
	case ReasonSecretSchemeForbid:
		return &instanceFailure{Reason: eventsources.ReasonSecretNotPermitted, Cause: f.Reason, Detail: f.Message}
	case ReasonSecretNotPermitted:
		return &instanceFailure{Reason: eventsources.ReasonSecretNotPermitted, Detail: f.Message}
	}
	return &instanceFailure{Reason: eventsources.ReasonSecretUnresolved, Detail: f.Message}
}

// instanceRun names one start of one instance's container, different for every
// start of any.
func instanceRun(epoch int64, attempt int32) string { return fmt.Sprintf("%d-%d", epoch, attempt) }

// instanceName names one start's files, network and labels on its host.
func instanceName(sourceID string, index int32, epoch int64, attempt int32) string {
	return fmt.Sprintf("%s-%d-%d-%d", sourceID, index, epoch, attempt)
}
