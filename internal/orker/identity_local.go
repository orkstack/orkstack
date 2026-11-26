package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/task"
)

// LocalIdentity is a local run's stand-ins for what an attempt's identities
// and credentials would be (`orkctl task run`, `orkctl activity test`): it is
// no attempt Temporal knows, so no engine issues it anything. They reach the
// container as an attempt's do: the same files, the same swap, the same mask.
// The input itself names none, so nothing of it is recorded by an engine.
type LocalIdentity struct {
	// Identities are those the caller supplies, with Mint to answer for them.
	Identities []*orkstackv1.IdentityRef
	Mint       func(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error)
	// Credentials are those the caller supplies, each a directory of files.
	Credentials []LocalCredential
}

// LocalCredential is one supplied credential: the regular files of Dir.
type LocalCredential struct{ Name, Dir string }

// LocalCredentialProvider is the provider task.json names for a supplied
// credential, which no provider issued.
const LocalCredentialProvider = "local"

// maxLocalCredentialFile bounds one file of a supplied credential.
const maxLocalCredentialFile = 1 << 20

// UseLocalIdentity makes d a local run's dispatcher, with these stand-ins.
func (d *OCIDispatcher) UseLocalIdentity(local LocalIdentity) {
	config := DefaultIdentityConfig()
	d.localIdentity, d.identityMint, d.identityConfig = &local, local.Mint, &config
}

// identityInput is in as its identities are set up from: itself, or a local
// run's with the ones its caller supplied.
func (d *OCIDispatcher) identityInput(in *orkstackv1.ExecuteTaskInput) *orkstackv1.ExecuteTaskInput {
	if d.localIdentity == nil || len(d.localIdentity.Identities) == 0 || in.GetIdentity() != nil {
		return in
	}
	out := proto.Clone(in).(*orkstackv1.ExecuteTaskInput)
	out.Identity = &orkstackv1.IdentityContext{DigestVersion: task.IdentitySpecVersion, Identities: d.localIdentity.Identities}
	return out
}

// localCredentials is a local run's credentials directory, nil with none
// supplied.
type localCredentials struct {
	dir  string
	refs []taskJSONCredential
}

// setupLocalCredentials writes the supplied credentials under root, each file
// masked before it is written. A failure leaves nothing.
func (d *OCIDispatcher) setupLocalCredentials(in *orkstackv1.ExecuteTaskInput, root string, mask *attemptMask) (_ *localCredentials, failure *Failure) {
	if d.localIdentity == nil || len(d.localIdentity.Credentials) == 0 || in.GetIdentity() != nil {
		return nil, nil
	}
	local := &localCredentials{dir: filepath.Join(root, "credentials")}
	defer func() {
		if failure != nil {
			_ = local.remove()
		}
	}()
	for _, c := range d.localIdentity.Credentials {
		files, err := ReadLocalCredential(c.Dir, d.Policy)
		if err != nil {
			return nil, FailCause(ReasonWorkdirFailed, err, "task %q: credential %q could not be read: %v", in.GetTaskId(), c.Name, err)
		}
		names := make([]string, 0, len(files))
		for name, content := range files {
			if err := maskLocalCredential(mask, c.Name, name, content); err != nil {
				return nil, FailCause(ReasonMaskCapacityExceeded, err, "task %q: credential %q cannot be masked in the task's logs: %s", in.GetTaskId(), c.Name, errorKind(err))
			}
			names = append(names, name)
		}
		slices.Sort(names)
		dir, err := provider.NewDir(local.dir, c.Name)
		if err == nil {
			err = dir.Put(provider.Bundle{Files: files}, time.Time{})
		}
		if err != nil {
			return nil, FailCause(ReasonWorkdirFailed, err, "task %q: credential %q could not be written: %s", in.GetTaskId(), c.Name, errorKind(err))
		}
		local.refs = append(local.refs, taskJSONCredential{Name: c.Name, Provider: LocalCredentialProvider,
			Path: credentialsMountPath + "/" + c.Name, Files: names})
	}
	if err := os.Chmod(local.dir, 0o555); err != nil {
		return nil, FailCause(ReasonWorkdirFailed, err, "task %q: the credentials directory could not be sealed: %s", in.GetTaskId(), errorKind(err))
	}
	return local, nil
}

// remove removes the credentials.
func (l *localCredentials) remove() error {
	if err := removeSealedTree(l.dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// maskLocalCredential masks a supplied file whole, which masks its lines, and
// each value it holds as JSON or INI. No provider said which are secret, so
// none of them is refused in values.json.
func maskLocalCredential(mask *attemptMask, credential, file string, content []byte) error {
	values, _ := provider.FileValues(content)
	for i, value := range append([][]byte{bytes.TrimSpace(content)}, values...) {
		if mask == nil || len(value) == 0 {
			continue
		}
		if _, err := mask.maskValue(fmt.Sprintf("credential:%s:%s:%d", credential, file, i), value); err != nil {
			return err
		}
	}
	return nil
}

// ReadLocalCredential reads a supplied credential: the regular files of dir,
// by name, within what a credential's bundle may hold. A file policy refuses
// as a secret's source is refused here too, whatever its directory is.
func ReadLocalCredential(dir string, policy SecurityPolicy) (map[string][]byte, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(entries) > provider.MaxBundleFiles {
		return nil, fmt.Errorf("it holds more than %d files", provider.MaxBundleFiles)
	}
	files := map[string][]byte{}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			return nil, fmt.Errorf("%s is not a regular file", e.Name())
		}
		if err := policy.AllowsSecretSource("file:" + filepath.Join(dir, e.Name())); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		// Not to wait on what replaced it since: what is read is what is
		// checked to be a regular file.
		f, err := root.OpenFile(e.Name(), os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, err
		}
		raw, err := readRegular(f, maxLocalCredentialFile+1)
		if err != nil {
			return nil, err
		}
		if len(raw) > maxLocalCredentialFile {
			return nil, fmt.Errorf("%s is over %d bytes", e.Name(), maxLocalCredentialFile)
		}
		files[e.Name()] = raw
	}
	if len(files) == 0 {
		return nil, errors.New("it holds no file")
	}
	if err := provider.ValidBundle(provider.Bundle{Files: files}); err != nil {
		return nil, err
	}
	return files, nil
}

// readRegular reads at most limit bytes of f, which it closes, if f is a
// regular file.
func readRegular(f *os.File, limit int64) ([]byte, error) {
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	return io.ReadAll(io.LimitReader(f, limit))
}

// ReadLocalToken reads the one-line file a local run's identity is supplied
// from, at most limit bytes of it, under the floor a secret's file source is
// held to: each time, since the path may lead elsewhere by then.
func ReadLocalToken(path string, limit int64, policy SecurityPolicy) ([]byte, error) {
	if err := policy.AllowsSecretSource("file:" + path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	return readRegular(f, limit)
}
