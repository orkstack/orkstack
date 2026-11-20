package orker

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// identityFileName is the DSL's rule for an identity's name (docs/DSL.md,
// rule 55), held again here because the name becomes a directory: the Orker
// never makes one from a name the compiler would have refused.
var identityFileName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// identityFiles is one identity's directory, as a workload sees it under
// /orkstack/identity/<name>/:
//
//	token      -> current/token       the compact JWT, no trailing newline
//	expiration -> current/expiration  the RFC 3339 instant it expires
//	current    -> .v<n>               replaced by one rename(2)
//	.v<n>/                            0555, its files 0444
//
// A refresh writes a whole new set and moves current onto it in one rename,
// so a reader sees the old set or the new one and never a mix; the set
// before is kept until the refresh after, for a reader that opened it just
// before the swap. The directory is 0555 between refreshes and only the
// Orker, its owner, makes it writable for the moment a refresh needs.
type identityFiles struct {
	dir string
	// seq numbers the sets this directory has made, failed ones included,
	// so a set a failed swap left behind never blocks the next; cur is the
	// set current points at, 0 before the first swap.
	seq int
	cur int
	// write writes one file of a new set; a test replaces it to fail one.
	write func(path string, data []byte) error
}

// newIdentityFiles makes parent/name with its fixed links, which point
// through current from the first swap on.
func newIdentityFiles(parent, name string) (*identityFiles, error) {
	if !identityFileName.MatchString(name) {
		return nil, fmt.Errorf("identity name %q is not one the DSL allows (%s)", name, identityFileName)
	}
	dir := filepath.Join(parent, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, fmt.Errorf("make the directory of identity %q: %w", name, err)
	}
	for _, link := range []string{"token", "expiration"} {
		if err := os.Symlink(filepath.Join("current", link), filepath.Join(dir, link)); err != nil {
			return nil, fmt.Errorf("link %s of identity %q: %w", link, name, err)
		}
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		return nil, fmt.Errorf("seal the directory of identity %q: %w", name, err)
	}
	return &identityFiles{dir: dir, write: writeIdentityFile}, nil
}

// writeIdentityFile writes one read-only file of a set.
func writeIdentityFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	cerr := f.Close()
	return errors.Join(werr, cerr)
}

// Swap makes token and expires the set a workload reads. err is a swap that
// did not happen: current stays on the set it was on, whole, and the swap is
// a failed refresh, retried as one. cleanup is what went wrong after current
// moved (an old set not removed, the directory not sealed again): the swap
// stands, and the caller reports it.
func (d *identityFiles) Swap(token []byte, expires time.Time) (cleanup, err error) {
	if len(token) == 0 || bytes.ContainsAny(token, "\r\n") {
		return nil, errors.New("an identity token is one line with no newline")
	}
	if err := os.Chmod(d.dir, 0o755); err != nil {
		return nil, fmt.Errorf("open the identity directory for a refresh: %w", err)
	}
	committed := false
	defer func() {
		if cerr := os.Chmod(d.dir, 0o555); cerr != nil {
			cerr = fmt.Errorf("seal the identity directory after a refresh: %w", cerr)
			if committed {
				cleanup = errors.Join(cleanup, cerr)
			} else {
				err = errors.Join(err, cerr)
			}
		}
	}()
	d.seq++
	next := d.seq
	set := ".v" + strconv.Itoa(next)
	setPath := filepath.Join(d.dir, set)
	if err := d.writeSet(setPath, token, expires); err != nil {
		return nil, errors.Join(err, d.discard(setPath))
	}
	tmp := filepath.Join(d.dir, ".current-"+strconv.Itoa(next))
	if err := os.Symlink(set, tmp); err != nil {
		return nil, errors.Join(fmt.Errorf("link the new set: %w", err), d.discard(setPath))
	}
	if err := os.Rename(tmp, filepath.Join(d.dir, "current")); err != nil {
		_ = os.Remove(tmp)
		return nil, errors.Join(fmt.Errorf("move current onto the new set: %w", err), d.discard(setPath))
	}
	committed = true
	prev := d.cur
	d.cur = next
	// The set before this one is kept for a reader that opened it just
	// before the swap; every other is read by nothing.
	return d.removeSetsExcept(next, prev), nil
}

// discard removes a set a failed swap made, saying so when it cannot.
func (d *identityFiles) discard(setPath string) error {
	if err := removeSealedTree(setPath); err != nil {
		return fmt.Errorf("remove the unused identity set %s: %w", filepath.Base(setPath), err)
	}
	return nil
}

func (d *identityFiles) writeSet(setPath string, token []byte, expires time.Time) error {
	if err := os.Mkdir(setPath, 0o700); err != nil {
		return fmt.Errorf("make a new identity set: %w", err)
	}
	if err := d.write(filepath.Join(setPath, "token"), token); err != nil {
		return fmt.Errorf("write the token: %w", err)
	}
	if err := d.write(filepath.Join(setPath, "expiration"), []byte(expires.UTC().Format(time.RFC3339))); err != nil {
		return fmt.Errorf("write the expiration: %w", err)
	}
	if err := os.Chmod(setPath, 0o555); err != nil {
		return fmt.Errorf("seal a new identity set: %w", err)
	}
	return nil
}

// removeSetsExcept removes every .v<k> but the two named.
func (d *identityFiles) removeSetsExcept(keep ...int) error {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return fmt.Errorf("list the identity directory: %w", err)
	}
	var errs []error
	for _, e := range entries {
		k, ok := strings.CutPrefix(e.Name(), ".v")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(k)
		if err != nil || slices.Contains(keep, n) {
			continue
		}
		if err := removeSealedTree(filepath.Join(d.dir, e.Name())); err != nil {
			errs = append(errs, fmt.Errorf("remove the old identity set %s: %w", e.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Remove removes the identity's directory, made writable first as the
// sealed input tree is.
func (d *identityFiles) Remove() error {
	return removeSealedTree(d.dir)
}
