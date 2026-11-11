//go:build unix

package dist_test

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// unprivilegedID is the user and group (nobody) a script runs as when the
// test itself is root. The account needs no entry in the passwd file: a
// process takes a numeric identity as it is.
const unprivilegedID = 65534

// dropPrivileges makes cmd run as an unprivileged account that owns the whole
// repository, as the contributor who runs `make reset` does. It is called
// only for root, whom no file mode stops, so a script that forgot to make a
// sealed workspace writable would pass as root and fail as everyone else; as
// the owner it fails here too. The repository's three directories are handed
// over file by file (a link itself, never its target), and the test's own
// temporary parent, which is private to root, is opened to be walked through.
func (r *resetRepo) dropPrivileges(cmd *exec.Cmd) {
	r.t.Helper()
	for _, dir := range []string{r.root, r.bin, r.logs} {
		must.OKf(r.t, os.Chmod(filepath.Dir(dir), 0o711), "open %s to the unprivileged account", filepath.Dir(dir))
		err := filepath.WalkDir(dir, func(path string, _ fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			return os.Lchown(path, unprivilegedID, unprivilegedID)
		})
		must.OKf(r.t, err, "hand %s to the unprivileged account", dir)
	}
	cmd.Dir = r.root
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: unprivilegedID, Gid: unprivilegedID}}
}
