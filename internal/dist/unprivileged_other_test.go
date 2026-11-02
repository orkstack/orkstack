//go:build !unix

package dist_test

import "os/exec"

// dropPrivileges: root is a Unix identity (os.Geteuid is -1 elsewhere), so
// run never calls this off Unix; it exists so the package builds there.
func (r *resetRepo) dropPrivileges(*exec.Cmd) {
	r.t.Fatal("running as root where no unprivileged account can be dropped to")
}
