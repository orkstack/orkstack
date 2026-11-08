package orker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	// attemptOwnerBytes is how much of an identity's SHA-256 an attempt
	// directory's name carries: sixteen hex digits, short enough to keep a name
	// readable and far too long for two identities on one work root to meet by
	// chance.
	attemptOwnerBytes = 8
	// maxAttemptDigits is the longest attempt number there is: an int32.
	maxAttemptDigits = 10
)

// attemptOwner is the segment of an attempt directory's name that says which
// Orker identity made it. An identity is up to 200 bytes of anything but
// whitespace and control characters, so it cannot be a path segment itself.
//
// It is in the name rather than in a file inside the directory because a name
// comes into being with the directory: there is no moment, however the process
// dies, when an attempt's directory exists and does not say whose it is.
func attemptOwner(orkerID string) string {
	sum := sha256.Sum256([]byte(orkerID))
	return hex.EncodeToString(sum[:attemptOwnerBytes])
}

// attemptDirOwner reads a work root entry's name. attempt says the name has the
// shape of an attempt's directory, ork-<run>-<task>-<attempt>-<owner>-<random>;
// owner is that owner segment, or empty for the shape a release before this one
// made, ork-<run>-<task>-<attempt>-<random>, which says nothing about whose it
// is. A run id or a task id may hold dashes and may spell a segment of this
// shape, so the name is read only from its end, which the Orker alone writes.
func attemptDirOwner(name string) (owner string, attempt bool) {
	if !strings.HasPrefix(name, "ork-") || strings.HasPrefix(name, pinDirPrefix) {
		return "", false
	}
	segments := strings.Split(name, "-")
	if len(segments) < 5 || !allDigits(segments[len(segments)-1]) {
		return "", false
	}
	before := segments[len(segments)-2]
	switch {
	case isAttemptOwner(before) && allDigits(segments[len(segments)-3]):
		return before, true
	case allDigits(before) && len(before) <= maxAttemptDigits:
		return "", true
	}
	return "", false
}

// isAttemptOwner holds a segment to what attemptOwner writes.
func isAttemptOwner(segment string) bool {
	if len(segment) != 2*attemptOwnerBytes {
		return false
	}
	for _, r := range segment {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// AttemptReapReport is what ReapAttemptDirectories found under a work root.
type AttemptReapReport struct {
	// Removed lists the directories it removed whole.
	Removed []string
	// Others lists the attempt directories it left because nothing says they
	// are this identity's: another identity's, which may be a live Orker's, and
	// those of a release that did not name their owner.
	Others []string
	// SecretsLeft lists the directories whose secrets it could not remove.
	SecretsLeft []string
}

// ReapAttemptDirectories removes every attempt directory under the work root
// that orkerID's previous life left, and leaves everything else. A process that
// is killed takes no exit path, so its attempts' directories stay, with the
// secret values staged in them in plain files on the host.
//
// It is safe only after this process has claimed the identity (claimIdentity):
// the lock is what says no live process answers to it, so an attempt directory
// of this identity found at boot is dead. The lock holds among processes that
// share a temp directory, and an explicit identity shared by two that do not is
// the misconfiguration docs/DEPLOY.md warns of, which Reap's removal of containers
// has always rested on too. Another identity's directory proves
// nothing about that identity's process, which is why it is never touched, the
// same rule Reap keeps for containers. A pin's directory is not an attempt's and
// ends by its own lease, release and reap (§6.4).
//
// It tries every directory and joins what it could not remove. What the Orker
// can remove goes even where something beside it cannot: a workload running as
// another uid can leave a directory the Orker cannot empty, which must not keep
// the secrets, so they go first.
func ReapAttemptDirectories(workRoot, orkerID string) (AttemptReapReport, error) {
	var report AttemptReapReport
	if orkerID == "" {
		return report, nil
	}
	root := workRoot
	if root == "" {
		root = os.TempDir()
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return report, nil
		}
		return report, fmt.Errorf("list the workspace root %s: %w", root, err)
	}
	mine := attemptOwner(orkerID)
	var failures []error
	for _, entry := range entries {
		// A link is not a directory here, and nothing behind one is reached.
		if !entry.IsDir() {
			continue
		}
		owner, attempt := attemptDirOwner(entry.Name())
		if !attempt {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		if owner != mine {
			report.Others = append(report.Others, dir)
			continue
		}
		if err := removeAttemptDirectory(dir, &report); err != nil {
			failures = append(failures, err)
			continue
		}
		report.Removed = append(report.Removed, dir)
	}
	return report, errors.Join(failures...)
}

// removeAttemptDirectory removes one dead attempt's directory, its secrets
// first and on their own, so that no failure elsewhere in the tree can stand
// between them and their removal and none is hidden behind another: os.RemoveAll
// reports only the first thing it could not remove.
func removeAttemptDirectory(dir string, report *AttemptReapReport) error {
	secretsErr := removeSealedTree(filepath.Join(dir, attemptSecretsDirName))
	if secretsErr != nil {
		report.SecretsLeft = append(report.SecretsLeft, dir)
		secretsErr = fmt.Errorf("remove the secrets of attempt directory %s: %w", dir, secretsErr)
	}
	if err := removeSealedTree(dir); err != nil {
		return errors.Join(secretsErr, fmt.Errorf("remove attempt directory %s: %w", dir, err))
	}
	return secretsErr
}
