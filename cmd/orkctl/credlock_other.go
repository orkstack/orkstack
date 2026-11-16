//go:build !unix

package main

import "os"

// lockFile is a no-op where advisory locks are not available: concurrent
// orkctl processes there may refresh one login twice, and the second must
// log in again.
func lockFile(*os.File) error { return nil }

func unlockFile(*os.File) error { return nil }
