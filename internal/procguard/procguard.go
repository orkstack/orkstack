// Package procguard hardens the ork and orker processes themselves
// (docs/DEPLOY.md, "The processes themselves").
//
// On Linux a process's memory can be read by its own user through a core
// dump, /proc/<pid>/mem or a debugger, and ork holds signing keys while orker
// holds tokens and the masker's forms. So both binaries make themselves
// undumpable first thing, before the configuration file, which may name
// secret files, is read: the kernel then writes no core dump for the
// process, gives its /proc entries to root and refuses a trace by another
// process of the same user. The flag is reset when a program is executed, so
// the container CLI an Orker runs, and the containers behind it, are
// untouched; for that reason the core-size limit, which is inherited, is not
// lowered.
//
// process.allow_debug leaves a process as it was, for an operator who must
// attach a debugger or take a dump, and is logged at warning at every boot
// while set. A prctl that fails is logged at error and the process starts all
// the same: this is hardening, never a reason to be down. Off Linux it does
// nothing.
package procguard

import (
	"fmt"
	"log/slog"
	"strings"
)

const (
	// SettingAllowDebug is the configuration key that leaves a process
	// dumpable and traceable.
	SettingAllowDebug = "process.allow_debug"
	// AllowDebugVar is its environment variable.
	AllowDebugVar = "ORK_PROCESS_ALLOW_DEBUG"
	// DefaultAllowDebug is off: both binaries make themselves undumpable.
	DefaultAllowDebug = false
)

// Harden makes this process undumpable. It is called first in main; its
// error, if any, is handed to Settle, which logs it once a logger exists.
func Harden() error { return setDumpable(false) }

// ParseAllowDebug reads process.allow_debug: empty is the default; true,
// false, 1 and 0 in any case; anything else is refused, naming the setting.
func ParseAllowDebug(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return DefaultAllowDebug, nil
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	}
	return false, fmt.Errorf("%s (%s): %q is not true or false", SettingAllowDebug, AllowDebugVar, raw)
}

// Settle finishes what Harden began, once the configuration is applied and a
// logger exists: it logs a Harden that failed at error, and with
// process.allow_debug set it makes the process dumpable again and says so at
// warning. Only a process.allow_debug it cannot read is an error, which the
// caller refuses boot with.
func Settle(getenv func(string) string, logger *slog.Logger, hardenErr error) error {
	allow, err := ParseAllowDebug(getenv(AllowDebugVar))
	if err != nil {
		return err
	}
	if !supported {
		return nil
	}
	if allow {
		if err := setDumpable(true); err != nil {
			logger.Error("process.allow_debug is set but the process could not be made dumpable again",
				"setting", AllowDebugVar, "error", err)
			return nil
		}
		logger.Warn("process.allow_debug is set: this process can be dumped and traced by its own user; "+
			"unset it once the debugging is done", "setting", AllowDebugVar)
		return nil
	}
	if hardenErr != nil {
		logger.Error("the process could not make itself undumpable; it starts all the same, "+
			"and its memory can be read by its own user", "error", hardenErr)
	}
	return nil
}
