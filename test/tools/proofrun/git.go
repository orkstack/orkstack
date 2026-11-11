package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

// gitOut runs git and returns stdout.
func gitOut(args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// changedPath is one path of the diff.
type changedPath struct {
	Path   string
	Status string // A, M, D, ?? (untracked)
}

// diffPaths lists every path that differs between base and the working tree,
// staged and untracked (not ignored) files included.
func diffPaths(base string) ([]changedPath, error) {
	out, err := gitOut("diff", "--no-renames", "--name-status", "-z", base, "--")
	if err != nil {
		return nil, err
	}
	var paths []changedPath
	f := strings.Split(out, "\x00")
	for i := 0; i+1 < len(f); i += 2 {
		if f[i] == "" {
			break
		}
		paths = append(paths, changedPath{Path: f[i+1], Status: f[i][:1]})
	}
	untracked, err := gitOut("ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	for _, p := range strings.Split(untracked, "\x00") {
		if p != "" {
			paths = append(paths, changedPath{Path: p, Status: "??"})
		}
	}
	sort.Slice(paths, func(i, j int) bool { return paths[i].Path < paths[j].Path })
	return paths, nil
}

// gitShow returns a file's content at rev, or nil when it did not exist.
func gitShow(rev, p string) []byte {
	cmd := exec.Command("git", "show", rev+":"+p)
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	return out
}
