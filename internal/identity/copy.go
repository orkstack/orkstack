package identity

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	keysCopyFormat   = 1
	keysCopyMaxBytes = 8 << 20
)

// keysCopy is identity.keys_copy_file's content: the engine's last good
// read of the published keys and its own clock at that read. It holds
// public keys only, and only an engine writes it (authn-authz.md §6.6).
type keysCopy struct {
	V         int                     `json:"v"`
	FetchedAt time.Time               `json:"fetchedAt"`
	Keys      artifacts.PublishedKeys `json:"keys"`
}

// parseKeysCopy reads a copy file's bytes and holds the document in it to
// the store's own rules.
func parseKeysCopy(raw []byte) (keysCopy, error) {
	if len(raw) > keysCopyMaxBytes {
		return keysCopy{}, fmt.Errorf("the keys copy is larger than %d bytes", keysCopyMaxBytes)
	}
	var wire struct {
		V         int            `json:"v"`
		FetchedAt time.Time      `json:"fetchedAt"`
		Keys      jsontext.Value `json:"keys"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return keysCopy{}, fmt.Errorf("decode the keys copy: %w", err)
	}
	if wire.V != keysCopyFormat {
		return keysCopy{}, fmt.Errorf("the keys copy is format %d, which this release does not read", wire.V)
	}
	if wire.FetchedAt.IsZero() {
		return keysCopy{}, errors.New("the keys copy says no fetchedAt")
	}
	doc, err := artifacts.ParsePublishedKeys(wire.Keys)
	if err != nil {
		return keysCopy{}, fmt.Errorf("the keys copy's document: %w", err)
	}
	return keysCopy{V: wire.V, FetchedAt: wire.FetchedAt.UTC(), Keys: doc}, nil
}

// adopt takes doc as read at now and, when it is adopted and a copy file
// is set, keeps it on disk.
func (i *Issuer) adopt(doc artifacts.PublishedKeys, now time.Time) {
	if !i.take(doc, now, nil) || i.cfg.CopyFile == "" {
		return
	}
	i.copyMu.Lock()
	defer i.copyMu.Unlock()
	// What is written is the document held now, which a slower call can
	// never set back: ordered by its sequence, then by when it was read.
	i.mu.Lock()
	held, at := i.doc, i.readAt
	i.mu.Unlock()
	if held.Sequence < i.copySeq || (held.Sequence == i.copySeq && !at.After(i.copyAt)) {
		return
	}
	i.copySeq, i.copyAt = held.Sequence, at
	err := writeKeysCopy(i.cfg.CopyFile, keysCopy{V: keysCopyFormat, FetchedAt: at, Keys: held})
	i.mu.Lock()
	i.copyErr = err
	i.mu.Unlock()
	if err != nil {
		i.log.Error("identity: the copy of the published signing keys could not be written", "path", i.cfg.CopyFile, "err", err)
	}
}

// writeKeysCopy writes the copy beside its place, 0600 and synced, and
// renames it over the old one.
func writeKeysCopy(path string, c keysCopy) error {
	body, err := stablejson.Marshal(c)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err = tmp.Chmod(0o600); err == nil {
		if _, err = tmp.Write(body); err == nil {
			err = tmp.Sync()
		}
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err != nil {
		_ = os.Remove(name) // the stage of a write that failed
	}
	return err
}

// loadCopy takes the copy on disk as the document, when this engine has
// read none: its age is counted from its fetchedAt, never from now.
func (i *Issuer) loadCopy() {
	if i.cfg.CopyFile == "" {
		return
	}
	i.mu.Lock()
	read := !i.readAt.IsZero()
	i.mu.Unlock()
	if read {
		return
	}
	// Whoever can write the copy can add a key: a link, or a file others
	// can write, is never read.
	info, err := os.Lstat(i.cfg.CopyFile)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			i.log.Error("identity: the copy of the published signing keys could not be read", "path", i.cfg.CopyFile, "err", err)
		}
		return
	}
	if !safeCopyFile(info) {
		i.log.Error("identity: the copy of the published signing keys is not used: it is not a regular file only its owner can write", "path", i.cfg.CopyFile, "mode", info.Mode().String())
		return
	}
	f, err := os.Open(i.cfg.CopyFile)
	if err != nil {
		i.log.Error("identity: the copy of the published signing keys could not be read", "path", i.cfg.CopyFile, "err", err)
		return
	}
	defer f.Close()
	// The file opened is held to the same rule: a link swapped in between
	// the two looks is not read.
	if opened, serr := f.Stat(); serr != nil || !safeCopyFile(opened) || !os.SameFile(info, opened) {
		i.log.Error("identity: the copy of the published signing keys changed as it was opened; not used", "path", i.cfg.CopyFile)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(f, keysCopyMaxBytes+1))
	if err == nil {
		var c keysCopy
		if c, err = parseKeysCopy(raw); err == nil {
			if i.takeCopy(c) {
				i.log.Warn("identity: the store cannot be read; deciding from the copy of the published signing keys", "path", i.cfg.CopyFile, "fetched_at", c.FetchedAt)
			}
			return
		}
	}
	i.log.Error("identity: the copy of the published signing keys is not used", "path", i.cfg.CopyFile, "err", err)
}

// safeCopyFile says a copy file may be read: a regular file only its owner
// can write.
func safeCopyFile(info os.FileInfo) bool {
	return info.Mode().IsRegular() && info.Mode().Perm()&0o022 == 0
}

// takeCopy adopts a copy loaded from disk only while nothing has been read
// from the store, and marks it so, under one hold of the lock: the store's
// next answer then replaces it whatever its sequence.
func (i *Issuer) takeCopy(c keysCopy) bool {
	now := i.now().UTC()
	if age := now.Sub(c.FetchedAt); age < 0 || age > i.cfg.CopyMaxAge {
		i.log.Error("identity: the copy of the published signing keys is not used: it is too old or fetched in the future",
			"path", i.cfg.CopyFile, "fetched_at", c.FetchedAt, "age", age.Round(time.Second), "max_age", i.cfg.CopyMaxAge)
		return false
	}
	i.mu.Lock()
	read := !i.readAt.IsZero()
	i.mu.Unlock()
	if read {
		return false
	}
	return i.take(c.Keys, c.FetchedAt, func() {
		if i.readAt.Equal(c.FetchedAt) {
			i.fromCopy = true
		}
	})
}
