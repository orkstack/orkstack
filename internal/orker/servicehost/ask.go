package servicehost

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// AskDir is the Orker's own directory of a service's conversation, bound
// read-only into the container (14 §3): every file is written under a
// temporary name beside its final one and renamed into place, through a
// handle the Orker holds, never through a path the container controls.
type AskDir struct {
	root *os.Root
	tmp  atomic.Uint64
}

// OpenAskDir opens dir, which the Orker created, as a service's ask/.
func OpenAskDir(dir string) (*AskDir, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("servicehost: open ask/ %s: %w", dir, err)
	}
	return &AskDir{root: root}, nil
}

// Close releases the handle.
func (a *AskDir) Close() error {
	if a == nil || a.root == nil {
		return nil
	}
	return a.root.Close()
}

// Write puts body at name ("alive.json", "lanes/2/poll.json"): written
// under a temporary name in name's directory, then renamed, so the
// container never sees half a file (14 §4.2, rule 2).
func (a *AskDir) Write(name string, body []byte) error {
	if err := fixedName(name); err != nil {
		return err
	}
	dir := path.Dir(name)
	if dir != "." {
		if err := a.root.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("servicehost: make ask/%s: %w", dir, err)
		}
	}
	tmp := path.Join(dir, ".tmp-"+strconv.Itoa(os.Getpid())+"-"+strconv.FormatInt(time.Now().UnixNano(), 36)+"-"+
		strconv.FormatUint(a.tmp.Add(1), 10)+"-"+path.Base(name))
	f, err := a.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("servicehost: write ask/%s: %w", name, err)
	}
	_, werr := f.Write(body)
	cerr := f.Close()
	if err := errors.Join(werr, cerr); err != nil {
		_ = a.root.Remove(tmp)
		return fmt.Errorf("servicehost: write ask/%s: %w", name, err)
	}
	if err := a.root.Rename(tmp, name); err != nil {
		_ = a.root.Remove(tmp)
		return fmt.Errorf("servicehost: place ask/%s: %w", name, err)
	}
	return nil
}

// WriteJSON puts v at name as stable JSON.
func (a *AskDir) WriteJSON(name string, v any) error {
	body, err := stablejson.Marshal(v)
	if err != nil {
		return fmt.Errorf("servicehost: encode ask/%s: %w", name, err)
	}
	return a.Write(name, body)
}
