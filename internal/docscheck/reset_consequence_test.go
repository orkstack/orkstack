package docscheck

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestResetConsequence_RenderedFromTheResponse: what a reset of a concurrency
// limit does is said by the API's response alone, so no client holds a
// sentence of it to drift from it.
func TestResetConsequence_RenderedFromTheResponse(t *testing.T) {
	root := repoRoot(t)
	// Each text's fixed words, between the values its verdict fills in.
	fragments := []string{
		"queued runs keep their slots and their order",
		"entries whose runs have ended or moved on are dropped",
		"No run is stopped, restarted or moved in line. Runs that ask for this key during the reset wait",
		"The reset starts a new one from the records of the runs holding or waiting for it",
		"The limit is not answering, so the runs it holds cannot all be known. The reset rebuilds it",
		"until every run that holds the key is found",
	}
	var texts strings.Builder
	for _, code := range []string{engine.ResetAnswering, engine.ResetAbsent, engine.ResetUnansweredForced} {
		texts.WriteString(engine.ResetConsequence(engine.ResetVerdict{Code: code, Key: "k", Limit: 2}))
	}
	for _, f := range fragments {
		if !strings.Contains(texts.String(), f) {
			t.Fatalf("no consequence holds %q: the list is out of step", f)
		}
	}
	read := 0
	for _, dir := range []string{"cmd/orkctl", "ui/src"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || strings.HasSuffix(path, "_test.go") || strings.Contains(path, "/testdata/") {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			read++
			// A sentence a source line wraps is still one.
			text := strings.Join(strings.Fields(strings.NewReplacer("\\n", " ", "\" +", "", "\"", "").Replace(string(b))), " ")
			for _, f := range fragments {
				if strings.Contains(text, f) {
					t.Errorf("%s holds %q; render the response's consequence", path, f)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if read < 100 {
		t.Fatalf("read %d files", read)
	}
}
