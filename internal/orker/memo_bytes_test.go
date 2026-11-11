package orker

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestMemoKey_PinnedWithTextAnEncoderCouldRespell pins a key whose command,
// env and secrets hold what an encoder could spell more than one way (HTML
// characters, U+2028, keys that sort differently by byte and by UTF-16 unit,
// text that is not valid UTF-8). The value was recorded with encoding/json
// before the module moved to encoding/json/v2: a memo written then is still
// found now.
func TestMemoKey_PinnedWithTextAnEncoderCouldRespell(t *testing.T) {
	in, staged := pinnedKeyInput()
	in.Command = []string{"sh", "-c", "echo '<b>' && echo   \xff"}
	in.Env = map[string]string{"é": "<1>", "Z": "&", "z": " ", "😀": "x", "�": "y", "BAD": "\xff"}
	in.Secrets = []*orkstackv1.SecretRef{{Name: "a<b", Source: "env:A&B"}}
	key := keyOf(t, in, []byte("{\"x\":\"<\"}\n"), []byte("{}\n"), staged, &resolvedEachItem{Bytes: []byte(`{"n":"<"}`), As: "it", Index: 2, Count: 3})
	const pinned = "4e19c907798c26cc41bf948cab2bfc48163386d9f5b7da049dfbd1192c3ec4fa"
	if key.Key != pinned {
		t.Fatalf("key = %q, pinned %q: the key's bytes moved, so every memo written before misses", key.Key, pinned)
	}
}
