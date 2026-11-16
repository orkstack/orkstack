package orker

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// TestMemoKey_PinnedWithTextAnEncoderCouldRespell pins a key whose command,
// env and secrets hold what an encoder could spell more than one way (HTML
// characters, U+2028, keys that sort differently by byte and by UTF-16 unit,
// text that is not valid UTF-8). The value was recorded with encoding/json
// before the module moved to encoding/json/v2: a memo written then is still
// found now. Since v0.4.0 a task with secrets carries one component more,
// secret_check, so its key is pinned again; without that component the
// document still hashes to the recorded value, so nothing else moved.
func TestMemoKey_PinnedWithTextAnEncoderCouldRespell(t *testing.T) {
	in, staged := pinnedKeyInput()
	in.Command = []string{"sh", "-c", "echo '<b>' && echo   \xff"}
	in.Env = map[string]string{"é": "<1>", "Z": "&", "z": " ", "😀": "x", "�": "y", "BAD": "\xff"}
	in.Secrets = []*orkstackv1.SecretRef{{Name: "a<b", Source: "env:A&B"}}
	key := keyOf(t, in, []byte("{\"x\":\"<\"}\n"), []byte("{}\n"), staged, &resolvedEachItem{Bytes: []byte(`{"n":"<"}`), As: "it", Index: 2, Count: 3})
	const pinned = "80e251b9c41c7a4d8a4dfaebeabeac3242d8f0e610c567c17f8772f5ae248a28"
	if key.Key != pinned {
		t.Fatalf("key = %q, pinned %q: the key's bytes moved, so every memo written before misses", key.Key, pinned)
	}
	if key.Components["secret_check"] != "1" {
		t.Fatalf("secret_check = %q; a task with secrets must carry it", key.Components["secret_check"])
	}
	before := map[string]string{}
	for name, value := range key.Components {
		if name != "secret_check" {
			before[name] = value
		}
	}
	document, err := stablejson.Marshal(memoKeyDocument{
		V: MemoKeyVersion, Namespace: "ml", Scope: "orkflow", Orkflow: "cached-pipeline", Task: "featurize", Components: before,
	})
	if err != nil {
		t.Fatal(err)
	}
	const recorded = "4e19c907798c26cc41bf948cab2bfc48163386d9f5b7da049dfbd1192c3ec4fa"
	if got := sha256Hex(document); got != recorded {
		t.Fatalf("without secret_check the key = %q, recorded %q: an encoder respelled a component", got, recorded)
	}
}
