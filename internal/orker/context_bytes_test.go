package orker

import (
	"os"
	"path/filepath"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestContextFiles_AreTheBytesEncodingJSONWrote pins the bytes a container
// reads in /orkstack/input (task.json, config.json, run.json), recorded with
// encoding/json before the module moved to encoding/json/v2. They are part of
// the activity contract, and config.json and run.json are also hashed into a
// cached task's memo key.
func TestContextFiles_AreTheBytesEncodingJSONWrote(t *testing.T) {
	ws := newTestWorkspace(t)
	in := &orkstackv1.ExecuteTaskInput{
		RunId: "ork-1", OrkflowName: "o", OrkflowVersion: "1.0<beta>& ", TaskId: "compile",
		Limits: testLimits(),
	}
	tc := TaskContext{
		Attempt: 2, Pool: "main", Namespace: "default", Network: NetworkNone,
		Image: &orkstackv1.ImageRef{Reference: "shell", Pinned: "x@sha256:y", IndexDigest: "sha256:y"},
	}
	if err := ws.WriteTaskJSON(in, tc); err != nil {
		t.Fatalf("WriteTaskJSON: %v", err)
	}
	tricky := map[string]any{
		"html": "<script>a && b</script>", "lines": "a b c", "bad": "\xff",
		"z": 1.0, "Z": []any{}, "é": nil, "nested": map[string]any{"b": []any{1.5, "x"}, "a": map[string]any{}},
		"nil list": []string(nil), "nil map": map[string]any(nil),
	}
	if err := ws.WriteConfigJSON(tricky); err != nil {
		t.Fatalf("WriteConfigJSON: %v", err)
	}
	if err := ws.WriteRunJSON(nil); err != nil {
		t.Fatalf("WriteRunJSON: %v", err)
	}
	for _, c := range []struct{ file, want string }{
		{"task.json", "{\n  \"schemaVersion\": 1,\n  \"idempotencyKey\": \"\",\n  \"run\": {\n    \"id\": \"ork-1\",\n    \"namespace\": \"default\",\n    \"orkflow\": {\n      \"name\": \"o\",\n      \"version\": \"1.0\\u003cbeta\\u003e\\u0026\\u2028\",\n      \"definitionDigest\": \"\"\n    }\n  },\n  \"task\": {\n    \"id\": \"compile\",\n    \"attempt\": 2,\n    \"pool\": \"main\"\n  },\n  \"image\": {\n    \"reference\": \"shell\",\n    \"pinned\": \"x@sha256:y\",\n    \"indexDigest\": \"sha256:y\"\n  },\n  \"limits\": {\n    \"timeoutSeconds\": 0,\n    \"cancellationGracePeriodSeconds\": 0,\n    \"valuesMaxBytes\": 262144,\n    \"artifactMaxCount\": 256,\n    \"artifactMaxBytes\": 5368709120,\n    \"logMaxBytes\": 16777216,\n    \"commitMaxBytes\": 4194304\n  },\n  \"network\": \"none\",\n  \"paths\": {\n    \"input\": \"/orkstack/input\",\n    \"config\": \"/orkstack/input/config.json\",\n    \"runInput\": \"/orkstack/input/run.json\",\n    \"secrets\": \"/orkstack/input/secrets\",\n    \"output\": \"/orkstack/output\",\n    \"values\": \"/orkstack/output/values.json\",\n    \"artifacts\": \"/orkstack/output/artifacts\",\n    \"tmp\": \"/orkstack/tmp\"\n  },\n  \"inputs\": {\n    \"artifacts\": [],\n    \"upstream\": []\n  },\n  \"secrets\": []\n}\n"},
		{"config.json", "{\n  \"Z\": [],\n  \"bad\": \"�\",\n  \"html\": \"\\u003cscript\\u003ea \\u0026\\u0026 b\\u003c/script\\u003e\",\n  \"lines\": \"a\\u2028b\\u2029c\",\n  \"nested\": {\n    \"a\": {},\n    \"b\": [\n      1.5,\n      \"x\"\n    ]\n  },\n  \"nil list\": null,\n  \"nil map\": null,\n  \"z\": 1,\n  \"é\": null\n}\n"},
		{"run.json", "{}\n"},
	} {
		raw, err := os.ReadFile(filepath.Join(ws.Input, c.file))
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != c.want {
			t.Errorf("%s moved.\n got: %q\nwant: %q", c.file, raw, c.want)
		}
	}
}
