package artifacts

import (
	"strings"
	"testing"
)

func TestObjectIDFromURI(t *testing.T) {
	digest := strings.Repeat("a", 64)
	localKey := objectKey("a/b", digest)
	s3Key := s3ObjectKey(Provenance{Orkflow: "flow", RunID: "ork-run", TaskID: "task"}, "result", digest)
	tests := []struct {
		name string
		uri  string
		want string
		ok   bool
	}{
		{name: "local", uri: "file:///tmp/root/" + localKey, want: localKey, ok: true},
		{name: "s3 objects layout", uri: "s3://bucket/" + localKey, want: localKey, ok: true},
		{name: "s3 provenance layout", uri: "s3://bucket/" + s3Key, want: s3Key, ok: true},
		{name: "no objects", uri: "file:///tmp/root/metadata/runs/run.jsonl"},
		{name: "non hex digest", uri: "file:///tmp/root/objects/n-YQ/not-hex"},
		{name: "traversal", uri: "file:///tmp/root/objects/n-YQ/../" + digest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ObjectIDFromURI(tc.uri)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("ObjectIDFromURI(%q) = %q, %v; want %q, %v", tc.uri, got, ok, tc.want, tc.ok)
			}
		})
	}
}
