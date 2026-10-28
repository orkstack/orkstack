package artifacts_test

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// FuzzNameRule_EveryBoundaryHoldsTheSameRule states AGENTS.md §7: names are
// validated, never sanitized, by the same rule at each boundary,
// ^[a-z][a-z0-9-]*$ in at most 200 bytes, for namespaces, Orkflow names, task
// ids and pools. Every package that holds a name to the rule must accept
// exactly the slugs; the artifact store's Orkflow name, a path component, may
// accept more (it is the key's safety, not the name's rule) but never refuses
// a slug.
func FuzzNameRule_EveryBoundaryHoldsTheSameRule(f *testing.F) {
	for _, seed := range []string{
		"payments", "a", "a-b-c", "a1", "1a", "-a", "a-", "A", "aB", "a_b", "a.b", "a b", "a/b", "..", ".", "a\n", "a\x00", "\u00e9", "\u212a", "\u0131",
		strings.Repeat("a", 200), strings.Repeat("a", 201), "default", "main", "ork-engine-events", "ork-pool-x", "a--b",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if name == "" {
			return // each boundary reads empty as its own default or as unset
		}
		slug := len(name) <= 200 && name[0] >= 'a' && name[0] <= 'z'
		for i := 1; slug && i < len(name); i++ {
			c := name[i]
			slug = c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-'
		}
		_, poolErr := task.QueueForPool(name, task.DefaultQueue)
		_, namespaceErr := artifacts.NamespaceKey(name)
		_, headErr := artifacts.OrkflowHeadKey("default", name)
		for boundary, err := range map[string]error{
			"dsl.ValidSlug":         dsl.ValidSlug("name", name),
			"triggers.ValidateName": triggers.ValidateName(name),
			"webhooks.ValidateName": webhooks.ValidateName(name, "payments"),
			"task.QueueForPool":     poolErr,
			"artifacts.Namespace":   namespaceErr,
		} {
			if (err == nil) != slug {
				t.Fatalf("%s(%q) = %v; the name rule says accepted=%v", boundary, name, err, slug)
			}
		}
		if slug && headErr != nil {
			t.Fatalf("artifacts.OrkflowHeadKey refused the slug %q: %v", name, headErr)
		}
	})
}

// FuzzKeys_AKeyTheParserAcceptsIsTheKeyTheBuilderWrites: every parser of an
// object key reports a key it did not generate as ok=false, so an accepted
// key is the one key the builder renders for what it parsed (two keys never
// name one record), and what a builder renders parses back to what built it.
func FuzzKeys_AKeyTheParserAcceptsIsTheKeyTheBuilderWrites(f *testing.F) {
	seeds := []string{
		"metadata/audit/2026/10/09/aud-20261009T000000.000000000Z-0123456789abcdef.json",
		"metadata/attempts/ork-1-1/build/0000000001.json", "metadata/attempts/ork-1-1/fan/00003/0000000002.json",
		"metadata/orkflows/payments/nightly/@head.json", "metadata/orkflows/payments/nightly/@current.json", "metadata/namespaces/payments.json",
		"metadata/memos/payments/o/nightly/build/" + strings.Repeat("a", 64) + ".json", "metadata/logs/ork-1-1/build/0000000001/0000000000",
		"metadata/logs/ork-1-1/fan/00001/0000000001/0000000007", "", "metadata/", "metadata/namespaces/.json", "metadata/namespaces/a/b.json",
		"metadata/attempts/ork-1-1/build/1.json", "metadata/attempts/ork-1-1/build/00000000001.json", "metadata/attempts/ork-1-1/build/0000000000.json",
		"metadata/attempts/ork-1-1/fan/1/0000000001.json", "metadata/attempts/ork%2D1/build/0000000001.json", "metadata/attempts/../x/0000000001.json",
	}
	stamped := artifacts.RunRecord{RunID: "ork-1-1", Namespace: "payments", Orkflow: "nightly", Status: "succeeded",
		Time: time.Date(2026, 10, 9, 12, 30, 45, 123456000, time.UTC), OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE, OriginID: "sch-0123456789abcdef"}
	for _, build := range []func(artifacts.RunRecord) (string, error){
		artifacts.RunIndexKey, artifacts.RunRecentIndexKey,
		func(rec artifacts.RunRecord) (string, error) {
			key, _, err := artifacts.RunOriginIndexKey(rec)
			return key, err
		},
	} {
		if key, err := build(stamped); err == nil {
			seeds = append(seeds, key)
		}
	}
	if key, err := artifacts.RunObjectRefKey("objects/a", "ork-1-1"); err == nil {
		seeds = append(seeds, key)
	}
	seeds = append(seeds, "metadata/index/runs/payments/nightly/succeeded/20261009T123045.123456Z/ork%2D1", "metadata/index/runs/a/b/c/d/e/f/g")
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		if entry, ok := artifacts.ParseRunIndexKey(key); ok {
			rebuilt, err := artifacts.RunIndexKeyForEntry(entry)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseRunIndexKey(%q) = %+v; RunIndexKeyForEntry writes %q (%v)", key, entry, rebuilt, err)
			}
		}
		if entry, ok := artifacts.ParseRunRecentIndexKey(key); ok {
			rebuilt, err := artifacts.RunRecentIndexKeyForEntry(entry)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseRunRecentIndexKey(%q) = %+v; RunRecentIndexKeyForEntry writes %q (%v)", key, entry, rebuilt, err)
			}
		}
		if entry, ok := artifacts.ParseRunOriginIndexKey(key); ok && artifacts.NormalizeRunStatus(entry.Status) == entry.Status {
			rebuilt, built, err := artifacts.RunOriginIndexKey(artifacts.RunRecord{
				RunID: entry.RunID, Namespace: entry.Namespace, Orkflow: entry.Orkflow, Status: entry.Status, Time: entry.Time,
				OriginKind: entry.OriginKind, OriginID: entry.OriginID,
			})
			if err != nil || !built || rebuilt != key {
				t.Fatalf("ParseRunOriginIndexKey(%q) = %+v; RunOriginIndexKey writes %q (%v)", key, entry, rebuilt, err)
			}
		}
		if runID, ok := artifacts.ParseRunObjectRefRunID(key); ok {
			if segments := strings.Split(key, "/"); len(segments) != 5 {
				t.Fatalf("ParseRunObjectRefRunID(%q) = %q for a key of %d segments", key, runID, len(segments))
			} else if again, err := artifacts.RunObjectRefKey("", runID); err != nil || !strings.HasSuffix(again, "/"+segments[4]) {
				t.Fatalf("ParseRunObjectRefRunID(%q) = %q, which re-encodes as %q (%v)", key, runID, again, err)
			}
		}
		if id, day, ok := artifacts.ParseAuditKey(key); ok {
			rebuilt, err := artifacts.AuditKey(&orkstackv1.AuditRecord{Id: id, At: timestamppb.New(day)})
			if err != nil || rebuilt != key {
				t.Fatalf("ParseAuditKey(%q) = %q, %v; AuditKey writes %q (%v)", key, id, day, rebuilt, err)
			}
		}
		if ref, ok := artifacts.ParseAttemptKey(key); ok {
			rebuilt, err := artifacts.AttemptKey(ref)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseAttemptKey(%q) = %+v; AttemptKey writes %q (%v)", key, ref, rebuilt, err)
			}
		}
		if ref, seq, truncated, ok := artifacts.ParseLogChunkKey(key); ok {
			rebuilt, err := artifacts.LogChunkKey(ref, seq, truncated)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseLogChunkKey(%q) = %+v, %d, %v; LogChunkKey writes %q (%v)", key, ref, seq, truncated, rebuilt, err)
			}
		}
		if ref, ok := artifacts.ParseMemoKey(key); ok {
			rebuilt, err := artifacts.MemoKey(ref)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseMemoKey(%q) = %+v; MemoKey writes %q (%v)", key, ref, rebuilt, err)
			}
		}
		if namespace, ok := artifacts.ParseNamespaceKey(key); ok {
			rebuilt, err := artifacts.NamespaceKey(namespace)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseNamespaceKey(%q) = %q; NamespaceKey writes %q (%v)", key, namespace, rebuilt, err)
			}
		}
		if namespace, name, ok := artifacts.ParseOrkflowHeadKey(key); ok {
			rebuilt, err := artifacts.OrkflowHeadKey(namespace, name)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseOrkflowHeadKey(%q) = %q, %q; OrkflowHeadKey writes %q (%v)", key, namespace, name, rebuilt, err)
			}
		}
		if namespace, name, ok := artifacts.ParseOrkflowCurrentKey(key); ok {
			rebuilt, err := artifacts.OrkflowCurrentKey(namespace, name)
			if err != nil || rebuilt != key {
				t.Fatalf("ParseOrkflowCurrentKey(%q) = %q, %q; OrkflowCurrentKey writes %q (%v)", key, namespace, name, rebuilt, err)
			}
		}
	})
}

// FuzzKeys_ABuiltKeyParsesBackToWhatBuiltIt: the other direction, from
// structured coordinates, including coordinates no key can hold, which are
// refused with an error rather than rendered.
func FuzzKeys_ABuiltKeyParsesBackToWhatBuiltIt(f *testing.F) {
	f.Add("ork-1-1", "build", int32(1), int32(0), false)
	f.Add("ork-1-1", "fan", int32(3), int32(7), true)
	f.Add("ork/1", "b/uild", int32(1), int32(0), false)
	f.Add("ork-1-1", "build", int32(0), int32(0), false)
	f.Add("ork-1-1", "build", int32(-1), int32(-1), true)
	f.Add("", "", int32(1), int32(0), false)
	f.Add("\u00e9", "a\x00b", int32(2147483647), int32(2147483647), true)
	f.Add("..", ".", int32(1), int32(0), false)
	f.Fuzz(func(t *testing.T, runID, taskID string, attempt, index int32, isItem bool) {
		item := artifacts.NoItem
		if isItem {
			item = artifacts.ItemAt(index)
		}
		ref := artifacts.AttemptRef{RunID: runID, TaskID: taskID, Item: item, Attempt: attempt}
		key, err := artifacts.AttemptKey(ref)
		if err != nil {
			return
		}
		back, ok := artifacts.ParseAttemptKey(key)
		if !ok || back != ref {
			t.Fatalf("AttemptKey(%+v) = %q, which parses as %+v, %v", ref, key, back, ok)
		}
		logRef := artifacts.LogRef{RunID: runID, TaskID: taskID, Item: item, Attempt: attempt}
		logKey, err := artifacts.LogChunkKey(logRef, int64(index&0x7fffffff), isItem)
		if err != nil {
			return
		}
		gotRef, seq, truncated, ok := artifacts.ParseLogChunkKey(logKey)
		if !ok || gotRef != logRef || seq != int64(index&0x7fffffff) || truncated != isItem {
			t.Fatalf("LogChunkKey(%+v) = %q, which parses as %+v, %d, %v, %v", logRef, logKey, gotRef, seq, truncated, ok)
		}
	})
}
