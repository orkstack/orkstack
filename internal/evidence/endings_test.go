package evidence

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// streamBody is an archive body: each of its words is "n", an event numbered
// n, "n:TYPE" one of that type, or "n-m", a gap.
func streamBody(words string) []byte {
	var b strings.Builder
	b.WriteString("{\"kind\":\"run\",\"run\":{\"id\":\"ork-1\"}}\n")
	for _, word := range strings.Fields(words) {
		var first, last int64
		if n, _ := fmt.Sscanf(word, "%d-%d", &first, &last); n == 2 {
			fmt.Fprintf(&b, "{\"kind\":\"gap\",\"gap\":{\"first\":%d,\"last\":%d,\"reason\":\"lost\"}}\n", first, last)
			continue
		}
		sequence, kind, typed := strings.Cut(word, ":")
		if !typed {
			kind = "TASK_LOG"
		}
		fmt.Fprintf(&b, "{\"kind\":\"event\",\"event\":{\"runId\":\"ork-1\",\"sequence\":%q,\"type\":\"EVENT_TYPE_%s\"}}\n", sequence, kind)
	}
	return []byte(b.String())
}

// resumedFixture is the bundle of a run resumed under 5 and 20, whose newest
// body is words and whose ending under 5 is gone.
func resumedFixture(words string) fixtureBundle {
	f := newFixture()
	first, newest := streamBody("1 2 3 4:RUN_FAILED"), streamBody(words)
	f.bytes[EndingPath(0)], f.bytes[runPath] = first, newest
	m := f.manifest
	members := []Member{presentMember(runPath, KindRun, "", 0, "", "metadata/run-resumes/ork-1/20.jsonl", newest),
		presentMember(EndingPath(0), KindEnding, "", 0, "", "metadata/runs/ork-1.jsonl", first),
		{Path: EndingPath(5), Kind: KindEnding, State: StateReclaimed, Reason: ReasonEndingMissing, Origin: "metadata/run-resumes/ork-1/5.jsonl"}}
	m.Members = append(members, m.Members[1:]...)
	m.Endings = []Ending{{Scope: 0, Path: EndingPath(0), RunStatus: "FAILED"}, {Scope: 5, Path: EndingPath(5)}, {Scope: 20, Path: runPath, RunStatus: "SUCCEEDED"}}
	m.Counts = CountMembers(m.Members)
	return f
}

const resumedStream = "1 2 3 4:RUN_FAILED 5:RUN_RESUMED 6 7:RUN_FAILED 20:RUN_RESUMED 21 22:RUN_SUCCEEDED"

// TestVerify_AResumedRunsStream: a stream that goes on at a resume's scope is
// whole, and one that skips a number anywhere else is not.
func TestVerify_AResumedRunsStream(t *testing.T) {
	for name, tc := range map[string]struct{ words, reason, want, got string }{
		"resumed twice":                        {words: resumedStream},
		"a gap it names":                       {words: "1 2-3 4:RUN_FAILED 5:RUN_RESUMED 6-19 20:RUN_RESUMED 21:RUN_SUCCEEDED"},
		"a plan that could not run":            {words: "1 2 3 4:RUN_FAILED 5:RUN_RESUMED 6:RUN_FAILED 20:RUN_FAILED"},
		"a resume canceled as it ended":        {words: "1 2 3 4:RUN_FAILED 20:RUN_CANCELED"},
		"a resume's own event is lost":         {words: "1 2 3 4:RUN_FAILED 20-21 22:RUN_SUCCEEDED"},
		"a gap that starts at no scope":        {words: "1 2 3 4:RUN_FAILED 19-21 22", reason: FailureEventSequence, want: "5", got: "19"},
		"a number skipped in the middle":       {words: "1 2 4:RUN_FAILED 5:RUN_RESUMED 20:RUN_RESUMED", reason: FailureEventSequence, want: "3", got: "4"},
		"a jump to no resume's scope":          {words: "1 2 3 4:RUN_FAILED 9:RUN_RESUMED", reason: FailureEventSequence, want: "5", got: "9"},
		"a jump to a scope, with another type": {words: "1 2 3 4:RUN_FAILED 5:RUN_RESUMED 20", reason: FailureEventSequence, want: "6", got: "20"},
		"a number twice":                       {words: "1 2 2", reason: FailureEventSequence, want: "3", got: "2"},
		"a gap over events":                    {words: "1 2 1-3", reason: FailureEventSequence, want: "3", got: "1"},
		"short of its own scope":               {words: "1 2 3 4:RUN_FAILED 5:RUN_RESUMED 6", reason: FailureEventSequence, want: "20", got: "6"},
		"a gap with no end":                    {words: "1 2-1 3", reason: FailureEventSequence, want: "2", got: "2"},
	} {
		t.Run(name, func(t *testing.T) {
			report, err := Verify(bytes.NewReader(writeFixture(t, resumedFixture(tc.words))))
			must.OK(t, err)
			if tc.reason == "" {
				if len(report.Failures) != 0 {
					t.Fatalf("failures %+v", report.Failures)
				}
				return
			}
			if len(report.Failures) != 1 || report.Failures[0] != (Failure{Path: runPath, Reason: tc.reason, Want: tc.want, Got: tc.got}) {
				t.Fatalf("failures %+v, want %s", report.Failures, tc.reason)
			}
		})
	}
}

// TestVerify_OnlyAResumedRunsStreamIsRead: a bundle that names no endings is
// hashed and nothing more, as every bundle before it was.
func TestVerify_OnlyAResumedRunsStreamIsRead(t *testing.T) {
	f := resumedFixture("1 9")
	f.manifest.Endings = nil
	report, err := Verify(bytes.NewReader(writeFixture(t, f)))
	must.OK(t, err)
	if !report.OK() {
		t.Fatalf("failures %+v", report.Failures)
	}
}

// TestVerify_AnEndingThatIsNoArchiveBody is a failure naming its member.
func TestVerify_AnEndingThatIsNoArchiveBody(t *testing.T) {
	for name, body := range map[string]string{"not JSON": "what", "another kind": "{\"kind\":\"note\"}\n", "no run line": "{\"kind\":\"gap\",\"gap\":{\"first\":1,\"last\":4}}\n"} {
		f := resumedFixture(resumedStream)
		f.bytes[EndingPath(0)] = []byte(body)
		f.manifest.Members[1] = presentMember(EndingPath(0), KindEnding, "", 0, "", "", []byte(body))
		report, err := Verify(bytes.NewReader(writeFixture(t, f)))
		must.OK(t, err)
		if len(report.Failures) != 1 || report.Failures[0].Path != EndingPath(0) || report.Failures[0].Reason != FailureBadStream {
			t.Errorf("%s: failures %+v", name, report.Failures)
		}
	}
}

// TestParseManifest_Endings: a manifest's endings are the run's, in order,
// each with its member, or the manifest is refused.
func TestParseManifest_Endings(t *testing.T) {
	for name, change := range map[string]func(*Manifest){
		"":                     func(*Manifest) {},
		"one ending":           func(m *Manifest) { m.Endings = m.Endings[2:] },
		"no first ending":      func(m *Manifest) { m.Endings = m.Endings[1:] },
		"out of order":         func(m *Manifest) { m.Endings[1].Scope, m.Endings[1].Path = 30, EndingPath(30) },
		"another path":         func(m *Manifest) { m.Endings[1].Path = runPath },
		"no member":            func(m *Manifest) { m.Members = append(m.Members[:2:2], m.Members[3:]...) },
		"the newest elsewhere": func(m *Manifest) { m.Endings[2].Path = EndingPath(20) },
	} {
		m := resumedFixture(resumedStream).manifest
		change(m)
		document, err := m.MarshalJSON()
		must.OK(t, err)
		got, err := ParseManifest(document)
		if (err == nil) != (name == "") {
			t.Errorf("%q = %v", name, err)
		}
		if name == "" && len(got.Endings) != 3 {
			t.Errorf("%d endings read back", len(got.Endings))
		}
	}
}

// FuzzStreamCheck: what a body of any shape is found to be does not depend on
// how its bytes arrive, and a body found whole accounts, counted without the
// check, for every number up to its last.
func FuzzStreamCheck(f *testing.F) {
	for _, words := range []string{resumedStream, "1 2-3 4", "1 5:RUN_FAILED", "5:RUN_RESUMED", "1 2 2", "", "0", "1 3-2"} {
		f.Add(words, uint8(1))
		f.Add(words, uint8(40))
	}
	f.Add("\n{\"kind\":\"event\"}", uint8(3))
	manifest := &Manifest{Endings: []Ending{{Scope: 0}, {Scope: 5}, {Scope: 20}}}
	f.Fuzz(func(t *testing.T, words string, chunk uint8) {
		body := streamBody(words)
		run := func(size int) *Failure {
			c := newStreamCheck(manifest, "")
			for at := 0; at < len(body); at += size {
				if n, err := c.Write(body[at:min(at+size, len(body))]); err != nil || n != min(size, len(body)-at) {
					t.Fatalf("Write = %d, %v", n, err)
				}
			}
			return c.end()
		}
		whole, parts := run(len(body)), run(int(chunk)+1)
		if (whole == nil) != (parts == nil) || (whole != nil && *whole != *parts) {
			t.Fatalf("%q: %+v whole, %+v in parts", words, whole, parts)
		}
		if whole != nil {
			if whole.Reason != FailureEventSequence && whole.Reason != FailureBadStream {
				t.Fatalf("%q: %+v", words, whole)
			}
			return
		}
		// Counted from the words, which a body found whole was made of.
		next := int64(1)
		for _, word := range strings.Fields(words) {
			var first, last int64
			if n, _ := fmt.Sscanf(word, "%d-%d", &first, &last); n != 2 {
				digits, _, _ := strings.Cut(word, ":")
				if n, _ := fmt.Sscanf(digits, "%d", &first); n != 1 {
					t.Fatalf("%q is whole with the word %q", words, word)
				}
				last = first
			}
			if first == 5 || first == 20 {
				next = max(next, first)
			}
			if first != next || last < first {
				t.Fatalf("%q is whole, and %q follows %d", words, word, next-1)
			}
			next = last + 1
		}
	})
}
