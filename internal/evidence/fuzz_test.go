package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

// FuzzVerify_AVerifiedMemberIsExactlyWhatTheBundleHolds builds bundles whose
// manifest lies about their members in the ways a damaged or forged export
// can (a wrong digest or size, an unlisted file, a listed file that is absent,
// a path listed twice, a directory where a file belongs, members out of
// order) and compares what Verify accepts against a count made without it.
// Verify's whole value is that "verified" is true: a member is verified only
// when the bundle holds, once, a regular file at a safe path with that
// digest and size, in manifest order, and every member is accounted for.
func FuzzVerify_AVerifiedMemberIsExactlyWhatTheBundleHolds(f *testing.F) {
	f.Add("tasks/a/values.json", []byte("{}"), "tasks/b/stdout.log", []byte("hello"), uint32(0), false)
	f.Add("a", []byte("x"), "b", []byte("y"), uint32(0), true)
	f.Add("a", []byte("x"), "b", []byte("y"), uint32(0x10), false)
	f.Add("a", []byte("x"), "b", []byte("y"), uint32(0x21), false)
	f.Add("a", []byte("x"), "b", []byte("y"), uint32(0x32), false)
	f.Add("a", []byte("x"), "b", []byte("y"), uint32(0x54), false)
	f.Add("a", []byte("x"), "b", []byte("y"), uint32(0x76), false)
	f.Add("a", []byte("x"), "b", []byte("y"), uint32(0x10), true) // a wrong file first still sets the order
	f.Add("../escape", []byte("x"), "/abs", []byte("y"), uint32(0), false)
	f.Add("same", []byte("x"), "same", []byte("y"), uint32(0), false)
	f.Add("", []byte(""), "b/../c", []byte(""), uint32(0), false)
	f.Fuzz(func(t *testing.T, name0 string, body0 []byte, name1 string, body1 []byte, modes uint32, swap bool) {
		names := []string{name0, name1}
		bodies := [][]byte{body0, body1}
		if name0 == name1 || name0 == ManifestPath || name1 == ManifestPath {
			return
		}
		if !utf8.ValidString(name0) || !utf8.ValidString(name1) {
			return // a manifest is JSON: a path that is not UTF-8 cannot be listed
		}
		// One member per entry; mode says how the manifest or the bundle lies.
		var members []Member
		type tarFile struct {
			index int
			name  string
			body  []byte
			dir   bool
		}
		var inTar []tarFile
		honest := map[string]bool{} // a member that should verify if it is also in order
		listed := map[string]bool{} // a member the manifest lists as present, right or wrong
		for i := range names {
			mode := (modes >> (4 * i)) & 15 % 8
			member := Member{Path: names[i], Kind: KindArtifact, SizeBytes: int64(len(bodies[i])), SHA256: digestOf(bodies[i]), State: StatePresent}
			switch mode {
			case 0: // honest
				honest[names[i]], listed[names[i]] = true, true
				members = append(members, member)
				inTar = append(inTar, tarFile{i, names[i], bodies[i], false})
			case 1: // wrong digest
				member.SHA256 = digestOf(append([]byte("x"), bodies[i]...))
				listed[names[i]] = true
				members = append(members, member)
				inTar = append(inTar, tarFile{i, names[i], bodies[i], false})
			case 2: // wrong size
				member.SizeBytes++
				listed[names[i]] = true
				members = append(members, member)
				inTar = append(inTar, tarFile{i, names[i], bodies[i], false})
			case 3: // not listed
				inTar = append(inTar, tarFile{i, names[i], bodies[i], false})
			case 4: // listed, not in the bundle
				listed[names[i]] = true
				members = append(members, member)
			case 5: // listed twice: the first listing verifies, the second is a failure
				honest[names[i]], listed[names[i]] = true, true
				members = append(members, member, member)
				inTar = append(inTar, tarFile{i, names[i], bodies[i], false})
			case 6: // reclaimed: absent as it must be
				member.State = StateReclaimed
				members = append(members, member)
			case 7: // a directory where a file belongs
				listed[names[i]] = true
				members = append(members, member)
				inTar = append(inTar, tarFile{i, names[i], nil, true})
			}
		}
		if swap {
			sort.SliceStable(inTar, func(a, b int) bool { return inTar[a].index > inTar[b].index })
		}
		manifest := &Manifest{SchemaVersion: SchemaVersion, RunID: "ork-1", Members: members, Counts: CountMembers(members)}
		document, err := manifest.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var bundle bytes.Buffer
		gz := gzip.NewWriter(&bundle)
		tw := tar.NewWriter(gz)
		write := func(name string, body []byte, dir bool) bool {
			header := &tar.Header{Name: name, Mode: 0o644, Typeflag: tar.TypeReg, Size: int64(len(body))}
			if dir {
				header = &tar.Header{Name: name, Mode: 0o755, Typeflag: tar.TypeDir}
			}
			if tw.WriteHeader(header) != nil {
				return false
			}
			_, err := tw.Write(body)
			return err == nil
		}
		if !write(ManifestPath, document, false) {
			t.Fatal("cannot write the manifest")
		}
		for _, entry := range inTar {
			if !write(entry.name, entry.body, entry.dir) {
				return // a name the tar format cannot carry
			}
		}
		if tw.Close() != nil || gz.Close() != nil {
			t.Fatal("cannot close the bundle")
		}

		report, err := Verify(bytes.NewReader(bundle.Bytes()))
		if err != nil || report.Manifest == nil {
			t.Fatalf("a readable bundle was not verified: %v", err)
		}

		// The count made without Verify: walk the bundle in tar order.
		want := map[string]bool{}
		furthest := -1
		listedAt := map[string]int{}
		duplicated := map[string]bool{}
		for i, member := range members {
			if _, again := listedAt[member.Path]; again {
				duplicated[member.Path] = true
				continue
			}
			listedAt[member.Path] = i
		}
		// Order is the bundle's, not a member's: a listed regular file at a
		// safe path moves the furthest position whether or not its bytes are
		// right, and a later file at an earlier position is out of order.
		for _, entry := range inTar {
			index := listedAt[entry.name]
			if entry.dir || !listed[entry.name] || unsafeName(entry.name) {
				continue
			}
			if index < furthest {
				continue
			}
			furthest = index
			if honest[entry.name] {
				want[entry.name] = true
			}
		}
		got := map[string]bool{}
		for _, member := range report.Verified {
			if got[member.Path] {
				t.Fatalf("%q is verified twice", member.Path)
			}
			got[member.Path] = true
		}
		if len(got) != len(want) {
			t.Fatalf("verified %v, a count of the bundle says %v (failures %+v)", got, want, report.Failures)
		}
		for path := range want {
			if !got[path] {
				t.Fatalf("verified %v, a count of the bundle says %v (failures %+v)", got, want, report.Failures)
			}
		}

		// Every member is accounted for once: a list, or a failure naming it.
		accounted := len(report.Verified) + len(report.Reclaimed) + len(report.Unrecorded) + len(report.Rejected)
		failed := map[string]bool{}
		for _, failure := range report.Failures {
			failed[failure.Path] = true
		}
		for _, member := range members {
			if member.State == StatePresent && !got[member.Path] && !failed[member.Path] {
				t.Fatalf("the present member %q is neither verified nor a failure: %+v", member.Path, report)
			}
		}
		if accounted > len(members) {
			t.Fatalf("%d members are accounted for among %d", accounted, len(members))
		}
		if report.OK() != (len(report.Failures) == 0) {
			t.Fatalf("OK = %v with %d failures", report.OK(), len(report.Failures))
		}
		if len(duplicated) > 0 && report.OK() {
			t.Fatalf("a bundle that lists %v twice is OK", duplicated)
		}
	})
}

// unsafeName is an entry name no extractor should honour, from the rule's
// text: absolute, a ".." segment or a NUL byte (and empty).
func unsafeName(name string) bool {
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) {
		return true
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

// FuzzParseManifest: a document is read as a manifest of schema version 1 or
// 2 or refused by name; a version 2 manifest names at least one event gap and
// every gap is a range, a version 1 manifest names none; and what it reads
// writes back to a document that reads back as the same manifest.
func FuzzParseManifest(f *testing.F) {
	golden, _ := (&Manifest{SchemaVersion: SchemaVersion, RunID: "ork-1"}).MarshalJSON()
	gapped, _ := (&Manifest{SchemaVersion: SchemaVersionEventGaps, RunID: "ork-1", EventGaps: []EventGap{{First: 2, Last: 5, Reason: "lost"}}}).MarshalJSON()
	for _, seed := range [][]byte{
		golden, gapped, []byte(`{}`), []byte(`{"schema_version":3}`), []byte(`{"schema_version":2}`), []byte(`{"schema_version":1,"event_gaps":[{"first":1,"last":2}]}`),
		[]byte(`{"schema_version":2,"event_gaps":[{"first":0,"last":2}]}`), []byte(`{"schema_version":2,"event_gaps":[{"first":3,"last":2}]}`),
		[]byte(`null`), []byte(``), []byte(`[]`), []byte(`{"schema_version":1,"members":[{"path":"a","state":"present","size_bytes":-1}]}`),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, document []byte) {
		manifest, err := ParseManifest(document)
		if err != nil {
			if manifest != nil || !strings.HasPrefix(err.Error(), "evidence: ") {
				t.Fatalf("ParseManifest(%q) = %v, %v", document, manifest, err)
			}
			return
		}
		if manifest.SchemaVersion != SchemaVersion && manifest.SchemaVersion != SchemaVersionEventGaps {
			t.Fatalf("ParseManifest(%q) read schema_version %d", document, manifest.SchemaVersion)
		}
		if (manifest.SchemaVersion == SchemaVersionEventGaps) != (len(manifest.EventGaps) > 0) {
			t.Fatalf("ParseManifest(%q): schema_version %d with %d gaps", document, manifest.SchemaVersion, len(manifest.EventGaps))
		}
		for _, gap := range manifest.EventGaps {
			if gap.First < 1 || gap.Last < gap.First {
				t.Fatalf("ParseManifest(%q) accepted the gap %d-%d", document, gap.First, gap.Last)
			}
		}
		written, err := manifest.MarshalJSON()
		if err != nil {
			return // a value the writer refuses (a time out of range)
		}
		again, err := ParseManifest(written)
		if err != nil {
			t.Fatalf("ParseManifest(%q) wrote %s, which does not read back: %v", document, written, err)
		}
		a, _ := json.Marshal(manifest)
		b, _ := json.Marshal(again)
		if !bytes.Equal(a, b) {
			t.Fatalf("ParseManifest(%q) differs after a round trip:\n%s\n%s", document, a, b)
		}
	})
}

// FuzzMemberName: an artifact's recorded name is one safe path segment: clean
// names verbatim, everything else encoded under a marker no clean name can
// start with, so two names never share a path.
func FuzzMemberName(f *testing.F) {
	for _, seed := range []string{"report.csv", ".", "..", "", "a/b", "~x", "../../etc/passwd", "name with space", "\u00e9", strings.Repeat("a", 200), strings.Repeat("a", 201), strings.Repeat("\xff", 400), "a\x00b"} {
		f.Add(seed, "other")
	}
	f.Fuzz(func(t *testing.T, name, other string) {
		member := MemberName(name)
		if member == "" || member == "." || member == ".." || strings.ContainsAny(member, "/\\\x00") {
			t.Fatalf("MemberName(%q) = %q, which is not one safe path segment", name, member)
		}
		if MemberName(name) != member {
			t.Fatalf("MemberName(%q) is not deterministic", name)
		}
		if name != other && MemberName(other) == member {
			t.Fatalf("MemberName(%q) and MemberName(%q) are both %q", name, other, member)
		}
		if strings.HasPrefix(member, encodedMarker) == cleanMemberName.MatchString(name) && name != "." && name != ".." {
			t.Fatalf("MemberName(%q) = %q: encoded exactly when the name is not clean", name, member)
		}
	})
}
