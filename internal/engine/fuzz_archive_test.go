package engine

import (
	"reflect"
	"sort"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// FuzzRunArchive_ABodyThatReadsIsOneTheWriterRenders: an archived run is read
// from object storage long after it was written, so a body that reads is a run
// with its events and its lost ranges as they were archived: writing what was
// read gives a body that reads as the same run, the same events in the same
// order and the same ranges. A body that does not read is refused naming a
// line, never partly read.
func FuzzRunArchive_ABodyThatReadsIsOneTheWriterRenders(f *testing.F) {
	run := &orkstackv1.Run{Id: "ork-1-1", OrkflowName: "nightly", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	events := []*orkstackv1.Event{{Sequence: 1, RunId: "ork-1-1"}, {Sequence: 2, RunId: "ork-1-1"}, {Sequence: 5, RunId: "ork-1-1"}}
	for _, gaps := range [][]EventGap{nil, {{First: 3, Last: 4, Reason: "missing"}}, {{First: 6, Last: 9, Reason: "altered"}, {First: 3, Last: 4, Reason: "missing"}}} {
		body, err := MarshalRunArchive(run, events, gaps...)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(body)
	}
	for _, seed := range []string{
		``, "\n\n", `{"kind":"run","run":{}}`, `{"kind":"run","run":{"id":"x"}}` + "\n" + `{"kind":"event","event":{"sequence":1}}`,
		`{"kind":"event","event":{}}`, `{"kind":"gap","gap":{"first":1,"last":0}}`, `{"kind":"gap"}`, `{"kind":"other"}`, `{`, `[]`, `{"kind":"run"}`,
		`{"kind":"run","run":{"id":"x"}}` + "\n" + `{"kind":"gap","gap":{"first":2,"last":3,"reason":"r"}}`, "{\"kind\":\"run\",\"run\":{\"id\":\"\xff\"}}",
		`{"kind":"run","run":{"id":"x"}}{"kind":"run","run":{"id":"y"}}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		run, events, err := UnmarshalRunArchive(body)
		gaps, gapErr := UnmarshalRunArchiveGaps(body)
		if err != nil {
			if run != nil || events != nil || err.Error() == "" {
				t.Fatalf("UnmarshalRunArchive(%q) = %v, %v, %v", body, run, events, err)
			}
			return
		}
		if run.GetId() == "" || gapErr != nil {
			return // the reader is more lenient than the writer about a run with no id and about a malformed gap line
		}
		written, err := MarshalRunArchive(run, events, gaps...)
		if err != nil {
			t.Fatalf("a body that read, %q, cannot be written: %v", body, err)
		}
		again, againEvents, err := UnmarshalRunArchive(written)
		againGaps, gapsErr := UnmarshalRunArchiveGaps(written)
		if err != nil || gapsErr != nil || !proto.Equal(again, run) || len(againEvents) != len(events) {
			t.Fatalf("an archive did not survive being written: %v, %v, %v", err, gapsErr, written)
		}
		for i := range events {
			if !proto.Equal(againEvents[i], events[i]) {
				t.Fatalf("event %d changed: %v became %v", i, events[i], againEvents[i])
			}
		}
		sorted := append([]EventGap(nil), gaps...)
		sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].First < sorted[j].First })
		if len(sorted) != len(againGaps) || len(sorted) > 0 && !reflect.DeepEqual(sorted, againGaps) {
			t.Fatalf("the lost ranges %v were written and read as %v", sorted, againGaps)
		}
	})
}

// FuzzDecodeSavedObjects: a switch snapshot and an earlier generation's event
// segment are read back only against the reference that names them. Bytes
// dressed in an honest reference are refused by name or read as a message;
// a reference that claims fewer decoded bytes than the object holds, a byte
// changed, and a missing reference are refused by name.
func FuzzDecodeSavedObjects(f *testing.F) {
	f.Add([]byte(nil), uint8(0), uint32(0))
	f.Add([]byte{0x0a, 0x00}, uint8(1), uint32(0))
	f.Add([]byte{0x0a, 0x03, 'a', 'b', 'c', 0x10, 0x01}, uint8(2), uint32(1))
	f.Add([]byte{0xff, 0xff, 0xff}, uint8(3), uint32(2))
	f.Add([]byte("junk that is not a protobuf"), uint8(4), uint32(0))
	f.Fuzz(func(t *testing.T, plain []byte, mode uint8, at uint32) {
		member := gzipForTest(t, plain)
		honest := &orkstackv1.ContinuationObject{
			SizeBytes: int64(len(member)), RawBytes: int64(len(plain)), Sha256: artifacts.ContinuationSHA256(member),
		}
		decoders := map[string]func([]byte, *orkstackv1.ContinuationObject) error{
			"snapshot": func(data []byte, obj *orkstackv1.ContinuationObject) error {
				_, err := DecodeSwitchSnapshot(data, obj)
				return err
			},
			"segment": func(data []byte, obj *orkstackv1.ContinuationObject) error {
				_, err := DecodeEventSegment(data, obj)
				return err
			},
		}
		for name, decode := range decoders {
			switch mode % 5 {
			case 0: // honest: refused by name, or read
				if err := decode(member, honest); err != nil {
					requireNamedRefusal(t, name+" over arbitrary bytes", err)
				}
			case 1: // no reference
				requireNamedRefusal(t, name+" with no reference", decode(member, nil))
			case 2: // a byte changed, the digest made to agree
				altered := append([]byte(nil), member...)
				altered[int(at)%len(altered)] ^= 0x01
				lied := &orkstackv1.ContinuationObject{SizeBytes: int64(len(altered)), RawBytes: honest.RawBytes, Sha256: artifacts.ContinuationSHA256(altered)}
				if err := decode(altered, lied); err != nil {
					requireNamedRefusal(t, name+" over an altered object", err)
				}
			case 3: // fewer decoded bytes recorded than the object holds
				if len(plain) == 0 {
					return
				}
				short := proto.Clone(honest).(*orkstackv1.ContinuationObject)
				short.RawBytes--
				requireNamedRefusal(t, name+" over a larger object than recorded", decode(member, short))
			case 4: // the stored bytes are not the ones the digest names
				altered := append([]byte(nil), member...)
				altered[int(at)%len(altered)] ^= 0x01
				requireNamedRefusal(t, name+" over bytes its digest does not name", decode(altered, honest))
			}
		}
	})
}
