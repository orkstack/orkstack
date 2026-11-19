package orker

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// procLocksCases are /proc/locks bodies and the holders of 00:1a:870304
// (device 26, inode 870304) parseProcLocks must find in them.
var procLocksCases = []struct {
	name, body string
	want       []int
	wantErr    bool
}{
	{"the spike's line", "16: FLOCK  ADVISORY  WRITE 708077 00:1a:870304 0 EOF\n", []int{708077}, false},
	{"other files and a waiter", "1: FLOCK  ADVISORY  WRITE 4121772 00:59:48444562 0 EOF\n" +
		"2: POSIX  ADVISORY  READ 99 08:02:870304 0 EOF\n" +
		"3: FLOCK  ADVISORY  WRITE 708077 00:1a:870304 0 EOF\n" +
		"3: -> FLOCK  ADVISORY  WRITE 708078 00:1a:870304 0 EOF\n", []int{708077}, false},
	{"a holder in another pid namespace", "5: FLOCK  ADVISORY  WRITE 0 00:1a:870304 0 EOF\n", nil, false},
	{"an OFD lock with no pid", "4: OFDLCK ADVISORY  READ -1 00:1a:870304 0 EOF\n", nil, false},
	{"a byte range", "5: POSIX  ADVISORY  WRITE 12 00:1a:1 0 100\n", nil, false},
	{"empty", "", nil, false},
	{"a lease", "6: LEASE  ACTIVE    READ 7 00:1a:870304 0 EOF\n", nil, false},
	{"too few fields", "16: FLOCK  ADVISORY  WRITE 708077\n", nil, true},
	{"no colon after the id", "16 FLOCK ADVISORY WRITE 708077 00:1a:870304 0 EOF\n", nil, true},
	{"a pid that is not a number", "16: FLOCK ADVISORY WRITE abc 00:1a:870304 0 EOF\n", nil, true},
	{"a device that is not hex", "16: FLOCK ADVISORY WRITE 1 0g:1a:870304 0 EOF\n", nil, true},
	{"an inode that is not a number", "16: FLOCK ADVISORY WRITE 1 00:1a:x 0 EOF\n", nil, true},
	{"too many fields", "16: FLOCK ADVISORY WRITE 1 00:1a:870304 0 EOF extra\n", nil, true},
}

func TestParseProcLocks(t *testing.T) {
	for _, tc := range procLocksCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProcLocks(strings.NewReader(tc.body), 26, 870304)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if err == nil && !slices.Equal(got, tc.want) {
				t.Fatalf("holders = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestParseProcLocks_DeviceEncoding: the kernel's dev_t, split into the major
// and minor /proc/locks prints in hex, for a device past the 8-bit minor.
func TestParseProcLocks_DeviceEncoding(t *testing.T) {
	// major 0x103, minor 0x125 in Linux's dev_t encoding
	dev := uint64(0x25) | uint64(0x103)<<8 | uint64(0x100)<<12
	body := "1: FLOCK ADVISORY WRITE 9 103:125:5 0 EOF\n"
	got, err := parseProcLocks(strings.NewReader(body), dev, 5)
	if err != nil || !slices.Equal(got, []int{9}) {
		t.Fatalf("holders = %v, %v; want [9]", got, err)
	}
}

// FuzzParseProcLocks states the parser's properties: it never panics; an error
// is always errProcLocksMalformed; a body it accepts names only positive pids
// written in it; and a trailing newline changes nothing.
func FuzzParseProcLocks(f *testing.F) {
	for _, tc := range procLocksCases {
		f.Add(tc.body, uint64(26), uint64(870304))
	}
	f.Fuzz(func(t *testing.T, body string, dev, ino uint64) {
		got, err := parseProcLocks(strings.NewReader(body), dev, ino)
		if err != nil {
			if !errors.Is(err, errProcLocksMalformed) {
				t.Fatalf("an error that is not errProcLocksMalformed: %v", err)
			}
			return
		}
		for _, pid := range got {
			if pid <= 0 {
				t.Fatalf("holder pid %d", pid)
			}
			if !strings.Contains(body, fmt.Sprintf(" %d ", pid)) {
				t.Fatalf("holder %d is not in the body %q", pid, body)
			}
		}
		again, err := parseProcLocks(strings.NewReader(body+"\n"), dev, ino)
		if err != nil || !slices.Equal(again, got) {
			t.Fatalf("a trailing newline changed the answer: %v, %v against %v", again, err, got)
		}
	})
}

// majorMinorCases are mountinfo devices and the dev_t parseMajorMinor makes
// of them; a case with ok false is refused.
var majorMinorCases = []struct {
	in    string
	major uint64
	minor uint64
	ok    bool
}{
	{"0:30", 0, 30, true},
	{"8:2", 8, 2, true},
	{"259:65536", 259, 65536, true},
	{"4095:255", 4095, 255, true},
	{"4096:256", 4096, 256, true},
	{"1:1048575", 1, 1048575, true},
	{"4294967295:4294967295", 4294967295, 4294967295, true},
	{"", 0, 0, false},
	{"0", 0, 0, false},
	{"0:", 0, 0, false},
	{":3", 0, 0, false},
	{"0x1:3", 0, 0, false},
	{"+1:3", 0, 0, false},
	{"1:3:4", 0, 0, false},
	{"4294967296:0", 0, 0, false},
}

// TestParseMajorMinor: a mountinfo device becomes the dev_t whose halves
// devMajorMinor gives back, which is how /proc/locks' line is matched.
func TestParseMajorMinor(t *testing.T) {
	for _, tc := range majorMinorCases {
		dev, err := parseMajorMinor(tc.in)
		if !tc.ok {
			if err == nil {
				t.Errorf("parseMajorMinor(%q) = %d, want an error", tc.in, dev)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseMajorMinor(%q) = %v", tc.in, err)
			continue
		}
		if major, minor := devMajorMinor(dev); major != tc.major || minor != tc.minor {
			t.Errorf("parseMajorMinor(%q) = %d:%d, want %d:%d", tc.in, major, minor, tc.major, tc.minor)
		}
	}
}

// FuzzParseMajorMinor: whatever it accepts is two decimal numbers that
// devMajorMinor splits back out of the dev_t, as they were written.
func FuzzParseMajorMinor(f *testing.F) {
	for _, tc := range majorMinorCases {
		f.Add(tc.in)
	}
	f.Fuzz(func(t *testing.T, in string) {
		dev, err := parseMajorMinor(in)
		if err != nil {
			return
		}
		major, minor := devMajorMinor(dev)
		var maj, min uint64
		if _, err := fmt.Sscanf(in, "%d:%d", &maj, &min); err != nil || maj != major || min != minor {
			t.Fatalf("parseMajorMinor(%q) splits back to %d:%d", in, major, minor)
		}
	})
}

// FuzzDevMajorMinor: makeDev and devMajorMinor are inverses for any two
// 32-bit halves, so a mountinfo device and a /proc/locks node compare equal
// exactly when their halves do.
func FuzzDevMajorMinor(f *testing.F) {
	for _, tc := range majorMinorCases {
		f.Add(uint32(tc.major), uint32(tc.minor))
	}
	f.Fuzz(func(t *testing.T, major, minor uint32) {
		gotMajor, gotMinor := devMajorMinor(makeDev(uint64(major), uint64(minor)))
		if gotMajor != uint64(major) || gotMinor != uint64(minor) {
			t.Fatalf("devMajorMinor(makeDev(%d, %d)) = %d, %d", major, minor, gotMajor, gotMinor)
		}
	})
}
