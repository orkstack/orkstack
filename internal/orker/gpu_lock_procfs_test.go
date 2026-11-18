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
