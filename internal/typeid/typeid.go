// Package typeid issues the time-ordered identifiers a run id and a minted
// Orkflow version are made of: a UUIDv7 in the lowercase base32 of the TypeID
// specification, 26 characters over a fixed alphabet with no separator.
//
// The encoding only writes. Nothing in OrkStack may parse an identifier or read
// a meaning out of its parts (AGENTS.md §7), so this package offers no decoder.
package typeid

import (
	"encoding/binary"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Length is how many characters New returns.
const Length = 26

// alphabet is Crockford's base32 in lowercase, the TypeID alphabet. It ascends
// in byte order, so fixed-length strings compare the way their values do: an id
// issued later sorts after one issued earlier in the same process.
const alphabet = "0123456789abcdefghjkmnpqrstvwxyz"

// New issues the next identifier. It is safe for concurrent use, and every id a
// process issues sorts after the one before it, whatever the wall clock does:
// uuid.NewV7 takes its timestamp from one process-wide counter that only moves
// forward (getV7Time in github.com/google/uuid). Each id also carries 62 random
// bits, so two processes differ without any coordination.
//
// It runs in the API process, never in workflow code: it reads the clock and
// the system's randomness.
func New() string {
	id, err := uuid.NewV7()
	if err != nil {
		// crypto/rand has not returned an error since Go 1.24, and library
		// code must not panic, so the fallback still has to keep two
		// processes on one host apart.
		id = fallbackV7(time.Now(), os.Getpid(), fallbackCounter.Add(1))
	}
	return encode(id)
}

// fallbackCounter separates the ids one process issues through fallbackV7.
var fallbackCounter atomic.Uint32

// fallbackV7 builds a well-formed UUIDv7 with no randomness: the millisecond
// clock, then what distinguishes this id, the sub-millisecond clock, the
// process and a counter, in the bits randomness would have filled.
func fallbackV7(now time.Time, pid int, counter uint32) uuid.UUID {
	var id uuid.UUID
	millis := uint64(now.UnixMilli())
	binary.BigEndian.PutUint64(id[:8], millis<<16)
	binary.BigEndian.PutUint16(id[6:8], 0x7000|uint16(now.Nanosecond())&0x0fff)
	binary.BigEndian.PutUint32(id[8:12], 0x80000000|uint32(pid)&0x3fffffff)
	binary.BigEndian.PutUint32(id[12:16], counter)
	return id
}

// encode renders 128 bits as 26 base32 characters, most significant first. The
// 130 bits the characters hold leave two leading zero bits, so the first
// character is never above '7'.
func encode(id uuid.UUID) string {
	high := binary.BigEndian.Uint64(id[:8])
	low := binary.BigEndian.Uint64(id[8:])
	var out [Length]byte
	for i := Length - 1; i >= 0; i-- {
		out[i] = alphabet[low&31]
		low = low>>5 | high<<59
		high >>= 5
	}
	return string(out[:])
}
