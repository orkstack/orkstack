package servicehost

import "math"

// AnswerBytes is the size of the answer filesystem of a service of that many
// lanes whose bodies are at most maxBody: each lane's next body is written beside
// the one it replaces. One too large for an int64 is the largest.
func AnswerBytes(lanes int32, maxBody int64) int64 {
	const room, base = 256 << 10, 1 << 20
	n := int64(max(lanes, 0))
	if n > 0 && maxBody > (math.MaxInt64-base)/n/2-room {
		return math.MaxInt64
	}
	return n*(2*max(maxBody, 0)+room) + base
}

// AnswerInodes is how many inodes it has.
func AnswerInodes(lanes int32) int { return 16*int(max(lanes, 0)) + 64 }
