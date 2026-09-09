package triggers

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
)

// The id and secret shapes. They mirror the webhook registry's: an id is a
// short prefixed hex token nothing treats as confidential, and a secret is 32
// bytes straight from crypto/rand.
const (
	// IDPrefix tags a trigger id, the way "wh-" tags a webhook's.
	IDPrefix = "trg-"
	// idBytes is the id's random half: 8 bytes, 16 hex characters.
	idBytes = 8
	// secretBytes is the bearer token's length. 32 bytes from crypto/rand is
	// why a fast hash is the right one to store it under: there is no
	// low-entropy space for a brute force to search, so a deliberately slow
	// KDF would only cost every caller latency on every invoke.
	secretBytes = 32
	// saltBytes is the per-trigger salt. It is not secret; it exists so one
	// stolen digest cannot be tested against another deployment's records.
	saltBytes = 16
	// CreateAttempts bounds how many fresh ids a create tries before it gives
	// up. A collision needs two identical 8-byte draws, so reaching the bound
	// means the random source is broken, not that the space is full.
	CreateAttempts = 8
)

// ErrIDCollision reports that repeated fresh ids all collided with a stored
// record. Every backend returns it, so the condition reads the same whichever
// one is wired.
var ErrIDCollision = errors.New("triggers: repeated id collision")

// idPattern is the id shape every storage backend validates before it touches
// a key, and the invoke adapter checks before it looks anything up. It lives
// beside the generator that produces it so the two cannot drift.
var idPattern = regexp.MustCompile(`^` + IDPrefix + `[0-9a-f]{16,}$`)

// ValidID reports whether id is shaped like a generated trigger id. An id that
// is not cannot name a stored record, so a caller answers "not found" rather
// than reporting a storage fault.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// NewID returns a fresh trigger id.
func NewID() (string, error) { return randomHex(IDPrefix, idBytes) }

// NewSecret returns a fresh bearer token. It is disclosed exactly once, in the
// creation response, and never stored.
func NewSecret() (string, error) { return randomHex("", secretBytes) }

// NewSalt returns a fresh per-trigger salt.
func NewSalt() ([]byte, error) {
	salt := make([]byte, saltBytes)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("triggers: generate salt: %w", err)
	}
	return salt, nil
}

// HashSecret returns sha256(salt || secret), which is the only form of a
// trigger's secret that is ever written down.
func HashSecret(salt []byte, secret string) []byte {
	digest := sha256.New()
	digest.Write(salt)
	digest.Write([]byte(secret))
	return digest.Sum(nil)
}

// VerifySecret reports whether presented is the secret record was created
// with. The comparison is constant time: the digest is the one thing an
// attacker holding an id would otherwise be able to search a byte at a time.
//
// A record with no verification material never verifies, and neither does an
// empty presented token, so a half-written record cannot authenticate anyone.
func VerifySecret(record Trigger, presented string) bool {
	if presented == "" || len(record.SecretSalt) == 0 || len(record.SecretHash) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare(record.SecretHash, HashSecret(record.SecretSalt, presented)) == 1
}

// randomHex returns prefix followed by size bytes of crypto-random hex.
func randomHex(prefix string, size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("triggers: generate token: %w", err)
	}
	return prefix + hex.EncodeToString(buf), nil
}
