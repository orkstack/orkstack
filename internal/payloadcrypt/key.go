// Package payloadcrypt seals what OrkStack hands Temporal for a run whose
// namespace selects a payload key (payload encryption, E105).
//
// Every value OrkStack hands the SDK is wrapped in Seal or Plain. For a run
// that is not sealed both return the value itself, so the SDK converts
// exactly what it converted before. For a sealed run, Seal returns a value
// the Converter turns into a payload encrypted under the run tree's key with
// AES-256-GCM, its context ("run:<id>" or "schedule:<id>") in the AAD.
//
// A run's key travels with the run: wrapped under an engines' payload key
// (a file every engine holds) in the run's plaintext memo, so workflow code
// opens it with arithmetic on bytes the history holds and calls nothing.
//
// Only the standard library's crypto is used: crypto/aes and crypto/cipher
// (AES-256-GCM), crypto/hkdf (HKDF-SHA256) and crypto/rand.
package payloadcrypt

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

// KeySize is the size of every key of this package: 256 bits.
const KeySize = 32

// Key is a 256-bit key. Every printed form of it is its id, so a key held in
// an error or a logged struct never shows its bytes.
type Key struct {
	id string
	b  [KeySize]byte
}

// NewKey copies raw, which must be exactly KeySize bytes.
func NewKey(raw []byte) (*Key, error) {
	if len(raw) != KeySize {
		return nil, fmt.Errorf("payloadcrypt: a key is %d bytes, not %d", KeySize, len(raw))
	}
	zero := true
	for _, b := range raw {
		zero = zero && b == 0
	}
	if zero {
		return nil, errors.New("payloadcrypt: a key of all zero bytes")
	}
	k := &Key{}
	copy(k.b[:], raw)
	sum := sha256.Sum256(raw)
	k.id = hex.EncodeToString(sum[:])[:16]
	return k, nil
}

// ID is the first 16 hex digits of the key's SHA-256.
func (k *Key) ID() string {
	if k == nil {
		return ""
	}
	return k.id
}

// String prints the id only.
func (k Key) String() string { return "payloadcrypt.Key(" + k.id + ")" }

// GoString prints the id only.
func (k Key) GoString() string { return k.String() }

// Format prints the id for every verb, so %x or %d cannot show the bytes.
func (k Key) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(k.String())) }

// LoadKeyFile reads a regular file of exactly KeySize bytes.
func LoadKeyFile(path string) (*Key, error) {
	f, err := os.Open(path) // an operator's key file, named by configuration
	if err != nil {
		return nil, fmt.Errorf("payloadcrypt: read key file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("payloadcrypt: read key file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("payloadcrypt: key file %s is not a regular file", path)
	}
	raw, err := io.ReadAll(io.LimitReader(f, KeySize+1))
	if err != nil {
		return nil, fmt.Errorf("payloadcrypt: read key file: %w", err)
	}
	k, err := NewKey(raw)
	clear(raw)
	if err != nil {
		return nil, fmt.Errorf("payloadcrypt: key file %s: %w", path, err)
	}
	return k, nil
}

// EngineKeys is an engine's list of engines' payload keys
// (payload_encryption.engine_key_files), in list order. Every key unwraps;
// the first that every engine seen holds wraps new runs.
type EngineKeys struct {
	order []*Key
	byID  map[string]*Key
}

// ErrNoCommonEngineKey is returned when no key of this engine's list is held
// by every engine seen: the admission is refused.
var ErrNoCommonEngineKey = errors.New("ENGINES_NO_PAYLOAD_ENCRYPTION: no engines' payload key is held by every engine")

// NewEngineKeys builds a list; it refuses an empty list and two entries
// holding one key.
func NewEngineKeys(keys ...*Key) (*EngineKeys, error) {
	if len(keys) == 0 {
		return nil, errors.New("payloadcrypt: no engines' payload key")
	}
	e := &EngineKeys{byID: make(map[string]*Key, len(keys))}
	for _, k := range keys {
		if k == nil {
			return nil, errors.New("payloadcrypt: a nil engines' payload key")
		}
		if _, dup := e.byID[k.id]; dup {
			return nil, fmt.Errorf("payloadcrypt: engines' payload key %s is listed twice", k.id)
		}
		e.byID[k.id] = k
		e.order = append(e.order, k)
	}
	return e, nil
}

// LoadEngineKeys reads each file of paths, in order.
func LoadEngineKeys(paths []string) (*EngineKeys, error) {
	keys := make([]*Key, 0, len(paths))
	for _, p := range paths {
		k, err := LoadKeyFile(p)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return NewEngineKeys(keys...)
}

// IDs are the ids this engine holds, in list order: what it advertises.
func (e *EngineKeys) IDs() []string {
	if e == nil {
		return nil
	}
	ids := make([]string, len(e.order))
	for i, k := range e.order {
		ids[i] = k.id
	}
	return ids
}

// Get is the key of id, if this engine holds it.
func (e *EngineKeys) Get(id string) (*Key, bool) {
	if e == nil {
		return nil, false
	}
	k, ok := e.byID[id]
	return k, ok
}

// WrapKey is the first key of this engine's list that every peer lists.
// peers must hold the advertised ids of every engine seen and of every
// process that could take the run's event publishes: a key wraps nothing
// until all of them hold it. With no peers it is this engine's first key.
func (e *EngineKeys) WrapKey(peers [][]string) (*Key, error) {
	if e == nil {
		return nil, ErrNoCommonEngineKey
	}
	held := make([]map[string]bool, len(peers))
	for i, ids := range peers {
		held[i] = make(map[string]bool, len(ids))
		for _, id := range ids {
			held[i][id] = true
		}
	}
next:
	for _, k := range e.order {
		for _, h := range held {
			if !h[k.id] {
				continue next
			}
		}
		return k, nil
	}
	return nil, ErrNoCommonEngineKey
}
