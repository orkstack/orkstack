// Package schedules holds the one thing about a schedule that is not the
// Temporal Schedule itself: the shape of its opaque id.
//
// A schedule's every other field lives in the cluster (internal/engine's
// TemporalScheduler); this package exists so the id generator sits beside the
// pattern that validates it, on pkg/triggers' pattern, without teaching
// pkg/triggers a second noun. Nothing parses an id or derives meaning from it.
package schedules

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	// IDPrefix tags a schedule id, the way "trg-" tags a trigger's.
	IDPrefix = "sch-"
	// idBytes is the id's random half: 8 bytes, 16 hex characters.
	idBytes = 8
)

// idPattern is exactly what NewID produces: the prefix and 16 lowercase hex
// characters.
var idPattern = regexp.MustCompile(`^` + IDPrefix + `[0-9a-f]{16}$`)

// ValidID reports whether id is shaped like a generated schedule id.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// NewID returns a fresh schedule id: 8 bytes from crypto/rand, hex encoded.
func NewID() (string, error) {
	buf := make([]byte, idBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("schedules: generate id: %w", err)
	}
	return IDPrefix + hex.EncodeToString(buf), nil
}

// ValidTimezone reports why name is not a time zone of the IANA database, which
// is the only kind a schedule's spec.timezone may name. time.LoadLocation also
// reads "Local" as the zone of whatever machine it runs on, so a schedule
// created with it would fire at a different instant on each host that read it.
func ValidTimezone(name string) error {
	_, err := LoadTimezone(name)
	return err
}

// LoadTimezone is ValidTimezone that also returns the zone, so a caller that
// needs both loads the zone database once. Every OrkStack field that names a
// zone is read through it — a schedule's spec.timezone and a stats series'
// timezone — so the two can never accept different names.
func LoadTimezone(name string) (*time.Location, error) {
	if name == "" {
		return nil, errors.New("is empty")
	}
	if name == "Local" {
		return nil, errors.New(`"Local" is the zone of the machine that reads it, not a zone of the IANA database`)
	}
	return time.LoadLocation(name)
}
