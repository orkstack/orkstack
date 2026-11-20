package servicehost

import (
	"encoding/json/v2"
)

// SchemaVersion is the one version of every contract file this release
// reads and writes (14 §4.2, rule 3).
const SchemaVersion = 1

// StepHeader is what every step of a lane carries: its schema version and
// the exchange it belongs to.
type StepHeader struct {
	SchemaVersion int   `json:"schemaVersion"`
	Seq           int64 `json:"seq"`
}

// ReadHeader reads a step's header from the copy the Orker made. A higher
// schemaVersion is refused; a field the Orker does not know is ignored
// (rule 3). Bytes that are not one JSON object, a duplicate key, invalid
// UTF-8, a missing or non-positive seq are violations of name.
func ReadHeader(name string, raw []byte) (StepHeader, error) {
	var h struct {
		SchemaVersion *int   `json:"schemaVersion"`
		Seq           *int64 `json:"seq"`
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return StepHeader{}, violation(name, "not one JSON object of the profile's shape")
	}
	switch {
	case h.SchemaVersion == nil:
		return StepHeader{}, violation(name, "no schemaVersion")
	case *h.SchemaVersion != SchemaVersion:
		return StepHeader{}, violation(name, "schemaVersion %d; this Orker reads %d", *h.SchemaVersion, SchemaVersion)
	case h.Seq == nil || *h.Seq < 1:
		return StepHeader{}, violation(name, "no seq of 1 or more")
	}
	return StepHeader{SchemaVersion: *h.SchemaVersion, Seq: *h.Seq}, nil
}
