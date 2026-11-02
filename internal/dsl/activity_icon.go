package dsl

import (
	"errors"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Activity icons (docs/ACTIVITY_IMAGES.md, "The icon").
//
// An image MAY draw itself in one OCI image label, beside its interface. The
// label is a label of its own, never a word of the interface label: an engine
// that predates it reads the interface label alone and strictly, so a new
// word there would cost that engine the whole interface. The label holds a
// small SVG of an allowlisted subset (activity_icon_svg.go); the engine reads
// it from the same config blob as the interface and returns it as a tree,
// never as markup.

// IconLabel is the OCI image label that carries an icon's SVG.
const IconLabel = "org.orkstack.activity.icon"

// ParseIconLabel turns the label's SVG into the icon's tree. An empty value
// declares no icon (nil, nil): it is how an image built FROM an icon-bearing
// base clears the label it inherits. The error is the human-readable reason
// the icon was refused; ActivityInfo.icon_error is it through SafeReason.
// Pure.
func ParseIconLabel(raw string) (*orkstackv1.ActivityIcon, error) {
	if raw == "" {
		return nil, nil
	}
	return ParseIconSVG(raw)
}

// PackIconLabel is the label value that declares icon: its canonical SVG
// (CanonicalIconSVG), held to every rule the engine reads the label by and
// read back as the same icon, so pack never writes a label the engine would
// refuse or read differently. A nil icon packs to "", the value that
// declares none.
func PackIconLabel(icon *orkstackv1.ActivityIcon) (string, error) {
	if icon == nil {
		return "", nil
	}
	label := CanonicalIconSVG(icon)
	back, err := ParseIconSVG(label)
	if err != nil {
		return "", err
	}
	if !proto.Equal(back, icon) {
		return "", errors.New("does not read back as the icon it was packed from")
	}
	return label, nil
}
