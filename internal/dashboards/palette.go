package dashboards

// The kit's colours are the UI's own, from ui/src/tokens.css's dark theme
// (Grafana's default), so a status reads the same in both
// (TestDashboards_PaletteFromTokens holds each to its token).
const (
	colorOK      = "#5DBF82" // --ok-mark
	colorFail    = "#F27A70" // --fail-mark
	colorWait    = "#E4B04E" // --wait-fill
	colorRun     = "#6EA8F2" // --run
	colorCancel  = "#8C9DAA" // --cancel
	colorPending = "#64798B" // --pend-mark
	colorAccent  = "#E39967" // --accent
)

// paletteTokens maps each colour to the dark-theme token it is read from.
var paletteTokens = map[string]string{
	colorOK:      "--ok-mark",
	colorFail:    "--fail-mark",
	colorWait:    "--wait-fill",
	colorRun:     "--run",
	colorCancel:  "--cancel",
	colorPending: "--pend-mark",
	colorAccent:  "--accent",
}

// Thresholds every stat shares, each a fresh slice: the first step is the
// base colour, from below every value; each later one starts at its value.
func okWarnFail(warn, fail float64) []threshold {
	return []threshold{{0, colorOK}, {warn, colorWait}, {fail, colorFail}}
}

func zeroOKElseFail() []threshold { return []threshold{{0, colorOK}, {1, colorFail}} }

func neutral() []threshold { return []threshold{{0, colorRun}} }
