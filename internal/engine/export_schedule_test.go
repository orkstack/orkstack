package engine

// The schedule conversions, exported to this package's external tests only,
// so the tests can walk them value by value against the real enumspb
// constants (orkflow-schedules §3.1.1).
var (
	SpecOverlapToTemporal     = specOverlapToTemporal
	OverrideOverlapToTemporal = overrideOverlapToTemporal
	SpecToTemporal            = specToTemporal
)
