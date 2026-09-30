package engine

// RunReadClient is the reader's narrowed cluster seam, exported to this
// package's external tests only, so an integration test can wrap the real SDK
// client and count what the reader asks the cluster.
type RunReadClient = runReadClient

// NewRunReaderOver is newRunReader for external tests: a reader over any
// RunReadClient, with the default data converter.
func NewRunReaderOver(c RunReadClient) *TemporalRunReader { return newRunReader(c, nil) }
