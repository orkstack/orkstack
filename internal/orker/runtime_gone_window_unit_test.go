//go:build !integration

package orker

import "time"

// The unit tests' fake event streams stay open after every stop they say, as
// a daemon that stays up does, so each outside stop would wait the whole
// window for a stream that never ends. They wait a little instead; a test of
// the window sets its own (TestDockerRuntime_Wait_TellsARuntimeThatWentAway…),
// and the integration build keeps the real one for real daemons.
func init() { defaultRuntimeGoneWindow = 50 * time.Millisecond }
