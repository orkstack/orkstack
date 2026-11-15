package temporalconn

import "context"

// APIKeyForTest is the key s would send now.
func APIKeyForTest(s Security, ctx context.Context) (string, error) {
	if s.apiKey == nil {
		return "", nil
	}
	return s.apiKey(ctx)
}

// HoldALookForTest marks the API key's watch as in the middle of a look
// that has not returned, started now.
func HoldALookForTest(s Security) {
	w := s.keyWatch
	w.mu.Lock()
	defer w.mu.Unlock()
	w.busy, w.busySince = true, w.o.Now()
}
