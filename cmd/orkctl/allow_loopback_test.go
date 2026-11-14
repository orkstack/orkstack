package main

import "github.com/orkstack/orkstack/internal/testsupport/webhooktest"

// The suites boot engines that register and deliver to httptest servers on
// loopback; this allows loopback in every engine they start.
func init() { webhooktest.AllowLoopbackInEngines() }
