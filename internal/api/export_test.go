package api

import (
	"context"
	"net/http"
	"time"
)

// SpecDifferenceForTest exposes specDifference to the external test package,
// so the automation planner's settings comparison can be held to it.
var SpecDifferenceForTest = specDifference

// NewRunIDForTest issues the next run id from the service's generator, so a
// test can prove a refused submission minted none.
func (s *Service) NewRunIDForTest() string { return s.newRunID() }

// TaskIsTerminalForTest asks taskIsTerminal for one task, the question a
// quiet log follower asks on every backoff step.
func (s *Service) TaskIsTerminalForTest(ctx context.Context, runID, namespace, taskID string) (bool, error) {
	return s.taskIsTerminal(ctx, TaskLogQuery{RunID: runID, Namespace: namespace, TaskID: taskID})
}

// EachMaxItemsOfForTest exposes eachMaxItemsOf: Config.EachMaxItems as the
// service reads it.
var EachMaxItemsOfForTest = eachMaxItemsOf

// MountReservedForTest mounts pattern the way NewMux mounts every API route.
func MountReservedForTest(mux *http.ServeMux, pattern string) {
	reservedMux{serveMux: mux}.Handle(pattern, http.NotFoundHandler())
}

// CapacityInventoryTTLForTest and CapacityWaitAfterForTest are the two
// durations InspectRun's capacity answer is bounded by, so a test can stand
// on either side of one without restating its value.
const (
	CapacityInventoryTTLForTest = capacityInventoryTTL
	CapacityWaitAfterForTest    = capacityWaitAfter
)

// SetInventoryTimerForTest replaces the timer that ends one read of the
// Orker inventory at its bound with one the test fires itself, so a read the
// fleet never finishes can be ended without waiting out the real bound. Call
// it before the first InspectRun.
func (s *Service) SetInventoryTimerForTest(afterFunc func(d time.Duration, f func()) (stop func() bool)) {
	s.inventory.afterFunc = afterFunc
}

// MostFailingForTest is GetStats' most-failing table, from a tally.
var MostFailingForTest = mostFailing
