package api

import (
	"context"
	"net/http"
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
