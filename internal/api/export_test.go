package api

// SpecDifferenceForTest exposes specDifference to the external test package,
// so the automation planner's settings comparison can be held to it.
var SpecDifferenceForTest = specDifference

// NewRunIDForTest issues the next run id from the service's generator, so a
// test can prove a refused submission minted none.
func (s *Service) NewRunIDForTest() string { return s.newRunID() }
