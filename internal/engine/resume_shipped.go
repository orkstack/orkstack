package engine

// ResumeShipped is whether a default build resumes runs. The change that does
// the last of ResumeBlockers sets it; cmd/ork wires resume by it
// (resumeSeam), and TestResumeShipped_OffWhileBlocked refuses it before.
const ResumeShipped = false

// ResumeBlockers are what resume still lacks.
var ResumeBlockers = []string{
	"the parts resume_parts.go still refuses, a concurrency limit and a force-canceled run",
	"orkctl, the dialog, and chaos and integration tests",
}
