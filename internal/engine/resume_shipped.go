package engine

// ResumeShipped is whether a default build resumes runs. The change that does
// the last of ResumeBlockers sets it; cmd/ork wires resume by it
// (resumeSeam), and TestResumeShipped_OffWhileBlocked refuses it before.
const ResumeShipped = false

// ResumeBlockers are what a resumed run still lacks. A resume without them
// would lose the run's second ending: an archive keeps one body a run, which
// is the first ending's.
var ResumeBlockers = []string{
	"archive: a body for each ending of a run, which the sweep and retirement ask for",
	"readers: the events a resume set aside, read from the earlier ending",
	"lists: the index rows of an ending that was set aside are retired",
	"artifact GC: the resume records are asked, not NoResumes",
	"retirement: the execution a resume set aside is retired with its run",
}
