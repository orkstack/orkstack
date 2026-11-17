package task

// ReasonArtifactReclaimed is the reason a read answers when the object it
// asks for is missing and the artifact GC finished with the run that
// produced it (docs/STORAGE.md, "Reclaiming space"): the bytes were deleted
// on purpose, and the recorded digest is all that is left.
const ReasonArtifactReclaimed = "ARTIFACT_RECLAIMED"
