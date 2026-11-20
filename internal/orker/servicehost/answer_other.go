//go:build !linux

package servicehost

// AnswerDir is unsupported off Linux: openat2's resolve flags are the only
// reader the contract allows (14 §4.3), so such an Orker hosts no service.
type AnswerDir struct{}

// NewAnswerDir is ErrUnsupported off Linux.
func NewAnswerDir(int) (*AnswerDir, error) { return nil, ErrUnsupported }

// OpenAnswerDir is ErrUnsupported off Linux.
func OpenAnswerDir(string) (*AnswerDir, error) { return nil, ErrUnsupported }

// Close does nothing.
func (*AnswerDir) Close() error { return nil }

// ReadStep is ErrUnsupported off Linux.
func (*AnswerDir) ReadStep(string, int64) ([]byte, error) { return nil, ErrUnsupported }

// ReadStepID is ErrUnsupported off Linux.
func (*AnswerDir) ReadStepID(string, int64) ([]byte, StepID, error) {
	return nil, StepID{}, ErrUnsupported
}

// OpenContainerAnswer is ErrUnsupported off Linux.
func OpenContainerAnswer(int, string) (*AnswerDir, error) { return nil, ErrUnsupported }
