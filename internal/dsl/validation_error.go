package dsl

// ValidationError carries the authored field path for a compile-time error.
// Error and Unwrap deliberately preserve the existing message and cause, so
// callers that only render errors retain their contract while API clients can
// attach the issue to a concrete editor field.
type ValidationError struct {
	Field string
	Err   error
}

func (e *ValidationError) Error() string { return e.Err.Error() }
func (e *ValidationError) Unwrap() error { return e.Err }
func validationError(field string, err error) error {
	if err == nil {
		return nil
	}
	return &ValidationError{Field: field, Err: err}
}
