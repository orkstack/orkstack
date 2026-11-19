package dsl

// WithTaskIdentity runs f with task identity available, as P13a's
// interpreter half will make it, and restores it after.
func WithTaskIdentity(f func()) {
	was := taskIdentityAvailable
	taskIdentityAvailable = true
	defer func() { taskIdentityAvailable = was }()
	f()
}
