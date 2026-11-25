package provider

// SetAfterWrite installs Dir's test hook on the directory of credential name,
// once the engine created it: it runs before `current` moves, and an error it
// returns is a write fault there.
func SetAfterWrite(e *Engine, name string, hook func() error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, c := range e.creds {
		if c.spec.Name == name && c.dir != nil {
			c.dir.afterWrite = hook
		}
	}
}

// SetDirAfterWrite installs the hook on a Dir.
func SetDirAfterWrite(d *Dir, hook func() error) { d.afterWrite = hook }

// RemoveTree removes path whatever its modes, as the attempt's clean-up does.
func RemoveTree(path string) error { return removeTree(path) }
