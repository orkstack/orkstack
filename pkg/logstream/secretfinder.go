package logstream

// NamedValue is one value a SecretFinder looks for, under the name a finding
// reports.
type NamedValue struct {
	Name  string
	Value []byte
}

// SecretFinder reports whether a document holds one of a set of values, in
// any of the spellings a masker masks (MaskForms), and which. It is the
// check a task's result is held to, built for that check alone and never
// shared with a stream's masker. It is immutable once built.
type SecretFinder struct {
	match *matcher
	// owner is the name of the first value that produces each form.
	owner map[string]string
}

// NewSecretFinder builds a finder over values' forms. A value shorter than
// MaskMinFormBytes has no form and is never found. A set past MaskMaxForms
// or MaskMaxFormBytes is refused whole with a *MaskCapacityError: a check
// that cannot hold every value must fail, never pass by looking for fewer.
func NewSecretFinder(values []NamedValue) (*SecretFinder, error) {
	owner := map[string]string{}
	bytes := 0
	forms := [][]byte{}
	for _, v := range values {
		for _, f := range MaskForms(v.Value) {
			key := string(f)
			if _, ok := owner[key]; ok {
				continue
			}
			owner[key] = v.Name
			bytes += len(f)
			forms = append(forms, f)
		}
	}
	if len(forms) > MaskMaxForms || bytes > MaskMaxFormBytes {
		return nil, &MaskCapacityError{Forms: len(forms), Bytes: bytes, MaxForms: MaskMaxForms, MaxBytes: MaskMaxFormBytes}
	}
	return &SecretFinder{match: newMatcher(forms), owner: owner}, nil
}

// Empty reports whether the finder has no form: nothing can be found.
func (f *SecretFinder) Empty() bool { return f == nil || f.match.empty() }

// Find reports the name of the value whose form ends first in p, and whether
// one does.
func (f *SecretFinder) Find(p []byte) (string, bool) {
	if f.Empty() {
		return "", false
	}
	state := int32(0)
	for i, b := range p {
		state = f.match.step(state, b)
		if n := f.match.nodes[state].out; n > 0 {
			// out is the length of a form that ends here: a suffix of the
			// bytes read, so it is exactly a registered form.
			if name, ok := f.owner[string(p[i+1-int(n):i+1])]; ok {
				return name, true
			}
		}
	}
	return "", false
}
