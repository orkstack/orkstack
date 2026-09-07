package config

import "fmt"

// Apply sets every variable this document mapped, via setenv, but ONLY when
// lookup reports the variable unset — a variable already present in the
// environment, even set to "", is never overwritten. This is the whole of
// the file's precedence rule (defaults < file < environment) and the reason
// every existing *FromEnv reader works unchanged: by the time one runs, the
// environment already holds whatever this call put there.
//
// It returns the variables it set, in Schema order, for a boot log or
// `--check` to report.
//
// A setenv failure is practically impossible (os.Setenv fails only for a
// malformed key or NUL byte, and every Env in Schema is a validated
// constant), but if setenv does return an error, Apply stops there and
// returns immediately: every variable set before the failing one stays set
// in the environment (Apply performs no rollback), and the ones after it
// are never attempted. The caller MUST treat a non-nil error as fatal and
// exit rather than continue booting against a partially-applied
// environment — this function does not, and cannot, undo what it already
// applied.
func (d *Document) Apply(lookup func(string) (string, bool), setenv func(string, string) error) ([]string, error) {
	var set []string
	for _, v := range Schema {
		rv, ok := d.values[v.Env]
		if !ok {
			continue
		}
		if _, present := lookup(v.Env); present {
			continue
		}
		if err := setenv(v.Env, rv.value); err != nil {
			return set, fmt.Errorf("config: set %s: %w", v.Env, err)
		}
		set = append(set, v.Env)
	}
	return set, nil
}
