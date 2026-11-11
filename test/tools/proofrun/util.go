package main

import (
	"strings"
)

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// multiFlag collects a repeatable flag.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, " ") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }
