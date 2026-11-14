package main

import (
	"testing"
)

const testOrkers = `orkers:
  - subject: cert:spiffe://acme.dev/orker/gpu-*
    pool: gpu
  - subject: serviceaccount:orker-main
    pool: main
    orker_ids: any
`

func TestAuthFromEnv_Orkers(t *testing.T) {
	c, err := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_ORKERS": testOrkers}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.orkers) != 2 || c.orkers[0].Pool != "gpu" || c.orkers[0].OrkerIDsAny || !c.orkers[1].OrkerIDsAny {
		t.Fatalf("orkers = %+v", c.orkers)
	}
	for name, doc := range map[string]string{
		"orker_ids other than any":  "orkers:\n  - subject: serviceaccount:o\n    pool: main\n    orker_ids: some\n",
		"an unknown key":            "orkers:\n  - subject: serviceaccount:o\n    pool: main\n    hosts: all\n",
		"a subject that is not one": "orkers:\n  - subject: robot\n    pool: main\n",
		"a pool that is not one":    "orkers:\n  - subject: serviceaccount:o\n    pool: Main!\n",
		"a second document":         "orkers: []\n---\norkers: []\n",
		"a merge key":               "base: &b {pool: main}\norkers:\n  - <<: *b\n    subject: serviceaccount:o\n",
	} {
		if _, err := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_ORKERS": doc})); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func FuzzParseOrkers(f *testing.F) {
	f.Add(testOrkers)
	f.Add("orkers: []\n")
	f.Add("orkers:\n  - subject: user:corp:x\n    pool: p\n    orker_ids: any\n")
	f.Fuzz(func(t *testing.T, doc string) {
		entries, err := parseOrkers([]byte(doc))
		if err != nil {
			return
		}
		// Whatever parses compiles: each entry is a subject and a pool.
		for _, e := range entries {
			if e.Subject == "" || e.Pool == "" {
				t.Fatalf("parsed an entry with no subject or pool: %+v", e)
			}
		}
	})
}
