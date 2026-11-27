package main

import (
	"strings"
	"testing"
)

func TestActions_AreTheOrkerPolicyOnTheBucket(t *testing.T) {
	got, err := actions("orkstack-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 23 || got[0] != "Read:orkstack-artifacts/orkflows/*" ||
		got[len(got)-1] != "Read:orkstack-artifacts/metadata/run-context/*" {
		t.Fatalf("actions: %v", got)
	}
	for _, a := range got {
		if !strings.Contains(a, ":orkstack-artifacts/") || strings.Contains(a, ",") {
			t.Errorf("%s is not one action on the bucket", a)
		}
	}
}

func TestActions_RefusesWhatIsNoBucketName(t *testing.T) {
	for _, bad := range []string{"", "a", "Orkstack", "a,b", "a b", "bucket/x", "-ab", strings.Repeat("a", 64)} {
		if _, err := actions(bad); err == nil {
			t.Errorf("actions(%q) was not refused", bad)
		}
	}
}
