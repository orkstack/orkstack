package api

import (
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestCacheIssues_ACachedTaskWithACredentialWarns: a credential is
// issued anew for each attempt and a memo hit runs none, so a cached task
// that declares one is warned about on tasks[i].credentials. Built from the
// proto.
func TestCacheIssues_ACachedTaskWithACredentialWarns(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "deploys", Tasks: []*orkstackv1.Task{
		{Id: "plain", Cache: &orkstackv1.CacheSpec{TtlSeconds: 3600}},
		{Id: "deploy", Cache: &orkstackv1.CacheSpec{TtlSeconds: 3600},
			Credentials: []*orkstackv1.CredentialRef{{Name: "aws", Grant: "deployer"}}},
		{Id: "uncached", Credentials: []*orkstackv1.CredentialRef{{Name: "aws", Grant: "deployer"}}},
	}}
	var found []string
	for _, issue := range cacheIssues(wf, 30*24*time.Hour) {
		if strings.HasSuffix(issue.GetField(), ".credentials") {
			found = append(found, issue.GetField())
			if issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
				!strings.Contains(issue.GetMessage(), `task "deploy"`) || !strings.Contains(issue.GetMessage(), "a cache hit issues no credential") {
				t.Errorf("issue = %v", issue)
			}
		}
	}
	if len(found) != 1 || found[0] != "tasks[1].credentials" {
		t.Fatalf("credential warnings on %v; want only tasks[1].credentials", found)
	}
}
