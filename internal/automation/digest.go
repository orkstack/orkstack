package automation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"

	"github.com/orkstack/orkstack/internal/enumname"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// digestInput is what a plan digest covers: the namespace and, for each
// actionable step in list order, which object changes and how. It is what a
// reviewer read, and nothing they did not: no carried request (a namespace
// etag, a trigger id already in `id`), no UNCHANGED, UNDECLARED or UNMANAGED
// step, no note and no issue.
type digestInput struct {
	Namespace string       `json:"namespace"`
	Steps     []digestStep `json:"steps"`
}

type digestStep struct {
	Action  string      `json:"action"` // CREATE, UPDATE, DELETE
	Kind    string      `json:"kind"`
	Name    string      `json:"name"`
	ID      string      `json:"id"`
	Changes [][3]string `json:"changes"` // field, before, after
}

// Digest is PlanAutomationResponse.plan_digest for these steps:
// "sha256:<64 lowercase hex>". With no actionable step it is still defined,
// so "no changes" is pinnable too.
func Digest(namespace string, steps []*orkstackv1.AutomationStep) string {
	in := digestInput{Namespace: artifacts.ResolveNamespace(namespace), Steps: []digestStep{}}
	for _, step := range steps {
		if !Actionable(step.GetAction()) {
			continue
		}
		entry := digestStep{
			Action:  enumname.Of(step.GetAction()),
			Kind:    enumname.Of(step.GetKind()),
			Name:    step.GetName(),
			ID:      step.GetId(),
			Changes: [][3]string{},
		}
		for _, change := range step.GetChanges() {
			entry.Changes = append(entry.Changes, [3]string{change.GetField(), change.GetBefore(), change.GetAfter()})
		}
		in.Steps = append(in.Steps, entry)
	}
	var buf bytes.Buffer
	_ = stablejson.Encode(&buf, in, stablejson.Unescaped) // a struct of strings cannot fail to encode
	sum := sha256.Sum256(buf.Bytes())
	return "sha256:" + hex.EncodeToString(sum[:])
}
