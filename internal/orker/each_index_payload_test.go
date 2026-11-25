package orker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// A line index is asked for with an artifact's name, URI and metadata, which
// a container chose: in a sealed run the request is sealed, and the answer,
// which names the source again, and a refusal, whose text does, are too.
func TestIndexEachLinesRaw_ASealedRunsRequestAndAnswer(t *testing.T) {
	const canary = "canary-7f3a"
	key := taskKey(t, 3)
	scope := payloadcrypt.NewScope(key, payloadcrypt.RunContext("ork-1"))
	keys := staticTaskKeys{payloadcrypt.RunContext("ork-1"): key}
	in := &orkstackv1.IndexEachLinesInput{RunId: "ork-1", TaskId: "convert", Producer: "list", Name: canary,
		Source: &orkstackv1.ArtifactRef{Name: canary, Uri: "mem://" + canary, Metadata: map[string]string{"k": canary}}}
	dc := payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil)
	raw := func(s *payloadcrypt.Scope) converter.RawValue {
		p, err := dc.ToPayload(payloadcrypt.Seal(s, in))
		if err != nil {
			t.Fatal(err)
		}
		return converter.NewRawValue(p)
	}
	answer := &orkstackv1.IndexEachLinesResult{Count: 3, Index: &orkstackv1.ArtifactRef{Name: canary, Uri: "mem://" + canary}}
	refuse := &Failure{Reason: ReasonPayloadUnreadable, Message: canary}

	t.Run("sealed", func(t *testing.T) {
		out, err := indexRaw(context.Background(), keys, raw(scope), func(_ context.Context, got *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			if got.GetName() != canary || got.GetSource().GetMetadata()["k"] != canary {
				t.Errorf("the request read as %v", got)
			}
			return answer, refuse.TemporalError()
		})
		p, convErr := dc.ToPayload(out)
		if convErr != nil || !payloadcrypt.IsSealed(p) {
			t.Fatalf("the answer is written %v (%v), want it sealed", p, convErr)
		}
		f := payloadcrypt.NewFailureConverter(dc, true).ErrorToFailure(err)
		if strings.Contains(f.String(), canary) || f.GetMessage() != ReasonPayloadUnreadable {
			t.Fatalf("the refusal is written %v, want its reason alone", f)
		}
	})
	t.Run("sealed, no key source", func(t *testing.T) {
		_, err := indexRaw(context.Background(), nil, raw(scope), func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			t.Error("a request that does not open was indexed")
			return nil, nil
		})
		if reason, final := reasonOf(t, err); reason != ReasonPayloadEncryptionNotConfigured || !final {
			t.Fatalf("refused %s (non-retryable %v)", reason, final)
		}
	})
	t.Run("sealed for another run", func(t *testing.T) {
		other := payloadcrypt.NewScope(key, payloadcrypt.RunContext("ork-2"))
		both := staticTaskKeys{payloadcrypt.RunContext("ork-1"): key, payloadcrypt.RunContext("ork-2"): key}
		_, err := indexRaw(context.Background(), both, raw(other), func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			t.Error("a request sealed for another run was indexed")
			return nil, nil
		})
		if reason, _ := reasonOf(t, err); reason != ReasonPayloadUnreadable {
			t.Fatalf("refused %s", reason)
		}
	})
	t.Run("not sealed", func(t *testing.T) {
		boom := errors.New("boom")
		out, err := indexRaw(context.Background(), nil, raw(payloadcrypt.Unsealed()), func(context.Context, *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			return answer, boom
		})
		if out != any(answer) || err != boom {
			t.Fatalf("= %v, %v; want the answer and the error themselves", out, err)
		}
	})
}
