package engine

import (
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// payloadToPlugin converts a proto payload into the dependency-free plugin
// payload used on the engine<->Orker seam.
func payloadToPlugin(p *orkstackv1.Payload) plugin.Payload {
	if p.GetData() == nil {
		return plugin.Payload{}
	}
	return plugin.Payload(p.GetData().AsMap())
}

// payloadFromPlugin converts a plugin payload back into proto form.
func payloadFromPlugin(p plugin.Payload) (*orkstackv1.Payload, error) {
	if len(p) == 0 {
		return &orkstackv1.Payload{}, nil
	}
	data, err := structpb.NewStruct(p)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return &orkstackv1.Payload{Data: data}, nil
}

// artifactsFromPlugin converts plugin artifact refs into proto refs.
func artifactsFromPlugin(refs []plugin.ArtifactRef) []*orkstackv1.ArtifactRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]*orkstackv1.ArtifactRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, &orkstackv1.ArtifactRef{
			Name:        ref.Name,
			Uri:         ref.URI,
			ContentType: ref.ContentType,
			SizeBytes:   ref.SizeBytes,
			Sha256:      ref.SHA256,
			Metadata:    ref.Metadata,
		})
	}
	return out
}

// artifactsToPlugin converts proto artifact refs into plugin refs.
func artifactsToPlugin(refs []*orkstackv1.ArtifactRef) []plugin.ArtifactRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]plugin.ArtifactRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, plugin.ArtifactRef{
			Name:        ref.GetName(),
			URI:         ref.GetUri(),
			ContentType: ref.GetContentType(),
			SizeBytes:   ref.GetSizeBytes(),
			SHA256:      ref.GetSha256(),
			Metadata:    ref.GetMetadata(),
		})
	}
	return out
}
