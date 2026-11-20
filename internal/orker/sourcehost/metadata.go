package sourcehost

import (
	"encoding/base64"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// SourceRef names a source in its messages' metadata documents.
type SourceRef struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Image string `json:"image"`
}

// metadataDocument is a message's metadata document (02 §1.3): only what is
// the same on every delivery of the message, so two deliveries write
// identical bytes.
type metadataDocument struct {
	SchemaVersion    int                 `json:"schemaVersion"`
	ID               string              `json:"id"`
	Subject          string              `json:"subject"`
	PublishedAt      string              `json:"publishedAt,omitzero"`
	Body             metadataBody        `json:"body"`
	Attributes       map[string][]string `json:"attributes"`
	BinaryAttributes map[string]string   `json:"binaryAttributes"`
	Group            string              `json:"group"`
	Broker           map[string]string   `json:"broker"`
	EventSource      SourceRef           `json:"eventSource"`
}

type metadataBody struct {
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
}

// MetadataDocument is the canonical metadata document of msg as offered.
func MetadataDocument(msg HandedMessage, offer Offer, src SourceRef) ([]byte, error) {
	doc := metadataDocument{
		SchemaVersion: 1, ID: msg.ID, Subject: msg.Subject,
		Body:       metadataBody{SizeBytes: offer.BodySize, SHA256: offer.BodyDigest},
		Attributes: msg.Attributes, BinaryAttributes: map[string]string{}, Group: msg.Group, Broker: msg.Broker,
		EventSource: src,
	}
	if doc.Attributes == nil {
		doc.Attributes = map[string][]string{}
	}
	if doc.Broker == nil {
		doc.Broker = map[string]string{}
	}
	for k, v := range msg.Binary {
		doc.BinaryAttributes[k] = base64.StdEncoding.EncodeToString(v)
	}
	if !msg.PublishedAt.IsZero() {
		doc.PublishedAt = msg.PublishedAt.UTC().Format(time.RFC3339Nano)
	}
	return stablejson.Marshal(doc)
}
