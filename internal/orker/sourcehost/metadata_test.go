package sourcehost

import "testing"

// TestMetadata_SameOnEveryDelivery: the document holds nothing that changes
// between deliveries, so two deliveries stage identical bytes.
func TestMetadata_SameOnEveryDelivery(t *testing.T) {
	first, _ := ReadMessage([]byte(specMessage))
	second := first
	second.DeliveryCount, second.Seq = 7, 99
	ref := SourceRef{ID: "evs-4a1c9e0b7d2f6a35", Name: "order-created", Image: "img@sha256:77ab"}
	offer := Offer{BodyDigest: "9f2c", BodySize: 4}
	a, err := MetadataDocument(first, offer, ref)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := MetadataDocument(second, offer, ref)
	if string(a) != string(b) {
		t.Fatalf("two deliveries differ:\n%s\n%s", a, b)
	}
	want := `{"schemaVersion":1,"id":"jetstream:ORDERS:48213:1762853400123456789","subject":"orders.eu.created","publishedAt":"2026-11-11T09:30:00Z",` +
		`"body":{"sizeBytes":4,"sha256":"9f2c"},"attributes":{"Ce-Type":["com.acme.order.created"]},"binaryAttributes":{"k":"AAE="},"group":"",` +
		`"broker":{"stream":"ORDERS","streamSequence":"48213"},"eventSource":{"id":"evs-4a1c9e0b7d2f6a35","name":"order-created","image":"img@sha256:77ab"}}`
	if string(a) != want {
		t.Fatalf("document\n%s\nwant\n%s", a, want)
	}
}
