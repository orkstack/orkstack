package artifacts

import (
	"context"
	"errors"
	"testing"
)

const testDeliveryID = "dlv-20261108T120000.000000000Z-0011223344556677"

// TestDeliveryBodies_StoreReadAndDelete: a body is stored at its content
// address, read back verified on both backends, refused when altered, and
// deleted idempotently.
func TestDeliveryBodies_StoreReadAndDelete(t *testing.T) {
	ctx := context.Background()
	for name, b := range blobBackends(t) {
		t.Run(name, func(t *testing.T) {
			store := &BlobDeliveryBodyStore{blobBase{b}}
			body := []byte(`{"runId":"ork-1","run":{}}`)
			sum, err := store.PutDeliveryBody(ctx, testDeliveryID, body)
			if err != nil {
				t.Fatalf("PutDeliveryBody: %v", err)
			}
			if again, err := store.PutDeliveryBody(ctx, testDeliveryID, body); err != nil || again != sum {
				t.Fatalf("a retried PutDeliveryBody = %q, %v; want the same digest", again, err)
			}
			got, err := store.GetDeliveryBody(ctx, testDeliveryID, sum)
			if err != nil || string(got) != string(body) {
				t.Fatalf("GetDeliveryBody = %q, %v", got, err)
			}
			key, _ := DeliveryBodyKey(testDeliveryID, sum)
			if want := DeliveryBodyPrefix + "/" + testDeliveryID + "/" + sum + ".json"; key != want {
				t.Fatalf("key = %q, want %q", key, want)
			}
			if err := b.put(ctx, key, "", []byte("tampered")); err != nil {
				t.Fatal(err)
			}
			if _, err := store.GetDeliveryBody(ctx, testDeliveryID, sum); !errors.Is(err, ErrDeliveryBodyAltered) {
				t.Fatalf("an altered body = %v, want ErrDeliveryBodyAltered", err)
			}
			if _, err := store.PutDeliveryBody(ctx, testDeliveryID, body); err == nil {
				t.Fatal("PutDeliveryBody accepted other bytes already at the key")
			}
			for range 2 {
				if err := store.DeleteDeliveryBody(ctx, testDeliveryID, sum); err != nil {
					t.Fatalf("DeleteDeliveryBody: %v", err)
				}
			}
			if _, err := store.GetDeliveryBody(ctx, testDeliveryID, sum); !errors.Is(err, ErrNotFound) {
				t.Fatalf("a deleted body = %v, want ErrNotFound", err)
			}
			if _, err := DeliveryBodyKey("../escape", sum); err == nil {
				t.Fatal("DeliveryBodyKey accepted a delivery id that is not one")
			}
			if _, err := DeliveryBodyKey(testDeliveryID, "not-hex"); err == nil {
				t.Fatal("DeliveryBodyKey accepted a digest that is not one")
			}
		})
	}
}
