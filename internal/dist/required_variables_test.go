package dist

import (
	"slices"
	"testing"
)

// TestRequiredVariables_BothRequiredForms: compose refuses to start without a
// variable written ${V:?message} or ${V?message}, so the bundle's .env must
// set both; a comment naming one requires nothing.
func TestRequiredVariables_BothRequiredForms(t *testing.T) {
	t.Parallel()
	compose := []byte("image: ${ORKSTACK_A_IMAGE:?set it}\n" +
		"image: ${ORKSTACK_B_IMAGE?set it}\n" +
		"x: ${ORKSTACK_A_IMAGE:?again}\n" +
		"y: ${ORKSTACK_PORT-8080}\n" +
		"  # ${ORKSTACK_C_IMAGE:?a comment}\n")
	got := requiredVariables(compose)
	if want := []string{"ORKSTACK_A_IMAGE", "ORKSTACK_B_IMAGE"}; !slices.Equal(got, want) {
		t.Fatalf("requiredVariables = %q, want %q", got, want)
	}
}
