// The authored documents' placeholders, and the one apiVersion they name.
//
// This module imports nothing, so a Bun unit test and a Playwright spec can
// both import it by relative path ($lib resolves only inside the SvelteKit
// build).

// The document format version every authored OrkStack document names (dsl.APIVersion).
// ui/test/authoredSchemas.test.ts holds it to the generated schemas' const.
export const API_VERSION = 'orkstack/v1';

// The Apply page's placeholder: a document that actually APPLIES. `uses:`,
// the only image-selecting field in the DSL, selects a first-party activity
// image. It mirrors examples/http-post.ork.yaml.
export const ORKFLOW_PLACEHOLDER = `apiVersion: ${API_VERSION}
kind: Orkflow
name: my-orkflow
version: "0.1.0"
description: Describe what this Orkflow does.

inputs:
  url:
    type: string
    default: "https://httpbin.org/get"

tasks:
  - id: fetch
    uses: http
    with:
      op: get
      url: "{{ .inputs.url }}"
      captureBodyAs: response
`;

// Admin → Automation's placeholder for one namespace's document. namespace is the
// name to show (namespaceLabel(ns)), so the default namespace reads "default".
export function automationPlaceholder(namespace: string): string {
	return `apiVersion: ${API_VERSION}\nkind: Automation\nnamespace: ${namespace}\n\ntriggers:\n  github-deploy:\n    orkflow: deploy`;
}
