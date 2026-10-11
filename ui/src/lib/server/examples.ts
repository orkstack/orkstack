import { exampleSources } from '$lib/examples';

// The example gallery is the repo's own `examples/` directory, read at build
// time. Vite resolves `import.meta.glob` relative to this file and inlines the
// `?raw` contents into the SERVER bundle only ($lib/server can never reach a
// client chunk), so `examples/` stays the single source of truth: no copy step
// to forget, no engine endpoint, and the browser downloads a document only
// when a reader picks it.
// ../../../../ walks server -> lib -> src -> ui -> repo root.
const RAW = import.meta.glob('../../../../examples/*.ork.yaml', {
	query: '?raw',
	import: 'default',
	eager: true
}) as Record<string, string>;

export const EXAMPLE_SOURCES = exampleSources(RAW);
