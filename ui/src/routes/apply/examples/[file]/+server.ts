import { exampleResponse } from '$lib/examples';
import { EXAMPLE_SOURCES } from '$lib/server/examples';
import type { EntryGenerator, RequestHandler } from './$types';

// One example's YAML, byte for byte as it is in examples/, written into the
// static build as /apply/examples/<file> — one file per example, so the
// engine serves each as it serves any other built file, and the browser
// downloads a document only when a reader picks it.
export const prerender = true;

export const entries: EntryGenerator = () => EXAMPLE_SOURCES.map(({ file }) => ({ file }));

export const GET: RequestHandler = ({ params }) => exampleResponse(EXAMPLE_SOURCES, params.file);
