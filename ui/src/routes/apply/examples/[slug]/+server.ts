import { exampleResponse } from '$lib/examples';
import { EXAMPLE_SOURCES } from '$lib/server/examples';
import type { RequestHandler } from './$types';

// One example's YAML, byte for byte as it is in examples/, or 404.
export const GET: RequestHandler = ({ params }) => exampleResponse(EXAMPLE_SOURCES, params.slug);
