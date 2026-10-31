import { gallery } from '#lib/examples.ts';
import { EXAMPLE_SOURCES } from '#lib/server/examples.ts';
import type { RequestHandler } from './$types';

// The gallery's cards — names and descriptions only — written into the static
// build as /apply/examples.json. A card's document is its own file
// (./examples/[file]), fetched when it is chosen.
export const prerender = true;

export const GET: RequestHandler = () => Response.json(gallery(EXAMPLE_SOURCES));
