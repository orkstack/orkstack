import { gallery } from '$lib/examples';
import { EXAMPLE_SOURCES } from '$lib/server/examples';
import type { PageServerLoad } from './$types';

// The gallery's cards: names and descriptions only. A card's document is
// fetched from ./examples/[slug] when it is chosen.
export const load: PageServerLoad = () => ({ examples: gallery(EXAMPLE_SOURCES) });
