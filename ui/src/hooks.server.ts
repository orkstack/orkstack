import type { Handle } from '@sveltejs/kit';
import { preloadAsset } from '$lib/preload';

// The UI has no server: this hook runs only while `bun run build` (and the
// Vite dev server) renders index.html, the SPA fallback page every route
// boots from. It decides what that page preloads — scripts and styles as
// SvelteKit would, plus the two latin font faces every page renders with
// ($lib/preload) — so they download beside the stylesheet, not after it.
export const handle: Handle = ({ event, resolve }) => resolve(event, { preload: preloadAsset });
