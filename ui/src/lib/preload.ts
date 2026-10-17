// What the first paint waits for, and what it does not.
//
// The fonts are the @fontsource-variable faces app.css imports: one face per
// script subset, each declared with its unicode-range, so a browser fetches a
// subset only when text on the page uses it — descriptions, inputs and logs
// may be in any script, so every subset stays declared. Two rules keep them
// off the critical path:
//
//   * no face is inlined into the stylesheet. Vite inlines any asset under
//     4 KiB as base64, which put a Cyrillic-ext face into the render-blocking
//     CSS of every page whether or not a Cyrillic character ever appears
//     (inlineAsset, used by vite.config.ts);
//   * the two faces every page does use — Archivo Narrow and Spline Sans
//     Mono, latin,
//     variable weight — are preloaded from the server-rendered HTML, so they
//     download beside the CSS rather than after it has been parsed
//     (preloadAsset, used by hooks.server.ts).

/** Vite's build.assetsInlineLimit: never for a font, the default otherwise. */
export function inlineAsset(filePath: string): boolean | undefined {
	return /\.woff2?$/.test(filePath) ? false : undefined;
}

/** SvelteKit's resolve({ preload }): scripts, styles and the two latin faces. */
export function preloadAsset({ type, path }: { type: 'js' | 'css' | 'font' | 'asset'; path: string }): boolean {
	if (type === 'js' || type === 'css') return true;
	return type === 'font' && /-latin-wght-normal\.[^/]*woff2$/.test(path);
}
