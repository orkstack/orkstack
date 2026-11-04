// A table wider than its column scrolls inside a named region (the rehype step
// in astro.config.mjs wraps it). Only a region that really scrolls takes a tab
// stop, so the keyboard can move it and a table that fits costs nothing.
export function wireScrollingTables(doc: Document): void {
	const regions = [...doc.querySelectorAll<HTMLElement>('.table-wrap')];
	if (regions.length === 0) return;
	const update = () => {
		for (const region of regions) {
			if (region.scrollWidth > region.clientWidth) region.tabIndex = 0;
			else region.removeAttribute('tabindex');
		}
	};
	update();
	new ResizeObserver(update).observe(doc.documentElement);
}
