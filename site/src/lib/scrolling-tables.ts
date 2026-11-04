// A table wider than its column scrolls inside its wrapper (sync-docs.mjs). A
// wrapper that really scrolls becomes a region named after its section, with a
// tab stop, so the keyboard can move it; one that fits stays a plain box, so it
// adds neither a landmark nor a stop.

/** The name of the section a table is in: the heading above it, nearest first. */
function nameOf(region: HTMLElement): string {
	for (let el = region.previousElementSibling; el; el = el.previousElementSibling) {
		const heading = el.matches('h2, h3, h4') ? el : el.querySelector('h2, h3, h4');
		const text = heading?.textContent?.trim();
		if (text) return `Table: ${text}`;
	}
	return 'Table';
}

export function wireScrollingTables(doc: Document): void {
	const regions = [...doc.querySelectorAll<HTMLElement>('.table-wrap')];
	if (regions.length === 0) return;
	const update = () => {
		for (const region of regions) {
			if (region.scrollWidth > region.clientWidth) {
				region.tabIndex = 0;
				region.setAttribute('role', 'region');
				region.setAttribute('aria-label', nameOf(region));
			} else {
				region.removeAttribute('tabindex');
				region.removeAttribute('role');
				region.removeAttribute('aria-label');
			}
		}
	};
	update();
	new ResizeObserver(update).observe(doc.documentElement);
}
