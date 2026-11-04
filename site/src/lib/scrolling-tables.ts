// A table wider than its column scrolls inside its wrapper (sync-docs.mjs). A
// wrapper that really scrolls becomes a region named after its section, with a
// tab stop, so the keyboard can move it; one that fits stays a plain box, so it
// adds neither a landmark nor a stop.

/** The name of the section a table is in: the heading above it, nearest first. */
function sectionOf(region: HTMLElement): string {
	for (let el = region.previousElementSibling; el; el = el.previousElementSibling) {
		// An entry's own heading names that entry, not the section the table is in.
		if (el.matches('.not-content')) continue;
		const heading = el.matches('h2, h3, h4') ? el : el.querySelector('h2, h3, h4');
		const text = heading?.textContent?.trim();
		if (text) return text;
	}
	return '';
}

export function wireScrollingTables(doc: Document): void {
	const regions = [...doc.querySelectorAll<HTMLElement>('.table-wrap')];
	if (regions.length === 0) return;
	const update = () => {
		const scrolling = regions.filter((r) => r.scrollWidth > r.clientWidth);
		for (const region of regions) {
			if (scrolling.includes(region)) continue;
			region.removeAttribute('tabindex');
			region.removeAttribute('role');
			region.removeAttribute('aria-label');
		}
		// Named after its section, and numbered when a section holds several, so
		// no two regions of a page share a name.
		const sections = scrolling.map(sectionOf);
		const seen = new Map<string, number>();
		scrolling.forEach((region, i) => {
			const section = sections[i];
			const of = sections.filter((s) => s === section).length;
			const nth = (seen.get(section) ?? 0) + 1;
			seen.set(section, nth);
			region.tabIndex = 0;
			region.setAttribute('role', 'region');
			region.setAttribute('aria-label', `Table${section ? `: ${section}` : ''}${of > 1 ? ` (${nth} of ${of})` : ''}`);
		});
	};
	update();
	const observer = new ResizeObserver(update);
	observer.observe(doc.documentElement);
	for (const region of regions) observer.observe(region);
}
