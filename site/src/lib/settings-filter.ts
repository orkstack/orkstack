// The filter on All settings: every word typed must appear in an entry, its
// folded details included. With no script the field stays hidden and every
// entry shows.

/** Whether an entry's text holds every word of a query, in any case. */
export function matchesFilter(text: string, query: string): boolean {
	const hay = text.toLowerCase();
	return query
		.toLowerCase()
		.split(/\s+/)
		.filter(Boolean)
		.every((word) => hay.includes(word));
}

/** The page's areas: each heading, the line under it and the entries it holds. */
function areasOf(root: Element): { head: HTMLElement[]; entries: HTMLElement[] }[] {
	const areas: { head: HTMLElement[]; entries: HTMLElement[] }[] = [];
	for (const child of root.children) {
		const el = child as HTMLElement;
		if (el.matches('h2, .sl-heading-wrapper')) areas.push({ head: [el], entries: [] });
		else if (el.matches('[data-setting]')) areas.at(-1)?.entries.push(el);
		else if (!el.matches('[data-settings-filter]')) areas.at(-1)?.head.push(el);
	}
	return areas.filter((a) => a.entries.length > 0);
}

const COUNT_DELAY_MS = 400;

export function wireSettingsFilter(doc: Document): void {
	const box = doc.querySelector<HTMLElement>('[data-settings-filter]');
	const input = box?.querySelector<HTMLInputElement>('[data-settings-input]');
	const count = box?.querySelector<HTMLElement>('[data-settings-count]');
	const root = box?.parentElement;
	if (!box || !input || !count || !root) return;
	const areas = areasOf(root);
	const total = areas.reduce((n, a) => n + a.entries.length, 0);
	box.hidden = false;
	// What the filter opened, so clearing it closes those and nothing the reader opened.
	const opened = new Set<HTMLDetailsElement>();
	let announce: ReturnType<typeof setTimeout>;
	const apply = () => {
		const query = input.value.trim();
		let shown = 0;
		for (const area of areas) {
			let any = false;
			for (const entry of area.entries) {
				const text = entry.textContent ?? '';
				const hit = matchesFilter(text, query);
				entry.hidden = !hit;
				any = any || hit;
				if (hit) shown++;
				// A match inside the folded text is shown, not left to be found.
				const more = entry.querySelector('details');
				if (!more) continue;
				const inFold = hit && query !== '' && !matchesFilter(text.replace(more.textContent ?? '', ''), query);
				if (inFold && !more.open) {
					more.open = true;
					opened.add(more);
				} else if (!inFold && opened.delete(more)) more.open = false;
			}
			for (const el of area.head) el.hidden = !any;
			// The contents link only to what is left.
			const id = (area.head[0].matches('h2') ? area.head[0] : area.head[0].querySelector('h2'))?.id;
			if (id) for (const link of doc.querySelectorAll(`a[href="#${id}"]`)) {
				const item = link.closest('li');
				if (item) item.hidden = !any;
			}
		}
		// The count is a live region: it speaks once the typing settles.
		clearTimeout(announce);
		announce = setTimeout(() => {
			count.textContent = query ? `${shown} of ${total} settings` : `${total} settings`;
		}, COUNT_DELAY_MS);
	};
	input.addEventListener('input', apply);
	// A browser may restore the field's text on the way back to the page.
	if (input.value) apply();
}
