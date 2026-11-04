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

export function wireSettingsFilter(doc: Document): void {
	const box = doc.querySelector<HTMLElement>('[data-settings-filter]');
	const input = box?.querySelector<HTMLInputElement>('[data-settings-input]');
	const count = box?.querySelector<HTMLElement>('[data-settings-count]');
	const root = box?.parentElement;
	if (!box || !input || !count || !root) return;
	const areas = areasOf(root);
	const total = areas.reduce((n, a) => n + a.entries.length, 0);
	box.hidden = false;
	const apply = () => {
		const query = input.value.trim();
		let shown = 0;
		for (const area of areas) {
			let any = false;
			for (const entry of area.entries) {
				const hit = matchesFilter(entry.textContent ?? '', query);
				entry.hidden = !hit;
				any = any || hit;
				if (hit) shown++;
				// A match inside the folded text is shown, not left to be found.
				const more = entry.querySelector('details');
				if (more) more.open = hit && query !== '' && !matchesFilter((entry.textContent ?? '').replace(more.textContent ?? '', ''), query);
			}
			for (const el of area.head) el.hidden = !any;
		}
		count.textContent = query ? `${shown} of ${total} settings` : `${total} settings`;
	};
	input.addEventListener('input', apply);
	apply();
}
