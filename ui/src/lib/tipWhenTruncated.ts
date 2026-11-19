// A one-line label that ends in an ellipsis keeps its whole text readable:
// its own text is its accessible name, and while it is cut short the one
// tooltip (tooltip.ts, `data-tip`) shows the whole text on hover and on
// keyboard focus. A label that fits carries no tip, so nothing repeats what
// is already on screen.

/** The tip a label of these widths carries: its text when cut short, else none. */
export function truncationTip(scrollWidth: number, clientWidth: number, text: string): string | undefined {
	return text && scrollWidth > clientWidth ? text : undefined;
}

/** Svelte action: keeps `data-tip` on node set to text exactly while node overflows. */
export function tipWhenTruncated(node: HTMLElement, text: string) {
	const measure = () => {
		const tip = truncationTip(node.scrollWidth, node.clientWidth, text);
		if (tip) node.dataset.tip = tip;
		else delete node.dataset.tip;
	};
	const observer = typeof ResizeObserver === 'undefined' ? undefined : new ResizeObserver(measure);
	observer?.observe(node);
	measure();
	return {
		update(next: string) {
			text = next;
			measure();
		},
		destroy() {
			observer?.disconnect();
		}
	};
}
