// A row's edit is addressable: its name links to this page with `edit=<id>`, and
// the page opens that row's dialog from the URL, so the state can be linked,
// reloaded and left with the back button. These are its pure pieces.
export const EDIT_PARAM = 'edit';

type Where = Pick<URL, 'pathname' | 'search'>;

/** This page's address with `edit` set, every other parameter kept. */
export function editHref(url: Where, id: string): string {
	const params = new URLSearchParams(url.search);
	params.set(EDIT_PARAM, id);
	return `${url.pathname}?${params.toString()}`;
}

/** This page's address with `edit` gone. */
export function closedHref(url: Where): string {
	const params = new URLSearchParams(url.search);
	params.delete(EDIT_PARAM);
	const search = params.toString();
	return search ? `${url.pathname}?${search}` : url.pathname;
}
