// Copying text, on every origin a deployment may serve the UI from.
//
// navigator.clipboard exists only in a secure context (https, or localhost),
// and an operator reaching the UI at http://ork.internal:3000 gets
// `undefined` — where `navigator.clipboard?.writeText(...)` did nothing at
// all and said nothing either. So a copy has three outcomes, each stated:
//
//   1. the Clipboard API wrote it ('copied');
//   2. the API is absent or refused, and the legacy path — a hidden textarea
//      and document.execCommand('copy'), which still works on a plain-HTTP
//      origin inside a click — wrote it ('copied');
//   3. neither could ('manual'): the caller selects the visible text and says
//      to copy it by hand. The command is always on screen, so the reader is
//      never left without it.

export type CopyOutcome = 'copied' | 'manual';

/** The message every caller shows for a 'manual' outcome. */
export const MANUAL_COPY_MESSAGE =
	'Clipboard is unavailable on this origin: the text is selected, copy it by hand (Ctrl+C / ⌘C).';

/** The browser surface copyText uses, injectable so each branch is testable. */
export interface CopyEnv {
	clipboard?: { writeText(text: string): Promise<void> } | undefined;
	document?: {
		createElement(tag: 'textarea'): HTMLTextAreaElement;
		body: { appendChild(node: Node): unknown; removeChild(node: Node): unknown } | null;
		execCommand(command: string): boolean;
		activeElement: Element | null;
	};
}

function browserEnv(): CopyEnv {
	if (typeof navigator === 'undefined' || typeof document === 'undefined') return {};
	return { clipboard: navigator.clipboard ?? undefined, document };
}

export async function copyText(text: string, env: CopyEnv = browserEnv()): Promise<CopyOutcome> {
	if (env.clipboard?.writeText) {
		try {
			await env.clipboard.writeText(text);
			return 'copied';
		} catch {
			// Refused (a permissions policy, a document without focus): the
			// legacy path below may still be allowed.
		}
	}
	const doc = env.document;
	if (doc?.body) {
		const area = doc.createElement('textarea');
		area.value = text;
		area.setAttribute('readonly', '');
		area.setAttribute('aria-hidden', 'true');
		area.style.position = 'fixed';
		area.style.top = '0';
		area.style.left = '0';
		area.style.opacity = '0';
		const previous = doc.activeElement as HTMLElement | null;
		doc.body.appendChild(area);
		let ok = false;
		try {
			area.select();
			ok = doc.execCommand('copy');
		} catch {
			ok = false;
		} finally {
			doc.body.removeChild(area);
			previous?.focus?.();
		}
		if (ok) return 'copied';
	}
	return 'manual';
}

/**
 * selectText selects an element's text, so a 'manual' outcome leaves the
 * reader one keystroke from the copy they asked for.
 */
export function selectText(element: Element | null | undefined): void {
	if (!element || typeof window === 'undefined') return;
	const selection = window.getSelection();
	if (!selection) return;
	const range = document.createRange();
	range.selectNodeContents(element);
	selection.removeAllRanges();
	selection.addRange(range);
}

/** selectByTestid selects the text of the element carrying data-testid=id. */
export function selectByTestid(testid: string): void {
	if (typeof document === 'undefined') return;
	selectText(document.querySelector(`[data-testid="${CSS.escape(testid)}"]`));
}
