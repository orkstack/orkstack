// What the command palette offers, as plain data: a command names what it
// does, never does it. The palette component turns an Action into a
// navigation, a theme change or a copy; a command that would CHANGE something
// in the deployment (cancel, run again, submit) is only ever an action that
// opens the page's own dialog, with its confirmation and its CLI equivalent
// (docs/UI_CLI_PARITY.md "Navigation and the command palette").
import { BUILD_PAGES, OPERATE_PAGES } from '../navigation';
import { withNamespace } from '../namespace';
import type { Theme } from '../theme';

export type Group = 'Pages' | 'Changed runs' | 'Runs' | 'Orkflows' | 'Tasks' | 'Actions' | 'Recent';

/** Display order of the groups when nothing is typed. */
export const GROUP_ORDER: readonly Group[] = ['Recent', 'Changed runs', 'Tasks', 'Pages', 'Actions', 'Runs', 'Orkflows'];

export type Action =
	| { kind: 'navigate'; href: string }
	| { kind: 'theme'; theme: Theme }
	| { kind: 'namespace'; name: string }
	| { kind: 'copy'; text: string; what: string }
	/** Hands control to a page: the page opens its own dialog, or acts on itself (select a task). */
	| { kind: 'page'; run: () => void };

export interface Command {
	/** Unique and stable: `command-palette-option-{id}`. Never derived from parsing an id. */
	id: string;
	group: Group;
	label: string;
	/** Muted context beside the label: a namespace, a status, a digest. */
	context?: string;
	/** Also matched, never shown. */
	keywords?: string;
	action: Action;
	/** Set on navigations a person may open in a new tab (Ctrl/Cmd+Enter). */
	href?: string;
}

/** Ids are testids too: anything outside [a-z0-9-] becomes '-'. */
export function commandId(...parts: string[]): string {
	return parts
		.join('-')
		.toLowerCase()
		.replace(/[^a-z0-9-]+/g, '-');
}

/** Go to every page in both modes, in the namespace the palette was opened in. */
export function pageCommands(namespace: string): Command[] {
	const build = BUILD_PAGES.map((p) => ({ p, label: `Go to ${p.title}` }));
	const operate = OPERATE_PAGES.map((p) => ({ p, label: `Go to ${p.title}` }));
	return [...build, ...operate].map(({ p, label }) => {
		const href = withNamespace(p.href, namespace);
		return {
			id: commandId('page', p.mode, p.id),
			group: 'Pages' as const,
			label,
			context: p.mode === 'build' ? 'Build' : 'Operate',
			keywords: p.label,
			action: { kind: 'navigate' as const, href },
			href
		};
	});
}

/** Switch the namespace picker to each other namespace. */
export function namespaceCommands(namespaces: readonly string[], current: string): Command[] {
	return namespaces
		.filter((name) => name !== current)
		.map((name) => ({
			id: commandId('namespace', name),
			group: 'Actions' as const,
			label: `Switch to namespace ${name}`,
			keywords: name,
			action: { kind: 'namespace' as const, name }
		}));
}

/** The three appearances, the current one marked. */
export function themeCommands(current: Theme): Command[] {
	const all: { theme: Theme; label: string }[] = [
		{ theme: 'system', label: 'Use the system appearance' },
		{ theme: 'light', label: 'Use the light appearance' },
		{ theme: 'dark', label: 'Use the dark appearance' }
	];
	return all.map(({ theme, label }) => ({
		id: commandId('theme', theme),
		group: 'Actions' as const,
		label,
		context: theme === current ? 'current' : undefined,
		keywords: `theme ${theme}`,
		action: { kind: 'theme' as const, theme }
	}));
}

/**
 * "Open run “q”": offered for any one-word query. It navigates and fetches
 * nothing — a run that does not exist is answered by the run page's own
 * not-found state — and nothing here parses the id or guesses its shape.
 */
export function openRunCommand(query: string, namespace: string): Command | null {
	const q = query.trim();
	if (q === '' || /\s/.test(q)) return null;
	const href = withNamespace(`/runs/${encodeURIComponent(q)}`, namespace);
	return {
		id: 'open-run-query',
		group: 'Runs',
		label: `Open run “${q}”`,
		action: { kind: 'navigate', href },
		href
	};
}

/** The engine's name rule (AGENTS.md §7): only such a query is sent as a name filter. */
export function nameQuery(query: string): string | null {
	const q = query.trim().toLowerCase();
	return q.length >= 2 && q.length <= 200 && /^[a-z][a-z0-9-]*$/.test(q) ? q : null;
}
