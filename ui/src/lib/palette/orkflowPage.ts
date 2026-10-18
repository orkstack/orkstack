// What the Orkflow page adds to the palette while it is open: Submit a run…
// (which only opens the page's own dialog — the palette never writes), the
// page's other tabs, and a jump to any task's row in the task register.
//
// Built per query and only while the palette is open (./context.ts): with
// nothing typed the definition is not scanned at all, and a typed query keeps
// the best few ids by the palette's own ranking, so a 4,000-task Orkflow never
// holds a command per task.
import type { Command } from './commands';
import { commandId } from './commands';
import { PROVIDER_LIMIT } from './context';
import { match } from './rank';

export const ORKFLOW_TABS = ['overview', 'versions', 'triggers', 'schedules', 'edit'] as const;

const TAB_LABEL: Record<(typeof ORKFLOW_TABS)[number], string> = {
	overview: 'Show overview',
	versions: 'Show versions',
	triggers: 'Show triggers',
	schedules: 'Show schedules',
	edit: 'Edit source'
};

export interface OrkflowPageOptions {
	name: string;
	/** The tab the page is on. */
	tab: string;
	/** The definition's task ids, read only when a query is typed. */
	tasks: () => readonly string[];
	/** The page's own link to a tab (namespace kept). */
	hrefOf: (tab: string) => string;
	/** False while the page holds no document to submit. */
	canSubmit: boolean;
	/** Opens the page's submit dialog. */
	openSubmit: () => void;
}

function ownCommands(o: OrkflowPageOptions): Command[] {
	const out: Command[] = [];
	if (o.canSubmit)
		out.push({ id: 'orkflow-submit', group: 'Actions', label: 'Submit a run…', context: o.name, action: { kind: 'page', run: o.openSubmit } });
	for (const tab of ORKFLOW_TABS) {
		if (tab === o.tab) continue;
		const href = o.hrefOf(tab);
		out.push({ id: commandId('orkflow-tab', tab), group: 'Pages', label: TAB_LABEL[tab], context: o.name, action: { kind: 'navigate', href }, href });
	}
	return out;
}

function taskJumps(o: OrkflowPageOptions, query: string): Command[] {
	const best: { id: string; grade: number; span: number }[] = [];
	const better = (a: (typeof best)[number], b: (typeof best)[number]) => b.grade - a.grade || a.span - b.span;
	for (const id of o.tasks()) {
		const m = match(id, query);
		if (!m) continue;
		best.push({ id, grade: m.grade, span: m.span });
		if (best.length > PROVIDER_LIMIT * 8) {
			best.sort(better);
			best.length = PROVIDER_LIMIT;
		}
	}
	best.sort(better);
	const overview = o.hrefOf('overview');
	return best.slice(0, PROVIDER_LIMIT).map((b) => {
		const href = `${overview}#task-row-${b.id}`;
		return {
			id: commandId('orkflow-task', b.id),
			group: 'Tasks' as const,
			label: `Jump to task ${b.id}`,
			keywords: b.id,
			action: { kind: 'navigate' as const, href },
			href
		};
	});
}

/** The page's commands for this query: its own that match first, then the best task ids. */
export function orkflowPageCommands(o: OrkflowPageOptions, query: string): Command[] {
	const q = query.trim();
	const own = ownCommands(o);
	if (!q) return own.slice(0, PROVIDER_LIMIT);
	return [...own.filter((c) => match(c.label, q)), ...taskJumps(o, q)].slice(0, PROVIDER_LIMIT);
}
