// The palette's two remote sources: runs by their Orkflow's name, and
// Orkflows by name, each a small page from the existing list RPCs. Loaded
// only with the palette (a dynamic import on first open), so no route pays
// for these clients until a person opens it. Nothing here calls GetRun: a run
// is opened by navigating to it, and its page answers whether it exists.
import { runClient } from '../clients/runs';
import { orkflowClient } from '../clients/orkflows';
import { FilterOperator } from '../gen/orkstack/v1/filter_pb';
import { filterOf } from '../listFilter';
import { withNamespace } from '../namespace';
import { statusLabel } from '../status';
import { commandId, nameQuery, type Command } from './commands';

export const REMOTE_PAGE_SIZE = 6;
export const REMOTE_DEBOUNCE_MS = 150;

export async function searchRuns(fetchFn: typeof fetch, namespace: string, query: string): Promise<Command[]> {
	const q = nameQuery(query);
	if (!q) return [];
	const res = await runClient(fetchFn).listRuns({
		namespace,
		pageSize: REMOTE_PAGE_SIZE,
		filters: [filterOf('orkflow_name', FilterOperator.CONTAINS, [q])]
	});
	return res.runs.map((run) => {
		const href = withNamespace(`/runs/${encodeURIComponent(run.id)}`, namespace);
		return {
			id: commandId('run', run.id),
			group: 'Runs' as const,
			label: `Open run ${run.id}`,
			context: `${run.orkflowName} · ${statusLabel(run.status)}`,
			keywords: run.orkflowName,
			action: { kind: 'navigate' as const, href },
			href
		};
	});
}

export async function searchOrkflows(fetchFn: typeof fetch, namespace: string, query: string): Promise<Command[]> {
	const q = nameQuery(query);
	if (!q) return [];
	const res = await orkflowClient(fetchFn).listOrkflows({
		namespace,
		pageSize: REMOTE_PAGE_SIZE,
		filters: [filterOf('name', FilterOperator.CONTAINS, [q])]
	});
	return res.orkflows.map((o) => {
		const href = withNamespace(`/orkflows/${encodeURIComponent(o.name)}`, namespace);
		return {
			id: commandId('orkflow', o.name),
			group: 'Orkflows' as const,
			label: `Open Orkflow ${o.name}`,
			context: `version ${o.version}`,
			keywords: o.name,
			action: { kind: 'navigate' as const, href },
			href
		};
	});
}
