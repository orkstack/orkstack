import { errorMessage, orkstack } from '$lib/client';
import { FilterOperator, filterOf } from '$lib/listFilter';
import type { Orkflow } from '$lib/gen/orkstack/v1/orkflow_pb';
import type { OrkflowVersion, Trigger } from '$lib/gen/orkstack/v1/service_pb';
import type { Schedule } from '$lib/gen/orkstack/v1/schedule_pb';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// The five views of one orkflow live under one route and one loader: what it
// does (Overview), what versions of it exist and which one runs (Versions), who
// outside this deployment may start it (Triggers), when it starts on its own
// (Schedules), and its source (Edit). The
// tab is in the URL so a reload, a back button and a pasted link all land where
// the sender was.
const TABS = ['overview', 'versions', 'triggers', 'schedules', 'edit'] as const;
export type OrkflowTab = (typeof TABS)[number];

function tabFrom(value: string | null): OrkflowTab {
	return (TABS as readonly string[]).includes(value ?? '') ? (value as OrkflowTab) : 'overview';
}

export const load: PageLoad = async ({ fetch, params, url }) => {
	const name = params.name;
	const version = url.searchParams.get('version') ?? '';
	// A name is unique WITHIN a namespace, so the pair is the orkflow's
	// address: without the namespace this reads whatever `default` happens to
	// call by the same name.
	const namespace = namespaceFrom(url);
	const tab = tabFrom(url.searchParams.get('tab'));
	const client = orkstack(fetch);

	// The reads are independent and are reported independently: a version
	// listing the engine will not answer must not blank the DAG, and a document
	// that cannot be read must not hide the versions that do exist. Each failure
	// carries its own retry (see the components), so one broken request costs
	// one request, not the page.
	//
	// The trigger listing is read HERE rather than in the tab, the way the Admin
	// Webhooks page reads its own: the loader owns the list, and a create or a
	// delete refreshes it through invalidateAll() instead of a second copy of
	// the same fact living in the component. It is fetched only for the tab that
	// shows it — the other three would pay a request for a list nobody renders.
	const [detail, catalog, triggerList, scheduleList] = await Promise.all([
		client.orkflows
			.getOrkflow({ name, version, namespace })
			.then((res) => ({ orkflow: res.orkflow ?? null, error: null as string | null }))
			.catch((err) => ({ orkflow: null as Orkflow | null, error: errorMessage(err) })),
		client.orkflows
			.listOrkflowVersions({ name, namespace })
			.then((res) => ({
				versions: res.versions,
				currentVersion: res.currentVersion,
				headVersion: res.headVersion,
				error: null as string | null
			}))
			.catch((err) => ({
				versions: [] as OrkflowVersion[],
				currentVersion: '',
				headVersion: '',
				error: errorMessage(err)
			})),
		tab === 'triggers'
			? client.triggers
					.listTriggers({ namespace, filters: [filterOf('orkflow_name', FilterOperator.EQUALS, [name])] })
					.then((res) => ({ triggers: res.triggers, error: null as string | null }))
					.catch((err) => ({ triggers: [] as Trigger[], error: errorMessage(err) }))
			: Promise.resolve({ triggers: [] as Trigger[], error: null as string | null }),
		// Schedules are read the way triggers are, for the tab that shows them.
		tab === 'schedules'
			? client.schedules
					.listSchedules({ namespace, filters: [filterOf('orkflow_name', FilterOperator.EQUALS, [name])] })
					.then((res) => ({ schedules: res.schedules, error: null as string | null }))
					.catch((err) => ({ schedules: [] as Schedule[], error: errorMessage(err) }))
			: Promise.resolve({ schedules: [] as Schedule[], error: null as string | null })
	]);

	// The editor is seeded from the SELECTED version — `?version=` when the URL
	// names one, and the head when it does not.
	//
	// Head stays the default — it is the lineage's tip, and selecting is
	// explicit — but a selection is honoured.
	//
	// Two versions travel from here, because an apply carries two tokens
	// (proto/orkstack/v1/service.proto): `editVersion` is the edit's
	// PROVENANCE, and `headVersion` is the optimistic-concurrency token — the
	// head this page observed when it fetched. When head cannot be established
	// the editor says so and refuses to apply: an apply with no token is an
	// apply that can bury a newer version.
	const editVersion = version || catalog.headVersion;
	// A version the engine will not answer is reported AS ITSELF rather than
	// replaced by head: the box must never hold a document the URL does not
	// name.
	let editYaml = '';
	let editError: string | null = null;
	// The Overview's read answers this one when it asked the same question —
	// which it did whenever the URL named a version, and otherwise only if the
	// version it resolved through the current pointer happens to be the head.
	const alreadyRead = version === editVersion || detail.orkflow?.version === editVersion;
	if (catalog.error) {
		editError = catalog.error;
	} else if (!catalog.headVersion) {
		editError = `orkflow ${name} has no stored version in this namespace`;
	} else if (alreadyRead) {
		if (detail.orkflow) editYaml = detail.orkflow.sourceYaml;
		else editError = detail.error ?? `the engine returned no document for ${name}@${editVersion}`;
	} else {
		try {
			const res = await client.orkflows.getOrkflow({ name, version: editVersion, namespace });
			const doc = res.orkflow ?? null;
			if (doc) editYaml = doc.sourceYaml;
			else editError = `the engine returned no document for ${name}@${editVersion}`;
		} catch (err) {
			editError = errorMessage(err);
		}
	}

	return {
		name,
		version,
		namespace,
		tab,
		orkflow: detail.orkflow,
		orkflowError: detail.error,
		versions: catalog.versions,
		currentVersion: catalog.currentVersion,
		headVersion: catalog.headVersion,
		versionsError: catalog.error,
		triggers: triggerList.triggers,
		triggersError: triggerList.error,
		schedules: scheduleList.schedules,
		schedulesError: scheduleList.error,
		editVersion,
		editYaml,
		editError
	};
};
