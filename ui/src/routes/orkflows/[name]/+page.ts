import { errorMessage, orkstack } from '$lib/client';
import type { Orkflow } from '$lib/gen/orkstack/v1/orkflow_pb';
import type { OrkflowVersion } from '$lib/gen/orkstack/v1/service_pb';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// The three views of one orkflow live under one route and one loader: what it
// does (Overview), what versions of it exist and which one runs (Versions), and
// its source (Edit). The tab is in the URL so a reload, a back button and a
// pasted link all land where the sender was.
const TABS = ['overview', 'versions', 'edit'] as const;
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

	// The two reads are independent and are reported independently: a version
	// listing the engine will not answer must not blank the DAG, and a document
	// that cannot be read must not hide the versions that do exist. Each failure
	// carries its own retry (see the components), so one broken request costs
	// one request, not the page.
	const [detail, catalog] = await Promise.all([
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
			}))
	]);

	// The editor is seeded from the HEAD version, not from whatever is current:
	// head is the edit lineage's tip and the base_version an apply must carry,
	// so editing anything else would send a token for a document the box is not
	// showing. When head cannot be established the editor says so and refuses to
	// apply — an apply with no token is an apply that can bury a newer version.
	let head: Orkflow | null = null;
	let headError: string | null = null;
	if (catalog.error) {
		headError = catalog.error;
	} else if (!catalog.headVersion) {
		headError = `orkflow ${name} has no stored version in this namespace`;
	} else if (detail.orkflow && detail.orkflow.version === catalog.headVersion) {
		head = detail.orkflow;
	} else {
		try {
			const res = await client.orkflows.getOrkflow({
				name,
				version: catalog.headVersion,
				namespace
			});
			head = res.orkflow ?? null;
			if (!head) headError = `the engine returned no document for ${name}@${catalog.headVersion}`;
		} catch (err) {
			headError = errorMessage(err);
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
		headYaml: head?.sourceYaml ?? '',
		headError
	};
};
