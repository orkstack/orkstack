import { errorMessage, orkstack } from '$lib/client';
import type { Namespace } from '$lib/gen/orkstack/v1/namespace_pb';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// This is the GLOBAL view: every namespace this deployment has,
// not the one the switcher selected. The switcher's value still travels in
// the URL, purely so a link out of this page (e.g. to Pools) can carry it —
// it never filters ListNamespaces.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const namespaces = loadNamespaces(fetch);
	const sinks = loadTelemetrySinks(fetch);
	const keys = loadImagePolicy(fetch);
	const [namespacesResult, sinksResult, keysResult] = await Promise.all([namespaces, sinks, keys]);
	return { namespace, ...namespacesResult, ...sinksResult, ...keysResult };
};

async function loadNamespaces(fetchFn: typeof fetch) {
	try {
		const res = await orkstack(fetchFn).namespaces.listNamespaces({});
		return {
			namespacesList: res.namespaces,
			namespacesError: null as string | null
		};
	} catch (err) {
		return {
			namespacesList: [] as Namespace[],
			namespacesError: errorMessage(err)
		};
	}
}

// loadTelemetrySinks reads the deployment's declared telemetry sinks
// (SystemService.ListTelemetrySinks) so the create/edit forms can offer a
// select of real names rather than a free-text field the registry would
// likely refuse. A failure here does not fail the page — it only means the
// forms fall back to a plain text input for the sink (see
// telemetrySinksError below in +page.svelte); a namespace admin can still
// read and write everything else.
async function loadTelemetrySinks(fetchFn: typeof fetch) {
	try {
		const res = await orkstack(fetchFn).system.listTelemetrySinks({});
		return {
			telemetrySinks: res.names,
			telemetrySinksError: null as string | null
		};
	} catch (err) {
		return {
			telemetrySinks: [] as string[],
			telemetrySinksError: errorMessage(err)
		};
	}
}

// loadImagePolicy reads the deployment's image policy (SystemService.
// GetImagePolicy) for its signing key NAMES only, so the create/edit forms can
// offer a select of `cosign:<name>` signers the engine will accept. A failure
// here does not fail the page — the forms fall back to a plain text input for
// the signer, on the same terms as the telemetry sink select.
async function loadImagePolicy(fetchFn: typeof fetch) {
	try {
		const res = await orkstack(fetchFn).system.getImagePolicy({});
		return {
			signingKeys: Object.keys(res.policy?.signingKeys ?? {}).sort(),
			signingKeysError: null as string | null
		};
	} catch (err) {
		return {
			signingKeys: [] as string[],
			signingKeysError: errorMessage(err)
		};
	}
}
