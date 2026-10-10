import { errorMessage, orkstack } from '$lib/client';
import type { Namespace } from '$lib/gen/orkstack/v1/namespace_pb';
import { pageNumber, pageSize } from '$lib/browsing';
import { namespaceFrom } from '$lib/namespace';
import { filtersFromUrl, type FieldFilter } from '$lib/listFilter';
import type { PageLoad } from './$types';

// This is the GLOBAL view: every namespace this deployment has,
// not the one the switcher selected. The switcher's value still travels in
// the URL, purely so a link out of this page (e.g. to Pools) can carry it —
// it never filters ListNamespaces.
//
// It is paged and filtered the way the Orkflows list is (orkflows/+page.ts):
// `size`, the engine's `cursor` and its `page` label, plus repeated `filter`
// parameters — the one structured list filter (lib/listFilter.ts). A page
// filtered by name alone reads only the documents it shows, so a registry of
// thousands opens as fast as a registry of ten.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const size = pageSize(url.searchParams.get('size') ?? '');
	const cursor = url.searchParams.get('cursor') ?? '';
	// The page number is a LABEL for the cursor beside it (runs/+page.ts):
	// without a cursor there is no later page to number, so it is 1.
	const page = cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1;
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	const namespaces = filterError
		? Promise.resolve({ namespacesList: [] as Namespace[], nextPageToken: '', namespacesError: filterError as string | null })
		: loadNamespaces(fetch, size, cursor, applied);
	const sinks = loadTelemetrySinks(fetch);
	const keys = loadImagePolicy(fetch);
	const [namespacesResult, sinksResult, keysResult] = await Promise.all([namespaces, sinks, keys]);
	return { namespace, size, cursor, page, applied, ...namespacesResult, ...sinksResult, ...keysResult };
};

async function loadNamespaces(fetchFn: typeof fetch, size: number, cursor: string, filters: FieldFilter[]) {
	try {
		const res = await orkstack(fetchFn).namespaces.listNamespaces({ pageSize: size, pageToken: cursor, filters });
		return {
			namespacesList: res.namespaces,
			nextPageToken: res.nextPageToken,
			namespacesError: null as string | null
		};
	} catch (err) {
		return {
			namespacesList: [] as Namespace[],
			nextPageToken: '',
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
