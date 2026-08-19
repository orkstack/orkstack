import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params, url }) => {
	const name = params.name;
	const version = url.searchParams.get('version') ?? '';
	// A name is unique WITHIN a namespace, so the pair is the orkflow's
	// address: without the namespace this reads whatever `default` happens to
	// call by the same name.
	const namespace = namespaceFrom(url);
	try {
		const res = await orkstack(fetch).orkflows.getOrkflow({ name, version, namespace });
		return { orkflow: res.orkflow ?? null, namespace, error: null as string | null };
	} catch (err) {
		return { orkflow: null, namespace, error: errorMessage(err) };
	}
};
