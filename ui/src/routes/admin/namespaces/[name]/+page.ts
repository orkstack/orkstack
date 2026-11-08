import { errorMessage } from '#lib/client.ts';
import { namespaceClient } from '#lib/clients/namespaces.ts';
import type { Namespace } from '#lib/gen/orkstack/v1/namespace_pb.ts';
import { namespaceFrom } from '#lib/namespace.ts';
import { loadImagePolicy, loadTelemetrySinks } from '#lib/namespaceFormData.ts';
import type { PageLoad } from './$types';

// One namespace's own address. `name` is the namespace this page is ABOUT;
// `ns` in the URL stays the switcher's choice, as on every Operate page.
export const load: PageLoad = async ({ fetch, url, params }) => {
	const namespace = namespaceFrom(url);
	const name = params.name;
	const read = namespaceClient(fetch)
		.getNamespace({ name })
		.then(
			(res) => ({ record: res.namespace ?? null, error: res.namespace ? null : `namespace ${name} came back empty` }),
			(err) => ({ record: null as Namespace | null, error: errorMessage(err) as string | null })
		);
	const [answer, sinks, keys] = await Promise.all([read, loadTelemetrySinks(fetch), loadImagePolicy(fetch)]);
	return { namespace, name, ...answer, ...sinks, ...keys };
};
