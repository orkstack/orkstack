import { errorMessage, orkstack } from '$lib/client';
import type { PluginInfo } from '$lib/gen/orkstack/v1/service_pb';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// The plugin inventory is GLOBAL (design §6.1): what any connected Orker can
// run, across every pool, not scoped by namespace. `namespace` still travels
// so a link out of this page can carry the switcher's choice.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	try {
		const res = await orkstack(fetch).plugins.listPlugins({});
		return { namespace, plugins: res.plugins, pluginsError: null as string | null };
	} catch (err) {
		return {
			namespace,
			plugins: [] as PluginInfo[],
			pluginsError: errorMessage(err)
		};
	}
};
