import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	try {
		const response = await orkstack(fetch).webhooks.listWebhooks({});
		return { webhooks: response.webhooks, namespace, error: null as string | null };
	} catch (err) {
		return { webhooks: [], namespace, error: errorMessage(err) };
	}
};
