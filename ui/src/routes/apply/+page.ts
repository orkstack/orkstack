import { errorMessage } from '$lib/client';
import { galleryHref, type OrkflowExample } from '$lib/examples';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// Apply reads nothing from the engine; it needs the namespace only so the
// document lands where the page says it will. A document that names its own
// namespace and disagrees with this one is refused by the engine rather than
// quietly stored somewhere its author will not look. The example gallery is a
// file of the build (routes/apply/examples.json), not an engine call.
export const load: PageLoad = async ({ url, fetch }) => {
	const namespace = namespaceFrom(url);
	try {
		const response = await fetch(galleryHref);
		if (!response.ok) throw new Error(`HTTP ${response.status}`);
		return { namespace, examples: (await response.json()) as OrkflowExample[], examplesError: '' };
	} catch (err) {
		return { namespace, examples: [] as OrkflowExample[], examplesError: `Could not load the examples: ${errorMessage(err)}` };
	}
};
