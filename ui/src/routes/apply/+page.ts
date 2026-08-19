import { namespaceFrom } from "$lib/namespace";
import type { PageLoad } from "./$types";

// Apply reads nothing from the engine; it needs the namespace only so the
// document lands where the page says it will. A document that names its own
// namespace and disagrees with this one is refused by the engine rather than
// quietly stored somewhere its author will not look.
export const load: PageLoad = ({ url }) => ({ namespace: namespaceFrom(url) });
