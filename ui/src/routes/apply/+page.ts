import { namespaceFrom } from "$lib/namespace";
import type { PageLoad } from "./$types";

// The example gallery is the repo's own `examples/` directory, read at build
// time. Vite resolves `import.meta.glob` relative to this file and inlines the
// `?raw` contents, so `examples/` stays the single source of truth: no copy
// step to forget, no engine endpoint, and an example that drifts from the DSL
// is caught by whatever already exercises those files, not by this page.
// ../../../../ walks apply -> routes -> src -> ui -> repo root.
const RAW = import.meta.glob("../../../../examples/*.ork.yaml", {
	query: "?raw",
	import: "default",
	eager: true,
}) as Record<string, string>;

export type OrkflowExample = {
	/** Basename with `.ork.yaml` stripped — `hello.ork.yaml` is `hello`. Used for test ids. */
	slug: string;
	file: string;
	name: string;
	description: string;
	yaml: string;
};

// A deliberately small reader for two top-level keys, not a YAML parser. It
// only has to answer "what does this document call itself" for five files the
// repo controls; a document whose `name:` this cannot find falls back to its
// filename rather than failing the build.
function topLevel(yaml: string, key: string): string {
	const lines = yaml.split("\n");
	const index = lines.findIndex((line) => line.startsWith(`${key}:`));
	if (index === -1) return "";
	const value = lines[index].slice(key.length + 1).trim();

	// Block scalar (`>-`, `|`, ...): the value is the indented lines below it.
	if (/^[|>][-+]?\d*$/.test(value)) {
		const folded = value.startsWith(">");
		const body: string[] = [];
		for (const line of lines.slice(index + 1)) {
			if (line.trim() === "") {
				body.push("");
				continue;
			}
			if (!/^\s/.test(line)) break;
			body.push(line.trim());
		}
		return body.join(folded ? " " : "\n").trim();
	}

	// Strip one layer of matching quotes; these documents quote a description
	// only when it contains a `:` that would otherwise read as a mapping.
	const quoted = /^(["'])([\s\S]*)\1$/.exec(value);
	return quoted ? quoted[2] : value;
}

// Glob keys arrive sorted by path, so the gallery's order is the directory's
// order and a new example needs no change here.
const EXAMPLES: OrkflowExample[] = Object.entries(RAW).map(([path, yaml]) => {
	const file = path.slice(path.lastIndexOf("/") + 1);
	const slug = file.replace(/\.ork\.yaml$/, "");
	return { slug, file, name: topLevel(yaml, "name") || slug, description: topLevel(yaml, "description"), yaml };
});

// Apply reads nothing from the engine; it needs the namespace only so the
// document lands where the page says it will. A document that names its own
// namespace and disagrees with this one is refused by the engine rather than
// quietly stored somewhere its author will not look.
export const load: PageLoad = ({ url }) => ({ namespace: namespaceFrom(url), examples: EXAMPLES });
