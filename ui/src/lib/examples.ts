// The example gallery's catalogue: the repo's own `examples/` directory.
//
// The gallery shows a name and a description per example; the YAML itself is
// fetched only when an example is chosen (routes/apply/examples/[file]), so
// twenty-odd documents are not inlined into the page's client chunk for a
// reader who opens none of them. The files are read at build time
// (lib/server/examples.ts) and written into the static build as files of
// their own; this module is the pure half.

export type OrkflowExample = {
	/** Basename with `.ork.yaml` stripped — `hello.ork.yaml` is `hello`. Used for test ids. */
	slug: string;
	file: string;
	name: string;
	description: string;
};

export type ExampleSource = OrkflowExample & { yaml: string };

// A deliberately small reader for two top-level keys, not a YAML parser. It
// only has to answer "what does this document call itself" for files the
// repo controls; a document whose `name:` this cannot find falls back to its
// filename rather than failing the build.
export function topLevel(yaml: string, key: string): string {
	const lines = yaml.split('\n');
	const index = lines.findIndex((line) => line.startsWith(`${key}:`));
	if (index === -1) return '';
	const value = lines[index].slice(key.length + 1).trim();

	// Block scalar (`>-`, `|`, ...): the value is the indented lines below it.
	if (/^[|>][-+]?\d*$/.test(value)) {
		const folded = value.startsWith('>');
		const body: string[] = [];
		for (const line of lines.slice(index + 1)) {
			if (line.trim() === '') {
				body.push('');
				continue;
			}
			if (!/^\s/.test(line)) break;
			body.push(line.trim());
		}
		return body.join(folded ? ' ' : '\n').trim();
	}

	// Strip one layer of matching quotes; these documents quote a description
	// only when it contains a `:` that would otherwise read as a mapping.
	const quoted = /^(["'])([\s\S]*)\1$/.exec(value);
	return quoted ? quoted[2] : value;
}

/**
 * The catalogue from {path: yaml}, ordered by path, so the gallery's order is
 * the directory's order and a new example needs no change here.
 */
export function exampleSources(raw: Record<string, string>): ExampleSource[] {
	return Object.entries(raw)
		.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
		.map(([path, yaml]) => {
			const file = path.slice(path.lastIndexOf('/') + 1);
			const slug = file.replace(/\.ork\.yaml$/, '');
			return { slug, file, name: topLevel(yaml, 'name') || slug, description: topLevel(yaml, 'description'), yaml };
		});
}

/** What the gallery page receives: everything but the documents. */
export function gallery(sources: ExampleSource[]): OrkflowExample[] {
	return sources.map(({ slug, file, name, description }) => ({ slug, file, name, description }));
}

/** The one document a card's "Use this example" fetches, by its file name; 404 for any other. */
export function exampleResponse(sources: ExampleSource[], file: string): Response {
	const found = sources.find((source) => source.file === file);
	if (!found) {
		return new Response(`no example named ${JSON.stringify(file)}\n`, {
			status: 404,
			headers: { 'content-type': 'text/plain; charset=utf-8' }
		});
	}
	// The build writes the body; the engine that serves the file names its
	// type and caching (internal/webui).
	return new Response(found.yaml, { headers: { 'content-type': 'text/yaml; charset=utf-8' } });
}

/** Where the page fetches the gallery's cards from. */
export const galleryHref = '/apply/examples.json';

/** Where the page fetches one example's YAML from: its file, under the gallery. */
export function exampleHref(slug: string): string {
	return `/apply/examples/${encodeURIComponent(slug)}.ork.yaml`;
}
