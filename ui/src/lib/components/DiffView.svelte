<script lang="ts">
	// The engine's unified diff, rendered verbatim.
	//
	// The diff is computed SERVER-side (DiffOrkflowVersions, proto comment) so
	// that `orkctl orkflow diff` and this page cannot disagree about what
	// changed. Rendering it therefore means COLOURING it and nothing else: the
	// spans below are inline, and each line is followed by a literal newline, so
	// the text in the DOM is byte-for-byte what the RPC answered. A pretty
	// re-layout here would be a second diff implementation wearing a stylesheet.
	//
	// A diff of two YAML documents (`lang="yaml"`) is also coloured as YAML,
	// once the classifier loads (src/lib/yaml/highlight.ts): each hunk's lines
	// in the document they belong to, a removed line left set back, and every
	// line still its own bytes. Other diffs (a run's JSON input) stay as they are.
	import { onMount } from 'svelte';
	import type { DiffRow } from '$lib/yaml/highlight';

	interface Props {
		diff: string;
		testid: string;
		lang?: 'yaml';
	}

	let { diff, testid, lang }: Props = $props();

	let colour = $state<((diff: string) => DiffRow[]) | null>(null);
	const rows = $derived(colour ? colour(diff) : null);

	onMount(() => {
		if (lang !== 'yaml') return;
		let gone = false;
		import('$lib/yaml/highlight')
			.then((m) => {
				if (!gone) colour = m.diffSegments;
			})
			.catch((err) => console.error('YAML colouring could not load; the diff stays plain:', err));
		return () => {
			gone = true;
		};
	});

	// A trailing newline in the answer would otherwise render as an extra blank
	// row; dropping the empty tail keeps the text identical either way.
	const lines = $derived.by(() => {
		const rows = diff.split('\n');
		if (rows.length > 0 && rows[rows.length - 1] === '') rows.pop();
		return rows;
	});

	function tone(line: string): string {
		// A drawing's revision marks, not a traffic light: an added line is
		// drafting blue, a removed one is set back on the sunken ground, and
		// red stays reserved for failure. The +/- before each line says which.
		if (line.startsWith('+++') || line.startsWith('---'))
			return 'bg-paper-sunken font-semibold text-ink-secondary';
		if (line.startsWith('@@')) return 'text-blue';
		if (line.startsWith('+')) return 'bg-blue-tint text-ink';
		if (line.startsWith('-')) return 'bg-paper-sunken text-ink-muted';
		if (line.startsWith('\\')) return 'text-ink-muted italic';
		return 'text-ink-secondary';
	}
</script>

<pre
	data-testid={testid}
	class="overflow-x-auto rounded-sm border border-rule-strong bg-paper py-2 font-mono text-sm leading-5 [font-variant-ligatures:none]"
>{#each lines as line, i (i)}<span class="inline-block min-w-full px-4 {tone(line)}">{#if rows?.[i]}{#each rows[i].segments as s, j (j)}{#if s.role}<span class="syntax-{s.role}">{s.text}</span>{:else}{s.text}{/if}{/each}{:else}{line}{/if}</span>{'\n'}{/each}</pre>
