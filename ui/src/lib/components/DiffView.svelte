<script lang="ts">
	// The engine's unified diff, rendered verbatim.
	//
	// The diff is computed SERVER-side (DiffOrkflowVersions, proto comment) so
	// that `orkctl orkflow diff` and this page cannot disagree about what
	// changed. Rendering it therefore means COLOURING it and nothing else: the
	// spans below are inline, and each line is followed by a literal newline, so
	// the text in the DOM is byte-for-byte what the RPC answered. A pretty
	// re-layout here would be a second diff implementation wearing a stylesheet.
	interface Props {
		diff: string;
		testid: string;
	}

	let { diff, testid }: Props = $props();

	// A trailing newline in the answer would otherwise render as an extra blank
	// row; dropping the empty tail keeps the text identical either way.
	const lines = $derived.by(() => {
		const rows = diff.split('\n');
		if (rows.length > 0 && rows[rows.length - 1] === '') rows.pop();
		return rows;
	});

	function tone(line: string): string {
		if (line.startsWith('+++') || line.startsWith('---')) return 'text-zinc-400';
		if (line.startsWith('@@')) return 'text-sky-300';
		if (line.startsWith('+')) return 'text-emerald-300';
		if (line.startsWith('-')) return 'text-rose-300';
		if (line.startsWith('\\')) return 'text-amber-300';
		return 'text-zinc-300';
	}
</script>

<pre
	data-testid={testid}
	class="overflow-x-auto rounded-lg border border-zinc-800 bg-zinc-950 px-4 py-3 font-mono text-xs leading-relaxed"
>{#each lines as line, i (i)}<span class={tone(line)}>{line}</span>{'\n'}{/each}</pre>
