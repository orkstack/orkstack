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
		if (line.startsWith('+++') || line.startsWith('---'))
			return 'bg-surface-sunken font-medium text-fg-muted';
		if (line.startsWith('@@')) return 'bg-info-50 text-info-700';
		if (line.startsWith('+')) return 'bg-succeeded-bg text-succeeded-fg';
		if (line.startsWith('-')) return 'bg-failed-bg text-failed-fg';
		if (line.startsWith('\\')) return 'text-warning-700 italic';
		return 'text-fg-secondary';
	}
</script>

<pre
	data-testid={testid}
	class="overflow-x-auto rounded-lg border border-border bg-surface py-2 font-mono text-[13px] leading-5 shadow-xs [font-variant-ligatures:none]"
>{#each lines as line, i (i)}<span class="inline-block min-w-full px-4 {tone(line)}">{line}</span>{'\n'}{/each}</pre>
