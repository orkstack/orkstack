<script lang="ts">
	// A read-only YAML document, coloured with the syntax pencils. The text is
	// there from the first paint, plain; the classifier (./highlight.ts, the
	// editor's own) arrives on mount and wraps the same text in spans, so what
	// a person selects and copies is the document byte for byte either way.
	import { onMount } from 'svelte';
	import type { Segment } from './highlight';

	interface Props {
		text: string;
		testid: string;
		class?: string;
	}

	let { text, testid, class: classes = '' }: Props = $props();

	let colour = $state<((text: string) => Segment[]) | null>(null);
	const segments = $derived(colour ? colour(text) : null);

	onMount(() => {
		let gone = false;
		import('./highlight')
			.then((m) => {
				if (!gone) colour = m.yamlSegments;
			})
			.catch((err) => console.error('YAML colouring could not load; the text stays plain:', err));
		return () => {
			gone = true;
		};
	});
</script>

<pre data-testid={testid} class={classes}>{#if segments}{#each segments as s, i (i)}{#if s.role}<span class="syntax-{s.role}">{s.text}</span>{:else}{s.text}{/if}{/each}{:else}{text}{/if}</pre>
