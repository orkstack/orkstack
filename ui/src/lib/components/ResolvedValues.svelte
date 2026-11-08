<script lang="ts">
	// What an awaited task was resolved with, on its panel, where a rejection says
	// its reason. A short value sits in the flow. A long one is one closed line the
	// same height, so it moves nothing until the reader opens it; its box is bounded,
	// and a value past what a run carries inline is read by reference only then.
	// Everything is plain text in a text node, with control characters shown escaped.
	import { onDestroy } from 'svelte';
	import {
		VALUES_SHOW_MAX,
		readableValues,
		resolvedValuesView,
		type ValuesRead
	} from '#lib/awaits.ts';
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import type { ArtifactRef, Payload } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { ArtifactKind } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { formatBytes } from '#lib/live.ts';
	import ErrorPanel from './ErrorPanel.svelte';

	interface Props {
		prefix: string;
		runId: string;
		namespace: string;
		taskId: string;
		output?: Payload;
		valuesRef?: ArtifactRef;
	}
	let { prefix, runId, namespace, taskId, output, valuesRef }: Props = $props();

	const view = $derived(resolvedValuesView(output, valuesRef));
	let open = $state(false);
	let read = $state.raw<ValuesRead | null>(null);
	let reading = $state(false);
	let failure = $state('');
	let abort: AbortController | undefined;
	onDestroy(() => abort?.abort());

	async function readValues(): Promise<void> {
		if (read || reading || !valuesRef) return;
		reading = true;
		failure = '';
		abort = new AbortController();
		const decoder = new TextDecoder();
		let text = '';
		let bytes = 0;
		let cut = false;
		try {
			for await (const chunk of runClient(fetch).readArtifact(
				{ runId, namespace, taskId, kind: ArtifactKind.VALUES, name: valuesRef.name },
				{ signal: abort.signal }
			)) {
				const room = VALUES_SHOW_MAX - bytes;
				text += decoder.decode(chunk.data.subarray(0, room), { stream: true });
				bytes += Math.min(chunk.data.length, room);
				if (chunk.data.length > room) {
					cut = true;
					abort.abort();
					break;
				}
			}
			read = readableValues(text, cut);
		} catch (err) {
			if (!abort.signal.aborted) failure = errorMessage(err);
		} finally {
			reading = false;
		}
	}
	function toggled(event: Event): void {
		open = (event.currentTarget as HTMLDetailsElement).open;
		if (open && view.kind === 'reference') void readValues();
	}
	const shown = $derived(view.kind === 'long' ? view.text : read?.text);
</script>

{#if view.kind === 'inline'}
	<p class="line"><code data-testid="{prefix}-values-inline">{view.text}</code></p>
{:else if view.kind !== 'none'}
	<details class="more" ontoggle={toggled}>
		<summary data-testid="{prefix}-values-more">{open ? 'Hide' : 'Show'} the values <span class="size">{formatBytes(view.bytes)}</span></summary>
		{#if open}
			{#if shown !== undefined}
				<!-- A tab stop on purpose: a box that scrolls must scroll from a keyboard. -->
				<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
				<pre class="box" tabindex="0" aria-label="The values of {taskId}" data-testid="{prefix}-values-full">{shown}</pre>
				{#if read?.cut}
					<p class="cut" data-testid="{prefix}-values-cut">The first 64 KiB of {formatBytes(view.bytes)}. The whole value is listed under Inspect.</p>
				{/if}
			{:else if failure}
				<ErrorPanel testid="{prefix}-values-error" retryTestid="{prefix}-values-retry" subject="the values of {taskId}" message={failure} busy={reading} onretry={() => void readValues()} />
			{:else}
				<p class="cut" data-testid="{prefix}-values-reading">Reading the values…</p>
			{/if}
		{/if}
	</details>
{/if}

<style>
	.line {
		margin: 0;
		font-size: 13.5px;
		line-height: 21px;
		color: var(--ink-2);
		overflow-wrap: anywhere;
	}
	.more summary {
		position: relative;
		font-size: 13.5px;
		line-height: 21px;
		font-weight: 600;
		color: var(--ink-2);
		cursor: pointer;
	}
	.more summary:hover {
		color: var(--ink);
	}
	/* A 40 px hit area, as a button's, with no height of its own. */
	.more summary::after {
		content: '';
		position: absolute;
		inset: -10px -4px;
	}
	.size {
		margin-left: 4px;
		font-weight: 400;
		color: var(--ink-3);
		font-variant-numeric: tabular-nums;
	}
	.box {
		max-height: 224px;
		margin: 6px 0 0;
		padding: 10px 12px;
		overflow: auto;
		border: 1px solid var(--line);
		border-radius: var(--r-sm);
		background: var(--surface-2);
		font-family: var(--font-code);
		font-size: 12.5px;
		line-height: 18px;
		color: var(--ink-2);
		white-space: pre-wrap;
		overflow-wrap: anywhere;
	}
	.cut {
		margin: 6px 0 0;
		font-size: 13px;
		color: var(--ink-3);
	}
</style>
