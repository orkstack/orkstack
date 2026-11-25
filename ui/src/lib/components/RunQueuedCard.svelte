<script lang="ts">
	// The "Queued for key" side card (E88, design §9.1 and §9.4): why the run
	// waits for a slot of its concurrency limit's key, where it stands in the
	// key's line, behind which runs, since when and until when, and the
	// limit's own note when the wait may take longer than the line explains.
	// Every line is `orkctl run get`'s Queued line (#lib/concurrency.ts); the
	// note is the server's text, shown as it comes. A run that waits because
	// it arrived needs no sentence: the heading and its key say so.
	import type { ConcurrencyQueued } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { timestampMs } from '@bufbuild/protobuf/wkt';
	import { instantSeconds } from '#lib/deliveries.ts';
	import { queuedBehind, queuedHeadline, queuedPlace, waitsOnArrival } from '#lib/concurrency.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import Icon from './Icon.svelte';
	import TimeAgo from './TimeAgo.svelte';

	interface Props {
		queued: ConcurrencyQueued;
		runHref: (runId: string) => string;
	}
	let { queued, runHref }: Props = $props();

	const behind = $derived(queuedBehind(queued));
	const timesOut = $derived(queued.timesOutAt);
	const arrival = $derived(waitsOnArrival(queued));
</script>

<section class="card" aria-labelledby="run-queued-heading" data-testid="run-queued">
	<div class="card-head"><h2 id="run-queued-heading">Queued</h2></div>
	{#if !arrival}<p class="lead" data-testid="run-queued-headline">{queuedHeadline(queued)}</p>{/if}
	<dl class="qkv">
		<dt>Key</dt>
		<dd class="font-mono" data-testid="run-queued-key">{queued.key}</dd>
		<dt>Limit</dt>
		<dd data-testid="run-queued-limit"><span class="num">{queued.limit}</span> {queued.limit === 1 ? 'run' : 'runs'} at once</dd>
		<dt>Place</dt>
		<dd data-testid="run-queued-place">{queued.positionKnown ? queuedPlace(queued) : 'Unknown'}</dd>
		{#if behind}
			<dt>Behind</dt>
			<dd data-testid="run-queued-behind">
				<ul class="holders" role="list">
					{#each behind.runs as id (id)}<li><a href={runHref(id)} class="font-mono" data-tip={id} data-testid="run-queued-holder"><span class="sr">{id}</span><span aria-hidden="true">{shortRunId(id)}</span></a></li>{/each}
					{#if behind.more > 0}<li data-testid="run-queued-more">and {behind.more} more</li>{/if}
				</ul>
				{#if behind.asOfArrival}<span class="block text-ink-3" data-testid="run-queued-as-of-arrival">as of its arrival</span>{/if}
			</dd>
		{/if}
		{#if queued.queuedSince}
			<dt>Since</dt>
			<dd data-testid="run-queued-since"><TimeAgo at={queued.queuedSince} /></dd>
		{/if}
		{#if timesOut}
			<dt>Times out</dt>
			<dd data-testid="run-queued-times-out"><time class="num" datetime={new Date(timestampMs(timesOut)).toISOString()}>{instantSeconds(timesOut)}</time></dd>
		{/if}
	</dl>
	{#if queued.note}
		<!-- The limit's note: why the wait may be longer than the line explains, in the server's words. -->
		<div class="alert mx-4 mb-4" data-testid="run-queued-note" data-note-code={queued.noteCode}>
			<span class="alert-ic"><Icon name="clock" /></span>
			<p class="min-w-0 flex-1 text-sm">{queued.note}</p>
		</div>
	{/if}
</section>

<style>
	.lead {
		margin: 0;
		padding: calc(var(--spacing) * 4) calc(var(--spacing) * 4) 0;
		font-size: var(--text-sm);
		font-weight: var(--font-weight-semibold);
		color: var(--color-ink);
		overflow-wrap: anywhere;
	}
	.qkv {
		display: grid;
		grid-template-columns: max-content 1fr;
		gap: calc(var(--spacing) * 2) calc(var(--spacing) * 4);
		margin: 0;
		padding: calc(var(--spacing) * 4);
		font-size: var(--text-sm);
	}
	.qkv dt {
		color: var(--color-ink-3);
		font-weight: var(--font-weight-semibold);
	}
	.holders {
		display: flex;
		flex-wrap: wrap;
		gap: 0 calc(var(--spacing) * 2);
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.qkv dd {
		margin: 0;
		color: var(--color-ink);
		min-width: 0;
		overflow-wrap: anywhere;
	}
</style>
