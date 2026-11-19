<script lang="ts">
	// The Paused side card (E103, design §5.7): who paused the run and why,
	// or the parent run whose pause holds it; when the pause ends as failed,
	// to the second with its zone, and the time left by this browser's clock;
	// what still runs; and the child runs paused with it. "Ending now" once its end has passed, while the run records it.
	import type { Timestamp } from '@bufbuild/protobuf/wkt';
	import { timestampMs } from '@bufbuild/protobuf/wkt';
	import type { Run } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { instantSeconds } from '#lib/deliveries.ts';
	import { pausedLine } from '#lib/pause.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import Principal from './Principal.svelte';
	import TimeAgo from './TimeAgo.svelte';

	interface Props {
		run: Run;
		running: number;
		children: number;
		failing: boolean;
		/** The browser's clock, ticking; a test injects it. */
		now: number;
		runHref: (runId: string) => string;
		/** Called once when the pause's end has passed: the page re-reads the run. */
		onended?: () => void;
	}
	let { run, running, children, failing, now, runHref, onended }: Props = $props();

	const own = $derived(run.pause);
	const parent = $derived(run.pausedByParent);
	// The child runs the pause did not reach, and why (Run.pause_warnings).
	const warnings = $derived(run.pauseWarnings);
	const endsAt = $derived<Timestamp | undefined>(own?.expiresAt ?? parent?.backstopAt);
	const endsMs = $derived(endsAt ? timestampMs(endsAt) : undefined);
	const ended = $derived(endsMs !== undefined && endsMs <= now);
	let toldEnded = false;
	$effect(() => {
		if (ended && !toldEnded) {
			toldEnded = true;
			onended?.();
		}
	});

	// The text is to the second, as `orkctl run get` and the pause's event
	// show it; the datetime attribute keeps the exact instant.
	function exact(at: Timestamp): string {
		return new Date(timestampMs(at)).toISOString();
	}
	function left(ms: number): string {
		const minutes = Math.round(ms / 60_000);
		if (minutes < 60) return `about ${Math.max(minutes, 1)} min left`;
		const hours = Math.round(minutes / 60);
		if (hours < 48) return `about ${hours} h left`;
		return `about ${Math.round(hours / 24)} days left`;
	}
</script>

<section class="card" aria-labelledby="run-paused-heading" data-testid="run-paused">
	<div class="card-head"><h2 id="run-paused-heading">Paused</h2></div>
	<dl class="pkv">
		{#if own}
			<dt>Paused by</dt>
			<dd data-testid="run-paused-by"><Principal name={own.pausedBy || 'an unnamed caller'} verified={own.pausedByVerified} /> <TimeAgo at={own.pausedAt} /></dd>
			{#if own.reason}<dt>Why</dt><dd data-testid="run-paused-reason">{own.reason}</dd>{/if}
		{/if}
		{#if parent}
			<dt>{own ? 'Also held by' : 'Held by'}</dt>
			<dd data-testid="run-paused-parent">Its parent run <a href={runHref(parent.parentRunId)} data-testid="run-paused-parent-link">{shortRunId(parent.parentRunId)}</a> is paused <TimeAgo at={parent.since} /></dd>
		{/if}
		{#if endsAt}
			<dt>{own && !failing ? 'Ends as failed at' : 'Ends at'}</dt>
			<dd data-testid="run-paused-ends">
				{#if ended}<span data-testid="run-paused-ending">Ending now</span>{:else}<time class="num" datetime={exact(endsAt)} data-tip={endsMs !== undefined ? left(endsMs - now) : undefined} data-testid="run-paused-ends-at">{instantSeconds(endsAt)}</time>{/if}
			</dd>
		{/if}
		<dt>Now</dt>
		<dd data-testid="run-paused-line">{pausedLine(running, failing)}</dd>
		{#if children > 0}<dt>Child runs</dt><dd data-testid="run-paused-children">{children} {children === 1 ? 'child run' : 'child runs'} paused with it</dd>{/if}
		{#if warnings.length > 0}
			<dt>Not paused</dt>
			<dd data-testid="run-paused-warnings">
				<ul class="warn">
					{#each warnings as w, i (i)}
						<li data-testid="run-paused-warning">
							Task {w.taskId}{#if w.childRunId}, child run <a href={runHref(w.childRunId)}>{shortRunId(w.childRunId)}</a>{/if}: {w.message}
						</li>
					{/each}
				</ul>
			</dd>
		{/if}
	</dl>
</section>

<style>
	.pkv {
		display: grid;
		grid-template-columns: max-content 1fr;
		gap: calc(var(--spacing) * 2) calc(var(--spacing) * 4);
		margin: 0;
		padding: calc(var(--spacing) * 4);
		font-size: var(--text-sm);
	}
	.pkv dt {
		color: var(--color-ink-3);
		font-weight: var(--font-weight-semibold);
	}
	.pkv dd {
		margin: 0;
		color: var(--color-ink);
	}
	.warn {
		display: grid;
		gap: calc(var(--spacing) * 1);
		margin: 0;
		padding: 0;
		list-style: none;
	}
</style>
