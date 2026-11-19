<script lang="ts">
	// Pause run and Unpause run on the run page's header (E103, design §5.7).
	// Pause is a hold on the run's own work, drawn as Cordon is: a neutral
	// button with the pause glyph past the action gap, opening a reason
	// dialog whose confirm is solid ink. The dialog asks the engine first
	// (PauseRun with dry_run) and shows what the pause will not hold back
	// before the confirm. Unpause sends at once. Both read "Pausing…" or
	// "Unpausing…" until the run's own event moves the status.
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { Run } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import { endsAfterChoices, pauseControls } from '#lib/pause.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import ConfirmDialog from './ConfirmDialog.svelte';
	import Icon from './Icon.svelte';

	interface Props {
		run: Run;
		status: Status;
		namespace: string;
		allowed: boolean;
		/** The href of a run in this namespace, for the parent run's link. */
		runHref: (runId: string) => string;
		/** Called once a request is recorded: the page re-reads the run. */
		onrecorded: () => void;
	}
	let { run, status, namespace, allowed, runHref, onrecorded }: Props = $props();

	let inFlight = $state<'pause' | 'unpause' | undefined>();
	// A request is in flight until the run's status says it applied, or the run ends.
	// A request is in flight until its event moves the status, or a later
	// snapshot shows it applied (the socket may be down), or the run ends.
	$effect(() => {
		if (inFlight === 'pause' && (status === Status.PAUSED || run.pause !== undefined)) inFlight = undefined;
		if (inFlight === 'unpause' && (status !== Status.PAUSED || run.pause === undefined)) inFlight = undefined;
	});
	const controls = $derived(pauseControls(run, status, { allowed, inFlight }));

	const choices = endsAfterChoices();
	let open = $state(false);
	let reason = $state('');
	let choice = $state(0);
	let warnings = $state<string[]>([]);
	let checking = $state(false);
	let busy = $state(false);
	let error = $state('');
	let unpauseError = $state('');

	function timeoutOf(index: number) {
		const seconds = choices[index]?.seconds;
		return seconds == null ? undefined : durationFromMs(seconds * 1000);
	}

	// The dry run: the engine's own checks and warnings for this length,
	// asked again whenever the length changes.
	let asked = 0;
	async function check(index: number): Promise<void> {
		const ticket = ++asked;
		checking = true;
		error = '';
		try {
			const resp = await runClient(fetch).pauseRun({ runId: run.id, namespace, timeout: timeoutOf(index), dryRun: true });
			if (ticket === asked) warnings = resp.warnings;
		} catch (err) {
			if (ticket === asked) {
				warnings = [];
				error = errorMessage(err);
			}
		} finally {
			if (ticket === asked) checking = false;
		}
	}

	function openDialog(): void {
		if (controls.pause.disabled) return;
		reason = '';
		choice = 0;
		warnings = [];
		open = true;
		void check(0);
	}

	async function pause(): Promise<void> {
		busy = true;
		error = '';
		try {
			await runClient(fetch).pauseRun({ runId: run.id, namespace, reason: reason.trim(), timeout: timeoutOf(choice) });
			inFlight = 'pause';
			open = false;
			onrecorded();
		} catch (err) {
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}

	async function unpause(): Promise<void> {
		if (controls.unpause.disabled) return;
		unpauseError = '';
		try {
			await runClient(fetch).unpauseRun({ runId: run.id, namespace });
			inFlight = 'unpause';
			onrecorded();
		} catch (err) {
			unpauseError = errorMessage(err);
		}
	}
</script>

{#if controls.pause.shown}
	<button
		type="button"
		data-testid="pause-run"
		aria-disabled={controls.pause.disabled ? 'true' : undefined}
		aria-describedby={controls.pause.disabled ? 'pause-run-why' : undefined}
		data-tip={controls.pause.disabled && controls.pause.disabled !== 'Pausing…' ? controls.pause.disabled : undefined}
		onclick={openDialog}
		class="btn"><Icon name="pause" />{controls.pause.disabled === 'Pausing…' ? 'Pausing…' : 'Pause run'}</button
	>
	{#if controls.pause.disabled}<span class="sr" id="pause-run-why" data-testid="pause-run-why">{controls.pause.disabled}</span>{/if}
{/if}
{#if controls.unpause.shown}
	<button
		type="button"
		data-testid="unpause-run"
		aria-disabled={controls.unpause.disabled ? 'true' : undefined}
		aria-describedby={controls.unpause.disabled ? 'unpause-run-why' : undefined}
		onclick={() => void unpause()}
		class="btn"><Icon name="play" />{controls.unpause.disabled === 'Unpausing…' ? 'Unpausing…' : 'Unpause run'}</button
	>
	{#if controls.unpause.parentRunId}
		<span id="unpause-run-why" class="text-sm text-ink-3" data-testid="unpause-run-why"
			>Paused because <a href={runHref(controls.unpause.parentRunId)} data-testid="unpause-parent-link">parent run {shortRunId(controls.unpause.parentRunId)}</a> is paused</span
		>
	{:else if controls.unpause.disabled}<span class="sr" id="unpause-run-why">{controls.unpause.disabled}</span>{/if}
	{#if unpauseError}<span class="text-sm text-failed-fg" role="alert" data-testid="unpause-run-error">{unpauseError}</span>{/if}
{/if}

{#if open}
	<ConfirmDialog
		testid="pause-run-confirm"
		title="Pause run {shortRunId(run.id)}?"
		acceptLabel="Pause run"
		cancelLabel="Keep running"
		glyph="pause"
		acceptTestid="pause-run-accept"
		cancelTestid="pause-run-dismiss"
		{busy}
		{error}
		destructive
		acceptDisabled={checking}
		acceptDisabledReason={checking ? 'Checking what the pause would hold…' : ''}
		onaccept={() => void pause()}
		oncancel={() => { open = false; error = ''; asked++; }}
	>
		{#snippet body()}
			<p>Running tasks finish under their own rules; nothing new starts, and its running child runs are paused too. Every deadline keeps running. Unpause it to carry on, in the order it would have taken.</p>
			<label class="text-dense font-semibold text-ink" for="pause-reason">Reason <span class="font-normal text-ink-3">(optional)</span></label>
			<textarea id="pause-reason" data-testid="pause-reason" bind:value={reason} rows="3" maxlength="1024" class="field w-full px-2.5 py-2 text-base" placeholder="Why should this run wait?"></textarea>
			<label class="text-dense font-semibold text-ink" for="pause-ends-after">Ends after</label>
			<select id="pause-ends-after" data-testid="pause-ends-after" class="field w-full px-2.5 py-2 text-base" bind:value={choice} onchange={(e) => void check(Number(e.currentTarget.value))} aria-describedby="pause-ends-after-hint">
				{#each choices as c, i (c.label)}<option value={i}>{c.label}</option>{/each}
			</select>
			<p id="pause-ends-after-hint" class="text-sm text-ink-3">If it is not unpaused by then, a run that is not failing ends Failed (RUN_PAUSE_TIMEOUT); a failing one ends with its own reason.</p>
			{#if warnings.length > 0}
				<ul class="pause-warnings" data-testid="pause-warnings" aria-label="What the pause will not hold back">
					{#each warnings as w (w)}<li>{w}</li>{/each}
				</ul>
			{/if}
		{/snippet}
	</ConfirmDialog>
{/if}

<style>
	.pause-warnings {
		display: grid;
		gap: calc(var(--spacing) * 1.5);
		margin: 0;
		padding: calc(var(--spacing) * 3);
		border: var(--rule-hair) solid var(--color-wait-mark);
		border-radius: var(--radius-sm);
		background-color: var(--color-wait-tint);
		color: var(--color-wait);
		font-size: var(--text-sm);
		list-style: none;
	}
</style>
