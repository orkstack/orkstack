<script lang="ts">
	// The Resume dialog: what a resume of this run would run again and what it
	// keeps, previewed before anything changes, then the resume itself.
	//
	// The preview is ResumeRun with dry_run; the confirm sends back exactly the
	// plan it showed (its digest and the ending it sets aside), so a plan that
	// changed in between is refused and never accepted unseen: the same rule
	// `orkctl run resume` keeps (docs/RESUME.md). Opening the dialog is the
	// question the CLI asks at its prompt.
	import { onDestroy, onMount } from 'svelte';
	import CliPopover from './CliPopover.svelte';
	import Dialog from './Dialog.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import Icon from './Icon.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import type { ResumeRunResponse } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { ResumeWarningKind } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import {
		ACCEPTANCES,
		MAX_RESUMES,
		acceptanceFor,
		MAX_RESUME_REASON,
		RESUME_CALL_TIMEOUT_MS,
		SECTION_LIMIT,
		planSections,
		reasonFits,
		unconfirmableReason,
		refusalLead,
		refusalOf,
		rerunSucceeded,
		resumeCommand,
		unknownAttempts,
		type Acceptance,
		type RefusalView
	} from '#lib/resume.ts';

	interface Props {
		runId: string;
		namespace: string;
		/** Retry from here: the chosen task. Empty for Resume. */
		fromTask?: string;
		onclose: () => void;
		/** The resume went ahead: RUN_RESUMED's sequence. */
		onresumed: (seqBase: bigint) => void;
		/** The run may have changed under the page (a confirm whose outcome is unknown). */
		onchanged: () => void;
		/** Whether the caller holds run.resume.accept_gap on this run's Orkflow. */
		mayAccept?: boolean;
		/** Why not, when mayAccept is false. */
		mayAcceptWhy?: string;
	}

	let { runId, namespace, fromTask = '', onclose, onresumed, onchanged, mayAccept = false, mayAcceptWhy = '' }: Props = $props();

	let plan = $state<ResumeRunResponse | undefined>(undefined);
	let loading = $state(false);
	let sending = $state(false);
	let error = $state('');
	let refusal = $state<RefusalView | undefined>(undefined);
	let acknowledged = $state(false);
	let reason = $state('');
	// The acceptances the preview and the confirm both carry, so the plan shown
	// is the plan they apply to (docs/RESUME.md, "Accepting a gap…").
	// Only a caller who may accept ever sends one: a control that is not shown
	// never carries a value.
	let wantGap = $state(false);
	// The highest stored event the last refusal named: what accepting a gap
	// accepts. When it moves, the acceptance is asked again.
	let gapUpTo = $state(0n);
	const acceptGap = $derived(mayAccept && wantGap && gapUpTo > 0n);
	const accepted = $derived(new Set<Acceptance>(acceptGap ? ['gap'] : []));
	// Once a refusal has offered a control, it stays for the dialog's life, so
	// focus stays on it while the preview runs again, and an administrator can
	// see, and take back, what the plan was previewed with.
	let offered = $state<Acceptance[]>([]);
	$effect(() => {
		const a = acceptanceFor(refusal);
		if (a && !offered.includes(a)) offered = [...offered, a];
	});

	let abort: AbortController | undefined;
	onDestroy(() => abort?.abort());

	const sections = $derived(plan ? planSections(plan) : []);
	const again = $derived(plan ? rerunSucceeded(plan) : 0);
	const unknown = $derived(plan ? unknownAttempts(plan) : { keys: [], unkeyed: 0 });
	const notes = $derived(plan ? plan.warnings.filter((w) => w.kind !== ResumeWarningKind.ATTEMPT_OUTCOME_UNKNOWN) : []);
	const unknownWarnings = $derived(
		plan ? plan.warnings.filter((w) => w.kind === ResumeWarningKind.ATTEMPT_OUTCOME_UNKNOWN) : []
	);
	const blocked = $derived(plan ? unconfirmableReason(plan) : '');
	const reasonOk = $derived(reasonFits(reason));
	const waitingFor = $derived(
		blocked ||
			(!reasonOk ? `The reason is longer than ${MAX_RESUME_REASON} bytes.` : '') ||
			(unknown.keys.length > 0 && !acknowledged ? 'Confirm that the earlier attempts above may still be running.' : '')
	);
	const canConfirm = $derived(!!plan && !loading && !sending && !refusal && waitingFor === '');

	// While a confirm is in flight the dialog stays: its answer (a refusal, or
	// an outcome not known) must reach the person who asked.
	function close(): void {
		if (!sending) onclose();
	}
	const title = $derived(fromTask ? `Retry run ${runId} from ${fromTask}` : `Resume run ${runId}`);
	const command = $derived(resumeCommand(namespace, runId, fromTask, unknown.keys.length > 0, reason.trim(), accepted, gapUpTo));

	function refused(r: RefusalView | undefined): void {
		refusal = r;
		if (!r || r.highest <= 0n) return;
		if (r.highest !== gapUpTo) wantGap = false;
		gapUpTo = r.highest;
	}

	async function preview(): Promise<void> {
		abort?.abort();
		const a = new AbortController();
		abort = a;
		loading = true;
		error = '';
		refusal = undefined;
		acknowledged = false;
		plan = undefined;
		try {
			const answer = await runClient(fetch).resumeRun(
				{
					runId,
					namespace,
					fromTaskId: fromTask,
					dryRun: true,
					acceptSequenceGap: acceptGap,
					acceptedHighestSeq: acceptGap ? gapUpTo : 0n
				},
				{ signal: a.signal, timeoutMs: RESUME_CALL_TIMEOUT_MS }
			);
			if (!a.signal.aborted) plan = answer;
		} catch (err) {
			if (a.signal.aborted) return;
			refused(refusalOf(err, false));
			if (!refusal) error = errorMessage(err);
		} finally {
			if (abort === a) loading = false;
		}
	}

	async function confirm(): Promise<void> {
		if (!plan || !canConfirm) return;
		const shown = plan;
		sending = true;
		error = '';
		try {
			const done = await runClient(fetch).resumeRun(
				{
					runId,
					namespace,
					reason: reason.trim(),
					fromTaskId: fromTask,
					expectedTerminalSequence: shown.terminalSequence,
					expectedPlanDigest: shown.planDigest,
					acknowledgedUnknownAttempts: unknown.keys,
					acceptSequenceGap: acceptGap,
					acceptedHighestSeq: acceptGap ? gapUpTo : 0n
				},
				{ timeoutMs: RESUME_CALL_TIMEOUT_MS }
			);
			sending = false;
			onresumed(done.seqBase);
		} catch (err) {
			refused(refusalOf(err, true));
			if (!refusal) error = errorMessage(err);
			if (refusal?.cls === 'unconfirmed') onchanged();
		} finally {
			sending = false;
		}
	}

	onMount(() => void preview());

	function toggle(a: Acceptance, box: HTMLInputElement): void {
		if (sending) {
			box.checked = accepted.has(a);
			return;
		}
		wantGap = box.checked;
		void preview();
	}

</script>

<Dialog {title} testid="resume-dialog" onclose={close} widthClass="max-w-2xl" glyph="retry">
	{#snippet body()}
		<div class="grid grid-cols-1 gap-4">
			<p class="max-w-prose text-ink-2">
				{#if fromTask}
					Task <span class="font-medium text-ink">{fromTask}</span>, and everything downstream of it, run again, with
					the failed tasks. Every other task keeps its result.
				{:else}
					The failed tasks, and everything downstream of them, run again. Every other task keeps its result. The run
					keeps its id, and its events carry on.
				{/if}
			</p>

			{#if loading}
				<p class="text-ink-3" data-testid="resume-loading" role="status">Working out what would run again…</p>
			{/if}

			{#if error}
				<ErrorPanel
					testid="resume-error"
					retryTestid="resume-retry"
					subject="the resume of {runId}"
					message={error}
					onretry={() => void preview()}
				/>
			{/if}

			{#if refusal}
				<div class="alert" data-testid="resume-refusal" data-class={refusal.cls} role="alert">
					<Icon name="alert" class="size-4 shrink-0 text-ink-2" />
					<div class="grid min-w-0 gap-1">
						<p class="font-medium text-ink">{refusalLead(refusal)}</p>
						<p class="text-ink-2">{refusal.message}</p>
						{#if refusal.added.length}
							<p class="text-ink-2" data-testid="resume-diff-added">Now also: {refusal.added.join(', ')}</p>
						{/if}
						{#if refusal.dropped.length}
							<p class="text-ink-2" data-testid="resume-diff-dropped">No longer: {refusal.dropped.join(', ')}</p>
						{/if}
						<p class="font-mono text-xs text-ink-3">{refusal.reason}</p>
					</div>
				</div>
			{/if}

			{#if offered.length}
				{#if mayAccept}
					<div class="grid gap-2" data-testid="resume-acceptances" role="group" aria-label="Accept and resume">
						{#each offered as a (a)}
							<label class="flex items-start gap-2 text-ink" data-testid="resume-accept-{a}">
								<input
									type="checkbox"
									class="mt-1"
									checked={accepted.has(a)}
									aria-disabled={sending ? 'true' : undefined}
									onchange={(e) => toggle(a, e.currentTarget)}
									data-testid="resume-accept-{a}-box"
								/>
								<span>
									{ACCEPTANCES[a].label}, up to event <span class="tabular-nums">{gapUpTo}</span>
									<span class="block text-ink-2">{ACCEPTANCES[a].consequence}</span>
								</span>
							</label>
						{/each}
					</div>
				{:else}
					<p class="text-ink-2" data-testid="resume-accept-denied">
						An administrator can accept this and resume. {mayAcceptWhy}
					</p>
				{/if}
			{/if}

			{#if plan && !refusal}
				<div class="grid grid-cols-1 gap-3" data-testid="resume-plan">
					{#each sections as section (section.key)}
						<section data-testid="resume-section-{section.key}" class="grid gap-1">
							<h3 class="text-label text-ink-3">
								{section.title} <span class="tabular-nums">{section.units.length}</span>
							</h3>
							<ul class="grid gap-0.5">
								{#each section.units.slice(0, SECTION_LIMIT) as unit (unit.name)}
									<li>
										<span class="text-ink">{unit.name}</span>{#if unit.why}<span class="text-ink-3"> · {unit.why}</span>{/if}
										{#if unit.detail}<span class="block text-xs text-ink-3">{unit.detail}</span>{/if}
									</li>
								{/each}
							</ul>
							{#if section.units.length > SECTION_LIMIT}
								<p class="text-xs text-ink-3 tabular-nums">and {section.units.length - SECTION_LIMIT} more</p>
							{/if}
						</section>
					{/each}
				</div>

				{#if again > 0}
					<p class="capline text-ink-2" data-testid="resume-repeats">
						{again === 1 ? '1 task that succeeded before runs again;' : `${again} tasks that succeeded before run again;`}
						their external side effects repeat.
					</p>
				{/if}

				{#each notes as note, i (i)}
					<p class="text-ink-2" data-testid="resume-note">{note.message}</p>
				{/each}

				{#if unknownWarnings.length}
					<label class="flex items-start gap-2 text-ink" data-testid="resume-unknown">
						<input type="checkbox" class="mt-1" bind:checked={acknowledged} data-testid="resume-unknown-ack" />
						<span>
							I understand that these earlier attempts may still be running:
							{#each unknownWarnings as w, i (i)}<span class="block text-ink-2">{w.message}</span>{/each}
						</span>
					</label>
				{/if}

				{#if plan.concurrencyEffect}
					<p class="text-ink-2" data-testid="resume-concurrency">{plan.concurrencyEffect}</p>
				{/if}

				<label class="grid gap-1">
					<span class="text-label text-ink-2">Reason <span class="font-normal text-ink-3">(recorded with the resume)</span></span>
					<textarea class="field min-h-16" rows="2" bind:value={reason} data-testid="resume-reason" aria-invalid={reasonOk ? undefined : 'true'}></textarea>
				</label>

				<p class="text-xs text-ink-3 tabular-nums" data-testid="resume-used">
					Resumes used: {plan.resumesUsed} of {MAX_RESUMES}
				</p>
			{/if}

			{#if plan && !refusal && waitingFor}
				<p id="resume-why-not" class="text-ink-2" data-testid="resume-why-not">{waitingFor}</p>
			{/if}

			<div class="flex flex-wrap items-center justify-end gap-2">
				<CliPopover testid="resume-cli" title="Command line: resume {runId}" commands={[{ label: 'Resume this run', command }]} />
				{#if refusal && (refusal.cls === 'wait' || refusal.cls === 'replan')}
					<button type="button" class="btn" data-testid="resume-preview-again" onclick={() => void preview()}>Preview again</button>
				{/if}
				<button type="button" class="btn btn-quiet" data-testid="resume-back" disabled={sending} onclick={() => close()}>Keep the run as it is</button>
				<button
					type="button"
					class="btn btn-primary"
					data-testid="resume-confirm"
					aria-disabled={canConfirm ? undefined : 'true'}
					aria-describedby={plan && !refusal && waitingFor ? 'resume-why-not' : undefined}
					disabled={sending}
					onclick={() => void confirm()}
				><Icon name="play" />{sending ? 'Resuming…' : fromTask ? `Retry from ${fromTask}` : 'Resume run'}</button>
			</div>
		</div>
	{/snippet}
</Dialog>
