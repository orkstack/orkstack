<script lang="ts">
	// The Schedules tab: when THIS orkflow starts on its own.
	//
	// A sibling of OrkflowTriggers and working the same way: the loader reads
	// the list, this component writes and then asks the loader to read it
	// again. Nothing here is optimistic — a row appears because the cluster
	// answered with it, and every field of a row (paused, note, next fire
	// times, recent fires) is the cluster's own answer for this read. OrkStack
	// stores nothing else about a schedule.
	import { invalidateAll } from '$app/navigation';
	import { create } from '@bufbuild/protobuf';
	import { timestampFromDate } from '@bufbuild/protobuf/wkt';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import Dialog from '$lib/components/Dialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Select from '$lib/components/Select.svelte';
	import { parseInputObject } from '$lib/authoring';
	import { errorMessage } from '$lib/client';
	import { scheduleClient } from '$lib/clients/schedules';
	import { PayloadSchema } from '$lib/gen/orkstack/v1/common_pb';
	import type { OrkflowVersion } from '$lib/gen/orkstack/v1/orkflow_service_pb';
	import { ScheduleSpecSchema, type Schedule } from '$lib/gen/orkstack/v1/schedule_pb';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import {
		OVERLAP_POLICY_SLUGS,
		fireOutcome,
		firesLabel,
		isoUTC,
		lastFire,
		overlapLabel,
		overlapPolicyFromSlug,
		parseDuration,
		scheduleCreateCli,
		scheduleDeleteCli,
		scheduleFireCli,
		schedulePauseCli,
		scheduleResumeCli,
		scheduleState,
		whenLabel
	} from '$lib/schedules';

	interface Props {
		name: string;
		namespace: string;
		versions: OrkflowVersion[];
		currentVersion: string;
		schedules: Schedule[];
		error: string | null;
	}

	let { name, namespace, versions, currentVersion, schedules, error }: Props = $props();

	// CURRENT is the "pin nothing" sentinel, spelled as OrkflowTriggers spells
	// it; it becomes the empty wire value, which is what an unpinned schedule
	// stores and resolves at each fire.
	const CURRENT = '__current__';

	let scheduleName = $state('');
	let selectedVersion = $state(CURRENT);
	let cronText = $state('');
	let intervalText = $state('');
	let timezone = $state('');
	let overlap = $state('skip');
	let jitterText = $state('');
	let catchupText = $state('');
	let pauseOnFailure = $state(false);
	let startAtText = $state('');
	let endAtText = $state('');
	let description = $state('');
	let inputText = $state('');
	let createPaused = $state(false);
	let creating = $state(false);
	let createError = $state('');

	let expanded = $state<Record<string, boolean>>({});
	let pausing = $state('');
	let pauseReason = $state('');
	let firing = $state('');
	let deleting = $state('');
	let actionBusy = $state(false);
	let actionError = $state('');
	let reloading = $state(false);

	const currentLabel = $derived(
		currentVersion || versions.find((entry) => entry.isCurrent)?.version || ''
	);
	const versionOptions = $derived([
		{
			value: CURRENT,
			label: currentLabel ? `Current (${currentLabel}) — resolved at each fire` : 'Current — resolved at each fire'
		},
		...versions.map((entry) => ({
			value: entry.version,
			label: `${entry.version}${entry.isCurrent ? ' — current' : ''}`
		}))
	]);
	const overlapOptions = OVERLAP_POLICY_SLUGS.map((slug) => ({ value: slug, label: slug }));

	// The list is the cluster's visibility index, which lags a create or a
	// delete by a moment. After either, the loader is asked again until the
	// row the write made or removed is (or is no longer) listed — a bounded
	// wait for the cluster's own answer, never a row this component invented.
	async function refreshUntil(done: () => boolean) {
		for (let attempt = 0; attempt < 20; attempt++) {
			await invalidateAll();
			if (done()) return;
			await new Promise((resolve) => setTimeout(resolve, 500));
		}
	}

	function parseTime(field: string, text: string): { value: Date | undefined } | { error: string } {
		const trimmed = text.trim();
		if (!trimmed) return { value: undefined };
		const date = new Date(trimmed);
		if (Number.isNaN(date.getTime())) return { error: `${field}: "${trimmed}" is not an RFC 3339 time.` };
		return { value: date };
	}

	async function createSchedule() {
		createError = '';
		const interval = parseDuration('Interval', intervalText);
		const jitter = parseDuration('Jitter', jitterText);
		const catchup = parseDuration('Catch-up window', catchupText);
		const startAt = parseTime('Start at', startAtText);
		const endAt = parseTime('End at', endAtText);
		for (const parsed of [interval, jitter, catchup, startAt, endAt]) {
			if ('error' in parsed) {
				createError = parsed.error;
				return;
			}
		}
		let input = undefined;
		if (inputText.trim()) {
			const parsed = parseInputObject(inputText);
			if ('error' in parsed) {
				createError = parsed.error;
				return;
			}
			input = create(PayloadSchema, { data: parsed.value });
		}
		const cron = cronText
			.split('\n')
			.map((line) => line.trim())
			.filter((line) => line !== '');
		const created = scheduleName.trim();
		creating = true;
		try {
			await scheduleClient(fetch).createSchedule({
				namespace,
				name: created,
				orkflowName: name,
				orkflowVersion: selectedVersion === CURRENT ? '' : selectedVersion,
				input,
				description,
				paused: createPaused,
				spec: create(ScheduleSpecSchema, {
					cron,
					interval: 'value' in interval ? interval.value : undefined,
					timezone: timezone.trim(),
					jitter: 'value' in jitter ? jitter.value : undefined,
					catchupWindow: 'value' in catchup ? catchup.value : undefined,
					startAt: 'value' in startAt && startAt.value ? timestampFromDate(startAt.value) : undefined,
					endAt: 'value' in endAt && endAt.value ? timestampFromDate(endAt.value) : undefined,
					overlap: overlapPolicyFromSlug(overlap),
					pauseOnFailure
				})
			});
			scheduleName = '';
			cronText = '';
			intervalText = '';
			description = '';
			inputText = '';
			await refreshUntil(() => schedules.some((row) => row.name === created));
		} catch (err) {
			// The draft stays in the form, and the server's refusal — the
			// cluster's own cron-syntax message included — is shown verbatim.
			createError = errorMessage(err);
		} finally {
			creating = false;
		}
	}

	async function act(run: () => Promise<unknown>, done: () => void, settled: () => boolean = () => true) {
		actionBusy = true;
		actionError = '';
		try {
			await run();
			done();
			await refreshUntil(settled);
		} catch (err) {
			actionError = errorMessage(err);
		} finally {
			actionBusy = false;
		}
	}

	const pause = () =>
		act(
			() => scheduleClient(fetch).pauseSchedule({ namespace, name: pausing, reason: pauseReason }),
			() => {
				pausing = '';
				pauseReason = '';
			}
		);
	const resume = (target: string) =>
		act(() => scheduleClient(fetch).resumeSchedule({ namespace, name: target }), () => {});
	// No overlap: the schedule's own policy applies to a fire from this button.
	const fire = () =>
		act(
			() => scheduleClient(fetch).fireSchedule({ namespace, name: firing }),
			() => (firing = '')
		);
	function remove() {
		const removed = deleting;
		return act(
			() => scheduleClient(fetch).deleteSchedule({ namespace, name: removed }),
			() => (deleting = ''),
			() => !schedules.some((row) => row.name === removed)
		);
	}

	async function reload() {
		reloading = true;
		try {
			await invalidateAll();
		} finally {
			reloading = false;
		}
	}

	const stateClasses: Record<string, string> = {
		active: 'border-success-200 bg-success-50 text-success-700',
		paused: 'border-warning-200 bg-warning-50 text-warning-800',
		unavailable: 'border-danger-200 bg-danger-50 text-danger-700'
	};

	const inputClass =
		'h-9 w-full rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none';
	const smallButton =
		'inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50';
</script>

<p class="mb-5 text-sm text-fg-secondary" data-testid="schedules-scope">
	Each schedule starts <span class="font-medium text-fg">{name}</span> in
	<span class="font-medium text-fg">{namespaceLabel(namespace)}</span> at the times it names. Temporal keeps
	the schedule; OrkStack stores nothing else about it.
</p>

<section class="mb-6 rounded-lg border border-border bg-surface p-4 shadow-xs">
	<h2 class="text-sm font-semibold text-fg">Create schedule</h2>
	<form
		class="mt-3 space-y-4"
		data-testid="schedule-create-form"
		onsubmit={(event) => {
			event.preventDefault();
			void createSchedule();
		}}
	>
		<div class="grid gap-4 sm:grid-cols-2">
			<div class="flex flex-col gap-1.5">
				<label for="schedule-name" class="text-sm font-medium text-fg">Name</label>
				<input
					id="schedule-name"
					data-testid="schedule-name"
					bind:value={scheduleName}
					placeholder="nightly-reconcile"
					class={inputClass}
				/>
			</div>
			<div class="flex flex-col gap-1.5">
				<label for="schedule-version" class="text-sm font-medium text-fg">Version</label>
				<Select
					id="schedule-version"
					testid="schedule-version"
					value={selectedVersion}
					options={versionOptions}
					disabled={creating}
					onchange={(next) => (selectedVersion = next)}
				/>
			</div>
			<div class="flex flex-col gap-1.5">
				<label for="schedule-cron" class="text-sm font-medium text-fg">
					Cron <span class="font-normal text-fg-muted">(one expression per line)</span>
				</label>
				<textarea
					id="schedule-cron"
					data-testid="schedule-cron"
					bind:value={cronText}
					rows="2"
					placeholder="0 2 * * *"
					class="w-full rounded-md border border-border bg-surface px-3 py-2 font-mono text-sm text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				></textarea>
			</div>
			<div class="flex flex-col gap-1.5">
				<label for="schedule-interval" class="text-sm font-medium text-fg">
					Interval <span class="font-normal text-fg-muted">(optional, e.g. 15m)</span>
				</label>
				<input id="schedule-interval" data-testid="schedule-interval" bind:value={intervalText} class={inputClass} />
			</div>
			<div class="flex flex-col gap-1.5">
				<label for="schedule-timezone" class="text-sm font-medium text-fg">
					Time zone <span class="font-normal text-fg-muted">(IANA; empty is UTC)</span>
				</label>
				<input
					id="schedule-timezone"
					data-testid="schedule-timezone"
					bind:value={timezone}
					placeholder="Europe/Lisbon"
					class={inputClass}
				/>
			</div>
			<div class="flex flex-col gap-1.5">
				<label for="schedule-overlap" class="text-sm font-medium text-fg">
					When the previous run is still going
				</label>
				<Select
					id="schedule-overlap"
					testid="schedule-overlap"
					value={overlap}
					options={overlapOptions}
					disabled={creating}
					onchange={(next) => (overlap = next)}
				/>
			</div>
		</div>

		<details class="rounded-md border border-border-subtle p-3">
			<summary class="cursor-pointer text-sm font-medium text-fg">Advanced</summary>
			<div class="mt-3 grid gap-4 sm:grid-cols-2">
				<div class="flex flex-col gap-1.5">
					<label for="schedule-jitter" class="text-sm font-medium text-fg">Jitter</label>
					<input id="schedule-jitter" data-testid="schedule-jitter" bind:value={jitterText} placeholder="30s" class={inputClass} />
				</div>
				<div class="flex flex-col gap-1.5">
					<label for="schedule-catchup-window" class="text-sm font-medium text-fg">Catch-up window</label>
					<input
						id="schedule-catchup-window"
						data-testid="schedule-catchup-window"
						bind:value={catchupText}
						placeholder="1h (at least 10s)"
						class={inputClass}
					/>
				</div>
				<div class="flex flex-col gap-1.5">
					<label for="schedule-start-at" class="text-sm font-medium text-fg">Start at (RFC 3339)</label>
					<input
						id="schedule-start-at"
						data-testid="schedule-start-at"
						bind:value={startAtText}
						placeholder="2026-10-01T00:00:00Z"
						class={inputClass}
					/>
				</div>
				<div class="flex flex-col gap-1.5">
					<label for="schedule-end-at" class="text-sm font-medium text-fg">End at (RFC 3339)</label>
					<input
						id="schedule-end-at"
						data-testid="schedule-end-at"
						bind:value={endAtText}
						placeholder="2026-12-31T23:59:59Z"
						class={inputClass}
					/>
				</div>
				<div class="flex flex-col gap-1.5 sm:col-span-2">
					<label for="schedule-description" class="text-sm font-medium text-fg">Description</label>
					<input id="schedule-description" data-testid="schedule-description" bind:value={description} class={inputClass} />
				</div>
				<div class="flex flex-col gap-1.5 sm:col-span-2">
					<label for="schedule-input" class="text-sm font-medium text-fg">
						Input <span class="font-normal text-fg-muted">(a JSON object, submitted as given at every fire)</span>
					</label>
					<textarea
						id="schedule-input"
						data-testid="schedule-input"
						bind:value={inputText}
						rows="3"
						placeholder={'{"mode": "full"}'}
						class="w-full rounded-md border border-border bg-surface px-3 py-2 font-mono text-sm text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
					></textarea>
				</div>
				<label class="flex items-center gap-2 text-sm text-fg">
					<input type="checkbox" data-testid="schedule-pause-on-failure" bind:checked={pauseOnFailure} />
					Pause the schedule when a fire fails or is refused
				</label>
			</div>
		</details>

		<label class="flex items-center gap-2 text-sm text-fg">
			<input type="checkbox" data-testid="schedule-create-paused" bind:checked={createPaused} />
			Create paused so I can review the next fire times first
		</label>

		{#if createError}
			<ErrorPanel testid="schedule-error" message={createError} />
		{/if}

		<button
			type="submit"
			data-testid="schedule-create"
			disabled={creating}
			class="inline-flex h-9 items-center justify-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50"
		>
			{creating ? 'Creating…' : 'Create schedule'}
		</button>
	</form>
</section>

{#if actionError && !pausing && !firing && !deleting}
	<div class="mb-4"><ErrorPanel testid="schedule-action-error" message={actionError} /></div>
{/if}

{#if error}
	<ErrorPanel
		testid="schedules-error"
		retryTestid="schedules-retry"
		subject="the schedules of {name} in namespace {namespaceLabel(namespace)}"
		message={error}
		busy={reloading}
		onretry={reload}
	/>
{:else if schedules.length === 0}
	<p data-testid="schedules-empty" class="text-sm text-fg-muted">
		No schedules exist for {name} in namespace {namespaceLabel(namespace)}. Create one above to start it at
		the times it names.
	</p>
{:else}
	<ul class="space-y-3" data-testid="schedule-list">
		{#each schedules as schedule (schedule.name)}
			{@const state = scheduleState(schedule)}
			{@const newest = lastFire(schedule)}
			{@const outcome = newest ? fireOutcome(newest) : undefined}
			<li data-testid="schedule-row-{schedule.name}" class="rounded-lg border border-border bg-surface p-4 shadow-xs">
				<div class="flex flex-wrap items-start justify-between gap-4">
					<div class="min-w-0 space-y-1">
						<p class="flex items-center gap-2">
							<span class="text-sm font-medium text-fg">{schedule.name}</span>
							<span
								data-testid="schedule-row-state-{schedule.name}"
								title={state === 'paused' ? schedule.note : state === 'unavailable' ? schedule.unavailableReason : ''}
								class="inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium tracking-wide uppercase {stateClasses[
									state
								]}">{state}</span
							>
							<span class="font-mono text-xs text-fg-muted">{schedule.id}</span>
						</p>
						{#if state === 'paused' && schedule.note}
							<p class="text-xs text-warning-800" data-testid="schedule-row-note-{schedule.name}">{schedule.note}</p>
						{/if}
						{#if state === 'unavailable'}
							<p class="text-xs text-danger-700">{schedule.unavailableReason}</p>
						{:else}
							<p class="text-sm text-fg-secondary">
								Runs {schedule.orkflowName}@{schedule.orkflowVersion || 'current'} · {whenLabel(schedule.spec)} ·
								overlap {overlapLabel(schedule.spec?.overlap ?? 0)}
							</p>
							{#if schedule.description}
								<p class="text-sm text-fg-secondary">{schedule.description}</p>
							{/if}
							<p class="text-xs text-fg-muted">
								Next fire <span data-testid="schedule-row-next-{schedule.name}"
									>{schedule.nextFireTimes.length > 0 ? isoUTC(schedule.nextFireTimes[0]) : '—'}</span
								>
								· Last run
								<span data-testid="schedule-row-last-run-{schedule.name}">
									{#if outcome?.kind === 'run'}
										<a
											href={withNamespace(`/runs/${outcome.runId}`, namespace)}
											class="font-mono text-brand-700 hover:underline">{outcome.runId}</a
										>
									{:else if outcome?.kind === 'refused'}
										<span class="text-danger-700" title={outcome.refusal}>refused</span>
									{:else}
										-
									{/if}
								</span>
							</p>
						{/if}
					</div>
					<div class="flex shrink-0 flex-wrap items-center gap-2">
						{#if state !== 'unavailable'}
							{#if schedule.paused}
								<button
									type="button"
									data-testid="schedule-resume-{schedule.name}"
									disabled={actionBusy}
									onclick={() => resume(schedule.name)}
									class={smallButton}>Resume</button
								>
								<CliEquivalent
									command={scheduleResumeCli(schedule.name, namespace)}
									testid="schedule-resume-cli-{schedule.name}"
									label="Resume {schedule.name} from the CLI"
								/>
							{:else}
								<button
									type="button"
									data-testid="schedule-pause-{schedule.name}"
									onclick={() => {
										pausing = schedule.name;
										pauseReason = '';
										actionError = '';
									}}
									class={smallButton}>Pause</button
								>
								<CliEquivalent
									command={schedulePauseCli(schedule.name, namespace, '')}
									testid="schedule-pause-cli-{schedule.name}"
									label="Pause {schedule.name} from the CLI"
								/>
							{/if}
							<button
								type="button"
								data-testid="schedule-fire-{schedule.name}"
								onclick={() => {
									firing = schedule.name;
									actionError = '';
								}}
								class={smallButton}>Fire now</button
							>
							<CliEquivalent
								command={scheduleFireCli(schedule.name, namespace)}
								testid="schedule-fire-cli-{schedule.name}"
								label="Fire {schedule.name} from the CLI"
							/>
							<CliEquivalent
								command={scheduleCreateCli(schedule)}
								testid="schedule-cli-{schedule.name}"
								label="The command that recreates {schedule.name}"
							/>
						{/if}
						<button
							type="button"
							data-testid="schedule-delete-{schedule.name}"
							onclick={() => {
								deleting = schedule.name;
								actionError = '';
							}}
							class="inline-flex h-8 items-center rounded-md border border-danger-200 bg-surface px-3 text-xs font-medium text-danger-700 shadow-xs transition-colors duration-150 hover:border-danger-300 hover:bg-danger-50 focus-visible:ring-2 focus-visible:ring-danger-500/40 focus-visible:outline-none"
						>
							Delete
						</button>
						{#if state !== 'unavailable'}
							<button
								type="button"
								data-testid="schedule-expand-{schedule.name}"
								aria-expanded={expanded[schedule.name] ? 'true' : 'false'}
								onclick={() => (expanded = { ...expanded, [schedule.name]: !expanded[schedule.name] })}
								class={smallButton}>{expanded[schedule.name] ? 'Hide fires' : 'Show fires'}</button
							>
						{/if}
					</div>
				</div>

				{#if expanded[schedule.name] && state !== 'unavailable'}
					<div class="mt-4 grid gap-4 border-t border-border-subtle pt-4 md:grid-cols-2">
						<div>
							<h3 class="mb-2 text-xs font-semibold text-fg">Next fires</h3>
							{#if schedule.nextFireTimes.length === 0}
								<p class="text-xs text-fg-muted" data-testid="schedule-next-fires-{schedule.name}">None.</p>
							{:else}
								<ol class="space-y-0.5 font-mono text-xs text-fg-secondary" data-testid="schedule-next-fires-{schedule.name}">
									{#each schedule.nextFireTimes as at, index (index)}
										<li>{isoUTC(at)}</li>
									{/each}
								</ol>
							{/if}
						</div>
						<div>
							<h3 class="mb-2 text-xs font-semibold text-fg">Recent fires</h3>
							<p class="mb-2 text-xs text-fg-muted" data-testid="schedule-fires-{schedule.name}">
								Fires: {firesLabel(schedule)}
							</p>
							{#if schedule.recentFires.length === 0}
								<p class="text-xs text-fg-muted" data-testid="schedule-recent-{schedule.name}">No fires yet.</p>
							{:else}
								<table class="w-full text-left text-xs" data-testid="schedule-recent-{schedule.name}">
									<thead class="text-fg-muted">
										<tr><th class="pr-3 font-medium">Scheduled</th><th class="pr-3 font-medium">Run</th></tr>
									</thead>
									<tbody class="font-mono text-fg-secondary">
										{#each schedule.recentFires as recent, index (index)}
											{@const result = fireOutcome(recent)}
											<tr>
												<td class="pr-3 align-top">{isoUTC(recent.scheduledAt)}</td>
												<td class="align-top">
													{#if result.kind === 'run'}
														<a href={withNamespace(`/runs/${result.runId}`, namespace)} class="text-brand-700 hover:underline"
															>{result.runId}</a
														>
													{:else if result.kind === 'refused'}
														<span class="text-danger-700">refused</span>
														<span class="block font-sans text-fg-secondary">{result.refusal}</span>
													{:else}
														<span class="italic">{result.kind}</span>
													{/if}
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							{/if}
						</div>
					</div>
				{/if}
			</li>
		{/each}
	</ul>
{/if}

{#if pausing}
	<Dialog testid="schedule-pause-dialog" title="Pause schedule {pausing}" onclose={() => (pausing = '')}>
		{#snippet body()}
			<form
				class="space-y-3"
				onsubmit={(event) => {
					event.preventDefault();
					void pause();
				}}
			>
				<p class="text-sm text-fg-secondary">
					No fire happens while it is paused; nothing is deleted. The reason is kept as the schedule's note and shown
					on every read.
				</p>
				<label for="schedule-pause-reason" class="text-sm font-medium text-fg">Reason (optional)</label>
				<input
					id="schedule-pause-reason"
					data-testid="schedule-pause-reason"
					bind:value={pauseReason}
					placeholder="Q3 close"
					class={inputClass}
				/>
				{#if actionError}
					<ErrorPanel testid="schedule-pause-error" message={actionError} />
				{/if}
				<div class="flex items-center justify-between gap-2">
					<CliEquivalent command={schedulePauseCli(pausing, namespace, pauseReason)} testid="schedule-pause-dialog-cli" />
					<button
						type="submit"
						data-testid="schedule-pause-accept"
						disabled={actionBusy}
						class="inline-flex h-9 items-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs hover:bg-brand-700 disabled:opacity-50"
						>{actionBusy ? 'Pausing…' : 'Pause'}</button
					>
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}

{#if firing}
	<ConfirmDialog
		testid="schedule-fire-confirm"
		title="Fire {firing} now?"
		acceptLabel="Fire now"
		acceptTestid="schedule-fire-accept"
		cancelTestid="schedule-fire-cancel"
		busy={actionBusy}
		error={actionError}
		onaccept={fire}
		oncancel={() => {
			firing = '';
			actionError = '';
		}}
	>
		{#snippet body()}
			<p>
				This starts a run of {name} now, as if its time had come, under the schedule's own overlap policy — even while
				the schedule is paused.
			</p>
			<p class="mt-2 text-fg-muted">
				Anything the run does outside OrkStack happens again: activities are at-least-once, and a second run repeats
				its external side effects.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}

{#if deleting}
	<ConfirmDialog
		testid="schedule-delete-confirm"
		title="Delete schedule {deleting}?"
		acceptLabel="Delete"
		acceptTestid="schedule-delete-accept"
		cancelTestid="schedule-delete-cancel"
		busy={actionBusy}
		error={actionError}
		destructive
		onaccept={remove}
		oncancel={() => {
			deleting = '';
			actionError = '';
		}}
	>
		{#snippet body()}
			<p>No fire happens after this. Runs it already started continue and keep their origin.</p>
			<p class="mt-2 font-mono text-xs text-fg-muted">{scheduleDeleteCli(deleting, namespace)}</p>
		{/snippet}
	</ConfirmDialog>
{/if}
