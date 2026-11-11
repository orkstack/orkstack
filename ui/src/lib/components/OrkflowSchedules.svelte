<script lang="ts">
	// The Schedules tab: when THIS orkflow starts on its own.
	//
	// A sibling of OrkflowTriggers and working the same way: the loader reads
	// the list, this component writes and then asks the loader to read it
	// again. Nothing here is optimistic — a row appears because the cluster
	// answered with it, and every field of a row (paused, note, next fire
	// times, recent fires) is the cluster's own answer for this read. OrkStack
	// stores nothing else about a schedule.
	import { shortVersion } from '#lib/version.ts';
	import { goto, invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import { untrack } from 'svelte';
	import { EDIT_PARAM, closedHref, editHref } from '#lib/editParam.ts';
	import CliEquivalent from '#lib/components/CliEquivalent.svelte';
	import CliPopover from '#lib/components/CliPopover.svelte';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import Dialog from '#lib/components/Dialog.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import ScheduleFields from '#lib/components/ScheduleFields.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { scheduleClient } from '#lib/clients/schedules.ts';
	import type { OrkflowVersion } from '#lib/gen/orkstack/v1/orkflow_service_pb.ts';
	import type { Schedule } from '#lib/gen/orkstack/v1/schedule_pb.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import {
		CURRENT_VERSION,
		EMPTY_SCHEDULE_FORM,
		fireOutcome,
		firesLabel,
		isoUTC,
		lastFire,
		overlapLabel,
		scheduleCreateCli,
		scheduleDeleteCli,
		scheduleFireCli,
		scheduleFormOf,
		schedulePauseCli,
		scheduleResumeCli,
		scheduleState,
		scheduleUpdateCli,
		scheduleWrite,
		whenLabel,
		type ScheduleForm
	} from '#lib/schedules.ts';

	interface Props {
		name: string;
		namespace: string;
		versions: OrkflowVersion[];
		currentVersion: string;
		schedules: Schedule[];
		error: string | null;
	}

	let { name, namespace, versions, currentVersion, schedules, error }: Props = $props();

	let scheduleName = $state('');
	let createForm = $state<ScheduleForm>({ ...EMPTY_SCHEDULE_FORM });
	let createPaused = $state(false);
	let creating = $state(false);
	let createError = $state('');

	let expanded = $state<Record<string, boolean>>({});

	// The edit dialog. An update replaces every mutable field, so the form opens
	// with all of them as stored; the row's name links to it, and the address
	// opens it, as a trigger's and a webhook's do.
	let editing = $state<Schedule | null>(null);
	let editForm = $state<ScheduleForm>({ ...EMPTY_SCHEDULE_FORM });
	let editAdvanced = $state(false);
	let editError = $state('');
	let saving = $state(false);

	function startEdit(schedule: Schedule) {
		editing = schedule;
		editForm = scheduleFormOf(schedule);
		// What is set is on show: a value out of sight is still saved.
		const { jitter, catchupWindow, startAt, endAt, description, input, pauseOnFailure } = editForm;
		editAdvanced = pauseOnFailure || [jitter, catchupWindow, startAt, endAt, description, input].some((text) => text !== '');
		editError = '';
	}
	function closeEdit() {
		void goto(closedHref(page.url), { reset: false });
	}
	$effect(() => {
		const editName = page.url.searchParams.get(EDIT_PARAM);
		const open = untrack(() => editing);
		if (!editName) {
			if (open) editing = null;
			return;
		}
		if (open?.name === editName) return;
		const listed = schedules.find((schedule) => schedule.name === editName);
		if (listed && scheduleState(listed) !== 'unavailable') untrack(() => startEdit(listed));
	});
	const editMissing = $derived.by(() => {
		const editName = page.url.searchParams.get(EDIT_PARAM);
		if (!editName || error) return '';
		const listed = schedules.find((schedule) => schedule.name === editName);
		if (!listed) return `Schedule ${editName} is not one of ${name}'s.`;
		return listed.unavailableReason ? `Schedule ${editName} cannot be read back to edit: ${listed.unavailableReason}` : '';
	});
	async function saveEdit() {
		if (!editing) return;
		editError = '';
		const written = scheduleWrite(editForm, editing);
		if ('error' in written) {
			editError = written.error;
			return;
		}
		saving = true;
		try {
			await scheduleClient(fetch).updateSchedule({ namespace, name: editing.name, ...written.value });
			await goto(closedHref(page.url), { invalidateAll: true, reset: false });
		} catch (err) {
			// The draft stays, and the server's refusal is shown verbatim.
			editError = errorMessage(err);
		} finally {
			saving = false;
		}
	}

	let pausing = $state('');
	let pauseReason = $state('');
	let firing = $state('');
	let deleting = $state('');
	let actionBusy = $state(false);
	let actionError = $state('');
	let reloading = $state(false);

	const currentLabel = $derived(
		shortVersion(currentVersion || versions.find((entry) => entry.isCurrent)?.version || '')
	);
	const versionOptions = $derived([
		{
			value: CURRENT_VERSION,
			label: currentLabel ? `Current (${currentLabel}) — resolved at each fire` : 'Current — resolved at each fire'
		},
		...versions.map((entry) => ({
			value: entry.version,
			label: `${shortVersion(entry.version)}${entry.isCurrent ? ' — current' : ''}`
		}))
	]);
	// A pin the versions listed here do not name stays a choice, so opening an
	// edit never reads as moving it.
	const editVersionOptions = $derived(
		editing?.orkflowVersion && !versions.some((entry) => entry.version === editing?.orkflowVersion)
			? [...versionOptions, { value: editing.orkflowVersion, label: shortVersion(editing.orkflowVersion) }]
			: versionOptions
	);

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

	async function createSchedule() {
		createError = '';
		const written = scheduleWrite(createForm);
		if ('error' in written) {
			createError = written.error;
			return;
		}
		const created = scheduleName.trim();
		creating = true;
		try {
			await scheduleClient(fetch).createSchedule({
				namespace,
				name: created,
				orkflowName: name,
				paused: createPaused,
				...written.value
			});
			scheduleName = '';
			createForm = { ...createForm, cron: '', interval: '', description: '', input: '' };
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
		active: 'border-rule-strong bg-paper-sunken text-ink',
		paused: 'border-rule-strong bg-paper-sunken text-ink-secondary',
		unavailable: 'border-red bg-red-tint text-red-strong'
	};

	const inputClass =
		'field h-9 w-full px-3 text-sm';
	const smallButton =
		'btn btn-sm';
</script>

<p class="mb-5 max-w-prose text-sm text-ink-secondary" data-testid="schedules-scope">
	Each schedule starts <span class="font-medium text-ink">{name}</span> at the times it names. Temporal keeps
	the schedule; OrkStack stores nothing else about it.
</p>

{#if editMissing}<div class="mb-4"><ErrorPanel testid="schedule-edit-missing" message={editMissing} /></div>{/if}

{#if actionError && !pausing && !firing && !deleting}
	<div class="mb-4"><ErrorPanel testid="schedule-action-error" message={actionError} /></div>
{/if}

{#if error}
	<ErrorPanel
		testid="schedules-error"
		retryTestid="schedules-retry"
		subject="the schedules of {name}"
		message={error}
		busy={reloading}
		onretry={reload}
	/>
{:else if schedules.length === 0}
	<p data-testid="schedules-empty" class="text-sm text-ink-muted">
		No schedules exist for {name}. Create one below to start it at the times it names.
	</p>
{:else}
	<ul class="border-t-2 border-ink" data-testid="schedule-list">
		{#each schedules as schedule (schedule.name)}
			{@const state = scheduleState(schedule)}
			{@const newest = lastFire(schedule)}
			{@const outcome = newest ? fireOutcome(newest) : undefined}
			<li data-testid="schedule-row-{schedule.name}" class="border-b border-rule py-3">
				<div class="flex flex-wrap items-start justify-between gap-4">
					<div class="min-w-0 space-y-1">
						<p class="flex items-center gap-2">
							{#if state === 'unavailable'}
								<span class="font-mono text-sm font-semibold">{schedule.name}</span>
							{:else}
								<a class="link font-mono text-sm font-semibold" data-testid="edit-schedule-{schedule.name}" data-sveltekit-reset="false" href={editHref(page.url, schedule.name)}>{schedule.name}</a>
							{/if}
							<span
								data-testid="schedule-row-state-{schedule.name}"
								data-tip={state === 'paused' ? schedule.note : state === 'unavailable' ? schedule.unavailableReason : ''}
								class="stamp {stateClasses[
									state
								]}">{state}</span
							>
							<span class="font-mono text-xs text-ink-muted">{schedule.id}</span>
						</p>
						{#if state === 'paused' && schedule.note}
							<p class="text-xs text-ink-secondary" data-testid="schedule-row-note-{schedule.name}">{schedule.note}</p>
						{/if}
						{#if state === 'unavailable'}
							<p class="severity-error text-xs">{schedule.unavailableReason}</p>
						{:else}
							<p class="text-sm text-ink-secondary">
								Runs {schedule.orkflowName}@{shortVersion(schedule.orkflowVersion) || 'current'} · {whenLabel(schedule.spec)} ·
								overlap {overlapLabel(schedule.spec?.overlap ?? 0)}
							</p>
							{#if schedule.description}
								<p class="text-sm text-ink-secondary">{schedule.description}</p>
							{/if}
							<p class="text-xs text-ink-muted">
								Next fire <span data-testid="schedule-row-next-{schedule.name}"
									>{schedule.nextFireTimes.length > 0 ? isoUTC(schedule.nextFireTimes[0]) : '—'}</span
								>
								· Last run
								<span data-testid="schedule-row-last-run-{schedule.name}">
									{#if outcome?.kind === 'run'}
										<a
											href={withNamespace(`/runs/${outcome.runId}`, namespace)}
											class="font-mono link">{outcome.runId}</a
										>
									{:else if outcome?.kind === 'refused'}
										<span class="text-red-strong" data-tip={outcome.refusal}>refused</span>
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
							<!-- The row's one terminal icon: every command of this schedule (ui/DESIGN.md, the Shell Icon Rule). -->
							<CliPopover
								testid="schedule-cli-{schedule.name}"
								title="Command line: schedule {schedule.name}"
								commands={[
									schedule.paused
										? { label: 'Resume it', command: scheduleResumeCli(schedule.name, namespace) }
										: { label: 'Pause it', command: schedulePauseCli(schedule.name, namespace, '') },
									{ label: 'Fire it now', command: scheduleFireCli(schedule.name, namespace) },
									{ label: 'Recreate it', command: scheduleCreateCli(schedule) }
								]}
							/>
						{/if}
						<button
							type="button"
							data-testid="schedule-delete-{schedule.name}"
							onclick={() => {
								deleting = schedule.name;
								actionError = '';
							}}
							class="btn btn-sm"
						>
							<Icon name="trash" />Delete
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
					<div class="mt-3 grid gap-4 border-t border-rule pt-3 md:grid-cols-2">
						<div>
							<h3 class="caps mb-2 text-ink-muted">Next fires</h3>
							{#if schedule.nextFireTimes.length === 0}
								<p class="text-xs text-ink-muted" data-testid="schedule-next-fires-{schedule.name}">None.</p>
							{:else}
								<ol class="space-y-0.5 font-mono text-xs text-ink-secondary" data-testid="schedule-next-fires-{schedule.name}">
									{#each schedule.nextFireTimes as at, index (index)}
										<li>{isoUTC(at)}</li>
									{/each}
								</ol>
							{/if}
						</div>
						<div>
							<h3 class="caps mb-2 text-ink-muted">Recent fires</h3>
							<p class="mb-2 text-xs text-ink-muted" data-testid="schedule-fires-{schedule.name}">
								Fires: {firesLabel(schedule)}
							</p>
							{#if schedule.recentFires.length === 0}
								<p class="text-xs text-ink-muted" data-testid="schedule-recent-{schedule.name}">No fires yet.</p>
							{:else}
								<table class="register text-xs" data-testid="schedule-recent-{schedule.name}">
									<thead class="text-ink-muted">
										<tr><th class="pr-3">Scheduled</th><th class="pr-3">Run</th></tr>
									</thead>
									<tbody class="font-mono text-ink-secondary">
										{#each schedule.recentFires as recent, index (index)}
											{@const result = fireOutcome(recent)}
											<tr>
												<td class="pr-3 align-top">{isoUTC(recent.scheduledAt)}</td>
												<td class="align-top">
													{#if result.kind === 'run'}
														<a href={withNamespace(`/runs/${result.runId}`, namespace)} class="link"
															>{result.runId}</a
														>
													{:else if result.kind === 'refused'}
														<span class="text-red-strong">refused</span>
														<span class="block font-sans text-ink-secondary">{result.refusal}</span>
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

<section class="region mt-8 pt-3">
	<h2 class="text-md font-semibold text-ink">Create schedule</h2>
	<form
		class="mt-3 max-w-3xl space-y-4"
		data-testid="schedule-create-form"
		onsubmit={(event) => {
			event.preventDefault();
			void createSchedule();
		}}
	>
		<ScheduleFields bind:form={createForm} prefix="schedule" {versionOptions} disabled={creating}>
			{#snippet leading()}
				<label for="schedule-name" class="text-sm font-medium text-ink">Name</label>
				<input
					id="schedule-name"
					data-testid="schedule-name"
					bind:value={scheduleName}
					placeholder="nightly-reconcile"
					class={inputClass}
				/>
			{/snippet}
		</ScheduleFields>

		<label class="flex items-center gap-2 text-sm text-ink">
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
			class="btn border-ink"
		>
			{creating ? 'Creating…' : 'Create schedule'}
		</button>
	</form>
</section>

{#if editing}
	<Dialog title="Edit schedule {editing.name}" testid="schedule-edit-dialog" onclose={closeEdit} widthClass="max-w-2xl">
		{#snippet body()}
			{@const command = editing ? scheduleUpdateCli(editing, editForm) : ''}
			<p class="mb-3 text-sm text-ink-secondary">
				Editing keeps this schedule's id, its fires and whether it is paused. Its name and Orkflow cannot change. If
				someone else saves while this is open, the last save wins.
			</p>
			<form
				class="space-y-4"
				onsubmit={(event) => {
					event.preventDefault();
					void saveEdit();
				}}
			>
				<ScheduleFields
					bind:form={editForm}
					prefix="schedule-edit"
					versionOptions={editVersionOptions}
					disabled={saving}
					advancedOpen={editAdvanced}
				>
					{#snippet leading()}
						<span class="text-sm font-medium text-ink">Orkflow</span>
						<span class="flex h-9 items-center font-mono text-sm text-ink-2" data-testid="schedule-edit-orkflow">{name}</span>
					{/snippet}
				</ScheduleFields>
				{#if editError}
					<ErrorPanel testid="schedule-edit-error" message={editError} />
				{/if}
				<div class="flex items-center justify-between gap-3">
					<button type="submit" data-testid="schedule-edit-submit" disabled={saving} class="btn btn-primary">
						{saving ? 'Saving…' : 'Save'}
					</button>
					{#if command}<CliEquivalent {command} testid="schedule-edit-cli" />{/if}
				</div>
			</form>
		{/snippet}
	</Dialog>
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
				<p class="text-sm text-ink-secondary">
					No fire happens while it is paused; nothing is deleted. The reason is kept as the schedule's note and shown
					on every read.
				</p>
				<label for="schedule-pause-reason" class="text-sm font-medium text-ink">Reason (optional)</label>
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
						class="btn btn-primary"
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
			<p class="mt-2 text-ink-muted">
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
		glyph="trash"
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
			<p class="mt-2 font-mono text-xs text-ink-muted">{scheduleDeleteCli(deleting, namespace)}</p>
		{/snippet}
	</ConfirmDialog>
{/if}
