<script lang="ts">
	// A COMPOSED TASK'S CARD BODY. It starts a child run of another Orkflow and
	// reaches no Orker, so there is no log pane and no image: what a reader
	// needs is WHICH Orkflow it runs (pinned, with a link), WHICH run that is
	// (once its start is confirmed), and how far that run has got — the
	// child's own tasks, rolled up and moved live from the child's own event
	// stream. A child task that itself composes shows its own child link and
	// is not expanded here: one level per card.
	//
	// The panel reads the child ONCE when its run id is known, then follows
	// the child's socket while it is not terminal, applying each TASK_* event
	// to its row; the child's terminal event triggers one re-read and the
	// socket closes itself. Unmounting closes it. At most one socket per card.
	import { onDestroy } from 'svelte';
	import CliEquivalent from './CliEquivalent.svelte';
	import StatusPill from './StatusPill.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import {
		applyChildEvent,
		childLabel,
		childOrkflowHref,
		childRollup,
		childRunHref,
		rollupSummary,
		type ChildRollup
	} from '$lib/compose';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import type { TaskChild } from '$lib/gen/orkstack/v1/run_pb';
	import { runCommand } from '$lib/runDetail';
	import { watchRunEvents } from '$lib/runEvents';
	import { isTerminal, isTerminalEventType } from '$lib/status';

	interface Props {
		/** The parent run. */
		runId: string;
		namespace: string;
		taskId: string;
		/** The composed task's TaskChild: from the definition, then the snapshot. */
		child?: TaskChild;
		status: Status;
	}

	let { runId, namespace, taskId, child, status }: Props = $props();

	const label = $derived(childLabel(child));
	const childRunId = $derived(child?.runId ?? '');

	let childStatus = $state<Status>(Status.UNSPECIFIED);
	let rollup = $state.raw<ChildRollup>({ tasks: [] });
	let readError = $state('');
	let read = $state(false);

	let stopSocket: (() => void) | undefined;
	let generation = 0;
	onDestroy(() => stopSocket?.());

	async function readChild(id: string, mine: number): Promise<boolean> {
		try {
			const res = await orkstack(fetch).runs.getRun({ runId: id, namespace });
			if (mine !== generation) return false;
			childStatus = res.run?.status ?? Status.UNSPECIFIED;
			rollup = childRollup(res.run);
			readError = '';
			read = true;
			return true;
		} catch (err) {
			if (mine !== generation) return false;
			readError = errorMessage(err);
			read = true;
			return false;
		}
	}

	// One read, then the child's own stream while it runs. Keyed on the child
	// run id alone: a re-render that hands the same id back changes nothing.
	$effect(() => {
		const id = childRunId;
		if (!id) return;
		const mine = ++generation;
		void (async () => {
			const ok = await readChild(id, mine);
			if (!ok || mine !== generation || isTerminal(childStatus)) return;
			stopSocket?.();
			stopSocket = watchRunEvents(id, namespace, (event) => {
				if (mine !== generation) return;
				rollup = applyChildEvent(rollup, event);
				if (isTerminalEventType(event.type)) {
					// The child's terminal facts arrive with one re-read.
					void readChild(id, mine);
				}
			});
		})();
		return () => {
			generation++;
			stopSocket?.();
			stopSocket = undefined;
		};
	});
</script>

<div
	class="mt-3 rounded-md border border-border-subtle bg-surface-sunken px-3 py-2 text-sm"
	data-testid="task-{taskId}-child"
>
	<p class="flex flex-wrap items-center gap-x-2 gap-y-1 text-fg-secondary">
		{#if childRunId}
			<span>Runs orkflow</span>
			<a
				href={childOrkflowHref(namespace, child?.orkflowName ?? '', child?.orkflowVersion ?? '')}
				data-testid="task-{taskId}-child-orkflow"
				class="font-mono text-xs text-brand-700 hover:underline">{label}</a
			>
			<span>as child run</span>
			<a
				href={childRunHref(namespace, childRunId)}
				data-testid="task-{taskId}-child-link"
				class="font-mono text-xs break-all text-brand-700 hover:underline">{childRunId}</a
			>
			{#if read && !readError}
				<StatusPill status={childStatus} testid="task-{taskId}-child-status" />
			{/if}
		{:else if status === Status.SKIPPED || isTerminal(status)}
			<span>Runs orkflow</span>
			<a
				href={childOrkflowHref(namespace, child?.orkflowName ?? '', child?.orkflowVersion ?? '')}
				data-testid="task-{taskId}-child-orkflow"
				class="font-mono text-xs text-brand-700 hover:underline">{label}</a
			>
			<span data-testid="task-{taskId}-child-none">— no child run was started.</span>
		{:else}
			<span>Will run orkflow</span>
			<a
				href={childOrkflowHref(namespace, child?.orkflowName ?? '', child?.orkflowVersion ?? '')}
				data-testid="task-{taskId}-child-orkflow"
				class="font-mono text-xs text-brand-700 hover:underline">{label}</a
			>
			<span>as a child run.</span>
		{/if}
	</p>

	{#if childRunId}
		{#if readError}
			<p class="mt-2 text-xs text-warning-700" data-testid="task-{taskId}-child-unavailable">
				Child run {childRunId} is no longer readable ({readError}).
			</p>
		{:else if read}
			<p class="mt-2 text-xs text-fg-muted" data-testid="task-{taskId}-child-rollup">{rollupSummary(rollup)}</p>
			<ul class="mt-1 space-y-1">
				{#each rollup.tasks as row (row.id)}
					<li
						class="flex flex-wrap items-center gap-2 text-xs"
						data-testid="task-{taskId}-child-task-{row.id}"
					>
						<span class="font-mono text-fg-secondary">{row.id}</span>
						<StatusPill status={row.status} testid="task-{taskId}-child-task-{row.id}-status" />
						{#if row.child?.runId}
							<a
								href={childRunHref(namespace, row.child.runId)}
								data-testid="task-{taskId}-child-task-{row.id}-child-link"
								class="font-mono text-brand-700 hover:underline"
								>{childLabel(row.child)} → {row.child.runId}</a
							>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
		<div class="mt-2 flex flex-wrap items-center gap-2 text-xs text-fg-muted">
			<CliEquivalent
				command={runCommand(namespace, ['run', 'get', childRunId])}
				testid="task-{taskId}-child-cli"
				label="Copy the command that reads the child run"
			/>
			<CliEquivalent
				command={runCommand(namespace, ['run', 'get', runId, '--tree'])}
				testid="task-{taskId}-child-tree-cli"
				label="Copy the command that reads this run with its children"
			/>
		</div>
	{/if}
</div>
