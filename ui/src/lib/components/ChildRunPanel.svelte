<script lang="ts">
	// A COMPOSED TASK'S CARD BODY. It starts a child run of another Orkflow and
	// reaches no Orker, so there is no log pane and no image: what a reader
	// needs is WHICH Orkflow it runs (pinned, with a link), WHICH run that is
	// (once its start is confirmed), and how far that run has got — the
	// child's own tasks, rolled up and moved live from the child's own event
	// stream. A child task that itself composes shows its own child link and
	// is not expanded here: one level per card.
	//
	// The panel reads the child ONCE when its run id is known AND the card is
	// on screen, then follows the child's socket while it is not terminal,
	// applying each TASK_* event to its row; the child's terminal event
	// triggers one re-read and the socket closes itself. Scrolling the card
	// away closes the socket, and unmounting closes it ($lib/childFollower):
	// a run of twenty composed tasks opens sockets only for the cards a reader
	// can see. At most one socket per card.
	import { onDestroy } from 'svelte';
	import { childFollower } from '$lib/childFollower';
	import ErrorPanel from './ErrorPanel.svelte';
	import CliEquivalent from './CliEquivalent.svelte';
	import StatusPill from './StatusPill.svelte';
	import { errorMessage } from '$lib/client';
	import { runClient } from '$lib/clients/runs';
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

	let streamFailure = $state('');
	let panel = $state<HTMLDivElement | undefined>(undefined);

	async function readChild(id: string, current: () => boolean): Promise<boolean> {
		try {
			const res = await runClient(fetch).getRun({ runId: id, namespace });
			if (!current()) return false;
			childStatus = res.run?.status ?? Status.UNSPECIFIED;
			rollup = childRollup(res.run);
			readError = '';
			read = true;
			return !isTerminal(childStatus);
		} catch (err) {
			if (!current()) return false;
			readError = errorMessage(err);
			read = true;
			return false;
		}
	}

	const follower = childFollower({
		read: readChild,
		watch: (id, current, onState) =>
			watchRunEvents(
				id,
				namespace,
				(event) => {
					if (!current()) return;
					rollup = applyChildEvent(rollup, event);
					if (isTerminalEventType(event.type)) {
						// The child's terminal facts arrive with one re-read.
						void readChild(id, current);
					}
				},
				onState
			),
		onFailure: (detail) => (streamFailure = detail)
	});
	onDestroy(() => follower.dispose());

	// Keyed on the child run id alone: a re-render that hands the same id back
	// changes nothing.
	$effect(() => {
		follower.setRunId(childRunId);
	});

	// On screen or not. Without an IntersectionObserver (an old browser) the
	// card counts as visible, which is what every card was before.
	$effect(() => {
		const node = panel;
		if (!node) return;
		if (typeof IntersectionObserver === 'undefined') {
			follower.setVisible(true);
			return;
		}
		const observer = new IntersectionObserver((entries) => {
			for (const entry of entries) follower.setVisible(entry.isIntersecting);
		});
		observer.observe(node);
		return () => observer.disconnect();
	});
</script>

<div
	bind:this={panel}
	class="mt-3 rounded-sm bg-paper-sunken px-3 py-2 text-sm"
	data-testid="task-{taskId}-child"
	data-following={read ? 'read' : 'idle'}
>
	<p class="flex flex-wrap items-center gap-x-2 gap-y-1 text-ink-secondary">
		{#if childRunId}
			<span>Runs orkflow</span>
			<a
				href={childOrkflowHref(namespace, child?.orkflowName ?? '', child?.orkflowVersion ?? '')}
				data-testid="task-{taskId}-child-orkflow"
				class="font-mono text-xs text-blue hover:underline">{label}</a
			>
			<span>as child run</span>
			<a
				href={childRunHref(namespace, childRunId)}
				data-testid="task-{taskId}-child-link"
				class="font-mono text-xs break-all text-blue hover:underline">{childRunId}</a
			>
			{#if read && !readError}
				<StatusPill status={childStatus} testid="task-{taskId}-child-status" />
			{/if}
		{:else if status === Status.SKIPPED || isTerminal(status)}
			<span>Runs orkflow</span>
			<a
				href={childOrkflowHref(namespace, child?.orkflowName ?? '', child?.orkflowVersion ?? '')}
				data-testid="task-{taskId}-child-orkflow"
				class="font-mono text-xs text-blue hover:underline">{label}</a
			>
			<span data-testid="task-{taskId}-child-none">— no child run was started.</span>
		{:else}
			<span>Will run orkflow</span>
			<a
				href={childOrkflowHref(namespace, child?.orkflowName ?? '', child?.orkflowVersion ?? '')}
				data-testid="task-{taskId}-child-orkflow"
				class="font-mono text-xs text-blue hover:underline">{label}</a
			>
			<span>as a child run.</span>
		{/if}
	</p>

	{#if childRunId}
		{#if readError}
			<p class="mt-2 text-xs text-ink-secondary" data-testid="task-{taskId}-child-unavailable">
				Child run {childRunId} is no longer readable ({readError}).
			</p>
		{:else if read}
			{#if streamFailure && !isTerminal(childStatus)}
				<div class="mt-2">
					<ErrorPanel
						testid="task-{taskId}-child-live-stopped"
						retryTestid="task-{taskId}-child-live-retry"
						retryLabel="Retry live updates"
						message="Live updates stopped: {streamFailure}."
						onretry={() => follower.retry()}
					/>
				</div>
			{/if}
			<p class="mt-2 text-xs text-ink-muted" data-testid="task-{taskId}-child-rollup">{rollupSummary(rollup)}</p>
			<ul class="mt-1 space-y-1">
				{#each rollup.tasks as row (row.id)}
					<li
						class="flex flex-wrap items-center gap-2 text-xs"
						data-testid="task-{taskId}-child-task-{row.id}"
					>
						<span class="font-mono text-ink-secondary">{row.id}</span>
						<StatusPill status={row.status} testid="task-{taskId}-child-task-{row.id}-status" />
						{#if row.child?.runId}
							<a
								href={childRunHref(namespace, row.child.runId)}
								data-testid="task-{taskId}-child-task-{row.id}-child-link"
								class="font-mono text-blue hover:underline"
								>{childLabel(row.child)} → {row.child.runId}</a
							>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
		<div class="mt-2 flex flex-wrap items-center gap-2 text-xs text-ink-muted">
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
