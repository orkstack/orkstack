<script lang="ts">
	// The selected task or fan-out item, in the panel under its row: what it asked
	// for, where it ran and what it is doing. It follows the selection.
	//
	//   running     InspectRun and TaskRun data only: attempts, Orker, pool, the
	//               request, the wait to start, and Now from the heartbeat.
	//   ended       the attempt record adds what was reserved, the phases bar and
	//               the image digest.
	//   not started the request only.
	import type { Snippet } from 'svelte';
	import ResolvedValues from './ResolvedValues.svelte';
	import type { ArtifactRef, Payload } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { timestampDate, type Timestamp } from '@bufbuild/protobuf/wkt';
	import { finishedWords } from '#lib/when.ts';
	import type { ImageRef, ResourceRequest } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { TaskAttempt } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { LiveAttemptState, type LiveAttempt } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { isTerminal } from '#lib/status.ts';
	import { formatDuration, shortDigestWords } from '#lib/runDetail.ts';
	import { formatBytes, heartbeatAge, liveLine, unavailableReason } from '#lib/live.ts';
	import { attemptPhases, phaseGroups, formatQuantity, type TaskTiming } from '#lib/timing.ts';
	import { fmtQuantity, fmtRequest } from '#lib/resources.ts';
	import Plate from './Plate.svelte';
	import RetryMark from './RetryMark.svelte';
	import StatusPill from './StatusPill.svelte';

	interface Props {
		/** "Task charged", "Item render[10]" */
		title: string;
		/** testid prefix of the facts that specs read: task-{id} or task-{id}-item-panel */
		prefix: string;
		status: Status;
		/** A running attempt no Orker has started, for want of room. */
		waiting?: boolean;
		/** What it waits for, when that is object storage rather than capacity. */
		waitingFor?: string;
		attempts: TaskAttempt[];
		attempt: number;
		orkerId: string;
		pool: string;
		requested?: ResourceRequest;
		scheduledAt?: Timestamp;
		startedAt?: Timestamp;
		finishedAt?: Timestamp;
		live?: LiveAttempt;
		liveError?: string | null;
		liveNowMs: number;
		now: { seconds: bigint; nanos: number };
		timing?: TaskTiming;
		image?: ImageRef;
		/** What the task's image is called: its catalog reference, else what it names. */
		imageReference?: string;
		/** The engine's whole sentence for how it ended, which its row only names. */
		reason?: string;
		/** What an awaited task was resolved with, shown where a reason would be. */
		resolved?: { runId: string; namespace: string; taskId: string; output?: Payload; valuesRef?: ArtifactRef };
		/** What the card says beyond the facts: its failure, cache, Inspect. */
		children?: Snippet;
		/** The head's words when nothing can be said yet (a fan-out with no item). */
		note?: string;
		panelTestid?: string;
	}

	let {
		title,
		prefix,
		status,
		waiting = false,
		waitingFor = 'Waiting for capacity',
		attempts,
		attempt,
		orkerId,
		pool,
		requested,
		scheduledAt,
		startedAt,
		finishedAt,
		live,
		liveError = null,
		liveNowMs,
		now,
		timing,
		image,
		imageReference = '',
		reason = '',
		resolved,
		children,
		note = '',
		panelTestid = 'item-card'
	}: Props = $props();

	const terminal = $derived(isTerminal(status));
	const last = $derived(attempts[attempts.length - 1]);
	const started = $derived(live?.attemptStartedAt ?? startedAt);
	const hasStarted = $derived(!!started || attempts.length > 0);
	const orker = $derived(orkerId || (live?.heartbeatAvailable ? live.orkerId : '') || last?.orkerId || '');
	const heard = $derived(live && status === Status.RUNNING);
	const line = $derived(live ? liveLine(live, liveNowMs) : undefined);
	const beat = $derived(live ? heartbeatAge(live, liveNowMs) : undefined);
	const timeoutAt = $derived.by(() => {
		if (!live?.attemptStartedAt || live.timeoutSeconds <= 0) return null;
		return new Date((Number(live.attemptStartedAt.seconds) + live.timeoutSeconds) * 1000).toLocaleTimeString('en-GB', { hour12: false });
	});
	const reserved = $derived(last?.resources ? fmtQuantity(last.resources) : '');
	const phases = $derived.by(() => {
		if (!last || !terminal) return null;
		const read = attemptPhases(last);
		if (read.kind !== 'phases') return null;
		const groups = phaseGroups(read.phases);
		const total = groups.reduce((sum, g) => sum + g.ms, 0);
		return total > 0 ? { groups, total } : null;
	});
	const digest = $derived(last?.image?.indexDigest || image?.indexDigest || '');
	// Clock times are on the 24-hour clock everywhere on this card.
	const clock = (ts: Timestamp) => new Date(Number(ts.seconds) * 1000).toLocaleTimeString('en-GB', { hour12: false });
	const secs = (ms: number) => (ms >= 60_000 ? `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s` : `${(ms / 1000).toFixed(1)}s`);
</script>

<section class="insp" id="item-card" aria-labelledby="item-card-heading" data-testid={panelTestid}>
	<div class="insp-head">
		<h3 id="item-card-heading" data-testid="{prefix}-label">{title}</h3>
		{#if waiting}
			<span class="stamp pill-wait" data-testid="{prefix}-card-status"><span class="mark-wait" aria-hidden="true"></span>{waitingFor}</span>
		{:else}
			<StatusPill {status} testid="{prefix}-card-status" />
		{/if}
	</div>
	{#if note}<p class="insp-note">{note}</p>{/if}
	<dl class="fstrip insp-strip">
		<div>
			<dt>Attempts</dt>
			<dd data-testid="{prefix}-attempt">
				{#if hasStarted || attempt > 0}<RetryMark attempts={Math.max(attempts.length, attempt, 1)} />{:else}not started{/if}
			</dd>
		</div>
		{#if orker}
			<div>
				<dt>Orker</dt>
				<dd class="font-mono text-xs" data-testid={live && !orkerId ? `${prefix}-live-orker` : `${prefix}-card-orker`}>{orker}</dd>
			</div>
		{/if}
		{#if pool}
			<div>
				<dt>Pool</dt>
				<dd>{pool}</dd>
			</div>
		{/if}
		{#if reserved}
			<div>
				<dt>Reserved</dt>
				<dd data-testid="{prefix}-reserved">{reserved}</dd>
			</div>
		{:else if requested && fmtRequest(requested)}
			<div>
				<dt>Requested</dt>
				<dd data-testid="{prefix}-requested">{fmtRequest(requested)}</dd>
			</div>
		{/if}
		{#if timing && timing.wait.kind !== 'na'}
			<div>
				<dt>Waited to start</dt>
				<dd data-testid="{prefix}-wait" data-ms={timing.wait.kind === 'value' ? Math.round(timing.wait.ms) : undefined} data-reason={timing.wait.kind === 'unavailable' ? timing.wait.reason : undefined}>
					{formatQuantity(timing.wait)}{#if timing.wait.kind === 'unavailable'} ({timing.wait.reason}){/if}
				</dd>
			</div>
		{:else if scheduledAt && started}
			<div>
				<dt>Waited to start</dt>
				<dd>{formatDuration(scheduledAt, started)}</dd>
			</div>
		{:else if !terminal && scheduledAt}
			<div>
				<dt>Waited to start</dt>
				<dd>{formatDuration(scheduledAt, now)} so far</dd>
			</div>
		{/if}
		{#if started}
			<div>
				<dt>Started</dt>
				<dd>{clock(started)}</dd>
			</div>
		{/if}
		{#if terminal && finishedAt}
			<div>
				<dt>Finished</dt>
				<dd data-testid="{prefix}-finished">{finishedWords(timestampDate(finishedAt), started ? timestampDate(started) : undefined)}</dd>
			</div>
		{/if}
		{#if terminal && timing && timing.run.kind !== 'na'}
			<div>
				<dt>Took</dt>
				<dd data-testid="{prefix}-run" data-ms={timing.run.kind === 'value' ? Math.round(timing.run.ms) : undefined} data-reason={timing.run.kind === 'unavailable' ? timing.run.reason : undefined}>
					{formatQuantity(timing.run)}{#if timing.run.kind === 'unavailable'} ({timing.run.reason}){/if}
				</dd>
			</div>
		{:else if terminal && started && finishedAt}
			<div>
				<dt>Took</dt>
				<dd>{formatDuration(started, finishedAt)}</dd>
			</div>
		{:else if !terminal && live?.attemptStartedAt}
			<div>
				<dt>Running for</dt>
				<dd data-testid="{prefix}-running-for"><span class="sr">{'Running for '}</span>{formatDuration(live.attemptStartedAt, { seconds: BigInt(Math.floor(liveNowMs / 1000)), nanos: (liveNowMs % 1000) * 1e6 })}</dd>
			</div>
		{/if}
		{#if timing?.preContract && timing.total.kind === 'value'}
			<div>
				<dt>Total</dt>
				<dd data-testid="{prefix}-total" data-tip="this run was recorded before scheduled time existed; total is dispatch to finish">{formatQuantity(timing.total)} (dispatch to finish)</dd>
			</div>
		{/if}
	</dl>
	{#if reason}
		<section class="insp-sec" aria-labelledby="item-reason-heading">
			<h4 id="item-reason-heading">Reason</h4>
			<p class="insp-line insp-reason" data-testid="{prefix}-reason">{reason}</p>
		</section>
	{/if}
	{#if resolved}
		<section class="insp-sec" aria-labelledby="item-values-heading" data-testid="{prefix}-values">
			<h4 id="item-values-heading">Values</h4>
			<!-- The panel moves with the selection, so another task's values start closed. -->
			{#key resolved.taskId}<ResolvedValues {prefix} {...resolved} />{/key}
		</section>
	{/if}
	{#if heard}
		<section class="insp-sec" aria-labelledby="item-now-heading">
			<h4 id="item-now-heading">Now{#if live?.heartbeatAvailable}<span> from its Orker's heartbeat, {beat?.label}</span>{:else if live}<span> {unavailableReason(live.heartbeatUnavailableReason)}</span>{/if}</h4>
			<!-- The whole sentence, for a task that is not plainly running or whose beat is stale. -->
			<p
				data-testid="{prefix}-live"
				data-live={line?.kind}
				data-stale={line?.stale}
				data-tip={line?.title}
				class={line?.kind === 'started' && !line.stale ? 'sr' : 'insp-line'}
			>{line?.text}</p>
			{#if live?.heartbeatAvailable && live.state === LiveAttemptState.STARTED}
				<dl class="fstrip insp-strip">
					<div>
						<dt>Phase</dt>
						<dd><code>{live.phase || '—'}</code></dd>
					</div>
					{#if live.containerId}
						<div>
							<dt>Container</dt>
							<dd class="font-mono text-xs">{live.containerId.slice(0, 12)}</dd>
						</div>
					{/if}
					<div>
						<dt>Logged</dt>
						<dd>{formatBytes(live.bytesLogged)}, stdout and stderr</dd>
					</div>
					{#if timeoutAt}
						<div>
							<dt>Timeout</dt>
							<dd>not before {timeoutAt}</dd>
						</div>
					{/if}
				</dl>
			{/if}
		</section>
	{:else if status === Status.RUNNING && liveError}
		<section class="insp-sec" aria-label="Now">
			<p data-testid="{prefix}-live" data-live="unavailable" data-stale="false" data-tip={liveError} class="insp-line">Live detail unavailable</p>
		</section>
	{/if}
	{#if phases}
		<section class="insp-sec" aria-labelledby="item-phases-heading" data-testid="{prefix}-phases">
			<h4 id="item-phases-heading">Phases of attempt {last.attempt}</h4>
			<div class="pbar" role="img" aria-label={phases.groups.map((g) => `${g.label} ${secs(g.ms)}`).join(', ')}>
				{#each phases.groups as group (group.key)}<i class="ph-{group.key}" style:width="{(100 * group.ms) / phases.total}%"></i>{/each}
			</div>
			<ul class="pkey">
				{#each phases.groups as group (group.key)}
					<li>
						<span><i class="ph-{group.key}"></i>{group.label}</span><b>{secs(group.ms)}</b>
						{#if group.key !== 'run'}<small>{group.phases.join(', ')}</small>{/if}
					</li>
				{/each}
			</ul>
		</section>
	{/if}
	{#if digest || imageReference}
		<section class="insp-sec" aria-labelledby="item-image-heading" data-testid="run-image">
			<h4 id="item-image-heading">Image</h4>
			{#if imageReference}<span class="insp-ref">{imageReference}</span>{/if}
			{#if digest}
				<div class="insp-plate"><Plate key="sha256" value={shortDigestWords(digest)} tip={digest} copy={digest} copyLabel="Copy digest" /></div>
				<span class="insp-ref">Resolved from its tag at dispatch; every attempt so far ran this digest.</span>
			{/if}
		</section>
	{/if}
	{#if children}{@render children()}{/if}
</section>

<style>
	.insp {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 10px;
		min-width: 0;
	}
	.insp-head {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 4px 12px;
	}
	.insp-head h3 {
		margin: 0;
		font-size: 13.5px;
		font-weight: 650;
	}
	.insp-note,
	.insp-line {
		margin: 0;
		font-size: 13.5px;
		color: var(--ink-2);
	}
	.insp-reason {
		overflow-wrap: anywhere;
	}
	.insp-strip > :global(div) {
		padding: 0 14px;
	}
	.insp-strip > :global(div:first-child) {
		padding-left: 0;
	}
	.insp-strip :global(dd) {
		font-size: var(--text-sm);
	}
	.insp-sec {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 6px;
	}
	.insp-sec > h4 {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 8px;
		margin: 0;
		font-size: 13px;
		font-weight: 650;
		color: var(--ink-2);
	}
	.insp-sec > h4 span {
		font-size: 12.5px;
		font-weight: 400;
		color: var(--ink-3);
	}
	@media (max-width: 640px) {
		.insp-strip > :global(div) {
			padding: 0;
		}
	}
	.insp-ref {
		font-size: 13px;
		color: var(--ink-3);
		overflow-wrap: anywhere;
	}
	.insp-plate {
		display: flex;
		min-width: 0;
	}
	.pkey {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(min(100%, 11rem), 1fr));
		gap: 4px 16px;
		margin: 0;
		padding: 0;
		font-size: 13px;
		list-style: none;
	}
	.pkey li {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		align-content: start;
		gap: 0 12px;
	}
	.pkey li span {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		color: var(--ink-2);
	}
	.pkey li i {
		display: inline-block;
		width: 10px;
		height: 10px;
		border-radius: 2px;
	}
	.pkey li b {
		text-align: right;
		font-weight: 600;
		font-variant-numeric: tabular-nums;
	}
	.pkey li small {
		grid-column: 1 / -1;
		font-size: 12px;
		color: var(--ink-3);
	}
	@media (forced-colors: active) {
		.pbar,
		.pkey li i {
			forced-color-adjust: none;
		}
	}
</style>
