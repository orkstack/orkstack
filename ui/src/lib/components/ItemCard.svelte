<script lang="ts">
	// The selected task or fan-out item: what it asked for, where it ran and
	// what it is doing. It follows the selection and announces it.
	//
	//   running     InspectRun and TaskRun data only: attempts, Orker, pool, the
	//               request, the wait to start, and Now from the heartbeat.
	//   ended       the attempt record adds what was reserved, the phases bar and
	//               the image digest.
	//   not started the request only.
	import type { Snippet } from 'svelte';
	import type { Timestamp } from '@bufbuild/protobuf/wkt';
	import type { ImageRef, ResourceRequest } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { TaskAttempt } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { LiveAttemptState, type LiveAttempt } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { isTerminal } from '#lib/status.ts';
	import { formatDuration, shortDigestWords } from '#lib/runDetail.ts';
	import { formatBytes, heartbeatAge, liveLine, unavailableReason } from '#lib/live.ts';
	import { attemptPhases, phaseGroups, formatQuantity, type TaskTiming } from '#lib/timing.ts';
	import { fmtQuantity, fmtRequest } from '#lib/resources.ts';
	import CliPopover from './CliPopover.svelte';
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
		cli: { label: string; command: string }[];
		cliTestid: string;
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
		cli,
		cliTestid,
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

<section class="card insp" id="item-card" aria-labelledby="item-card-heading" data-testid={panelTestid}>
	<div class="card-head">
		<h2 id="item-card-heading" data-testid="{prefix}-label">{title}</h2>
		<div class="card-end">
			{#if waiting}
				<span class="stamp pill-wait" data-testid="{prefix}-card-status"><span class="mark-wait" aria-hidden="true"></span>Waiting for capacity</span>
			{:else}
				<StatusPill {status} testid="{prefix}-card-status" />
			{/if}
			{#if cli.length}<CliPopover testid={cliTestid} title="Command line: {title}" commands={cli} />{/if}
		</div>
	</div>
	{#if note}<p class="insp-note">{note}</p>{/if}
	<dl class="kv">
		<dt>Attempts</dt>
		<dd data-testid="{prefix}-attempt">
			{#if hasStarted || attempt > 0}<RetryMark attempts={Math.max(attempts.length, attempt, 1)} />{:else}not started{/if}
		</dd>
		{#if orker}
			<dt>Orker</dt>
			<dd class="font-mono text-xs" data-testid={live && !orkerId ? `${prefix}-live-orker` : `${prefix}-card-orker`}>{orker}</dd>
		{/if}
		{#if pool}
			<dt>Pool</dt>
			<dd>{pool}</dd>
		{/if}
		{#if reserved}
			<dt>Reserved</dt>
			<dd data-testid="{prefix}-reserved">{reserved}</dd>
		{:else if requested && fmtRequest(requested)}
			<dt>Requested</dt>
			<dd data-testid="{prefix}-requested">{fmtRequest(requested)}</dd>
		{/if}
		{#if timing && timing.wait.kind !== 'na'}
			<dt>Waited to start</dt>
			<dd data-testid="{prefix}-wait" data-ms={timing.wait.kind === 'value' ? Math.round(timing.wait.ms) : undefined} data-reason={timing.wait.kind === 'unavailable' ? timing.wait.reason : undefined}>
				{formatQuantity(timing.wait)}{#if timing.wait.kind === 'unavailable'} ({timing.wait.reason}){/if}
			</dd>
		{:else if !terminal && scheduledAt && started}
			<dt>Waited to start</dt>
			<dd>{formatDuration(scheduledAt, started)}</dd>
		{:else if !terminal && scheduledAt}
			<dt>Waited to start</dt>
			<dd>{formatDuration(scheduledAt, now)} so far</dd>
		{/if}
		{#if started}
			<dt>Started</dt>
			<dd>{clock(started)}</dd>
		{/if}
		{#if terminal && timing && timing.run.kind !== 'na'}
			<dt>Took</dt>
			<dd data-testid="{prefix}-run" data-ms={timing.run.kind === 'value' ? Math.round(timing.run.ms) : undefined} data-reason={timing.run.kind === 'unavailable' ? timing.run.reason : undefined}>
				{formatQuantity(timing.run)}{#if timing.run.kind === 'unavailable'} ({timing.run.reason}){/if}
			</dd>
		{:else if terminal && started && finishedAt}
			<dt>Took</dt>
			<dd>{formatDuration(started, finishedAt)}</dd>
		{:else if !terminal && live?.attemptStartedAt}
			<dt>Running for</dt>
			<dd data-testid="{prefix}-running-for"><span class="sr">{'Running for '}</span>{formatDuration(live.attemptStartedAt, { seconds: BigInt(Math.floor(liveNowMs / 1000)), nanos: (liveNowMs % 1000) * 1e6 })}</dd>
		{/if}
		{#if timing?.preContract && timing.total.kind === 'value'}
			<dt>Total</dt>
			<dd data-testid="{prefix}-total" data-tip="this run was recorded before scheduled time existed; total is dispatch to finish">{formatQuantity(timing.total)} (dispatch to finish)</dd>
		{/if}
	</dl>
	{#if heard}
		<section class="insp-sec" aria-labelledby="item-now-heading">
			<h3 id="item-now-heading">Now{#if live?.heartbeatAvailable}<span> from its Orker's heartbeat, {beat?.label}</span>{:else if live}<span> {unavailableReason(live.heartbeatUnavailableReason)}</span>{/if}</h3>
			<!-- The whole sentence, for a task that is not plainly running or whose beat is stale. -->
			<p
				data-testid="{prefix}-live"
				data-live={line?.kind}
				data-stale={line?.stale}
				data-tip={line?.title}
				class={line?.kind === 'started' && !line.stale ? 'sr' : 'insp-line'}
			>{line?.text}</p>
			{#if live?.heartbeatAvailable && live.state === LiveAttemptState.STARTED}
				<dl class="kv">
					<dt>Phase</dt>
					<dd><code>{live.phase || '—'}</code></dd>
					{#if live.containerId}
						<dt>Container</dt>
						<dd class="font-mono text-xs">{live.containerId.slice(0, 12)}</dd>
					{/if}
					<dt>Logged</dt>
					<dd>{formatBytes(live.bytesLogged)}, stdout and stderr</dd>
					{#if timeoutAt}
						<dt>Timeout</dt>
						<dd>not before {timeoutAt}</dd>
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
			<h3 id="item-phases-heading">Phases of attempt {last.attempt}</h3>
			<div class="pbar insp-pad" role="img" aria-label={phases.groups.map((g) => `${g.label} ${secs(g.ms)}`).join(', ')}>
				{#each phases.groups as group (group.key)}<i class="ph-{group.key}" style:width="{(100 * group.ms) / phases.total}%"></i>{/each}
			</div>
			<ul class="pkey insp-pad">
				{#each phases.groups as group (group.key)}
					<li>
						<span><i class="ph-{group.key}"></i>{group.label}</span><b>{secs(group.ms)}</b>
						{#if group.key !== 'run'}<small>{group.phases.join(', ')}</small>{/if}
					</li>
				{/each}
			</ul>
		</section>
	{/if}
	{#if terminal && digest}
		<section class="insp-sec" aria-labelledby="item-image-heading">
			<h3 id="item-image-heading">Image</h3>
			<div class="insp-pad"><Plate key="sha256" value={shortDigestWords(digest)} tip={digest} copy={digest} copyLabel="Copy digest" small /></div>
		</section>
	{/if}
	{#if children}{@render children()}{/if}
</section>

<style>
	.insp-note {
		margin: 0;
		padding: 10px 16px;
		border-bottom: 1px solid var(--line);
		font-size: 13.5px;
		color: var(--ink-2);
	}
	.insp-sec {
		display: grid;
		gap: 8px;
		padding: 0 0 10px;
		border-top: 1px solid var(--line);
	}
	.insp-sec > h3 {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 8px;
		margin: 0;
		padding: 10px 16px 4px;
		font-size: 13px;
		font-weight: 650;
		color: var(--ink-2);
	}
	.insp-sec > h3 span {
		font-size: 12.5px;
		font-weight: 400;
		color: var(--ink-3);
	}
	.insp-sec :global(.kv) {
		padding: 0;
	}
	.insp-line {
		margin: 0;
		padding: 0 16px;
		font-size: 13.5px;
		color: var(--ink-2);
	}
	.insp-pad {
		margin: 0 16px;
	}
	.pkey {
		display: grid;
		gap: 4px;
		padding: 0;
		font-size: 13px;
		list-style: none;
	}
	.pkey li {
		display: grid;
		grid-template-columns: auto auto;
		justify-content: space-between;
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
