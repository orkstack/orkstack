<script lang="ts">
	// The last 7 days of a namespace, in five cards: outcomes, how long runs
	// take, the most failing Orkflows, failures by reason and the flakiest
	// tasks. Every chart prints its values and carries a table for assistive
	// technology; colour never stands alone (a status keeps its mark).
	// "Runs created each day" draws only when the engine answers a daily
	// series; without one the window's totals stand alone.
	import type { GetFailureBreakdownResponse, GetFlakeIndexResponse, GetStatsResponse } from '#lib/gen/orkstack/v1/stats_pb.ts';
	import { durationMs, finishedRuns, fmtDuration, runCount, type DayBar } from '#lib/stats.ts';
	import { shortInstant } from '#lib/when.ts';
	import { fmtCategoryWords } from '#lib/failureWords.ts';

	interface Props {
		week: GetStatsResponse | null;
		weekError: string | null;
		failures: GetFailureBreakdownResponse | null;
		flakes: GetFlakeIndexResponse | null;
		/** The daily series, oldest first, when the engine has one. */
		days?: DayBar[];
		/** Only outcomes and how long runs take: the Dashboard's two charts. */
		overview?: boolean;
	}

	let { week, weekError, failures, flakes, days = [], overview = false }: Props = $props();

	const n = (v: bigint | undefined) => Number(v ?? 0n);
	const counts = $derived(week?.runsByStatus ?? {});
	const succeeded = $derived(n(counts.succeeded));
	const failed = $derived(n(counts.failed));
	const canceled = $derived(n(counts.canceled));
	const running = $derived(n(counts.running));
	const finished = $derived(finishedRuns(counts));
	const created = $derived(succeeded + failed + canceled + running);
	const parts = $derived(
		[
			{ key: 'succeeded', n: succeeded, fill: 'var(--ok-mark)', mark: 'mark-square' },
			{ key: 'canceled', n: canceled, fill: 'var(--viz-cancel)', mark: 'mark-strike' },
			{ key: 'failed', n: failed, fill: 'var(--fail-mark)', mark: 'mark-tri' },
			{ key: 'still running', n: running, fill: 'var(--run)', mark: 'mark-dot' }
		].filter((p) => p.n > 0)
	);
	const segs = $derived.by(() => {
		let x = 0;
		const total = Math.max(1, parts.reduce((a, p) => a + p.n, 0));
		return parts.map((p) => {
			const w = (100 * p.n) / total;
			const s = { ...p, x, w };
			x += w;
			return s;
		});
	});
	const rate = $derived(week?.successRateDefined ? Math.round(week.successRate * 100) : null);

	// Durations on a log axis from 1 s to 1 h.
	const LOG_MAX = Math.log10(3600);
	const lp = (ms: number | null) => (ms === null ? 0 : Math.min(100, Math.max(0, (Math.log10(Math.max(1, ms / 1000)) / LOG_MAX) * 100)));
	const p50 = $derived(durationMs(week?.p50Duration));
	const p95 = $derived(durationMs(week?.p95Duration));
	const ticks = [
		{ s: 1, l: '1s' },
		{ s: 10, l: '10s' },
		{ s: 60, l: '1m' },
		{ s: 600, l: '10m' },
		{ s: 3600, l: '1h' }
	];

	const mostFailing = $derived((week?.mostFailing ?? []).slice(0, 4).map((m) => ({ name: m.orkflowName, failed: n(m.failed), total: Math.max(1, n(m.total)) })));
	const reasons = $derived((failures?.groups ?? []).slice(0, 4).map((g) => ({ code: g.code || 'no code', category: fmtCategoryWords(g.category), n: n(g.runCount) })));
	const reasonMax = $derived(Math.max(1, ...reasons.map((r) => r.n)));
	const flaky = $derived(
		(flakes?.groups ?? [])
			.filter((g) => g.retryRateDefined && n(g.runsRetried) > 0)
			.slice(0, 4)
			.map((g) => ({ name: `${g.orkflowName} · ${g.taskId}`, rate: g.retryRate, detail: `${n(g.runsRetried)} of ${n(g.runsObserved)} runs retried${g.dominantCode ? ` · mostly ${g.dominantCode}` : ''}` }))
	);
	const flakyMax = $derived(Math.max(0.0001, ...flaky.map((f) => f.rate)));
	const dayMax = $derived(Math.max(1, ...days.map((d) => d.total)));
	const pct = (v: number) => `${Math.round(v * 100)}%`;
</script>

{#snippet hbar(frac: number, fill: string)}
	<svg class="bar" height="8" aria-hidden="true"
		><rect class="track" width="100%" height="8" rx="4" /><rect width="{Math.max(0, Math.min(1, frac)) * 100}%" height="8" rx="4" fill={fill} /></svg
	>
{/snippet}

<div class="vizgrid" data-testid="runs-insights-body">
	<figure class="card viz wide" aria-labelledby="viz-outcomes" data-testid="viz-outcomes">
		<h3 id="viz-outcomes">Outcomes {#if week}<small>{runCount(created)} created</small>{/if}</h3>
		{#if weekError}
			<p class="viz-none" data-testid="viz-outcomes-error">The numbers for these 7 days could not be read: {weekError}</p>
		{:else if !week}
			<p class="viz-none">Reading the last 7 days…</p>
		{:else}
			<div class="vsplit" class:solo={days.length === 0}>
				<div class="grid gap-2.5">
					<p class="big" data-testid="viz-success-rate">
						{rate === null ? '—' : `${rate}%`}<small>{rate === null ? 'no run finished yet' : `succeeded, ${succeeded} of ${finished} finished`}</small>
					</p>
					<svg class="bar bar-round" height="14" aria-hidden="true"
						><rect class="track" width="100%" height="14" rx="7" />{#each segs as s (s.key)}<rect
								x="{s.x}%"
								width="{s.w}%"
								height="14"
								fill={s.fill}
								data-tip="{s.n} {s.key}"
							/>{/each}{#each segs.slice(1) as s (s.key)}<line x1="{s.x}%" x2="{s.x}%" y1="0" y2="14" stroke="var(--surface)" stroke-width="2" />{/each}</svg
					>
					<div class="vkey">
						{#each parts as p (p.key)}<span><span class={p.mark === 'mark-dot' ? 'mark-dot' : `${p.mark} key-mark`} aria-hidden="true"></span>{p.n} {p.key}</span>{/each}
					</div>
				</div>
				{#if days.length > 0}
					<div class="cols" data-testid="viz-days">
						<span class="text-sm text-ink-3">Runs created each day; the failed ones on top. Totals and failed runs below</span>
						<svg class="bar" height="64" aria-hidden="true"
							><line class="gl" x1="0" x2="100%" y1="63.5" y2="63.5" />{#each days as d, i (i)}{@const w = 100 / days.length}{@const hr = (60 * d.total) / dayMax}{@const hf = (60 * d.failed) / dayMax}<rect
									x="{i * w + w * 0.2}%"
									y={64 - hr + hf + (hf > 0 ? 2 : 0)}
									width="{w * 0.6}%"
									height={Math.max(0, hr - hf - (hf > 0 ? 2 : 0))}
									rx="2"
									fill="var(--meter)"
								/>{#if hf > 0}<rect x="{i * w + w * 0.2}%" y={64 - hr} width="{w * 0.6}%" height={hf} rx="2" fill="var(--fail-mark)" />{/if}<rect
									x="{i * w}%"
									y="0"
									width="{w}%"
									height="64"
									fill="transparent"
									data-tip="{d.label}{d.partial ? ' (so far)' : ''}: {d.total} runs, {d.failed} failed"
								/>{/each}</svg
						>
						<div class="axis" aria-hidden="true">{#each days as d, i (i)}<span>{d.day}</span>{/each}</div>
						<div class="axis" aria-hidden="true">{#each days as d, i (i)}<span class="num">{d.total}</span>{/each}</div>
						<div class="axis fails" aria-hidden="true">{#each days as d, i (i)}<span class="num"><i class="mark-tri key-mark"></i>{d.failed}</span>{/each}</div>
						<div class="sr">
							<table>
								<caption>Runs created each day, last 7 days</caption>
								<thead><tr><th scope="col">Day</th><th scope="col">Runs</th><th scope="col">Failed</th></tr></thead>
								<tbody>{#each days as d, i (i)}<tr><th scope="row">{d.label}{d.partial ? ', so far' : ''}</th><td>{d.total}</td><td>{d.failed}</td></tr>{/each}</tbody>
							</table>
						</div>
					</div>
				{/if}
			</div>
			<figcaption>Canceled runs count as finished, so canceling cannot raise the rate.</figcaption>
		{/if}
	</figure>

	<figure class="card viz" aria-labelledby="viz-durations" data-testid="viz-durations">
		<h3 id="viz-durations">How long runs take</h3>
		{#if !week || p50 === null}
			<p class="viz-none">{week ? 'No finished run to measure yet.' : 'Reading the last 7 days…'}</p>
		{:else}
			<div class="kpis">
				<p class="big">{fmtDuration(week.p50Duration)}<small>median</small></p>
				<p class="big">{fmtDuration(week.p95Duration)}<small>95th percentile</small></p>
			</div>
			<div class="dstrip">
				<svg class="bar" height="36" aria-hidden="true" style:overflow="visible"
					>{#each ticks as t (t.s)}<line class="gl" x1="{lp(t.s * 1000)}%" x2="{lp(t.s * 1000)}%" y1="4" y2="32" />{/each}<rect
						x="{lp(p50)}%"
						y="14"
						width="{Math.max(0, lp(p95) - lp(p50))}%"
						height="8"
						rx="4"
						fill="var(--navy-tint)"
						stroke="var(--meter)"
					/><circle cx="{lp(p50)}%" cy="18" r="7" fill="var(--meter)" data-tip="Median {fmtDuration(week.p50Duration)}" /><circle
						cx="{lp(p95)}%"
						cy="18"
						r="7"
						fill="var(--surface)"
						stroke="var(--meter)"
						stroke-width="2.5"
						data-tip="95th percentile {fmtDuration(week.p95Duration)}"
					/></svg
				>
				<div class="taxis" aria-hidden="true">{#each ticks as t (t.s)}<span style:left="{lp(t.s * 1000)}%">{t.l}</span>{/each}</div>
			</div>
			<figcaption>
				From {Number(week.durationSampleCount)} finished {Number(week.durationSampleCount) === 1 ? 'run' : 'runs'}{#if week.durationCoverageSince}{' '}created since {shortInstant(week.durationCoverageSince)}{/if}: durations reach back only as far as the run records the cluster still keeps.
			</figcaption>
		{/if}
	</figure>

	{#if !overview}
	<figure class="card viz" aria-labelledby="viz-failing" data-testid="viz-most-failing">
		<h3 id="viz-failing">Most failing Orkflows</h3>
		{#if mostFailing.length === 0}
			<p class="viz-none">{week ? 'No Orkflow failed in these 7 days.' : 'Reading the last 7 days…'}</p>
		{:else}
			<div class="hbars">
				{#each mostFailing as m (m.name)}
					<div class="hbar"><span class="l">{m.name}</span><span class="v num">{m.failed} of {m.total}</span>{@render hbar(m.failed / m.total, 'var(--fail-mark)')}</div>
				{/each}
			</div>
			<div class="sr">
				<table>
					<caption>Failed runs by Orkflow</caption>
					<thead><tr><th scope="col">Orkflow</th><th scope="col">Failed</th><th scope="col">Finished</th></tr></thead>
					<tbody>{#each mostFailing as m (m.name)}<tr><th scope="row">{m.name}</th><td>{m.failed}</td><td>{m.total}</td></tr>{/each}</tbody>
				</table>
			</div>
			<figcaption>Failed runs of all that finished.</figcaption>
		{/if}
	</figure>

	<figure class="card viz" aria-labelledby="viz-reasons" data-testid="viz-reasons">
		<h3 id="viz-reasons">Failures by reason</h3>
		{#if reasons.length === 0}
			<p class="viz-none">{failures ? 'No failed run in these 7 days.' : 'Reading the failed runs…'}</p>
		{:else}
			<div class="hbars">
				{#each reasons as r (r.code + r.category)}
					<div class="hbar"><span class="l"><code>{r.code}</code></span><span class="v num">{r.n}</span>{@render hbar(r.n / reasonMax, 'var(--fail-mark)')}<small>{r.category}</small></div>
				{/each}
			</div>
			<div class="sr">
				<table>
					<caption>Failed runs by reason</caption>
					<thead><tr><th scope="col">Reason</th><th scope="col">Category</th><th scope="col">Runs</th></tr></thead>
					<tbody>{#each reasons as r (r.code + r.category)}<tr><th scope="row">{r.code}</th><td>{r.category}</td><td>{r.n}</td></tr>{/each}</tbody>
				</table>
			</div>
			{#if failures}<figcaption>
					{Number(failures.failedRunsScanned) === Number(failures.failedRunsInWindow)
						? `All ${Number(failures.failedRunsInWindow)} failed runs were read.`
						: `${Number(failures.failedRunsScanned)} of ${Number(failures.failedRunsInWindow)} failed runs were read.`}
				</figcaption>{/if}
		{/if}
	</figure>

	<figure class="card viz" aria-labelledby="viz-flaky" data-testid="viz-flaky">
		<h3 id="viz-flaky">Flakiest tasks <small>needed a retry</small></h3>
		{#if flaky.length === 0}
			<p class="viz-none">{flakes ? 'No task needed a retry in these 7 days.' : 'Reading the attempts…'}</p>
		{:else}
			<div class="hbars">
				{#each flaky as f (f.name)}
					<div class="hbar"><span class="l">{f.name}</span><span class="v num">{pct(f.rate)}</span>{@render hbar(f.rate / flakyMax, 'var(--meter)')}<small>{f.detail}</small></div>
				{/each}
			</div>
			<div class="sr">
				<table>
					<caption>Tasks that needed more than one attempt</caption>
					<thead><tr><th scope="col">Task</th><th scope="col">Retry rate</th><th scope="col">Detail</th></tr></thead>
					<tbody>{#each flaky as f (f.name)}<tr><th scope="row">{f.name}</th><td>{pct(f.rate)}</td><td>{f.detail}</td></tr>{/each}</tbody>
				</table>
			</div>
			<figcaption>A retried task can still succeed; no other number shows it.</figcaption>
		{/if}
	</figure>
	{/if}
</div>

<style>
	.vizgrid {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 12px;
	}
	.viz {
		margin: 0;
		padding: 14px 16px;
		display: grid;
		gap: 10px;
		align-content: start;
	}
	.viz.wide {
		grid-column: span 2;
	}
	.viz h3 {
		margin: 0;
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 6px 10px;
		font-size: 14.5px;
		font-weight: 650;
	}
	.viz h3 small {
		font-size: 13px;
		font-weight: 400;
		color: var(--ink-3);
	}
	.viz figcaption,
	.viz-none {
		margin: 0;
		font-size: 13px;
		color: var(--ink-3);
	}
	.kpis {
		display: flex;
		flex-wrap: wrap;
		gap: 6px 24px;
		align-items: baseline;
	}
	.big {
		margin: 0;
		font-size: 26px;
		font-weight: 700;
		letter-spacing: -0.02em;
		line-height: 1.1;
		font-variant-numeric: tabular-nums;
	}
	.big small {
		margin-left: 6px;
		font-size: 13.5px;
		font-weight: 500;
		letter-spacing: 0;
		color: var(--ink-3);
	}
	.bar-round {
		border-radius: 7px;
	}
	:global(.viz svg.bar) {
		display: block;
		width: 100%;
	}
	:global(.viz .track) {
		fill: var(--surface-3);
	}
	.viz .gl {
		stroke: var(--line-2);
		stroke-width: 1;
	}
	.vkey {
		display: flex;
		flex-wrap: wrap;
		gap: 4px 14px;
		font-size: 13px;
		color: var(--ink-2);
		font-variant-numeric: tabular-nums;
	}
	.vkey > span {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	.key-mark {
		display: inline-flex;
	}
	.vsplit {
		display: grid;
		grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr);
		gap: 18px;
		align-items: end;
	}
	.vsplit.solo {
		grid-template-columns: minmax(0, 1fr);
	}
	.cols {
		display: grid;
		gap: 4px;
	}
	.axis {
		display: grid;
		grid-template-columns: repeat(7, minmax(0, 1fr));
		font-size: 13px;
		color: var(--ink-3);
		text-align: center;
	}
	.axis.fails {
		color: var(--ink-2);
	}
	.axis.fails span {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		gap: 4px;
	}
	.dstrip {
		display: grid;
		gap: 4px;
	}
	.taxis {
		position: relative;
		height: 18px;
		font-size: 13px;
		color: var(--ink-3);
	}
	.taxis span {
		position: absolute;
		transform: translateX(-50%);
		white-space: nowrap;
	}
	.taxis span:first-child {
		transform: none;
	}
	.taxis span:last-child {
		transform: translateX(-100%);
	}
	.hbars {
		display: grid;
		gap: 8px;
	}
	.hbar {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 2px 10px;
		align-items: center;
		font-size: 13.5px;
	}
	.hbar .l {
		min-width: 0;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.hbar .v {
		color: var(--ink-2);
		text-align: right;
	}
	.hbar :global(svg),
	.hbar small {
		grid-column: 1 / -1;
	}
	.hbar small {
		font-size: 13px;
		color: var(--ink-3);
	}
	@media (max-width: 1180px) {
		.vizgrid {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
	}
	@media (max-width: 900px) {
		.vizgrid,
		.vsplit {
			grid-template-columns: minmax(0, 1fr);
		}
		.viz.wide {
			grid-column: auto;
		}
	}
	@media (forced-colors: active) {
		.viz svg {
			forced-color-adjust: none;
		}
	}
</style>
