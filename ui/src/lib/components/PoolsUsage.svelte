<script lang="ts">
	// What this namespace reserved over the last 7 days, and against what budget.
	// Usage is reservation multiplied by how long an Orker held it, derived on
	// read (StatsService.GetUsage), never measured use. Every chart prints its
	// values and carries a table for assistive technology.
	import { durationFromMs, type Duration } from '@bufbuild/protobuf/wkt';
	import { errorMessage } from '#lib/client.ts';
	import { statsClient } from '#lib/clients/stats.ts';
	import { FailureGroupBy, type GetUsageResponse } from '#lib/gen/orkstack/v1/stats_pb.ts';
	import { namespaceLabel } from '#lib/namespace.ts';
	import { fmtGoDuration } from '#lib/usage.ts';

	let { namespace }: { namespace: string } = $props();

	const WEEK_MS = 7 * 24 * 3600_000;
	const SHOWN = 4;
	let week = $state.raw<GetUsageResponse | null>(null);
	let budgetRead = $state.raw<GetUsageResponse | null>(null);
	let failure = $state('');
	let budgetFailure = $state('');

	$effect(() => {
		const ns = namespace;
		let live = true;
		const stats = statsClient(fetch);
		failure = '';
		budgetFailure = '';
		void (async () => {
			try {
				const res = await stats.getUsage({ namespace: ns, lookback: durationFromMs(WEEK_MS), groupBy: [FailureGroupBy.ORKFLOW] });
				if (!live) return;
				week = res;
				// A budget is judged over its own period: ask that, when it is not this one.
				const period: Duration | undefined = res.budget?.period;
				if (res.budgetWindow || !period) {
					budgetRead = res;
					return;
				}
				try {
					const over = await stats.getUsage({ namespace: ns, lookback: period, groupBy: [] });
					if (live) budgetRead = over;
				} catch (err) {
					if (live) budgetFailure = errorMessage(err);
				}
			} catch (err) {
				if (live) failure = errorMessage(err);
			}
		})();
		return () => {
			live = false;
		};
	});

	const hours = (seconds: number) => seconds / 3600;
	const fmtHours = (h: number) => (h >= 10 ? Math.round(h).toLocaleString('en-US') : h >= 0.1 ? h.toFixed(1) : h > 0 ? '<0.1' : '0');
	const rows = $derived.by(() => {
		const sorted = [...(week?.rows ?? [])].sort((a, b) => b.cpuSeconds - a.cpuSeconds);
		const top = sorted.slice(0, SHOWN).map((row) => ({ name: row.orkflowName || 'Unnamed', h: hours(row.cpuSeconds) }));
		const rest = sorted.slice(SHOWN);
		if (rest.length) top.push({ name: `${rest.length} ${rest.length === 1 ? 'other' : 'others'}`, h: hours(rest.reduce((sum, row) => sum + row.cpuSeconds, 0)) });
		return top;
	});
	const barMax = $derived(Math.max(0.0001, ...rows.map((row) => row.h)));
	const budget = $derived(budgetRead?.budget ?? week?.budget);
	const used = $derived(hours(budgetRead?.total?.cpuSeconds ?? 0));
	const allowed = $derived(budget ? Number(budget.cpuSeconds) / 3600 : 0);
	const share = $derived(allowed > 0 ? Math.min(100, Math.round((100 * used) / allowed)) : 0);
	const gibHours = $derived(hours(budgetRead?.total?.gibSeconds ?? 0));
</script>

<h2 class="usage-h" data-testid="pools-usage-heading">Last 7 days</h2>
{#if failure}
	<p class="text-sm text-ink-2" data-testid="pools-usage-error">What was reserved could not be read: {failure}</p>
{:else}
	<div class="vizgrid" data-testid="pools-usage">
		<figure class="card viz" aria-labelledby="pools-usage-h">
			<h3 id="pools-usage-h">Reserved by Orkflow <small>last 7 days</small></h3>
			{#if !week}
				<p class="viz-none">Reading what was reserved…</p>
			{:else if rows.length === 0}
				<p class="viz-none" data-testid="pools-usage-empty">Nothing was reserved in these 7 days.</p>
			{:else}
				<div class="hbars" aria-hidden="true">
					{#each rows as row (row.name)}
						<div class="hbar" data-testid="pools-usage-row">
							<span class="l">{row.name}</span><span class="v">{fmtHours(row.h)} cpu-h</span>
							<svg class="bar" height="8"><rect class="track" width="100%" height="8" rx="4" /><rect class="fill" width="{(100 * row.h) / barMax}%" height="8" rx="4" /></svg>
						</div>
					{/each}
				</div>
				<div class="sr">
					<table>
						<caption>cpu-hours reserved by Orkflow, last 7 days</caption>
						<thead><tr><th scope="col">Orkflow</th><th scope="col">cpu-hours</th></tr></thead>
						<tbody>{#each rows as row (row.name)}<tr><th scope="row">{row.name}</th><td>{fmtHours(row.h)}</td></tr>{/each}</tbody>
					</table>
				</div>
			{/if}
			<figcaption>Reservation multiplied by how long an Orker held it; not measured use.</figcaption>
		</figure>
		<figure class="card viz" aria-labelledby="pools-budget-h" data-testid="pools-budget">
			{#if budget}
				<h3 id="pools-budget-h">Budget of namespace {namespaceLabel(namespace)} <small>{budget.enforce ? 'enforced' : 'reported only'}</small></h3>
				{#if budgetFailure}
					<p class="viz-none" data-testid="pools-budget-error">What it has used could not be read: {budgetFailure}</p>
				{:else if !budgetRead}
					<p class="viz-none">Reading what it has used…</p>
				{:else}
					<div class="kpis"><span class="big" data-testid="pools-budget-used">{fmtHours(used)}<small>of {fmtHours(allowed)} cpu-hours per {fmtGoDuration(budget.period)}</small></span></div>
					<svg class="bar" height="14" role="img" aria-label="{share}% of the budget used"><rect class="track" width="100%" height="14" rx="7" /><rect class="fill" width="{share}%" height="14" /></svg>
					<div class="vkey"><span><i></i>{share}% used</span></div>
					<figcaption>{budget.enforce ? 'An enforced budget refuses new runs once it is used up; runs already accepted finish.' : 'A soft budget is reported and refuses nothing.'} {fmtHours(gibHours)} GiB-hours reserved.</figcaption>
				{/if}
			{:else}
				<h3 id="pools-budget-h">Budget of namespace {namespaceLabel(namespace)}</h3>
				<p class="viz-none" data-testid="pools-budget-none">{week ? 'No budget: what this namespace reserves is unbounded.' : 'Reading its budget…'}</p>
			{/if}
		</figure>
	</div>
{/if}

<style>
	.usage-h {
		margin: 24px 0 10px;
		font-size: 16px;
		font-weight: 650;
	}
	.vizgrid {
		display: grid;
		grid-template-columns: minmax(0, 1.3fr) minmax(0, 1fr);
		gap: 12px;
	}
	.viz {
		display: grid;
		gap: 10px;
		align-content: start;
		margin: 0;
		padding: 14px 16px;
	}
	.viz h3 {
		margin: 0;
		font-size: 14px;
		font-weight: 650;
	}
	.viz h3 small {
		margin-left: 4px;
		font-size: 13px;
		font-weight: 400;
		color: var(--ink-3);
	}
	.viz figcaption {
		text-wrap: pretty;
	}
	.viz figcaption,
	.viz-none {
		margin: 0;
		font-size: 13px;
		color: var(--ink-3);
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
		overflow: hidden;
		min-width: 0;
		color: var(--ink);
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.hbar .v {
		color: var(--ink-2);
		font-variant-numeric: tabular-nums;
		text-align: right;
	}
	.hbar svg {
		grid-column: 1 / -1;
	}
	.bar {
		display: block;
		width: 100%;
	}
	.track {
		fill: var(--surface-3);
	}
	.fill {
		fill: var(--meter);
	}
	.kpis {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 6px 24px;
	}
	.big {
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
	.vkey {
		display: flex;
		font-size: 13px;
		color: var(--ink-2);
	}
	.vkey span {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	.vkey i {
		display: inline-block;
		width: 10px;
		height: 10px;
		border-radius: 2px;
		background: var(--meter);
	}
	@media (max-width: 900px) {
		.vizgrid {
			grid-template-columns: minmax(0, 1fr);
		}
	}
	@media (forced-colors: active) {
		.fill {
			fill: Highlight;
		}
		.vkey i {
			forced-color-adjust: none;
		}
	}
</style>
