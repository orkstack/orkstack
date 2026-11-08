<script lang="ts">
	// What a task waits for when no Orker of its pool has room, or when its pool's
	// Orkers cannot reach object storage: what would have to change, how things
	// stand now, and what ends it. From the run's live read and the pool's Orkers;
	// nothing here is an estimate of when.
	import { waitRoomNote } from '#lib/placement.ts';
	import { activityClient } from '#lib/clients/activities.ts';
	import { FilterOperator, filterOf } from '#lib/listFilter.ts';
	import type { Orker } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import type { CapacityWait, StoreOutageWait } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { operatorCordoned } from '#lib/orkers.ts';
	import { fmtQuantity, reservedOf } from '#lib/resources.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import Icon from './Icon.svelte';

	interface Props {
		/** "render[12]" */
		label: string;
		/** The wait for room; `outage` says the task waits for object storage instead. */
		wait?: CapacityWait;
		outage?: StoreOutageWait;
		namespace: string;
		/** How many of this run's own tasks and items hold room on this pool now. */
		ownRunning?: number;
		/** How long this run's finished tasks and items on this pool took, if any did. */
		took?: string;
	}

	let { label, wait, outage, namespace, ownRunning = 0, took = '' }: Props = $props();
	const roomNote = $derived(waitRoomNote(ownRunning, took));
	const pool = $derived(outage?.pool ?? wait?.pool ?? '');

	let orkers = $state.raw<Orker[]>([]);
	$effect(() => {
		const poolName = pool;
		let live = true;
		activityClient(fetch)
			.listOrkers({ filters: [filterOf('pool', FilterOperator.EQUALS, [poolName])] })
			.then(
				(res) => {
					if (live) orkers = res.orkers;
				},
				() => {
					// The wait itself is still said; only the per-task cap is not.
				}
			);
		return () => {
			live = false;
		};
	});
	const cap = $derived(orkers.find((o) => o.maxRequest)?.maxRequest);
	const cordoned = $derived(orkers.filter(operatorCordoned).length);
	const outages = $derived(orkers.filter((o) => o.storeOutage).length);
</script>

<section class="card" aria-labelledby="run-waits-heading" data-testid="run-waits-for">
	<div class="card-head"><h2 id="run-waits-heading">What {label} waits for</h2></div>
	<div class="diag">
		{#if outage}
			<div class="r2">
				<span class="mark-wait" aria-hidden="true"></span>
				<div>Object storage to answer the Orkers of pool {outage.pool}<small>{outage.orkers} {outage.orkers === 1 ? 'Orker has' : 'Orkers have'} cordoned {outage.orkers === 1 ? 'itself' : 'themselves'} since {fmtTimestamp(outage.since)}</small></div>
			</div>
			<div class="r2">
				<span class="mark-dot" aria-hidden="true"></span>
				<div data-testid="run-waits-store-outage">This task uses no attempt while it waits, and starts by itself once object storage answers.</div>
			</div>
			<div class="flex"><a class="btn btn-sm" data-testid="run-waits-pool" href={withNamespace(`/admin/pools#pool-${encodeURIComponent(outage.pool)}`, namespace)}>View pool {outage.pool}</a></div>
		{:else if wait}
			<div class="r2">
				<span class="mark-wait" aria-hidden="true"></span>
				<div>Room for a whole per-task cap on pool {wait.pool}{#if cap}<small>{fmtQuantity(cap)}: an Orker polls only while its largest task fits beside what it has reserved</small>{/if}</div>
			</div>
			<div class="r2">
				<Icon name="pool" />
				<div>
					Pool {wait.pool}: {#if reservedOf(wait.reserved, wait.capacity)}<b data-testid="run-waits-reserved">{reservedOf(wait.reserved, wait.capacity)}</b> reserved{:else}full{/if}
					<small>{wait.orkers} {wait.orkers === 1 ? 'Orker' : 'Orkers'}{#if orkers.length}, {cordoned === 0 ? 'none' : cordoned} cordoned{#if outages > 0}, <span data-testid="run-waits-store-outages">{outages} cannot reach object storage</span>{/if}{/if}</small>
				</div>
			</div>
			<div class="r2">
				<span class="mark-dot" aria-hidden="true"></span>
				<div>Room frees when an Orker of pool {wait.pool} has a whole per-task cap free{#if roomNote}<small>{roomNote}</small>{/if}</div>
			</div>
			<div class="flex"><a class="btn btn-sm" data-testid="run-waits-pool" href={withNamespace(`/admin/pools#pool-${encodeURIComponent(wait.pool)}`, namespace)}>View pool {wait.pool}</a></div>
		{/if}
	</div>
</section>
