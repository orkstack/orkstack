<script lang="ts">
	// What a task waits for when no Orker of its pool has room: what would have
	// to be free, what stands reserved now, and what frees it. From the run's
	// live read and the pool's Orkers; nothing here is an estimate of when.
	import { activityClient } from '#lib/clients/activities.ts';
	import { FilterOperator, filterOf } from '#lib/listFilter.ts';
	import type { Orker } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import type { CapacityWait } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { fmtQuantity, reservedOf } from '#lib/resources.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import Icon from './Icon.svelte';

	interface Props {
		/** "render[12]" */
		label: string;
		wait: CapacityWait;
		namespace: string;
		/** How many of this run's own tasks and items hold room on this pool now. */
		ownRunning: number;
		/** How long this run's finished tasks and items on this pool took, if any did. */
		took: string;
	}

	let { label, wait, namespace, ownRunning, took }: Props = $props();

	let orkers = $state.raw<Orker[]>([]);
	$effect(() => {
		const pool = wait.pool;
		let live = true;
		activityClient(fetch)
			.listOrkers({ filters: [filterOf('pool', FilterOperator.EQUALS, [pool])] })
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
	const cordoned = $derived(orkers.filter((o) => o.cordoned).length);
</script>

<section class="card" aria-labelledby="run-waits-heading" data-testid="run-waits-for">
	<div class="card-head"><h2 id="run-waits-heading">What {label} waits for</h2></div>
	<div class="diag">
		<div class="r2">
			<span class="mark-wait" aria-hidden="true"></span>
			<div>Room for a whole per-task cap on pool {wait.pool}{#if cap}<small>{fmtQuantity(cap)}: an Orker polls only while its largest task fits beside what it has reserved</small>{/if}</div>
		</div>
		<div class="r2">
			<Icon name="pool" />
			<div>
				Pool {wait.pool}: {#if reservedOf(wait.reserved, wait.capacity)}<b data-testid="run-waits-reserved">{reservedOf(wait.reserved, wait.capacity)}</b> reserved{:else}full{/if}
				<small>{wait.orkers} {wait.orkers === 1 ? 'Orker' : 'Orkers'}{#if orkers.length}, {cordoned === 0 ? 'none' : cordoned} cordoned{/if}</small>
			</div>
		</div>
		<div class="r2">
			<span class="mark-dot" aria-hidden="true"></span>
			<div>Room frees when an Orker of pool {wait.pool} has a whole per-task cap free{#if ownRunning > 0 || took}<small>{#if ownRunning > 0}{ownRunning} of the tasks there are this run's{/if}{#if ownRunning > 0 && took}; {/if}{#if took}its finished ones took {took}{/if}</small>{/if}</div>
		</div>
		<div class="flex"><a class="btn btn-sm" data-testid="run-waits-pool" href={withNamespace(`/admin/pools#pool-${encodeURIComponent(wait.pool)}`, namespace)}>View pool {wait.pool}</a></div>
	</div>
</section>
