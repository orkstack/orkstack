<script lang="ts">
	import { EventType, type Event } from '$lib/gen/orkstack/v1/run_pb';
	import {
		EVENT_FILTERS,
		isFailureEvent,
		matchesFilter,
		type EventFilter
	} from '$lib/eventBuffer';
	import { eventTypeLabel, fmtTime } from '$lib/status';

	// The run's event stream, read on request.
	//
	// This is an INSPECTION view: the page's default content is its tasks and
	// their output, and the events are what a reader opens when they want to
	// know what the engine did and in which order. It renders the page's
	// buffer — it holds no subscription of its own, so opening and closing it
	// costs a socket nothing (see the run page's socket effect).
	//
	// Legible at a glance means three things here: aligned columns, so a
	// sequence and a time can be read down rather than hunted along; a chip
	// per kind, so run lifecycle, task lifecycle and notices part company at a
	// distance; and failures distinct TWICE — a rose chip and a tinted row —
	// because the colour of one word is not enough to find the row that
	// matters in two hundred.

	interface Props {
		events: Event[];
		/** How many events fell off the front of the page's capped buffer. */
		dropped: number;
		/** How many the socket delivered in total, dropped ones included. */
		total: number;
		/** The socket never delivered anything: say so rather than "no events". */
		streamBroken: boolean;
		runId: string;
		cap: number;
	}

	let { events, dropped, total, streamBroken, runId, cap }: Props = $props();

	let filter = $state<EventFilter>('all');
	const shown = $derived(events.filter((event) => matchesFilter(event, filter)));
	const lastSequence = $derived(
		events.length > 0 ? events[events.length - 1].sequence.toString() : '—'
	);

	// The same follow rule the log panes use, for the same reason: a stream
	// arriving must not yank a reader away from the row they are reading.
	let box: HTMLDivElement | undefined = $state(undefined);
	let following = $state(true);

	function onScroll() {
		if (!box) return;
		// A box with nothing to scroll cannot carry a gesture; reading one as
		// "the reader is at the tail" would resume the follow whenever a
		// filter shrank the list below one screen.
		if (box.scrollHeight - box.clientHeight <= 8) return;
		following = box.scrollHeight - box.scrollTop - box.clientHeight <= 8;
	}

	$effect(() => {
		shown.length; // track
		if (!box || !following) return;
		box.scrollTop = box.scrollHeight;
	});

	function chipClasses(event: Event): string {
		if (isFailureEvent(event)) return 'text-rose-300 ring-rose-400/40 bg-rose-500/10';
		switch (event.type) {
			case EventType.RUN_CREATED:
			case EventType.RUN_STARTED:
			case EventType.RUN_SUCCEEDED:
			case EventType.RUN_CANCELED:
				return 'text-sky-300 ring-sky-400/30 bg-sky-500/10';
			case EventType.TASK_SUCCEEDED:
				return 'text-emerald-400 ring-emerald-400/30 bg-emerald-500/10';
			case EventType.TASK_LOG:
				return 'text-amber-300 ring-amber-400/30 bg-amber-500/10';
			default:
				return 'text-zinc-400 ring-zinc-600/40 bg-zinc-700/20';
		}
	}
</script>

<section data-testid="event-inspector" class="rounded-lg border border-zinc-800 bg-zinc-950/60">
	<div
		class="sticky top-0 z-10 flex flex-wrap items-center gap-2 rounded-t-lg border-b border-zinc-800 bg-zinc-950/95 px-3 py-2 text-xs backdrop-blur"
	>
		{#each EVENT_FILTERS as chip (chip.id)}
			<button
				type="button"
				data-testid="event-filter-{chip.id}"
				aria-pressed={filter === chip.id}
				onclick={() => (filter = chip.id)}
				class="rounded-full px-2.5 py-0.5 ring-1 {filter === chip.id
					? 'bg-emerald-500/15 text-emerald-300 ring-emerald-400/40'
					: 'text-zinc-400 ring-zinc-700 hover:text-zinc-200'}"
			>
				{chip.label}
			</button>
		{/each}
		<span class="ml-auto text-zinc-500" data-testid="event-count">
			{shown.length}{shown.length === events.length ? '' : ` of ${events.length}`} events
		</span>
		<span class="text-zinc-600" data-testid="event-last-seq">last #{lastSequence}</span>
	</div>

	{#if dropped > 0}
		<p data-testid="event-dropped" class="border-b border-zinc-800 px-3 py-1.5 text-xs text-amber-300">
			Showing the last {cap} of {total} events; {dropped} earlier ones are no longer held here.
			The whole stream is the run's history:
			<code class="text-amber-200">orkctl run watch {runId} --json</code>
		</p>
	{/if}

	<div
		bind:this={box}
		onscroll={onScroll}
		data-testid="event-log"
		class="max-h-[32rem] overflow-y-auto px-2 py-1.5 font-mono text-xs"
	>
		{#if events.length === 0 && streamBroken}
			<p data-testid="event-log-offline" class="px-1 py-1 text-amber-300">
				Not receiving events. The engine refused or dropped this page's websocket, so this
				run cannot update live — reload to see its current state. If the engine serves the UI
				from another origin, it needs
				<code class="text-amber-200">ORK_WS_ALLOWED_ORIGINS</code> to include it.
			</p>
		{:else if events.length === 0}
			<p data-testid="event-log-waiting" class="px-1 py-1 text-zinc-500">Waiting for events…</p>
		{:else if shown.length === 0}
			<p data-testid="event-filter-empty" class="px-1 py-1 text-zinc-500">
				No {filter} events in the {events.length} this page holds.
			</p>
		{:else}
			{#each shown as event (event.sequence.toString())}
				<div
					data-testid="event-entry"
					data-kind={eventTypeLabel(event.type)}
					data-failure={isFailureEvent(event)}
					class="grid grid-cols-[3.5rem_5.5rem_9.5rem_8rem_minmax(0,1fr)] items-baseline gap-2 rounded px-1 py-0.5 {isFailureEvent(
						event
					)
						? 'bg-rose-500/10'
						: ''}"
				>
					<span class="text-right text-zinc-600">#{event.sequence.toString()}</span>
					<span class="text-zinc-500">{fmtTime(event.timestamp)}</span>
					<span class="justify-self-start rounded-full px-2 py-0.5 ring-1 {chipClasses(event)}">
						{eventTypeLabel(event.type)}
					</span>
					<span class="truncate text-zinc-400">{event.taskId ? `[${event.taskId}]` : ''}</span>
					<span class="break-all text-zinc-300">{event.message}</span>
				</div>
			{/each}
		{/if}
	</div>
</section>
