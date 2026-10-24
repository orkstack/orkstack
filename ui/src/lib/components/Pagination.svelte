<script lang="ts">
	// The footer of a cursor-paged list: Previous, what is on screen, Next.
	//
	// One rule, and everything here follows from it: it is GIVEN its hrefs, it
	// owns no navigation state, and it never shows a total the API did not
	// send. The pages know their coordinates and build the addresses; what a
	// reader sees of them is this component's business and nobody else's —
	// the same split ErrorPanel makes, given `message` and `onretry` and
	// owning neither.
	//
	// ListRuns and ListOrkflows answer with a `next_page_token` and no count
	// (proto/orkstack/v1/service.proto), so there is no page count to render
	// and no "of N" to put beside the page number. The status line states what
	// is known — which page this is, how many rows it holds, and whether it is
	// the last — because a made-up total is worse than an absent one.
	import type { Snippet } from 'svelte';
	import Icon from './Icon.svelte';

	interface Props {
		/** testid prefix: 'runs' → runs-previous, runs-next, runs-first, runs-page-status */
		testid: string;
		/** 1-based, client-counted: the URL carries it, the engine knows nothing about it. */
		page: number;
		/** rows on this page */
		count: number;
		/** singular noun for the status line, pluralised by count: 'run' | 'Orkflow' */
		noun: string;
		/** undefined = disabled */
		previousHref?: string;
		nextHref?: string;
		/** shown beside a disabled Previous when the path back is not known */
		firstHref?: string;
		/**
		 * Called as Next is followed, before the router moves.
		 *
		 * The page records the cursor it is about to leave (it is the only one
		 * that knows where it keeps that); this component still holds nothing.
		 */
		onnext?: () => void;
		/**
		 * Optional content at the left of the row, before the status line.
		 *
		 * A deliberate hole rather than a page-size prop: Orkflows applies its
		 * page size on change and puts the control here, while on Runs page
		 * size is a field of the filter form and this row has none. Either way
		 * this component does not know what page size is.
		 */
		children?: Snippet;
	}

	let {
		testid,
		page,
		count,
		noun,
		previousHref = undefined,
		nextHref = undefined,
		firstHref = undefined,
		onnext = undefined,
		children = undefined
	}: Props = $props();

	// The app's quiet button (the `.btn` primitive): a disabled direction is
	// an <a> with no href, so it is not a link at all, plus the
	// `aria-disabled` and `pointer-events-none opacity-50` every disabled
	// control here wears.
	const BUTTON = 'btn';
	const DISABLED = ' pointer-events-none opacity-50';

	// Page 1 that is also the last page says nothing about pages: there are
	// none to move between, so the count is the whole truth.
	const rows = $derived(`${count} ${count === 1 ? noun : `${noun}s`}`);
	const status = $derived(
		page === 1 && !nextHref ? rows : `Page ${page} · ${rows}${nextHref ? '' : ' · last page'}`
	);
	// The path back is unknown: a pasted link, a new tab, a reload in a browser
	// that refuses storage. The first page is the only earlier page this
	// browser can name, and offering it is honester than a Previous that would
	// have to invent a cursor.
	const showFirst = $derived(page > 1 && !previousHref && firstHref !== undefined);
</script>


<div class="mt-4 flex items-center justify-between gap-3" data-testid="{testid}-pagination">
	<div class="flex items-center gap-3">
		{@render children?.()}
		<span class="figure text-sm text-ink-muted" data-testid="{testid}-page-status">{status}</span>
	</div>
	<div class="flex items-center gap-2">
		{#if showFirst}<a data-testid="{testid}-first" href={firstHref} class={BUTTON}>First page</a>{/if}
		<a data-testid="{testid}-previous" aria-disabled={!previousHref} href={previousHref} class="{BUTTON}{previousHref ? '' : DISABLED}"><Icon name="chevronLeft" />Previous</a>
		<a data-testid="{testid}-next" aria-disabled={!nextHref} href={nextHref} onclick={() => onnext?.()} class="{BUTTON}{nextHref ? '' : DISABLED}">Next<Icon name="chevronRight" /></a>
	</div>
</div>
