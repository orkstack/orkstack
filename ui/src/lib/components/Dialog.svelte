<script lang="ts">
	// The modal shell: a native modal <dialog> (showModal), so the page behind
	// is inert, Tab stays inside and the browser answers Escape; a header with
	// a glyph, a title and a way out; whatever the caller renders as its body.
	//
	// It does not toggle itself — the caller renders it inside `{#if}` and
	// owns the state that decides. Focus returns to whatever opened it.
	import { onMount, type Snippet } from 'svelte';
	import type { IconName } from '#lib/icons.ts';
	import Icon from './Icon.svelte';

	interface Props {
		title: string;
		testid: string;
		onclose: () => void;
		body: Snippet;
		/** Wider than ConfirmDialog's question when the body is a form. */
		widthClass?: string;
		/** The glyph in the header. */
		glyph?: IconName;
	}

	let { title, testid, onclose, body, widthClass = 'max-w-lg', glyph = 'info' }: Props = $props();

	let dialog = $state<HTMLDialogElement>();
	onMount(() => {
		const opener = document.activeElement as HTMLElement | null;
		dialog?.showModal();
		return () => {
			if (opener?.isConnected) opener.focus();
			else document.getElementById('content')?.focus();
		};
	});

	function cancel(event: Event): void {
		event.preventDefault();
		onclose();
	}

	// Only a press that started AND ended on the backdrop closes: a text
	// selection released over it never discards the form.
	let downOnBackdrop = false;
	function backdrop(event: MouseEvent): void {
		if (event.target === dialog && downOnBackdrop) onclose();
		downOnBackdrop = false;
	}
</script>

<dialog
	bind:this={dialog}
	data-testid={testid}
	data-overlay
	aria-label={title}
	oncancel={cancel}
	onpointerdown={(e) => (downOnBackdrop = e.target === dialog)}
	onclick={backdrop}
	class="m-auto flex max-h-[85vh] w-[calc(100%-2rem)] {widthClass} flex-col overflow-hidden rounded-md border border-line-2 bg-surface p-0 text-ink shadow-overlay"
>
	<header class="flex shrink-0 items-center gap-2.5 border-b border-line px-4.5 py-3">
		<Icon name={glyph} class="size-4 text-ink-2" />
		<h2 class="min-w-0 flex-1 text-md font-heading text-ink">{title}</h2>
		<button type="button" data-testid="{testid}-close" aria-label="Close" onclick={() => onclose()} class="btn btn-quiet btn-icon btn-sm -mr-1.5">
			<Icon name="close" />
		</button>
	</header>
	<!--
		The panel's own scroll, not the page's: a dialog has no other way to
		reach content taller than the viewport.
	-->
	<div class="scroll-panel overflow-y-auto px-4.5 py-4 text-dense text-ink-2">{@render body()}</div>
</dialog>
