<script lang="ts">
	// A question with two answers, as a native modal <dialog>. A destructive
	// one (Cancel run…, Cordon…, a delete) names its safe choice on the back
	// button ("Keep running", "Back"), and its confirm is solid ink with the
	// same glyph as the trigger — never red: red is a status colour only. The
	// back button takes the focus first, so Enter never destroys by accident.
	// A required value in the body holds the confirm aria-disabled, with its
	// reason; Escape and the back button both answer "no".
	import { onMount, type Snippet } from 'svelte';
	import type { IconName } from '#lib/icons.ts';
	import Icon from './Icon.svelte';

	interface Props {
		title: string;
		testid: string;
		acceptLabel?: string;
		/** The back button's words: the safe choice. */
		cancelLabel?: string;
		acceptTestid: string;
		cancelTestid: string;
		busy?: boolean;
		error?: string;
		/** The destructive recipe: solid ink confirm, focus on the back button. */
		destructive?: boolean;
		/** Holds the confirm aria-disabled while the body still needs a value. */
		acceptDisabled?: boolean;
		/** Why the confirm is held (shown and wired to it), when it is. */
		acceptDisabledReason?: string;
		/** The glyph in the header and on the confirm. */
		glyph?: IconName;
		body: Snippet;
		onaccept: () => void;
		oncancel: () => void;
	}

	let {
		title,
		testid,
		acceptLabel = 'Confirm',
		cancelLabel = 'Cancel',
		acceptTestid,
		cancelTestid,
		busy = false,
		error = '',
		destructive = false,
		acceptDisabled = false,
		acceptDisabledReason = '',
		glyph,
		body,
		onaccept,
		oncancel
	}: Props = $props();

	let dialog = $state<HTMLDialogElement>();
	let back = $state<HTMLButtonElement>();
	onMount(() => {
		const opener = document.activeElement as HTMLElement | null;
		dialog?.showModal();
		if (destructive) back?.focus();
		return () => {
			// The opener may be gone (Cordon… became Uncordon, a run ended):
			// focus then goes to the page's main region, never to nothing.
			if (opener?.isConnected) opener.focus();
			else document.getElementById('content')?.focus();
		};
	});

	const held = $derived(busy || acceptDisabled);

	function accept(): void {
		if (held) return;
		onaccept();
	}

	function cancel(event: Event): void {
		event.preventDefault();
		if (!busy) oncancel();
	}
</script>

<dialog
	bind:this={dialog}
	data-testid={testid}
	data-overlay
	aria-label={title}
	oncancel={cancel}
	class="m-auto flex max-h-[calc(100dvh-2rem)] w-[calc(100%-2rem)] max-w-[30rem] flex-col overflow-hidden rounded-md border border-line-2 bg-surface p-0 text-ink shadow-overlay"
>
	<header class="flex shrink-0 items-center gap-2.5 border-b border-line px-4.5 py-3">
		{#if glyph}<Icon name={glyph} class="size-4 text-ink-2" />{/if}
		<h2 class="min-w-0 flex-1 text-md font-heading text-ink">{title}</h2>
	</header>
	<div class="scroll-panel min-h-0 overflow-y-auto">
		<div class="grid gap-2.5 px-4.5 pt-4 text-base text-ink-2">{@render body()}</div>
		{#if error}
			<p data-testid="{testid}-error" role="alert" class="severity-error mx-4.5 mt-3 text-dense font-semibold">{error}</p>
		{/if}
		{#if acceptDisabled && acceptDisabledReason}
			<p id="{testid}-held" class="mx-4.5 mt-3 text-sm text-ink-3">{acceptDisabledReason}</p>
		{/if}
	</div>
	<footer class="flex shrink-0 flex-wrap justify-end gap-2 px-4.5 pt-4 pb-4">
		<button bind:this={back} type="button" data-testid={cancelTestid} data-confirm-cancel disabled={busy} onclick={() => oncancel()} class="btn">
			{cancelLabel}
		</button>
		<button
			type="button"
			data-testid={acceptTestid}
			aria-disabled={held ? 'true' : undefined}
			aria-describedby={acceptDisabled && acceptDisabledReason ? `${testid}-held` : undefined}
			onclick={accept}
			class="btn {destructive ? 'btn-destroy' : 'btn-primary'}"
		>
			{#if glyph && !busy}<Icon name={glyph} />{/if}{busy ? 'Working…' : acceptLabel}
		</button>
	</footer>
</dialog>
