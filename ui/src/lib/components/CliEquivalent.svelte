<script lang="ts">
	// "Here is the command that does what this page just did."
	//
	// Every one of these used to be its own shape — a <details> block, a text
	// link that copied silently, a bare paragraph — and each of them competed
	// with the primary action beside it for the reader's attention. One icon,
	// in one place, that SHOWS the command before it copies it: a reader who
	// wants the CLI finds it in the same corner every time, and a reader who
	// does not is not asked to read a shell command to use a button.
	import Popover from './Popover.svelte';

	interface Props {
		command: string;
		testid: string;
		/** Names the icon button for anyone who cannot see the glyph. */
		label?: string;
	}

	let { command, testid, label = 'CLI equivalent' }: Props = $props();

	let open = $state(false);
	// The copy result is stated rather than animated away, the same way the run
	// page already reports its copies: a status that vanishes on a timer is one
	// a reader can miss entirely.
	let copied = $state('');

	function toggle(): void {
		open = !open;
		if (open) copied = '';
	}

	function copy(): void {
		void navigator.clipboard?.writeText(command).then(
			() => (copied = 'Copied'),
			() => (copied = 'Copy failed')
		);
	}
</script>

<Popover
	{open}
	onOpenChange={(next) => (open = next)}
	testid="{testid}-popover"
	align="end"
>
	{#snippet trigger()}
		<button
			type="button"
			aria-label={label}
			aria-expanded={open}
			data-testid="{testid}-trigger"
			onclick={toggle}
			class="inline-flex h-8 w-8 items-center justify-center rounded-md border border-border bg-surface text-fg-muted shadow-xs transition-colors duration-150 hover:bg-surface-hover hover:text-fg focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none"
		>
			<!-- A shell prompt: the window, the caret, the cursor. -->
			<svg
				viewBox="0 0 16 16"
				fill="none"
				stroke="currentColor"
				stroke-width="1.4"
				stroke-linecap="round"
				stroke-linejoin="round"
				aria-hidden="true"
				class="size-4"
			>
				<rect x="1.5" y="2.5" width="13" height="11" rx="1.5" />
				<path d="M4.5 6.25 6.75 8 4.5 9.75" />
				<path d="M8.5 10.25h3" />
			</svg>
		</button>
	{/snippet}
	{#snippet content()}
		<pre
			data-testid="{testid}-command"
			class="max-w-md overflow-x-auto rounded bg-surface-sunken p-2 font-mono text-xs text-fg"><code
				>{command}</code
			></pre>
		<div class="mt-2 flex items-center gap-2">
			<button
				type="button"
				data-testid="{testid}-copy"
				onclick={copy}
				class="inline-flex h-8 items-center justify-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none"
			>
				Copy command
			</button>
			{#if copied}
				<span data-testid="{testid}-copied" class="text-xs text-fg-muted">{copied}</span>
			{/if}
		</div>
	{/snippet}
</Popover>
