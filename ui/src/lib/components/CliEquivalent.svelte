<script lang="ts">
	// "Here is the command that does what this page just did."
	//
	// Every one of these used to be its own shape — a <details> block, a text
	// link that copied silently, a bare paragraph — and each of them competed
	// with the primary action beside it for the reader's attention. One icon,
	// in one place, that SHOWS the command before it copies it: a reader who
	// wants the CLI finds it in the same corner every time, and a reader who
	// does not is not asked to read a shell command to use a button.
	import Icon from './Icon.svelte';
	import Popover from './Popover.svelte';
	import { copyText, MANUAL_COPY_MESSAGE, selectByTestid } from '$lib/clipboard';

	interface Props {
		command: string;
		testid: string;
		/** Names the icon button for anyone who cannot see the glyph. */
		label?: string;
		/**
		 * Words drawn beside the glyph, in the mono — the command's own name
		 * ("run logs") — where several of these sit side by side and a row of
		 * identical prompts would not say which is which.
		 */
		text?: string;
	}

	let { command, testid, label = 'CLI equivalent', text = undefined }: Props = $props();

	let open = $state(false);
	// The copy result is stated rather than animated away, the same way the run
	// page already reports its copies: a status that vanishes on a timer is one
	// a reader can miss entirely.
	let copied = $state('');

	function toggle(): void {
		open = !open;
		if (open) copied = '';
	}

	// On an origin with no clipboard the command shown above is selected
	// instead, and the status says to copy it by hand ($lib/clipboard).
	async function copy(): Promise<void> {
		if ((await copyText(command)) === 'copied') {
			copied = 'Copied';
			return;
		}
		selectByTestid(`${testid}-command`);
		copied = MANUAL_COPY_MESSAGE;
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
			class="inline-flex items-center justify-center gap-1.5 rounded-sm border border-rule-strong bg-paper text-ink-secondary transition-colors hover:bg-hover hover:text-ink {text
				? 'h-7 px-2'
				: 'size-8'}"
		>
			<!-- A shell prompt: the window, the caret, the cursor. -->
			<Icon name="terminal" />{#if text}<span class="font-mono text-xs whitespace-nowrap">{text}</span>{/if}
		</button>
	{/snippet}
	{#snippet content()}
		<pre
			data-testid="{testid}-command"
			class="max-w-md overflow-x-auto border border-rule bg-paper-sunken p-2 font-mono text-xs text-ink"><code
				>{command}</code
			></pre>
		<div class="mt-2 flex items-center gap-2">
			<button
				type="button"
				data-testid="{testid}-copy"
				onclick={() => void copy()}
				class="btn btn-sm"
			>
				Copy command
			</button>
			{#if copied}
				<span data-testid="{testid}-copied" class="text-xs text-ink-muted">{copied}</span>
			{/if}
		</div>
	{/snippet}
</Popover>
