<script lang="ts" module>
	export interface CliCommand {
		/** What it does, in words: "Watch this run". */
		label: string;
		/** The orkctl command line, every one of which exists in docs/CLI.md. */
		command: string;
	}
</script>

<script lang="ts">
	// The command line for a screen, only behind the terminal icon (brief
	// §6.4): a popover listing this screen's orkctl commands, each with a copy
	// icon, which turns to a check once it has copied.
	// It closes on Escape and when focus leaves it.
	import { tick } from 'svelte';
	import Icon from './Icon.svelte';
	import Popover from './Popover.svelte';
	import { copyText, MANUAL_COPY_MESSAGE, selectByTestid } from '#lib/clipboard.ts';

	interface Props {
		commands: CliCommand[];
		testid: string;
		/** The popover's heading. */
		title?: string;
	}

	let { commands, testid, title = 'Command line for this page' }: Props = $props();

	let open = $state(false);
	let status = $state('');
	let copiedAt = $state(-1);

	async function copy(i: number): Promise<void> {
		// Emptied first, so a second copy of one command is announced too.
		status = '';
		copiedAt = -1;
		await tick();
		if ((await copyText(commands[i].command)) === 'copied') {
			status = `Copied: ${commands[i].label}`;
			copiedAt = i;
			return;
		}
		selectByTestid(`${testid}-command-${i}`);
		status = MANUAL_COPY_MESSAGE;
	}
</script>

<Popover {open} onOpenChange={(next) => (open = next)} testid="{testid}-popover" align="end">
	{#snippet trigger()}
		<button
			type="button"
			class="btn btn-icon"
			aria-label="Command line for this page"
			aria-haspopup="dialog"
			aria-expanded={open}
			data-tip="Command line"
			data-testid="{testid}-trigger"
			onclick={() => {
				open = !open;
				status = '';
				copiedAt = -1;
			}}><Icon name="term" /></button
		>
	{/snippet}
	{#snippet content()}
		<div role="dialog" aria-label={title} class="grid w-[min(35rem,calc(100vw-2rem))] gap-3 text-left">
			<div>
				<h2 class="text-md font-heading">{title}</h2>
				<p class="text-dense text-ink-3">The same, from orkctl.</p>
			</div>
			{#each commands as cmd, i (cmd.command)}
				<div class="grid gap-1">
					<span class="text-dense font-semibold text-ink-2">{cmd.label}</span>
					<div class="flex items-start gap-2">
						<pre
							data-testid="{testid}-command-{i}"
							class="min-w-0 flex-1 rounded-sm border border-line bg-surface-2 px-2.5 py-1.5 font-mono text-xs leading-normal break-words whitespace-pre-wrap text-ink">{cmd.command}</pre>
						<button type="button" class="btn btn-sm btn-icon" aria-label="Copy: {cmd.label}" data-tip="Copy" data-testid="{testid}-copy-{i}" onclick={() => void copy(i)}
							><Icon name={copiedAt === i ? 'apply' : 'copy'} /></button
						>
					</div>
				</div>
			{/each}
			<p class="min-h-5 text-dense text-ok" role="status" data-testid="{testid}-status">{status}</p>
		</div>
	{/snippet}
</Popover>
