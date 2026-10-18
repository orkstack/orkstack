<script lang="ts">
	// The persistent row along the bottom edge: what this page is, said the
	// way a drawing's title block says it. Each cell stacks a tracked-caps
	// label over its value; STATUS writes the status word in its status ink
	// after its drawn mark. Below 640 px the first two cells stay and the rest
	// fold into "Page details".
	//
	// It never shows a run id: a run's id is its page's heading, and a second
	// copy of it on a list page would make the id ambiguous to find.
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { statusLabel } from '$lib/status';
	import { statusMark } from '$lib/statusMarks';
	import { titleCellTestid, type TitleField } from '$lib/sheet';
	import { copyText } from '$lib/clipboard';
	import Icon from './Icon.svelte';

	let { fields, lifted = false }: { fields: TitleField[]; lifted?: boolean } = $props();

	// Below 640 px: the first cell and STATUS (or the second cell) stay.
	const kept = $derived.by(() => {
		const status = fields.findIndex((f) => f.status !== undefined);
		return new Set([0, status > 0 ? status : 1]);
	});
	const folded = $derived(fields.filter((_, i) => !kept.has(i)));

	let copied = $state('');
	async function copyValue(f: TitleField): Promise<void> {
		if (f.copy && (await copyText(f.copy)) === 'copied') copied = f.label;
	}

	function statusInk(status: Status | undefined): string {
		if (status === Status.FAILED) return 'text-red';
		if (status === Status.RUNNING || status === Status.AWAITING) return 'text-blue';
		return 'text-ink';
	}
</script>

{#snippet value(f: TitleField)}
	{#if f.status !== undefined}
		{@const mark = statusMark(f.status)}
		<span class="inline-flex items-center gap-2 {statusInk(f.status)}">
			<span class="stamp border-transparent bg-transparent p-0 {mark.inner ? '' : mark.className}" aria-hidden="true"
				>{#if mark.inner}<span class={mark.className}></span>{/if}</span
			>{statusLabel(f.status)}
		</span>
	{:else if f.href}
		<a href={f.href} class="link {f.mono ? 'font-mono' : ''}" title={f.title}>{f.value}</a>
	{:else}
		<span class={f.mono ? 'font-mono' : ''} title={f.title}>{f.value}</span>
	{/if}
	{#if f.copy}
		<button
			type="button"
			data-testid="{titleCellTestid(f.label)}-copy"
			class="btn-icon -my-1 inline-flex size-6 items-center justify-center align-middle"
			aria-label={copied === f.label ? `Copied the ${f.label.toLowerCase()}` : `Copy the whole ${f.label.toLowerCase()}`}
			title={copied === f.label ? 'Copied' : f.copy}
			onclick={() => void copyValue(f)}><Icon name={copied === f.label ? 'check' : 'copy'} /></button
		>
	{/if}
{/snippet}

<footer
	aria-label="Page summary"
	data-testid="title-block"
	class="sticky bottom-0 z-30 border-t-2 border-ink bg-paper transition-shadow {lifted ? 'shadow-lift' : ''}"
>
	<div
		class="grid h-titleblock grid-cols-[2fr_repeat(var(--_cells),1fr)] max-sm:grid-cols-[minmax(0,1fr)_fit-content(45%)_auto]"
		style:--_cells={Math.max(fields.length - 1, 1)}
	>
		{#each fields as f, i (f.label)}
			<dl
				data-testid={titleCellTestid(f.label)}
				class="flex min-w-0 flex-col justify-center border-r border-ink px-3 last:border-r-0 sm:px-4 {kept.has(i) ? '' : 'max-sm:hidden'}"
			>
				<dt class="caps text-ink-muted">{f.label}</dt>
				<dd class="truncate text-lg font-semibold">{@render value(f)}</dd>
			</dl>
		{/each}
		{#if folded.length}
			<details class="relative flex items-center px-3 sm:hidden">
				<summary class="caps cursor-pointer text-ink-secondary">Page details</summary>
				<div class="absolute right-0 bottom-full mb-0.5 w-64 border border-ink bg-paper p-3 shadow-overlay">
					{#each folded as f (f.label)}
						<dl class="py-1">
							<dt class="caps text-ink-muted">{f.label}</dt>
							<dd class="truncate text-md font-semibold">{@render value(f)}</dd>
						</dl>
					{/each}
				</div>
			</details>
		{/if}
	</div>
</footer>
