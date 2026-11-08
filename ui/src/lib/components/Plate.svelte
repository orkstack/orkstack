<script lang="ts">
	// A plate: a key cell and a mono value, the one way an identifier, a
	// version or a digest is set (brief §5). A run id carries a copy button;
	// a version never does. Below 600 px a long value shows its short form;
	// the full one stays in the copy and the command line.
	import Icon from './Icon.svelte';
	import { copyText } from '#lib/clipboard.ts';
	import { onDestroy, tick } from 'svelte';
	import { shortVersion, versionTip } from '#lib/version.ts';

	interface Props {
		key: string;
		value: string;
		/** Shown in place of `value` below 600 px (a run id's `ork-…080915`). */
		short?: string;
		/** Names the copy button ("Copy run id"); no button without it. */
		copyLabel?: string;
		small?: boolean;
		testid?: string;
		/** Said by the tooltip on the value. */
		tip?: string;
		/** What the button copies, when the value shown is a short form of it. */
		copy?: string;
		/** The whole width of its card. */
		wide?: boolean;
		/** It sits inside a link, which is the tab stop. */
		inLink?: boolean;
	}

	let { key, value, short, copyLabel, small = false, testid, tip, copy: copied, wide = false, inLink = false }: Props = $props();
	// A version looks the same on every page, whoever draws its plate.
	const isVersion = $derived(key === 'version');
	const shown = $derived(isVersion ? shortVersion(value) : value);
	const tipShown = $derived(tip ?? (isVersion ? versionTip(value) : undefined));
	// What the last copy did, in a live region that is always there: a copy
	// never silently does nothing (docs/UI_CLI_PARITY.md).
	let said = $state('');
	let clear: ReturnType<typeof setTimeout> | undefined;
	onDestroy(() => clearTimeout(clear));
	async function copy(): Promise<void> {
		clearTimeout(clear);
		said = '';
		await tick();
		said = (await copyText(copied ?? value)) === 'copied' ? 'Copied' : 'The browser refused to copy; select the value and copy it by hand';
		clear = setTimeout(() => (said = ''), 2500);
	}
</script>

<span class="plate" class:plate-sm={small} class:plate-wide={wide} data-testid={testid}
	><span class="plate-k">{key}</span>{' '}<!-- A shortened version is a tab stop, so its whole value reaches a keyboard and a screen reader. --><!-- svelte-ignore a11y_no_noninteractive_tabindex --><span class="plate-v" data-tip={tipShown} tabindex={isVersion && !inLink && versionTip(value) ? 0 : undefined}
		>{#if short}<span class="full">{shown}</span><span class="short" aria-hidden="true">{short}</span>{:else}{shown}{/if}</span
	>{#if copyLabel}<button type="button" aria-label={copyLabel} data-tip={copyLabel} onclick={copy}
			><Icon name={said === 'Copied' ? 'apply' : 'copy'} /></button
		><span class={said && said !== 'Copied' ? 'plate-said' : 'sr'} role="status">{said}</span>{/if}</span
>

<style>
	.plate-wide {
		display: flex;
		width: 100%;
	}
	.plate-said {
		display: flex;
		align-items: center;
		padding: 0 8px;
		border-left: 1px solid var(--line);
		color: var(--fail);
		font-size: 12.5px;
		white-space: normal;
	}
	.short {
		display: none;
	}
	@media (max-width: 600px) {
		.full {
			position: absolute;
			width: 1px;
			height: 1px;
			overflow: hidden;
			clip-path: inset(50%);
			white-space: nowrap;
		}
		.short {
			display: inline;
		}
	}
</style>
