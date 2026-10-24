<script lang="ts">
	// The menu below 1024 px: a native modal <dialog> (focus held inside, the
	// page behind inert, Escape closes it) holding the Primary navigation, the
	// Operate pages, the namespace and the appearance. The caller renders it
	// only while open, so its Primary nav and namespace picker are never a
	// second copy of the top bar's.
	import type { Snippet } from 'svelte';
	import Icon from './Icon.svelte';
	import { BUILD_PAGES, OPERATE_PAGES } from '$lib/navigation';

	interface Props {
		pathname: string;
		link: (path: string) => string;
		onclose: () => void;
		picker: Snippet;
		themeSwitch: Snippet;
	}

	let { pathname, link, onclose, picker, themeSwitch }: Props = $props();

	let dialog = $state<HTMLDialogElement | undefined>(undefined);
	const opener = typeof document === 'undefined' ? null : (document.activeElement as HTMLElement | null);

	$effect(() => {
		dialog?.showModal();
		return () => opener?.focus?.();
	});

	function current(href: string): 'page' | undefined {
		if (href === '/' || href === '/admin') return pathname === href ? 'page' : undefined;
		return pathname === href || pathname.startsWith(href + '/') ? 'page' : undefined;
	}

	const ITEM =
		'flex h-9 items-center px-3 text-sm font-medium text-ink-secondary hover:bg-hover hover:text-ink current:font-semibold current:text-ink current:bg-blue-tint';
</script>

<dialog
	bind:this={dialog}
	data-overlay
	data-testid="menu-drawer"
	aria-label="Menu"
	oncancel={(e) => {
		e.preventDefault();
		onclose();
	}}
	onclick={(e) => {
		if (e.target === dialog) onclose();
	}}
	class="m-0 ml-auto h-dvh max-h-dvh w-80 max-w-[calc(100vw-3rem)] border-l-(length:--rule-region) border-ink bg-paper p-0 text-ink shadow-overlay backdrop:bg-scrim"
>
	<div class="flex h-full flex-col">
		<div class="flex h-bar items-center justify-between border-b border-rule px-gutter">
			<span class="caps text-ink-muted">Menu</span>
			<button type="button" class="btn btn-icon" aria-label="Close menu" onclick={onclose}>
				<Icon name="close" />
			</button>
		</div>
		<div class="flex-1 overflow-y-auto py-3" role="presentation" onclick={(e) => (e.target as HTMLElement).closest('a') && onclose()}>
			<nav aria-label="Primary">
				<p class="caps px-3 pb-1 text-ink-muted">Build</p>
				{#each BUILD_PAGES as p (p.id)}
					<a
						href={link(p.href)}
						data-testid={p.id === 'overview' ? 'nav-dashboard' : undefined}
						aria-current={current(p.href)}
						class={ITEM}
						>{#if p.id === 'apply'}Apply<span class="sr-only"> Orkflow</span>{:else}{p.label}{/if}</a
					>
				{/each}
				<p class="caps mt-4 px-3 pb-1 text-ink-muted">Operate</p>
				{#each OPERATE_PAGES as p (p.id)}
					<a
						href={link(p.href)}
						data-testid={p.id === 'overview' ? 'nav-admin' : undefined}
						aria-current={current(p.href)}
						class={ITEM}>{p.label}</a
					>
				{/each}
			</nav>
		</div>
		<div class="space-y-4 border-t border-rule p-gutter">
			<div>
				<p class="caps mb-1.5 text-ink-muted" aria-hidden="true">Namespace</p>
				{@render picker()}
			</div>
			<div class="flex items-center justify-between">
				<p class="caps text-ink-muted" aria-hidden="true">Appearance</p>
				{@render themeSwitch()}
			</div>
		</div>
	</div>
</dialog>
