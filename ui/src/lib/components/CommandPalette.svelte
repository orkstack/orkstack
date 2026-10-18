<script lang="ts">
	// Ctrl/Cmd+K: jump to any page, run, Orkflow or task, and run the common
	// actions — built from standard controls. A native modal <dialog> holds a
	// combobox <input> and a listbox of grouped options; the input keeps the
	// focus the whole time and the active option is its aria-activedescendant.
	//
	// It never writes. A command that changes something opens the page's own
	// dialog, with its confirmation and its CLI equivalent; the rest navigate,
	// copy, or switch the appearance or the namespace.
	//
	// This component and the clients it searches with are loaded on first open
	// (the root layout imports it dynamically), so no page pays for them until
	// a person asks.
	import { goto } from '$app/navigation';
	import Icon from './Icon.svelte';
	import { appearance } from '$lib/theme.svelte';
	import { copyText, MANUAL_COPY_MESSAGE } from '$lib/clipboard';
	import { NEVER_SEEN, heldRuns, loadHeld } from '$lib/heldRuns';
	import {
		GROUP_ORDER,
		commandId,
		namespaceCommands,
		openRunCommand,
		pageCommands,
		themeCommands,
		type Command
	} from '$lib/palette/commands';
	import { providedCommands } from '$lib/palette/context';
	import { paletteKeydown } from '$lib/palette/keyboard';
	import { grouped, rank } from '$lib/palette/rank';
	import { readRecents } from '$lib/palette/recent';
	import { REMOTE_DEBOUNCE_MS, searchOrkflows, searchRuns } from '$lib/palette/sources';
	import { withNamespace } from '$lib/namespace';
	import { errorMessage } from '$lib/client';

	interface Props {
		namespace: string;
		namespaces: string[];
		onclose: () => void;
		onnamespace: (name: string) => void;
	}

	let { namespace, namespaces, onclose, onnamespace }: Props = $props();

	let dialog = $state<HTMLDialogElement | undefined>(undefined);
	let input = $state<HTMLInputElement | undefined>(undefined);
	let query = $state('');
	let active = $state(0);
	let remote = $state<Command[]>([]);
	let remoteError = $state('');
	let copied = $state('');

	const opener = typeof document === 'undefined' ? null : (document.activeElement as HTMLElement | null);

	$effect(() => {
		dialog?.showModal();
		input?.focus();
		return () => opener?.focus?.();
	});

	const recents = readRecents();
	const recentCommands: Command[] = recents
		.filter((r) => r.namespace === namespace)
		.map((r, i) => {
			const href = withNamespace(
				r.kind === 'run' ? `/runs/${encodeURIComponent(r.id)}` : `/orkflows/${encodeURIComponent(r.id)}`,
				r.namespace
			);
			return {
				id: `recent-${i}`,
				group: 'Recent' as const,
				label: r.kind === 'run' ? `Open run ${r.id}` : `Open Orkflow ${r.id}`,
				context: r.kind === 'run' ? r.orkflow : 'Orkflow',
				keywords: r.orkflow,
				action: { kind: 'navigate' as const, href },
				href
			};
		});
	const recency = new Map(recents.map((r, i) => [`recent-${i}`, r.at]));

	// CHANGED RUNS: the runs this browser holds a changed mark for, read from
	// its own store ($lib/heldRuns) — no request.
	const changedCommands: Command[] = $derived(heldRuns(loadHeld(), namespace)
		.slice(0, 8)
		.map((h) => {
			const href = withNamespace(`/runs/${encodeURIComponent(h.id)}`, namespace);
			return {
				id: commandId('changed', h.id),
				group: 'Changed runs' as const,
				label: `Open run ${h.id}`,
				context: h.seen === NEVER_SEEN ? `${h.now}, failed while you were away` : `${h.now}, was ${h.seen.toLowerCase()}`,
				keywords: h.orkflow,
				action: { kind: 'navigate' as const, href },
				href
			};
		}));

	const pages = $derived(pageCommands(namespace));
	const namespaceSwitches = $derived(namespaceCommands(namespaces, namespace));
	const themes = $derived(themeCommands(appearance.choice));

	// Remote search: debounced, two characters at least, cancelled by the next
	// keystroke. Each source shows its answer as it arrives, so a slow one
	// never holds back the other.
	let pending = $state(0);
	$effect(() => {
		const q = query;
		remote = [];
		remoteError = '';
		pending = 0;
		if (q.trim().length < 2) return;
		let cancelled = false;
		const timer = setTimeout(() => {
			for (const search of [searchRuns, searchOrkflows]) {
				pending++;
				search(fetch, namespace, q)
					.then((found) => {
						if (!cancelled) remote = [...remote, ...found];
					})
					.catch((err) => {
						if (!cancelled) remoteError = errorMessage(err);
					})
					.finally(() => {
						if (!cancelled) pending--;
					});
			}
		}, REMOTE_DEBOUNCE_MS);
		return () => {
			cancelled = true;
			clearTimeout(timer);
		};
	});
	const searching = $derived(pending > 0);

	const results = $derived.by(() => {
		const q = query;
		// With nothing typed: recents, the page's own commands, every page and
		// the appearance. The namespace switches wait for a word, so a
		// deployment of many namespaces does not bury the pages.
		const typed = q.trim() !== '';
		const all: Command[] = [
			...(typed ? [] : recentCommands),
			...changedCommands,
			...providedCommands(q),
			...pages,
			...themes,
			...(typed ? namespaceSwitches : []),
			...remote
		];
		const open = openRunCommand(q, namespace);
		const ranked = rank(all, q, GROUP_ORDER, recency);
		// "Open run “q”" goes after what matched by name, never before an exact page.
		return open ? [...ranked, open] : ranked;
	});
	const groups = $derived(grouped(results));
	const flat = $derived(groups.flatMap((g) => g.items));

	$effect(() => {
		void query;
		active = 0;
	});

	const optionId = (c: Command) => `command-palette-option-${c.id}`;
	const activeId = $derived(flat[active] ? optionId(flat[active]) : undefined);

	$effect(() => {
		if (!activeId) return;
		document.getElementById(activeId)?.scrollIntoView({ block: 'nearest' });
	});

	async function run(c: Command, newTab = false): Promise<void> {
		const a = c.action;
		if (a.kind === 'navigate') {
			if (newTab) {
				window.open(a.href, '_blank', 'noopener');
				return;
			}
			onclose();
			await goto(a.href);
		} else if (a.kind === 'theme') {
			appearance.set(a.theme);
			onclose();
		} else if (a.kind === 'namespace') {
			onclose();
			onnamespace(a.name);
		} else if (a.kind === 'copy') {
			const outcome = await copyText(a.text);
			copied = outcome === 'copied' ? `Copied ${a.what}` : MANUAL_COPY_MESSAGE;
		} else {
			// The palette closes BEFORE a page opens its own dialog, so the two
			// never stack.
			onclose();
			a.run();
		}
	}

	function onkeydown(event: KeyboardEvent): void {
		const r = paletteKeydown(event.key, event, active, flat.length);
		if (r.kind === 'pass') return;
		event.preventDefault();
		if (r.kind === 'move') active = r.active;
		else if (r.kind === 'run') void run(flat[r.active], r.newTab && flat[r.active].href !== undefined);
		else if (r.kind === 'close') onclose();
	}

	const status = $derived(
		(flat.length === 0 ? 'No match' : `${flat.length} result${flat.length === 1 ? '' : 's'}`) +
			(searching ? ' · searching runs and Orkflows…' : '')
	);
</script>

<dialog
	bind:this={dialog}
	data-testid="command-palette"
	aria-label="Command palette"
	oncancel={(e) => {
		e.preventDefault();
		onclose();
	}}
	onclick={(e) => {
		if (e.target === dialog) onclose();
	}}
	class="mx-auto mt-16 w-[min(40rem,calc(100vw-2rem))] sm:mt-24 max-w-none rounded-md border-(length:--rule-region) border-ink bg-sheet p-0 text-ink shadow-overlay animate-overlay-in backdrop:bg-scrim motion-reduce:animate-none"
>
	<div class="flex items-center gap-2 border-b border-rule px-3">
		<Icon name="search" class="size-4 text-ink-muted" />
		<input
			bind:this={input}
			bind:value={query}
			{onkeydown}
			data-testid="command-palette-input"
			type="text"
			role="combobox"
			aria-expanded="true"
			aria-controls="command-palette-list"
			aria-autocomplete="list"
			aria-activedescendant={activeId}
			aria-label="Jump to a page, run, Orkflow or action"
			placeholder="Jump to a page, run, Orkflow or action…"
			autocomplete="off"
			spellcheck="false"
			class="h-12 min-w-0 flex-1 bg-transparent px-2 text-md text-ink -outline-offset-2 placeholder:text-ink-placeholder"
		/>
		<button type="button" data-testid="command-palette-close" class="btn btn-icon" onclick={onclose}>
			<Icon name="close" /><span class="sr-only">Close</span>
		</button>
	</div>

	<ul
		id="command-palette-list"
		data-testid="command-palette-list"
		role="listbox"
		aria-label="Results"
		class="max-h-[min(26rem,60vh)] overflow-y-auto py-1"
	>
		{#each groups as g (g.group)}
			{@const headingId = `command-palette-group-${g.group.toLowerCase().replace(/\s+/g, '-')}-label`}
			<li role="presentation">
				<ul role="group" aria-labelledby={headingId} data-testid="command-palette-group-{g.group.toLowerCase().replace(/\s+/g, '-')}">
					<li role="presentation" id={headingId} class="caps px-3 pt-3 pb-1 text-ink-muted">{g.group}</li>
					{#each g.items as c (c.id)}
						{@const i = flat.indexOf(c)}
						<li
							id={optionId(c)}
							data-testid={optionId(c)}
							role="option"
							aria-selected={i === active}
							onclick={() => void run(c)}
							onmousemove={() => (active = i)}
							onkeydown={() => {}}
							class="flex cursor-pointer items-baseline gap-3 px-3 py-2 text-sm {i === active ? 'bg-blue-tint text-ink' : 'text-ink'}"
						>
							<span class="min-w-0 flex-1 truncate {i === active ? 'font-semibold' : ''}">{c.label}</span>
							{#if c.context}<span class="shrink-0 truncate text-xs text-ink-muted">{c.context}</span>{/if}
							{#if i === active}<kbd class="kbd shrink-0" aria-hidden="true">Enter</kbd>{/if}
						</li>
					{/each}
				</ul>
			</li>
		{/each}
	</ul>
	{#if flat.length === 0}
		<p data-testid="command-palette-empty" class="px-3 pb-4 text-sm text-ink-muted">
			{searching ? 'Searching runs and Orkflows…' : `Nothing matches “${query}”.`}
		</p>
	{/if}
	{#if remoteError}
		<p data-testid="command-palette-error" class="severity-error border-t border-rule px-3 py-2 text-xs">
			Error: could not search runs and Orkflows: {remoteError}
		</p>
	{/if}
	<div class="flex items-center justify-between gap-3 border-t border-rule px-3 py-2 text-xs text-ink-muted">
		<p data-testid="command-palette-status" role="status" aria-live="polite">{copied || status}</p>
		<p aria-hidden="true" class="max-sm:hidden"><kbd class="kbd">↑</kbd> <kbd class="kbd">↓</kbd> move · <kbd class="kbd">Enter</kbd> open · <kbd class="kbd">Esc</kbd> close</p>
	</div>
</dialog>
