<script lang="ts">
	// The dropdown this app uses everywhere it used to use a native <select>.
	//
	// A native select cannot be styled to match the rest of these controls —
	// the popup is the operating system's, not the page's — so every one of
	// them read as a foreign object in a form built out of the token set. This
	// is the same control, rebuilt out of a button and a panel: the button
	// carries combobox semantics, the panel is a listbox of options, and the
	// keyboard does what a select's keyboard does (arrows to move, Home/End to
	// jump, type-ahead to find, Enter to take, Escape to leave it alone).
	//
	// The reader's KEY IDEA to keep: moving the highlight is not choosing.
	// Arrowing through a native select on a form commits as it moves on some
	// platforms and not on others; here it never does. `onchange` fires on
	// exactly two gestures — Enter/Space on a highlighted row, or a click on a
	// row — and Escape is guaranteed to leave the value it found.
	import { dismissOnOutsideOrEscape } from '$lib/dismiss';

	interface Option {
		value: string;
		label: string;
	}

	interface Props {
		value: string;
		options: Option[];
		onchange: (value: string) => void;
		disabled?: boolean;
		testid: string;
		/** Set when a sibling <label for=...> names this field. */
		id?: string;
		/** 'console' is the dark log-pane surface, where this sits among 11px mono chrome. */
		variant?: 'default' | 'console';
	}

	let {
		value,
		options,
		onchange,
		disabled = false,
		testid,
		id = undefined,
		variant = 'default'
	}: Props = $props();

	let open = $state(false);
	// highlighted is where the keyboard IS, which is not what the field holds:
	// -1 means nothing is highlighted yet. Committing is what moves `value`.
	let highlighted = $state(-1);
	let trigger = $state<HTMLButtonElement | undefined>(undefined);
	let panel = $state<HTMLDivElement | undefined>(undefined);

	// Type-ahead state is deliberately not reactive: nothing renders from it,
	// and a keystroke buffer that invalidated the panel on every letter would
	// re-render the list for no visible reason.
	let typed = '';
	let typedAt = 0;
	const TYPEAHEAD_RESET_MS = 500;

	const isConsole = $derived(variant === 'console');
	const selectedIndex = $derived(options.findIndex((option) => option.value === value));
	// A value with no matching option shows itself rather than a blank button:
	// a namespace pinned to a sink that has since left the list is a thing the
	// operator needs to SEE, not a control that looks unset.
	const selectedLabel = $derived(selectedIndex >= 0 ? options[selectedIndex].label : value);
	const listboxId = $derived(`${testid}-listbox`);
	const activeOptionId = $derived(
		open && highlighted >= 0 && highlighted < options.length
			? `${testid}-option-${options[highlighted].value}`
			: undefined
	);

	function openPanel(at?: number): void {
		if (disabled) return;
		open = true;
		highlighted = at ?? (selectedIndex >= 0 ? selectedIndex : 0);
	}

	function closePanel(): void {
		open = false;
		highlighted = -1;
		typed = '';
	}

	// commit is the only path to `onchange`. Re-picking what is already
	// selected is not a change, which is what a native select does too.
	function commit(index: number): void {
		const option = options[index];
		closePanel();
		trigger?.focus();
		if (option && option.value !== value) onchange(option.value);
	}

	function move(to: number): void {
		if (options.length === 0) return;
		highlighted = Math.min(Math.max(to, 0), options.length - 1);
	}

	// Type-ahead searches from the row AFTER the highlight for a fresh buffer
	// (so pressing "e" twice walks the e's) and from the highlight itself while
	// a buffer is being extended (so "en" can still land on the row "e" found).
	function typeahead(char: string): void {
		if (options.length === 0) return;
		const now = Date.now();
		typed = now - typedAt > TYPEAHEAD_RESET_MS ? char : typed + char;
		typedAt = now;
		const needle = typed.toLocaleLowerCase();
		const from = highlighted >= 0 ? highlighted : selectedIndex >= 0 ? selectedIndex : 0;
		const start = typed.length > 1 ? from : from + 1;
		for (let step = 0; step < options.length; step += 1) {
			const index = (start + step + options.length) % options.length;
			if (options[index].label.toLocaleLowerCase().startsWith(needle)) {
				if (!open) openPanel(index);
				else highlighted = index;
				return;
			}
		}
	}

	function onKeydown(event: KeyboardEvent): void {
		if (disabled) return;
		const key = event.key;
		if (!open) {
			if (key === 'ArrowDown' || key === 'ArrowUp' || key === 'Enter' || key === ' ') {
				event.preventDefault();
				openPanel(key === 'ArrowUp' ? options.length - 1 : undefined);
				return;
			}
		} else {
			switch (key) {
				case 'ArrowDown':
					event.preventDefault();
					move(highlighted + 1);
					return;
				case 'ArrowUp':
					event.preventDefault();
					move(highlighted <= 0 ? 0 : highlighted - 1);
					return;
				case 'Home':
					event.preventDefault();
					move(0);
					return;
				case 'End':
					event.preventDefault();
					move(options.length - 1);
					return;
				case 'Enter':
				case ' ':
					event.preventDefault();
					commit(highlighted >= 0 ? highlighted : selectedIndex);
					return;
				case 'Escape':
					event.preventDefault();
					// Stopped here on purpose: this press closed THIS panel and
					// has nothing left to say to a dialog around it.
					event.stopPropagation();
					closePanel();
					trigger?.focus();
					return;
				case 'Tab':
					// Moving on is not choosing either.
					closePanel();
					return;
			}
		}
		if (key.length === 1 && !event.metaKey && !event.ctrlKey && !event.altKey) {
			event.preventDefault();
			typeahead(key);
		}
	}

	// Two states, told apart on purpose: the SELECTED row is what the field
	// holds, the HIGHLIGHTED row is where the keyboard (or the pointer) is
	// hovering without having taken it. A control that painted them the same
	// would make arrowing look like it had already changed the value.
	function optionClass(index: number, option: Option): string {
		const selected = option.value === value;
		const active = index === highlighted;
		if (isConsole) {
			const tone = selected
				? active
					? 'bg-white/20 text-console-fg'
					: 'bg-white/10 text-console-fg'
				: active
					? 'bg-white/5 text-console-fg'
					: 'text-console-fg';
			return `block w-full cursor-pointer px-1.5 py-1 text-left text-[11px] ${tone}`;
		}
		const tone = selected
			? active
				? 'bg-brand-100 text-brand-700'
				: 'bg-brand-50 text-brand-700'
			: active
				? 'bg-surface-hover text-fg'
				: 'text-fg';
		return `block w-full cursor-pointer px-3 py-1.5 text-left text-sm ${tone}`;
	}

	// Keep the highlight on screen in a list longer than the panel.
	$effect(() => {
		if (!open || highlighted < 0 || !panel) return;
		panel
			.querySelector<HTMLElement>(`[data-index="${highlighted}"]`)
			?.scrollIntoView({ block: 'nearest' });
	});
</script>

<div
	class="relative {isConsole ? 'inline-block' : 'block w-full'}"
	use:dismissOnOutsideOrEscape={() => {
		if (open) closePanel();
	}}
>
	<button
		bind:this={trigger}
		{id}
		type="button"
		role="combobox"
		aria-haspopup="listbox"
		aria-expanded={open}
		aria-controls={listboxId}
		aria-activedescendant={activeOptionId}
		data-testid={testid}
		{disabled}
		onclick={() => (open ? closePanel() : openPanel())}
		onkeydown={onKeydown}
		class={isConsole
			? 'inline-flex h-6 w-full items-center justify-between gap-1 rounded-sm border border-console-border bg-console-bg px-1.5 text-left text-[11px] text-console-fg hover:border-console-muted focus-visible:border-brand-500 focus-visible:ring-2 focus-visible:ring-brand-500/25 focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50'
			: 'inline-flex h-9 w-full items-center justify-between gap-2 rounded-md border border-border bg-surface px-3 text-left text-sm text-fg shadow-xs hover:border-border-strong focus-visible:border-brand-500 focus-visible:ring-2 focus-visible:ring-brand-500/25 focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50'}
	>
		<span class="truncate">{selectedLabel}</span>
		<svg
			viewBox="0 0 20 20"
			fill="none"
			stroke="currentColor"
			stroke-width="1.5"
			stroke-linecap="round"
			stroke-linejoin="round"
			aria-hidden="true"
			class="shrink-0 {isConsole ? 'size-3 text-console-muted' : 'size-4 text-fg-muted'}"
		>
			<path d="m6 8 4 4 4-4" />
		</svg>
	</button>

	{#if open}
		<div
			bind:this={panel}
			id={listboxId}
			role="listbox"
			aria-labelledby={id}
			data-testid="{testid}-listbox"
			class={isConsole
				? 'absolute z-10 mt-1 max-h-64 w-full overflow-auto rounded-sm border border-console-border bg-console-header normal-case shadow-md'
				: 'absolute z-10 mt-1 max-h-64 w-full overflow-auto rounded-md border border-border bg-surface shadow-md'}
		>
			{#each options as option, index (option.value)}
				<button
					type="button"
					role="option"
					tabindex="-1"
					id="{testid}-option-{option.value}"
					data-testid="{testid}-option-{option.value}"
					data-index={index}
					aria-selected={option.value === value}
					onclick={() => commit(index)}
					onmousemove={() => (highlighted = index)}
					class={optionClass(index, option)}
				>
					{option.label}
				</button>
			{/each}
		</div>
	{/if}
</div>
