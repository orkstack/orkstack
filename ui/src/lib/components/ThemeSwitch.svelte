<script lang="ts">
	// Appearance: follow the system, or choose light or dark. Three pressed
	// buttons in one group; the choice is shared with the drawer and the
	// palette through $lib/theme.svelte.ts.
	import Icon from './Icon.svelte';
	import { appearance } from '$lib/theme.svelte';
	import type { Theme } from '$lib/theme';
	import type { IconName } from '$lib/icons';

	const choices: { theme: Theme; icon: IconName; label: string; testid: string }[] = [
		{ theme: 'system', icon: 'monitor', label: 'Use system appearance', testid: 'theme-system' },
		{ theme: 'light', icon: 'sun', label: 'Light appearance', testid: 'theme-light' },
		{ theme: 'dark', icon: 'moon', label: 'Dark appearance', testid: 'theme-dark' }
	];
</script>

<div role="group" aria-label="Appearance" class="inline-flex items-center rounded-sm border border-rule-strong">
	{#each choices as c (c.theme)}
		<button
			type="button"
			data-testid={c.testid}
			aria-label={c.label}
			title={c.label}
			aria-pressed={appearance.choice === c.theme}
			onclick={() => appearance.set(c.theme)}
			class="inline-flex size-7 items-center justify-center text-ink-muted transition-colors not-first:border-l not-first:border-rule hover:text-ink aria-pressed:bg-ink aria-pressed:text-on-ink"
		>
			<Icon name={c.icon} class="size-3.5" />
		</button>
	{/each}
</div>
