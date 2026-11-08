<script lang="ts">
	// The fields of a schedule, one markup for the create form and the edit
	// dialog so the two cannot disagree about what a schedule holds. `prefix`
	// names each control's id and test id; `leading` is the first grid cell,
	// the name a create asks for or the Orkflow an edit cannot change.
	import type { Snippet } from 'svelte';
	import Select from '#lib/components/Select.svelte';
	import { CURRENT_VERSION, OVERLAP_POLICY_SLUGS, type ScheduleForm } from '#lib/schedules.ts';

	interface Props {
		form: ScheduleForm;
		prefix: string;
		versionOptions: { value: string; label: string }[];
		disabled: boolean;
		advancedOpen?: boolean;
		leading: Snippet;
	}

	let { form = $bindable(), prefix, versionOptions, disabled, advancedOpen = false, leading }: Props = $props();

	const overlapOptions = OVERLAP_POLICY_SLUGS.map((slug) => ({ value: slug, label: slug }));
	const inputClass = 'field h-9 w-full px-3 text-sm';
</script>

<div class="grid gap-4 sm:grid-cols-2">
	<div class="flex flex-col gap-1.5">{@render leading()}</div>
	<div class="flex flex-col gap-1.5">
		<label for="{prefix}-version" class="text-sm font-medium text-ink">Version</label>
		<Select
			id="{prefix}-version"
			testid="{prefix}-version"
			value={form.version || CURRENT_VERSION}
			options={versionOptions}
			{disabled}
			onchange={(next) => (form.version = next === CURRENT_VERSION ? '' : next)}
		/>
	</div>
	<div class="flex flex-col gap-1.5">
		<label for="{prefix}-cron" class="text-sm font-medium text-ink">
			Cron <span class="font-normal text-ink-muted">(one expression per line)</span>
		</label>
		<textarea
			id="{prefix}-cron"
			data-testid="{prefix}-cron"
			bind:value={form.cron}
			rows="2"
			placeholder="0 2 * * *"
			class="field w-full px-3 py-2 font-mono text-sm"
		></textarea>
	</div>
	<div class="flex flex-col gap-1.5">
		<label for="{prefix}-interval" class="text-sm font-medium text-ink">
			Interval <span class="font-normal text-ink-muted">(optional, e.g. 15m)</span>
		</label>
		<input id="{prefix}-interval" data-testid="{prefix}-interval" bind:value={form.interval} class={inputClass} />
	</div>
	<div class="flex flex-col gap-1.5">
		<label for="{prefix}-timezone" class="text-sm font-medium text-ink">
			Time zone <span class="font-normal text-ink-muted">(IANA; empty is UTC)</span>
		</label>
		<input
			id="{prefix}-timezone"
			data-testid="{prefix}-timezone"
			bind:value={form.timezone}
			placeholder="Europe/Lisbon"
			class={inputClass}
		/>
	</div>
	<div class="flex flex-col gap-1.5">
		<label for="{prefix}-overlap" class="text-sm font-medium text-ink">
			When the previous run is still going
		</label>
		<Select
			id="{prefix}-overlap"
			testid="{prefix}-overlap"
			value={form.overlap}
			options={overlapOptions}
			{disabled}
			onchange={(next) => (form.overlap = next)}
		/>
	</div>
</div>

<details class="border-y border-rule py-2" open={advancedOpen}>
	<summary class="cursor-pointer text-sm font-semibold text-ink marker:text-ink-muted hover:text-blue">Advanced</summary>
	<div class="mt-3 grid gap-4 sm:grid-cols-2">
		<div class="flex flex-col gap-1.5">
			<label for="{prefix}-jitter" class="text-sm font-medium text-ink">Jitter</label>
			<input id="{prefix}-jitter" data-testid="{prefix}-jitter" bind:value={form.jitter} placeholder="30s" class={inputClass} />
		</div>
		<div class="flex flex-col gap-1.5">
			<label for="{prefix}-catchup-window" class="text-sm font-medium text-ink">Catch-up window</label>
			<input
				id="{prefix}-catchup-window"
				data-testid="{prefix}-catchup-window"
				bind:value={form.catchupWindow}
				placeholder="1h (at least 10s)"
				class={inputClass}
			/>
		</div>
		<div class="flex flex-col gap-1.5">
			<label for="{prefix}-start-at" class="text-sm font-medium text-ink">Start at (RFC 3339)</label>
			<input
				id="{prefix}-start-at"
				data-testid="{prefix}-start-at"
				bind:value={form.startAt}
				placeholder="2026-10-01T00:00:00Z"
				class={inputClass}
			/>
		</div>
		<div class="flex flex-col gap-1.5">
			<label for="{prefix}-end-at" class="text-sm font-medium text-ink">End at (RFC 3339)</label>
			<input
				id="{prefix}-end-at"
				data-testid="{prefix}-end-at"
				bind:value={form.endAt}
				placeholder="2026-12-31T23:59:59Z"
				class={inputClass}
			/>
		</div>
		<div class="flex flex-col gap-1.5 sm:col-span-2">
			<label for="{prefix}-description" class="text-sm font-medium text-ink">Description</label>
			<input id="{prefix}-description" data-testid="{prefix}-description" bind:value={form.description} class={inputClass} />
		</div>
		<div class="flex flex-col gap-1.5 sm:col-span-2">
			<label for="{prefix}-input" class="text-sm font-medium text-ink">
				Input <span class="font-normal text-ink-muted">(a JSON object, submitted as given at every fire)</span>
			</label>
			<textarea
				id="{prefix}-input"
				data-testid="{prefix}-input"
				bind:value={form.input}
				rows="3"
				placeholder={'{"mode": "full"}'}
				class="field w-full px-3 py-2 font-mono text-sm"
			></textarea>
		</div>
		<label class="flex items-center gap-2 text-sm text-ink">
			<input type="checkbox" data-testid="{prefix}-pause-on-failure" bind:checked={form.pauseOnFailure} />
			Pause the schedule when a fire fails or is refused
		</label>
	</div>
</details>
