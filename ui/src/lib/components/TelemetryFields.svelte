<script lang="ts">
	// The three telemetry fields shared, field-for-field, between the create
	// form and each row's edit form on /admin/namespaces: the sink (a select
	// over `sinks`, or a plain text fallback when `sinksError` is set — the
	// same fallback ListTelemetrySinks failing forces on both forms), the
	// task-logs three-way select, and the attributes textarea.
	//
	// `idPrefix` carries the "ns-create" / "ns-edit" half of every
	// data-testid; `testIdSuffix` carries the "" / "-{ns.name}" half — the
	// same two halves each testid was already built from before this
	// extraction, kept verbatim so nothing that reads them (Playwright specs,
	// unit tests) has to change.
	import type { TaskLogExport } from '$lib/gen/orkstack/v1/telemetry_pb';
	import Select from './Select.svelte';

	export type TaskLogsChoice = 'inherit' | 'enabled' | 'disabled';

	interface Props {
		sink: string;
		taskLogs: TaskLogsChoice;
		attributesText: string;
		sinks: string[];
		sinksError: string | null;
		idPrefix: string;
		testIdSuffix?: string;
		attributesError?: string;
		/** Shown only on create today — the edit form's textarea has never carried this caption. */
		attributesHint?: string;
		/** The create form gives its attributes error a stable testid; the edit form's has never had one. Omit to render the error with no testid, as edit always has. */
		attributesErrorTestId?: string;
	}

	let {
		sink = $bindable(),
		taskLogs = $bindable(),
		attributesText = $bindable(),
		sinks,
		sinksError,
		idPrefix,
		testIdSuffix = '',
		attributesError = '',
		attributesHint = '',
		attributesErrorTestId = ''
	}: Props = $props();

	// The blank entry is the deployment default, and it is first for the same
	// reason it was the first <option> before: it is what a namespace that has
	// never been told otherwise already uses.
	const sinkOptions = $derived([
		{ value: '', label: '(deployment default)' },
		...sinks.map((name) => ({ value: name, label: name }))
	]);

	const taskLogsOptions = [
		{ value: 'inherit', label: 'Inherit (Orker default)' },
		{ value: 'enabled', label: 'Enabled' },
		{ value: 'disabled', label: 'Disabled' }
	];
</script>

<label class="flex flex-col gap-1.5 text-sm">
	<span class="field-label mb-0">Telemetry sink <span class="field-hint">(blank = deployment default)</span></span>
	{#if sinksError}
		<!-- ListTelemetrySinks failed; fall back to a plain text field
		     rather than blocking the rest of the form on it. -->
		<input
			data-testid="{idPrefix}-telemetry-sink{testIdSuffix}"
			bind:value={sink}
			placeholder="default"
			class="field h-9 px-3 font-mono"
		/>
	{:else}
		<Select
			testid="{idPrefix}-telemetry-sink{testIdSuffix}"
			value={sink}
			options={sinkOptions}
			onchange={(next) => (sink = next)}
		/>
	{/if}
</label>
<label class="flex flex-col gap-1.5 text-sm">
	<span class="field-label mb-0">Task logs</span>
	<Select
		testid="{idPrefix}-task-logs{testIdSuffix}"
		value={taskLogs}
		options={taskLogsOptions}
		onchange={(next) => (taskLogs = next as TaskLogsChoice)}
	/>
</label>
<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
	<span class="field-label mb-0">Telemetry attributes <span class="field-hint">(one key=value per line)</span></span>
	<textarea
		data-testid="{idPrefix}-telemetry-attributes{testIdSuffix}"
		bind:value={attributesText}
		rows="3"
		placeholder="team=payments"
		class="field px-3 py-2 font-mono text-xs"
	></textarea>
	{#if attributesHint}
		<span class="text-xs text-ink-muted">{attributesHint}</span>
	{/if}
	{#if attributesError}
		{#if attributesErrorTestId}
			<span class="severity-error text-xs" data-testid={attributesErrorTestId}
				>{attributesError}</span
			>
		{:else}
			<span class="severity-error text-xs">{attributesError}</span>
		{/if}
	{/if}
</label>
