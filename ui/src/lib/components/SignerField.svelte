<script lang="ts">
	// The namespace forms' "Require signed by" control (Namespace.
	// require_signed_by): a select of None and `cosign:<name>` for each signing
	// key the deployment's image policy holds, or a plain text field when that
	// list could not be read — the same fallback TelemetryFields gives the sink
	// select. Whether the key exists is the engine's answer; its refusal is
	// shown verbatim by the page.
	import Select from './Select.svelte';

	interface Props {
		value: string;
		keys: string[];
		keysError: string | null;
		testid: string;
		error?: string;
		errorTestid?: string;
	}

	let { value = $bindable(), keys, keysError, testid, error = undefined, errorTestid = undefined }: Props =
		$props();

	const options = $derived([
		{ value: '', label: 'None' },
		...keys.map((name) => ({ value: `cosign:${name}`, label: `cosign:${name}` }))
	]);
</script>

<label class="flex flex-col gap-1.5 text-sm">
	<span class="font-medium text-fg">Require signed by</span>
	{#if keysError}
		<input
			data-testid={testid}
			bind:value
			placeholder="cosign:<key-name>"
			class="h-9 rounded-md border border-border bg-surface px-3 text-fg focus:border-brand-500"
		/>
	{:else}
		<Select {testid} {value} {options} onchange={(next) => (value = next)} />
	{/if}
	<span class="text-xs text-fg-muted"
		>Every image executed in this namespace must carry a valid cosign signature by this key.
		Tightens the deployment image policy; cannot loosen it.</span
	>
	{#if error}
		<span class="text-xs text-danger-700" data-testid={errorTestid}>{error}</span>
	{/if}
</label>
