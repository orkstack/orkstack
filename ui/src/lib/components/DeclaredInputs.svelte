<script lang="ts">
	import type { JsonObject, JsonValue } from '@bufbuild/protobuf';
	import { inputSpecs, setInput } from '$lib/authoring';

	interface Props {
		inputs?: JsonObject;
		value: JsonObject;
		onchange: (value: JsonObject) => void;
		oninvalid: (message: string) => void;
		resetKey: string;
	}

	let { inputs, value, onchange, oninvalid, resetKey }: Props = $props();
	const specs = $derived(inputSpecs(inputs));
	let drafts = $state<Record<string, string>>({});
	let errors = $state<Record<string, string>>({});
	let seenResetKey = $state('');
	$effect(() => {
		if (resetKey === seenResetKey) return;
		seenResetKey = resetKey;
		drafts = {};
		errors = {};
		oninvalid('');
	});
	function setError(name: string, message: string) {
		const next = { ...errors };
		if (message) next[name] = message;
		else delete next[name];
		errors = next;
		oninvalid(Object.values(next).join('; '));
	}

	function composite(value: JsonValue | undefined): string {
		return value === undefined ? '' : JSON.stringify(value, null, 2);
	}

	function updateComposite(name: string, type: string, raw: string) {
		drafts = { ...drafts, [name]: raw };
		try {
			const parsed: unknown = JSON.parse(raw);
			if (type === 'object' && (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed))) throw new Error('must be a JSON object');
			if (type === 'array' && !Array.isArray(parsed)) throw new Error('must be a JSON array');
			onchange(setInput(value, name, parsed as JsonValue));
			setError(name, '');
		} catch (err) {
			setError(name, `${name} ${err instanceof Error ? err.message : 'must be valid JSON'}`);
		}
	}
	function updateNumber(name: string, raw: string) {
		if (raw === '') { onchange(setInput(value, name, undefined)); setError(name, ''); return; }
		const number = Number(raw);
		if (!Number.isFinite(number)) { setError(name, `${name} must be a finite number`); return; }
		onchange(setInput(value, name, number));
		setError(name, '');
	}
</script>

{#if specs.length > 0}
	<fieldset class="space-y-4" data-testid="declared-inputs">
		<legend class="text-sm font-medium text-fg">Declared inputs</legend>
		{#each specs as spec (spec.name)}
			<div class="flex flex-col gap-1.5">
				<label for="input-{spec.name}" class="text-sm text-fg-secondary">
					{spec.name}{#if spec.required}<span class="ml-1 text-danger-600">required</span>{/if}
					<span class="ml-1 text-xs text-fg-muted">{spec.type}</span>
				</label>
				{#if spec.type === 'boolean'}
					<label class="flex items-center gap-2 text-sm text-fg">
						<input
							id="input-{spec.name}"
							data-testid="declared-input-{spec.name}"
							type="checkbox"
							checked={value[spec.name] === true}
							onchange={(event) => onchange(setInput(value, spec.name, event.currentTarget.checked))}
						/>
						Enabled
					</label>
				{:else if spec.type === 'object' || spec.type === 'array' || spec.type === 'json'}
					<textarea
						id="input-{spec.name}"
						data-testid="declared-input-{spec.name}"
						rows="3"
						value={drafts[spec.name] ?? composite(value[spec.name])}
						oninput={(event) => updateComposite(spec.name, spec.type, event.currentTarget.value)}
						class="rounded-md border border-border bg-surface px-3 py-2 font-mono text-sm text-fg"
					></textarea>
				{:else}
					<input
						id="input-{spec.name}"
						data-testid="declared-input-{spec.name}"
						type={spec.type === 'number' ? 'number' : 'text'}
						step={spec.type === 'number' ? 'any' : undefined}
						value={value[spec.name] === undefined ? '' : String(value[spec.name])}
						oninput={(event) => {
							const raw = event.currentTarget.value;
							if (spec.type === 'number') updateNumber(spec.name, raw);
							else onchange(setInput(value, spec.name, raw));
						}}
						class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg"
					/>
				{/if}
				{#if errors[spec.name]}<p class="text-xs text-danger-700">{errors[spec.name]}</p>{/if}
				{#if spec.defaultValue !== undefined}
					<p class="text-xs text-fg-muted">Default: <code>{JSON.stringify(spec.defaultValue)}</code></p>
				{/if}
			</div>
		{/each}
	</fieldset>
{/if}
