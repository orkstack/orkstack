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
		<legend class="caps mb-1 text-ink-muted">Declared inputs</legend>
		{#each specs as spec (spec.name)}
			<div class="flex flex-col gap-1.5">
				<label for="input-{spec.name}" class="text-sm text-ink">
					<span class="font-mono">{spec.name}</span>{#if spec.required}<span class="caps ml-2 text-red-strong">required</span>{/if}
					<span class="figure ml-2 text-xs text-ink-muted">{spec.type}</span>
				</label>
				{#if spec.type === 'boolean'}
					<label class="flex items-center gap-2 text-sm text-ink">
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
						class="field px-3 py-2 font-mono text-sm"
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
						class="field h-9 px-3 text-sm"
					/>
				{/if}
				{#if errors[spec.name]}<p class="severity-error text-xs">{errors[spec.name]}</p>{/if}
				{#if spec.defaultValue !== undefined}
					<p class="text-xs text-ink-muted">Default: <code>{JSON.stringify(spec.defaultValue)}</code></p>
				{/if}
			</div>
		{/each}
	</fieldset>
{/if}
