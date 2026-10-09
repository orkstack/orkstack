<script lang="ts">
	// The one filter form every growing list uses (docs/UI_CLI_PARITY.md
	// "Filtering lists"). What a list may be filtered on is NOT written here:
	// it is the (orkstack.v1.filterable) option the list request declares,
	// read from the generated descriptors (lib/listFilter.ts specFor), so the
	// browser offers exactly the fields `orkctl --filter help` prints and the
	// engine accepts.
	//
	// The form holds a DRAFT of the applied filters (the Runs page's rule):
	// refilled from `applied` after every navigation, applied only on submit,
	// and the page's own controls passed as `children` — page size on a paged
	// list — are applied with the same submit, never on change.
	//
	// It blocks nothing the CLI would send. An empty label key goes out as
	// `labels.`, an empty time as "", exactly as `--filter 'labels.=x'` or
	// `--filter 'created_at>='` would, and the server's refusal is shown
	// verbatim by the page — one validator, so the two clients cannot disagree.
	import type { Snippet } from 'svelte';
	import type { DescMessage } from '@bufbuild/protobuf';
	import CliEquivalent from './CliEquivalent.svelte';
	import Select from './Select.svelte';
	import { sinceInputValue } from '$lib/browsing';
	import {
		FilterOperator,
		OPERATOR_LABELS,
		escapeValue,
		fieldOf,
		filterFlags,
		filterOf,
		specFor,
		utcInstant,
		type FieldFilter,
		type FilterField
	} from '$lib/listFilter';

	interface Props {
		/** The list request whose allowlist this form offers, e.g. ListNamespacesRequestSchema. */
		request: DescMessage;
		/** The filters in force, parsed from the URL by the page's load. */
		applied: FieldFilter[];
		/** testid prefix: 'namespaces' → namespaces-filters, namespaces-filter-field-0, … */
		testid: string;
		/** The command before its flags, each word already shell-safe: ['orkctl', 'namespace', 'list']. */
		command: string[];
		/** Navigate to the list with these filters, on a first page. */
		onapply: (filters: FieldFilter[]) => void;
		/** The list's address with no filters, no cursor, the same size. */
		clearHref: string;
		/** Controls applied with the same submit (page size). */
		children?: Snippet;
	}

	let { request, applied, testid, command, onapply, clearHref, children }: Props = $props();

	const spec = $derived(specFor(request));

	// One draft row per filter. `path` is the declared path (a labels map by
	// its own path, its key apart); `text` is a text, bool, single enum value,
	// or a datetime-local reading; `picked` is an enum `in`'s values.
	interface Row {
		path: string;
		key: string;
		op: FilterOperator;
		text: string;
		picked: string[];
	}

	function declared(path: string): FilterField | undefined {
		return spec.fields.find((f) => f.path === path);
	}

	function canonicalEnum(field: FilterField, value: string): string {
		const upper = value.toUpperCase().replaceAll('-', '_');
		return field.values.find((v) => v === upper || upper.endsWith(`_${v}`)) ?? value;
	}

	function rowFromFilter(f: FieldFilter): Row {
		const hit = fieldOf(spec, f.field);
		if (!hit) {
			// A field this list does not declare (a pasted URL): kept as it was
			// sent, so the server's refusal names it and the reader can fix it.
			return { path: f.field, key: '', op: f.op, text: f.values.map(escapeValue).join(','), picked: [] };
		}
		const { field, key } = hit;
		const row: Row = { path: field.path, key, op: f.op, text: '', picked: [] };
		if (field.kind === 'time') row.text = sinceInputValue(f.values[0] ?? '');
		else if (field.kind === 'enum' && f.op === FilterOperator.IN) row.picked = f.values.map((v) => canonicalEnum(field, v));
		else if (field.kind === 'enum') row.text = canonicalEnum(field, f.values[0] ?? '');
		else if (f.op === FilterOperator.IN) row.text = f.values.map(escapeValue).join(',');
		else row.text = f.values[0] ?? '';
		return row;
	}

	function blankRow(field: FilterField): Row {
		return { path: field.path, key: '', op: field.operators[0], text: field.kind === 'bool' ? 'true' : '', picked: [] };
	}

	// splitList reads an `in` value the way the grammar does: unescaped commas
	// separate values, `\,` is a literal comma. An empty item is kept, for the
	// server to refuse.
	function splitList(text: string): string[] {
		const values: string[] = [];
		let item = '';
		for (let i = 0; i < text.length; i += 1) {
			if (text[i] === '\\' && i + 1 < text.length) {
				i += 1;
				item += text[i];
			} else if (text[i] === ',') {
				values.push(item);
				item = '';
			} else {
				item += text[i];
			}
		}
		values.push(item);
		return values;
	}

	// isoFromLocal turns a datetime-local reading into the RFC 3339 instant a
	// filter carries; "" stays "" for the server to refuse.
	function isoFromLocal(local: string): string {
		return local ? utcInstant(new Date(local).toISOString()) : '';
	}

	function filterFromRow(row: Row): FieldFilter {
		const field = declared(row.path);
		const path = field?.kind === 'labels' ? `${row.path}.${row.key}` : row.path;
		if (field?.kind === 'time') return filterOf(path, row.op, [isoFromLocal(row.text)]);
		if (field?.kind === 'enum' && row.op === FilterOperator.IN) return filterOf(path, row.op, [...row.picked]);
		if (row.op === FilterOperator.IN) return filterOf(path, row.op, splitList(row.text));
		return filterOf(path, row.op, [row.text]);
	}

	// Seeded from `applied` so the server renders the draft too, and refilled
	// after every navigation (an effect does not run on the server).
	function rowsFromApplied(): Row[] {
		return applied.map(rowFromFilter);
	}
	let rows = $state<Row[]>(rowsFromApplied());
	$effect(() => {
		rows = rowsFromApplied();
	});

	function addRow() {
		rows = [...rows, blankRow(spec.fields[0])];
	}
	function removeRow(i: number) {
		rows = rows.filter((_, at) => at !== i);
	}
	function setField(i: number, path: string) {
		const field = declared(path);
		if (field) rows[i] = blankRow(field);
	}
	function setOp(i: number, op: string) {
		rows[i].op = Number(op) as FilterOperator;
	}
	function togglePicked(i: number, value: string, on: boolean) {
		const picked = rows[i].picked.filter((v) => v !== value);
		rows[i].picked = on ? [...picked, value] : picked;
	}
	function submit(event: SubmitEvent) {
		event.preventDefault();
		onapply(rows.map(filterFromRow));
	}

	const fieldOptions = $derived(
		spec.fields.map((f) => ({ value: f.path, label: `${f.label} (${f.kind === 'labels' ? `${f.path}.KEY` : f.path})` }))
	);
	function operatorOptions(row: Row) {
		const field = declared(row.path);
		const ops = field ? field.operators : [row.op];
		return ops.map((op) => ({ value: String(op), label: OPERATOR_LABELS[op] }));
	}
	const cli = $derived([...command, ...filterFlags(applied)].join(' '));

	const inputClass =
		'h-9 w-full rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs placeholder:text-fg-placeholder focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none';
</script>

<form
	role="search"
	class="mb-5 rounded-lg border border-border bg-surface p-4 shadow-xs"
	data-testid="{testid}-filters"
	onsubmit={submit}
>
	{#if rows.length === 0}
		<p class="text-sm text-fg-muted" data-testid="{testid}-filters-none">No filters applied. Add one to narrow this list.</p>
	{/if}
	<div class="flex flex-col gap-3">
		{#each rows as row, i (i)}
			{@const field = declared(row.path)}
			<div class="grid items-end gap-2 sm:grid-cols-[minmax(0,14rem)_minmax(0,9rem)_minmax(0,11rem)_minmax(0,1fr)_auto]" data-testid="{testid}-filter-row-{i}">
				<div class="flex flex-col gap-1.5 text-sm">
					<label class="font-medium text-fg" for="{testid}-filter-field-{i}">Field</label>
					<Select id="{testid}-filter-field-{i}" testid="{testid}-filter-field-{i}" value={row.path} options={fieldOptions} onchange={(next) => setField(i, next)} />
				</div>
				{#if field?.kind === 'labels'}
					<label class="flex flex-col gap-1.5 text-sm">
						<span class="font-medium text-fg">Key</span>
						<input class="{inputClass} font-mono" data-testid="{testid}-filter-key-{i}" bind:value={rows[i].key} placeholder="team" autocomplete="off" spellcheck="false" />
					</label>
				{:else}
					<div></div>
				{/if}
				<div class="flex flex-col gap-1.5 text-sm">
					<label class="font-medium text-fg" for="{testid}-filter-op-{i}">Operator</label>
					<Select id="{testid}-filter-op-{i}" testid="{testid}-filter-op-{i}" value={String(row.op)} options={operatorOptions(row)} onchange={(next) => setOp(i, next)} />
				</div>
				<div class="flex flex-col gap-1.5 text-sm">
					<span class="font-medium text-fg">Value</span>
					{#if field?.kind === 'enum' && row.op === FilterOperator.IN}
						<div class="flex flex-wrap gap-x-3 gap-y-1" role="group" aria-label="{field.label} values" data-testid="{testid}-filter-value-{i}">
							{#each field.values as value (value)}
								<label class="inline-flex items-center gap-1.5 text-sm text-fg">
									<input
										type="checkbox"
										class="size-4 accent-brand-600"
										data-testid="{testid}-filter-value-{i}-{value}"
										checked={row.picked.includes(value)}
										onchange={(event) => togglePicked(i, value, event.currentTarget.checked)}
									/>
									{value}
								</label>
							{/each}
						</div>
					{:else if field?.kind === 'enum'}
						<Select testid="{testid}-filter-value-{i}" value={row.text} options={field.values.map((v) => ({ value: v, label: v }))} onchange={(next) => (rows[i].text = next)} />
					{:else if field?.kind === 'bool'}
						<Select testid="{testid}-filter-value-{i}" value={row.text} options={[{ value: 'true', label: 'true' }, { value: 'false', label: 'false' }]} onchange={(next) => (rows[i].text = next)} />
					{:else if field?.kind === 'time'}
						<input type="datetime-local" class={inputClass} data-testid="{testid}-filter-value-{i}" bind:value={rows[i].text} />
						<span class="text-xs text-fg-muted tabular-nums" data-testid="{testid}-filter-utc-{i}">{row.text ? `${isoFromLocal(row.text)} (UTC)` : 'no instant'}</span>
					{:else}
						<input
							class="{inputClass} font-mono"
							data-testid="{testid}-filter-value-{i}"
							bind:value={rows[i].text}
							placeholder={row.op === FilterOperator.IN ? 'a,b (\\, is a literal comma)' : 'empty matches an unset value'}
							autocomplete="off"
							spellcheck="false"
						/>
					{/if}
				</div>
				<button
					type="button"
					class="inline-flex h-9 items-center justify-center rounded-md border border-border bg-surface px-3 text-sm font-medium text-fg-secondary shadow-xs transition-colors duration-150 hover:bg-surface-hover"
					data-testid="{testid}-filter-remove-{i}"
					aria-label="Remove this filter"
					onclick={() => removeRow(i)}>Remove</button
				>
			</div>
			{#if field?.requires}
				<p class="-mt-1 text-xs text-fg-muted" data-testid="{testid}-filter-requires-{i}">
					needs an equals filter on {field.requires}
				</p>
			{/if}
		{/each}
	</div>
	{#if children}
		<div class="mt-4 flex flex-wrap items-end gap-4">{@render children()}</div>
	{/if}
	<div class="mt-4 flex flex-wrap items-center gap-4 border-t border-border-subtle pt-4">
		<button
			type="button"
			class="inline-flex h-9 items-center justify-center rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
			data-testid="{testid}-filter-add"
			onclick={addRow}>Add filter</button
		>
		<button
			type="submit"
			class="inline-flex h-9 items-center justify-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800"
			data-testid="{testid}-filters-apply">Apply filters</button
		>
		{#if applied.length > 0}
			<a href={clearHref} class="text-sm font-medium text-brand-700 hover:underline" data-testid="{testid}-filters-clear">Clear filters</a>
		{/if}
		<div class="ml-auto">
			<CliEquivalent testid="{testid}-filters-cli" label="CLI equivalent of these filters" command={cli} />
		</div>
	</div>
</form>
