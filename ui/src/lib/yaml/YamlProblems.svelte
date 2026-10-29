<script lang="ts">
	// The problems of the document being edited, as a list a keyboard and a
	// screen reader can use: the engine's issues first (it is the authority),
	// then the editor's own (YAML syntax and the generated schema). Each entry
	// names its line and column, which moves the editor's cursor there, and
	// says its severity with the drawn mark and the word (ui/DESIGN.md,
	// "Notes and Holds").
	//
	// An engine issue keeps the shape the Validate results always had — its
	// test id, field and severity attributes, and "field: sentence" as the
	// engine wrote it — so whoever reads those reads them unchanged.
	import type { Finding, YamlSession } from './session.svelte';

	interface Props {
		session: YamlSession;
		/** The engine issues' list and entries' test ids. */
		listTestid: string;
		issueTestid: string;
		/** The editor findings' test id prefix. */
		testid: string;
		/** The level of the lists' headings, under the page's own. */
		level?: 2 | 3 | 4;
	}

	let { session, listTestid, issueTestid, testid, level = 2 }: Props = $props();

	const engine = $derived(session.engineFindings);
	const local = $derived(session.localFindings);
	const e = $derived(session.engine);
	const current = $derived(e.kind === 'answered' && e.text === session.text && e.namespace === session.namespace);

	const SOURCE = { syntax: 'YAML syntax', schema: 'schema', engine: 'engine' } as const;

	/** A sentence's `quoted` words as code, the rest as text. */
	function parts(sentence: string): { text: string; code: boolean }[] {
		return sentence
			.split(/(`[^`]+`)/)
			.filter((p) => p !== '')
			.map((p) => (/^`[^`]+`$/.test(p) ? { text: p.slice(1, -1), code: true } : { text: p, code: false }));
	}
</script>

{#snippet head(f: Finding)}
	<span class="text-xs">
		{#if f.line !== null}
			<button
				type="button"
				class="figure inline-flex min-h-6 min-w-6 items-center text-ink-secondary underline decoration-rule-strong underline-offset-2 hover:text-ink"
				disabled={!session.jump}
				onclick={() => session.jump?.(f.from, f.to)}
				aria-label="{f.line}:{f.col}, go to line {f.line}, column {f.col}">{f.line}:{f.col}</button
			>
		{/if}
	</span>
	<span class="caps text-red-strong">{f.severity === 'error' ? 'Error' : 'Warning'}</span>
{/snippet}

{#snippet sentence(text: string)}{#each parts(text) as part, i (i)}{#if part.code}<code class="font-mono">{part.text}</code>{:else}{part.text}{/if}{/each}{/snippet}

{#if engine.length > 0}
	<div data-testid={listTestid} class="space-y-2 border-t border-rule pt-3 text-sm">
		<svelte:element this={`h${level}`} class="caps text-ink-muted">
			Validation results{current ? '' : ' · from the last check'}
		</svelte:element>
		<ul role="list" class="space-y-1.5">
			{#each engine as f, i (i)}
				<li class="yaml-problem">
					{@render head(f)}
					<p
						data-testid={issueTestid}
						data-field={f.field ?? ''}
						data-severity={f.severity}
						class={f.severity === 'error' ? 'severity-error' : 'severity-warning'}
					><span class="font-semibold">{f.field || 'document'}</span>: {f.raw ?? f.message}</p>
				</li>
			{/each}
		</ul>
	</div>
{/if}

{#if local.length > 0}
	<div data-testid="{testid}-findings" class="space-y-2 border-t border-rule pt-3 text-sm">
		<svelte:element this={`h${level}`} class="caps text-ink-muted">Editor checks · YAML syntax and the generated schema</svelte:element>
		<ul role="list" class="space-y-1.5">
			{#each local as f, i (i)}
				<li class="yaml-problem">
					{@render head(f)}
					<p
						data-testid="{testid}-finding"
						data-source={f.source}
						data-severity={f.severity}
						data-line={f.line}
						class={f.severity === 'error' ? 'severity-error' : 'severity-warning'}
					>
						<span class="sr-only">{SOURCE[f.source]}: </span>{#if f.path}<span class="font-semibold">{f.path}</span
							>{': '}{/if}{@render sentence(f.message)}
					</p>
				</li>
			{/each}
		</ul>
	</div>
{/if}

<style>
	/* Position, severity word, sentence: one row per problem, the columns
	   lined up down the list. */
	.yaml-problem {
		display: grid;
		grid-template-columns: calc(var(--spacing) * 14) calc(var(--spacing) * 16) 1fr;
		align-items: baseline;
		column-gap: calc(var(--spacing) * 3);
	}
</style>
