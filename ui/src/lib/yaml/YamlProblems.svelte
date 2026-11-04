<script lang="ts">
	// The problems of the document being edited, as a list a keyboard and a
	// screen reader can use: the engine's issues first (it is the authority),
	// then the editor's own (YAML syntax and the generated schema). Each entry
	// names its line and column, which moves the editor's cursor there, and
	// says its severity with the drawn mark and the word.
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

{#snippet place(f: Finding)}
	<span class="yp-place">
		{#if f.line !== null}
			<button
				type="button"
				class="inline-flex min-h-6 min-w-6 items-center font-mono underline underline-offset-2"
				disabled={!session.jump}
				onclick={() => session.jump?.(f.from, f.to)}
				aria-label="{f.line}:{f.col}, go to line {f.line}, column {f.col}">{f.line}:{f.col}</button
			>
		{/if}
	</span>
{/snippet}

{#snippet severity(f: Finding)}
	<span class="yp-sev" class:w={f.severity !== 'error'}><span class={f.severity === 'error' ? 'mk-fail' : 'mk-warn'} aria-hidden="true"></span>{f.severity === 'error' ? 'Error' : 'Warning'}</span>{' '}
{/snippet}

{#snippet sentence(text: string)}{#each parts(text) as part, i (i)}{#if part.code}<code class="font-mono">{part.text}</code>{:else}{part.text}{/if}{/each}{/snippet}

{#if engine.length > 0}
	<div data-testid={listTestid} class="yp-group">
		<svelte:element this={`h${level}`} class="yp-head">
			Validation results{current ? '' : ' · from the last check'}
		</svelte:element>
		<ul role="list">
			{#each engine as f, i (i)}
				<li class="yaml-problem">
					{@render place(f)}
					<div class="yp-body">
						{@render severity(f)}<p
							data-testid={issueTestid}
							data-field={f.field ?? ''}
							data-severity={f.severity}
							class={f.severity === 'error' ? 'severity-error' : 'severity-warning'}
						><span class="font-semibold">{f.field || 'document'}</span>: {f.raw ?? f.message}</p>
					</div>
				</li>
			{/each}
		</ul>
	</div>
{/if}

{#if local.length > 0}
	<div data-testid="{testid}-findings" class="yp-group">
		<svelte:element this={`h${level}`} class="yp-head">Editor checks · YAML syntax and the generated schema</svelte:element>
		<ul role="list">
			{#each local as f, i (i)}
				<li class="yaml-problem">
					{@render place(f)}
					<div class="yp-body">
						{@render severity(f)}<p
							data-testid="{testid}-finding"
							data-source={f.source}
							data-severity={f.severity}
							data-line={f.line}
							class={f.severity === 'error' ? 'severity-error' : 'severity-warning'}
						>
							<span class="sr-only">{SOURCE[f.source]}: </span>{#if f.path}<span class="font-semibold">{f.path}</span
								>{': '}{/if}{@render sentence(f.message)}
						</p>
					</div>
				</li>
			{/each}
		</ul>
	</div>
{/if}

<style>
	/* Its place, then its severity and sentence: one row per problem, the
	   sentence wrapping in its own column at any width. */
	.yp-group {
		padding-top: 8px;
		border-top: 1px solid var(--line);
	}
	.yp-head {
		margin: 0;
		font-size: 12.5px;
		font-weight: 400;
		color: var(--ink-3);
	}
	.yaml-problem {
		display: grid;
		grid-template-columns: 52px minmax(0, 1fr);
		align-items: baseline;
		gap: 4px 12px;
		padding: 12px 0;
		border-bottom: 1px solid var(--line);
		font-size: 14px;
	}
	.yaml-problem:last-child {
		border-bottom: 0;
	}
	.yp-place button {
		font-size: 12.5px;
		color: var(--link);
	}
	.yp-sev {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		margin-right: 6px;
		font-size: 13px;
		font-weight: 700;
		color: var(--fail);
	}
	.yp-sev.w {
		color: var(--ink-2);
	}
	/* The word and its mark stand beside the sentence; the sentence is ink. */
	.yp-body p {
		display: inline;
		margin: 0;
		color: var(--ink);
		overflow-wrap: anywhere;
	}
	.yp-body p::before {
		content: none;
	}
</style>
