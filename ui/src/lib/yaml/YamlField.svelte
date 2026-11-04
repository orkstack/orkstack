<script lang="ts">
	// An authored document's field: the YAML editor, loaded when the field is
	// first shown, over a plain textarea that is there from the first paint.
	//
	// The textarea is the field until the editor's code arrives (and stays the
	// field if it never does): it holds the same value and the same test id, so
	// nothing typed or filled in the meantime is lost — the editor opens on the
	// value as it stands, and takes the focus if the textarea had it. Below the
	// field, one line says what is known about the document (status.ts) and how
	// to leave the editor by keyboard.
	import { onMount, untrack, type Component, type Snippet } from 'svelte';
	import type { YamlSession } from './session.svelte';
	import type { DocumentKind } from './types';

	interface Props {
		value: string;
		kind: DocumentKind;
		namespace: string;
		session: YamlSession;
		testid: string;
		/** The field's accessible name, when no visible label names it. */
		label: string;
		/** A visible label's id; preferred over `label` when given. */
		labelledby?: string;
		placeholder?: string;
		rows?: number;
		oninput?: () => void;
		/** Drawn between the code and the line below it. */
		between?: Snippet;
		/** The line below is one strip: a problem's first finding in it, its place a button. */
		strip?: boolean;
	}

	let sheetOpen = $state(false);

	let {
		value = $bindable(),
		kind,
		namespace,
		session,
		testid,
		label,
		labelledby,
		placeholder = '',
		rows = 18,
		oninput,
		between,
		strip = false
	}: Props = $props();

	// The editor component, once its module has loaded. Typed loosely: it is
	// imported, not referenced, so the route's own chunk never carries it.
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let Editor = $state<Component<any> | null>(null);
	let textarea = $state<HTMLTextAreaElement | null>(null);
	let focusEditor = $state(false);
	let selection = $state<{ anchor: number; head: number } | null>(null);

	const statusId = $derived(`${testid}-status`);
	const sentence = $derived(session.editor === 'ready' || session.engine.kind !== 'idle' ? session.status.sentence : '');
	const firstProblem = $derived(
		strip && session.status.tone === 'problems' ? ([...session.engineFindings, ...session.localFindings].find((f) => f.severity === 'error') ?? null) : null
	);
	const firstWords = $derived.by(() => {
		if (!firstProblem) return '';
		const where = firstProblem.field || firstProblem.path;
		return `${where ? `${where}: ` : ''}${(firstProblem.raw ?? firstProblem.message).replaceAll('`', '')}`;
	});
	const hintId = $derived(`${testid}-keys`);

	onMount(() => {
		let gone = false;
		session.editor = 'loading';
		import('./YamlEditor.svelte')
			.then((module) => {
				if (gone) return;
				focusEditor = textarea !== null && document.activeElement === textarea;
				if (textarea) selection = { anchor: textarea.selectionStart, head: textarea.selectionEnd };
				Editor = module.default;
			})
			.catch((err) => {
				// The plain field stays, and works: say why the editor did not.
				console.error('YAML editor could not load; the plain field stays:', err);
				if (!gone) session.editor = 'failed';
			});
		return () => {
			gone = true;
			session.dispose();
		};
	});

	const tone = $derived(session.status.tone);

	// The session's idea of what is on screen follows the field at once, in
	// either mode, so an answer about other text or another namespace never
	// reads as current — the status leaves "no problems" with the keystroke.
	$effect.pre(() => {
		session.text = value;
		session.namespace = namespace;
	});

	// Without the editor (still loading, or it could not load) the field asks
	// the engine itself, so "checking" is never said of a check nobody runs.
	// Whether the YAML parses is the engine's to say here.
	$effect(() => {
		const text = value;
		const ns = namespace;
		if (session.editor !== 'ready') untrack(() => session.schedule(text, ns, true));
	});

	// The live region speaks only when a check has settled — the engine's
	// answer, or that it could not answer — never "checking" or a syntax count
	// mid-word, so typing is not narrated. Its sentence is the visible one.
	$effect(() => {
		const settled = session.engine.kind === 'answered' || session.engine.kind === 'failed';
		if (settled && (tone === 'clean' || tone === 'problems' || tone === 'unavailable')) {
			session.announcement = session.status.sentence;
		}
	});
</script>

<div class="yaml-field" data-editor={session.editor}>
	<!-- The document as the form holds it: the editor draws only the lines in
	     view, so this is where the whole text is read back (ui/e2e/yaml.ts). -->
	<input type="hidden" name={testid} data-testid="{testid}-value" {value} />
	{#if Editor}
		<Editor
			bind:value
			{kind}
			{namespace}
			{session}
			{testid}
			{label}
			{labelledby}
			describedBy="{statusId} {hintId}"
			{placeholder}
			minLines={rows}
			focus={focusEditor}
			{selection}
			{oninput}
		/>
	{:else}
		<textarea
			bind:this={textarea}
			bind:value
			oninput={() => oninput?.()}
			data-testid={testid}
			aria-label={labelledby ? undefined : label}
			aria-labelledby={labelledby}
			aria-describedby="{statusId} {hintId}"
			{rows}
			{placeholder}
			spellcheck="false"
			class="field w-full px-3 py-2 font-mono text-sm leading-5 [font-variant-ligatures:none] [tab-size:2] whitespace-pre"
		></textarea>
	{/if}
	{@render between?.()}
	<div class="yaml-foot mt-1.5 flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 text-xs">
		<p
			id={statusId}
			data-testid="{testid}-status"
			data-tone={tone}
			data-tip={strip && sentence ? sentence : undefined}
			class="yaml-status {tone === 'problems' ? 'severity-error' : tone === 'unavailable' ? 'text-red-strong' : 'text-ink-secondary'}"
		>
			{#if firstProblem}<span class="sr">{sentence}</span><span aria-hidden="true">Fix {session.status.errors} {session.status.errors === 1 ? 'problem' : 'problems'} to apply</span>{:else}{sentence}{/if}
		</p>
		{#if firstProblem}
			<span class="yaml-first" data-testid="{testid}-first-problem">
				{#if firstProblem.line !== null}
					<button type="button" class="font-mono" disabled={!session.jump} onclick={() => session.jump?.(firstProblem.from, firstProblem.to)} aria-label="{firstProblem.line}:{firstProblem.col}, go to line {firstProblem.line}, column {firstProblem.col}">{firstProblem.line}:{firstProblem.col}</button>
				{/if}
				<span class="ptext">{firstWords}</span>
			</span>
		{/if}
		<p role="status" class="sr-only" data-testid="{testid}-announce">{session.announcement}</p>
		<p id={hintId} class="yaml-keys text-ink-3">
			{#if session.editor === 'ready'}
				<kbd class="kbd">F8</kbd> next problem<span class="k-long">{' · '}<kbd class="kbd">Ctrl</kbd>+<kbd class="kbd">Space</kbd> suggests</span>{' · '}<button type="button" class="linkbtn" data-testid="{testid}-shortcuts" aria-haspopup="dialog" onclick={() => (sheetOpen = true)}>Shortcuts</button><span class="sr">. Escape, then Tab within 2 s, leaves the editor.</span>
			{/if}
		</p>
	</div>
	{#if sheetOpen}
		{#await import('#lib/components/Dialog.svelte') then { default: Dialog }}
			<Dialog title="Editor shortcuts" testid="{testid}-shortcuts-sheet" glyph="kbd" onclose={() => (sheetOpen = false)}>
				{#snippet body()}
					<div class="sheet" data-testid="{testid}-shortcuts-list">
						<h3>Suggest and explain</h3>
						<dl>
							<dt>Suggest keys and values for the cursor's place</dt><dd><kbd class="kbd">Ctrl</kbd> <kbd class="kbd">Space</kbd></dd>
							<dt>Explain the key under the cursor</dt><dd><kbd class="kbd">Ctrl</kbd> <kbd class="kbd">Shift</kbd> <kbd class="kbd">Space</kbd></dd>
							<dt>Move through suggestions</dt><dd><kbd class="kbd">↑</kbd> or <kbd class="kbd">↓</kbd></dd>
							<dt>Accept a suggestion</dt><dd><kbd class="kbd">Enter</kbd></dd>
							<dt>Close the suggestions</dt><dd><kbd class="kbd">Esc</kbd></dd>
						</dl>
						<h3>Problems</h3>
						<dl>
							<dt>Go to the next problem</dt><dd><kbd class="kbd">F8</kbd></dd>
						</dl>
						<h3>Indenting and leaving the editor</h3>
						<dl>
							<dt>Indent or outdent</dt><dd><kbd class="kbd">Tab</kbd> or <kbd class="kbd">Shift</kbd> <kbd class="kbd">Tab</kbd></dd>
							<dt>Leave the editor with the keyboard</dt><dd><kbd class="kbd">Esc</kbd> then <kbd class="kbd">Tab</kbd> within 2 s</dd>
							<dt>Let Tab move focus instead of indenting</dt><dd><kbd class="kbd">Ctrl</kbd> <kbd class="kbd">M</kbd></dd>
						</dl>
						<h3>History</h3>
						<dl>
							<dt>Undo</dt><dd><kbd class="kbd">Ctrl</kbd> <kbd class="kbd">Z</kbd></dd>
							<dt>Redo</dt><dd><kbd class="kbd">Ctrl</kbd> <kbd class="kbd">Shift</kbd> <kbd class="kbd">Z</kbd></dd>
						</dl>
					</div>
				{/snippet}
			</Dialog>
		{/await}
	{/if}
</div>

<style>
	.yaml-first {
		display: flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
	}
	.yaml-first button {
		flex: none;
		font-size: 12.5px;
		font-variant-numeric: tabular-nums;
		color: var(--link);
		text-decoration: underline;
		text-underline-offset: 0.2em;
		cursor: pointer;
	}
	.ptext {
		min-width: 0;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		color: var(--ink-2);
	}
	.yaml-keys {
		margin-left: auto;
		white-space: nowrap;
	}
	.linkbtn {
		color: var(--link);
		font-weight: 600;
		text-decoration: underline;
		text-underline-offset: 0.2em;
		cursor: pointer;
	}
	.sheet {
		display: grid;
		gap: 8px;
		font-size: 14px;
	}
	.sheet h3 {
		margin: 8px 0 0;
		font-size: 13px;
		font-weight: 650;
		color: var(--ink-2);
	}
	.sheet dl {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 6px 16px;
		margin: 0;
	}
	.sheet dd {
		margin: 0;
		text-align: right;
		white-space: nowrap;
	}
	@media (max-width: 640px) {
		.yaml-keys {
			margin-left: 0;
			white-space: normal;
		}
		.k-long {
			display: none;
		}
	}
</style>
