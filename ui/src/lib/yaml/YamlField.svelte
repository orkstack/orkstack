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
	import { onMount, untrack, type Component } from 'svelte';
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
	}

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
		oninput
	}: Props = $props();

	// The editor component, once its module has loaded. Typed loosely: it is
	// imported, not referenced, so the route's own chunk never carries it.
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let Editor = $state<Component<any> | null>(null);
	let textarea = $state<HTMLTextAreaElement | null>(null);
	let focusEditor = $state(false);
	let selection = $state<{ anchor: number; head: number } | null>(null);

	const statusId = $derived(`${testid}-status`);
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
	<div class="mt-1.5 flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 text-xs">
		<p
			id={statusId}
			data-testid="{testid}-status"
			data-tone={tone}
			class="yaml-status {tone === 'problems' ? 'severity-error' : tone === 'unavailable' ? 'text-red-strong' : 'text-ink-secondary'}"
		>
			{session.editor === 'ready' || session.engine.kind !== 'idle' ? session.status.sentence : ''}
		</p>
		<p role="status" class="sr-only" data-testid="{testid}-announce">{session.announcement}</p>
		<p id={hintId} class="text-ink-muted">
			{#if session.editor === 'ready'}
				Tab indents · Escape, then Tab within 2 s, leaves (Ctrl-M lets Tab leave) · Ctrl-Space suggests keys ·
				Ctrl-Shift-Space explains a key · F8 goes to the next problem
			{/if}
		</p>
	</div>
</div>
