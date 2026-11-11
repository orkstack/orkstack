<script lang="ts" module>
	import type { ActivityInfo } from '#lib/gen/orkstack/v1/activity_service_pb.ts';

	type Answer = { info: ActivityInfo | null; error: string };

	// One answer per reference for the page's life: a rail that follows the
	// caret asks about each `uses:` once, never per keystroke. A failed read is
	// kept a short while too, so a reference that does not resolve — usual
	// while a `uses:` is being typed — is not asked again, and does not flash
	// "Reading…", at every rest of the caret; the first rest after that asks
	// again.
	const RETRY_FAILED_AFTER_MS = 10_000;
	const answers = new Map<string, { answer: Promise<Answer>; failedAt: number | null }>();
</script>

<script lang="ts">
	// The docs rail of the editor (layout C): the activity of the task the
	// caret is in — its image, its op and the `with:` keys it takes, which of
	// them this task sets, and the values it returns — read from the image's
	// declared interface (InspectActivity). A description the image does not
	// declare shows nothing; nothing here is guessed.
	import { activityClient } from '#lib/clients/activities.ts';
	import DocText from './DocText.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { activityKind } from '#lib/activityIcon.ts';
	import type { TaskAtCursor } from '#lib/yaml/taskAt.ts';
	import ActivityIcon from './ActivityIcon.svelte';
	import Icon from './Icon.svelte';
	import Plate from './Plate.svelte';

	interface Props {
		task: TaskAtCursor | null;
		open: boolean;
		ontoggle: () => void;
	}

	let { task, open, ontoggle }: Props = $props();

	function inspect(reference: string): Promise<Answer> {
		const known = answers.get(reference);
		if (known && (known.failedAt === null || Date.now() - known.failedAt < RETRY_FAILED_AFTER_MS)) return known.answer;
		const entry: { answer: Promise<Answer>; failedAt: number | null } = { answer: Promise.resolve({ info: null, error: '' }), failedAt: null };
		entry.answer = activityClient(fetch)
			.inspectActivity({ reference })
			.then(
				(res) => ({ info: res.activity ?? null, error: '' }),
				(err) => {
					entry.failedAt = Date.now();
					return { info: null, error: errorMessage(err) };
				}
			);
		answers.set(reference, entry);
		return entry.answer;
	}

	// The caret rests before the rail asks: moving through a document does
	// not fire a read per task it passes.
	let settled = $state<TaskAtCursor | null>(null);
	$effect(() => {
		const t = task;
		const timer = setTimeout(() => (settled = t), 400);
		return () => clearTimeout(timer);
	});

	// The header's icon and count follow the answer the body has drawn.
	let answered = $state<{ uses: string; info: ActivityInfo } | null>(null);
	// The task's activity, when it names one: nothing else has an image to read.
	const reference = $derived(settled?.kind === 'activity' ? settled.uses : '');
	// Asked at every rest of the caret, which is what lets a failed read be
	// tried again once its while has passed; until then it is the same answer,
	// so nothing is drawn again.
	const answer = $derived.by(() => {
		void settled;
		return reference ? inspect(reference) : null;
	});
	$effect(() => {
		const uses = reference;
		const asked = answer;
		if (!uses || !asked) return;
		let live = true;
		void asked.then((a) => {
			if (live && a.info) answered = { uses, info: a.info };
		});
		return () => {
			live = false;
		};
	});
	const shownInfo = $derived(answered && answered.uses === reference ? answered.info : null);
	function keysOf(info: ActivityInfo | null, op: string): string[] {
		const iface = info?.interface;
		if (!iface) return [];
		return Object.keys({ ...iface.fields, ...(op && iface.ops[op] ? iface.ops[op].fields : {}) }).filter((k) => k !== 'op');
	}
	const headKeys = $derived(keysOf(shownInfo, settled?.op || shownInfo?.interface?.defaultOp || ''));

	let body = $state<HTMLElement>();
	let overflows = $state(false);
	$effect(() => {
		void settled;
		if (!body) return;
		const el = body;
		const measure = () => (overflows = el.scrollHeight > el.clientHeight + 1);
		measure();
		const ro = new ResizeObserver(measure);
		ro.observe(el);
		return () => ro.disconnect();
	});

	const typeOf = (t: string) => t || 'any';
</script>

<aside class="rail" class:collapsed={!open} aria-label="Activity docs" data-testid="editor-docs">
	<div class="rail-head">
		{#if settled && reference}
			<p class="rail-title">
				<ActivityIcon kind={activityKind(reference)} icon={shownInfo?.icon} class="size-4.5 text-navy-text" />
				<span class="min-w-0"><b>{settled.id || 'This task'}</b> uses <b class="font-mono">{reference}</b></span>
			</p>
			<p class="rail-sub" data-testid="editor-docs-sub">line {settled.line}{#if headKeys.length}{' · '}<span data-testid="editor-docs-set">{settled.set.filter((k) => headKeys.includes(k)).length} of {headKeys.length} set</span>{/if}</p>
		{:else if settled}
			<p class="rail-title"><b class="min-w-0">{settled.id || 'This task'}</b></p>
			<p class="rail-sub" data-testid="editor-docs-sub">line {settled.line}</p>
		{:else}
			<p class="rail-title"><span>Activity docs</span></p>
		{/if}
		<button
			type="button"
			class="btn btn-sm btn-quiet btn-icon rail-toggle"
			aria-expanded={open}
			aria-controls="editor-docs-body"
			aria-label={settled?.id ? `Docs for ${settled.id}` : 'Activity docs'}
			data-tip={open ? 'Hide the docs' : 'Show the docs'}
			data-testid="editor-docs-toggle"
			onclick={ontoggle}><Icon name={open ? 'chev-right' : 'chev-left'} /></button
		>
	</div>
	<!-- A tab stop only while it scrolls, so a keyboard can reach what overflows (WCAG 2.1.1). -->
	<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
	<div
		id="editor-docs-body"
		class="rail-body scroll-panel"
		bind:this={body}
		hidden={!open}
		role="region"
		aria-label="Docs for {settled?.id || 'the task at the caret'}"
		tabindex={overflows ? 0 : undefined}
	>
		{#if settled && answer}
			{#await answer}
				<p class="rail-note">Reading {reference}…</p>
			{:then answer}
				{#if answer.error}
					<p class="rail-note" data-testid="editor-docs-error">Its interface could not be read: {answer.error}</p>
				{:else if answer.info}
					{@const info = answer.info}
					{@const iface = info.interface}
					{@const op = settled.op || iface?.defaultOp || ''}
					{@const fields = { ...(iface?.fields ?? {}), ...(op && iface?.ops[op] ? iface.ops[op].fields : {}) }}
					{@const keys = Object.keys(fields).filter((k) => k !== 'op')}
					{@const docs = info.docs}
					{@const opDocs = op ? docs?.ops[op] : undefined}
					<div class="grid gap-2 px-4 pt-3">
						{#if docs?.description}<p class="rail-summary"><DocText text={docs.description} limit={160} testid="editor-docs-summary" /></p>{/if}
						{#if info.indexDigest}<Plate key="image" value={`${info.reference.split('@')[0]}@${info.indexDigest.slice(0, 19)}…`} tip={`${info.reference.split('@')[0]}@${info.indexDigest}`} small />{/if}
						{#if op}<div class="text-dense text-ink-2">Op <code class="font-semibold">{op}</code>{#if !settled.op && iface?.defaultOp}<span class="text-ink-3"> (the image's default)</span>{/if}{#if opDocs?.description}: <DocText class="inline" text={opDocs.description} limit={120} testid="editor-docs-op" />{/if}</div>{/if}
					</div>
					{#if !iface}
						<p class="rail-note">This image declares no interface, so OrkStack cannot say what its <code>with:</code> takes.</p>
					{:else if keys.length === 0}
						<p class="rail-note">Its <code>with:</code> takes no keys.</p>
					{:else}
						<h3 id="editor-docs-keys-label" class="rail-label">Keys of {op ? `op ${op}` : 'this activity'}</h3>
						<ul class="kblocks" aria-labelledby="editor-docs-keys-label" data-testid="editor-docs-keys">
							{#each keys as key (key)}
								{@const f = fields[key]}
								{@const doc = opDocs?.fields[key] ?? docs?.fields[key]}
								<li class="kblock" data-testid="editor-docs-field-{key}">
									<div class="kb-head">
										<code class="kb-key">{key}</code>
										<span class="kb-meta">{typeOf(f.type)}</span>
										{#if f.required}<span class="kb-meta kb-required">required</span>{/if}
										{#if settled.set.includes(key)}
											<span class="kb-set is-set"><Icon name="apply" class="size-4 text-ok" />set</span>
										{:else}
											<span class="kb-set">not set</span>
										{/if}
									</div>
									{#if doc}
										<p class="kb-doc"><DocText text={doc} limit={280} lines={6} testid="editor-docs-key-{key}" /></p>
									{/if}
								</li>
							{/each}
						</ul>
						{#if Object.keys(iface.values ?? {}).length}
							<h3 id="editor-docs-values-label" class="rail-label">Values it returns</h3>
							<ul class="kblocks" aria-labelledby="editor-docs-values-label" data-testid="editor-docs-values">
								{#each Object.keys(iface.values) as value (value)}
									<li class="kblock">
										<div class="kb-head">
											<code class="kb-key">{value}</code>
											<span class="kb-meta">{typeOf(iface.values[value]?.type ?? '')}</span>
										</div>
										{#if docs?.values[value]}
											<p class="kb-doc"><DocText text={docs.values[value]} limit={280} lines={6} /></p>
										{/if}
									</li>
								{/each}
							</ul>
						{/if}
					{/if}
				{/if}
			{/await}
		{:else if settled}
			{@const who = settled.id || 'This task'}
			<!-- The caret is in a task that has no image to read: say which kind, never "place the caret in a task". -->
			<p class="rail-note" data-testid="editor-docs-none" data-kind={settled.kind}>
				{#if settled.kind === 'await'}
					<b>{who}</b> waits to be resolved from outside the run, so it runs no activity.
				{:else if settled.kind === 'sleep'}
					<b>{who}</b> sleeps for a set time, so it runs no activity.
				{:else if settled.kind === 'orkflow'}
					<b>{who}</b> starts a child run of <code>{settled.uses.slice('orkflow:'.length)}</code>, so it runs no activity of its own.
				{:else}
					<b>{who}</b> names no activity yet: its <code>uses:</code> says which one runs it.
				{/if}
			</p>
		{:else}
			<p class="rail-note" data-testid="editor-docs-empty">Place the caret in a task to see what its activity takes.</p>
		{/if}
	</div>
</aside>

<style>
	.rail {
		position: sticky;
		top: calc(var(--spacing-bar) + 16px);
		display: flex;
		flex-direction: column;
		max-height: calc(100dvh - var(--spacing-bar) - 32px);
		min-width: 0;
		border: 1px solid var(--line);
		border-radius: var(--r);
		background: var(--surface);
		box-shadow: var(--shadow);
	}
	.rail-head {
		position: relative;
		display: grid;
		gap: 2px;
		padding: 12px 48px 12px 16px;
		border-bottom: 1px solid var(--line);
	}
	.rail.collapsed .rail-head {
		border-bottom: 0;
	}
	.rail-title {
		display: flex;
		align-items: center;
		gap: 8px;
		margin: 0;
		font-size: 14px;
		overflow-wrap: anywhere;
	}
	.rail-sub {
		margin: 0;
		font-size: 13px;
		color: var(--ink-3);
	}
	.rail-toggle {
		position: absolute;
		top: 10px;
		right: 8px;
	}
	.rail-body {
		min-height: 0;
		overflow-y: auto;
		scrollbar-gutter: stable;
	}
	.rail-body:focus-visible {
		outline-offset: -2px;
	}
	.rail-note {
		margin: 0;
		padding: 10px 16px;
		font-size: 13.5px;
		color: var(--ink-2);
	}
	.rail-label {
		margin: 12px 0 0;
		padding: 0 16px 4px;
		border-bottom: 1px solid var(--line);
		font-size: 12px;
		font-weight: 600;
		color: var(--ink-3);
	}
	.kblocks {
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.kblock {
		padding: 10px 16px;
		border-bottom: 1px solid var(--line);
		font-size: 13.5px;
	}
	.kb-head {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 2px 8px;
	}
	.kb-key {
		min-width: 0;
		font-weight: 600;
		color: var(--ink);
		overflow-wrap: anywhere;
	}
	.kb-meta {
		color: var(--ink-3);
	}
	.kb-required {
		font-weight: 600;
		color: var(--ink-2);
	}
	.kb-set {
		display: inline-flex;
		align-self: center;
		align-items: center;
		gap: 4px;
		margin-left: auto;
		color: var(--ink-3);
		white-space: nowrap;
	}
	.kb-set.is-set {
		color: var(--ink-2);
	}
	.kb-doc {
		margin: 4px 0 0;
		font-size: 14px;
		color: var(--ink-2);
	}
	.rail-summary {
		margin: 0;
		font-size: 14px;
		color: var(--ink-2);
	}
	@media (max-width: 1180px) {
		.rail {
			position: static;
			max-height: none;
		}
		.rail-body {
			overflow: visible;
		}
	}
</style>
