<script lang="ts">
	// AN AWAITED TASK'S CARD BODY. There is no container, so there are no logs
	// and no image to inspect: what a reader needs is what the task waits for,
	// until when, who resolved it — and, while it waits, the two actions that
	// resolve it, each beside the `orkctl run resolve` that does the same.
	//
	// The page never marks the task resolved on the RPC's success alone. A
	// resolution is DELIVERED into the run's history and the run applies it
	// next — it can still lose a race — so after a successful call the card
	// says "Delivered" until TASK_RESOLVED or a terminal status arrives on the
	// event stream.
	import type { JsonObject } from '@bufbuild/protobuf';
	import CliEquivalent from './CliEquivalent.svelte';
	import DeclaredInputs from './DeclaredInputs.svelte';
	import Dialog from './Dialog.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import { errorMessage } from '$lib/client';
	import { runClient } from '$lib/clients/runs';
	import {
		awaitCallbackUrl,
		awaitCurl,
		awaitDeadline,
		awaitRemaining,
		awaitStateLabel,
		resolveCli,
		type Timestamp
	} from '$lib/awaits';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import type { AwaitSpec } from '$lib/gen/orkstack/v1/orkflow_pb';
	import type { TaskAwait } from '$lib/gen/orkstack/v1/run_pb';

	interface Props {
		runId: string;
		namespace: string;
		task: {
			id: string;
			status: Status;
			await?: TaskAwait;
			error?: { code: string };
			startedAt?: Timestamp;
		};
		/** The pinned definition's await, for the timeout, schema and callback. */
		spec?: AwaitSpec;
		runTerminal: boolean;
		now: Timestamp;
		/** The engine address the page was opened at. */
		origin: string;
	}

	let { runId, namespace, task, spec, runTerminal, now, origin }: Props = $props();

	const timeoutSeconds = $derived(spec?.timeoutSeconds ?? 0);
	const callback = $derived(task.await?.callback ?? spec?.callback ?? false);
	// The snapshot's deadline once read; until then TASK_AWAITING's own
	// timestamp (the page stamps it as startedAt) plus the timeout — the same
	// instant, because the engine stamps both from one workflow.Now.
	const deadline = $derived(awaitDeadline(task, timeoutSeconds, task.startedAt));
	const awaiting = $derived(task.status === Status.AWAITING);
	const stateLabel = $derived(
		awaitStateLabel({
			status: task.status,
			await: task.await,
			error: task.error,
			timeoutSeconds,
			opened: task.startedAt !== undefined || task.await?.deadline !== undefined
		})
	);
	const callbackUrl = $derived(awaitCallbackUrl(origin, runId, task.id));

	let dialog = $state<'resolve' | 'reject' | null>(null);
	let mode = $state<'fields' | 'json'>('fields');
	let fieldsValue = $state<JsonObject>({});
	let jsonText = $state('{}');
	let typedError = $state('');
	let reason = $state('');
	let busy = $state(false);
	let error = $state('');
	let delivered = $state<'' | 'delivered' | 'repeat'>('');

	const hasSchema = $derived(Object.keys(spec?.schema ?? {}).length > 0);

	function open(which: 'resolve' | 'reject') {
		dialog = which;
		error = '';
		busy = false;
		if (which === 'resolve') {
			mode = hasSchema ? 'fields' : 'json';
			// An unticked checkbox reads as false, so a required boolean starts
			// as false rather than absent: what is sent is what is shown.
			const seeded: JsonObject = {};
			for (const [name, decl] of Object.entries(spec?.schema ?? {})) {
				if (decl && typeof decl === 'object' && !Array.isArray(decl) && decl.type === 'boolean' && decl.required === true) {
					seeded[name] = false;
				}
			}
			fieldsValue = seeded;
			jsonText = '{}';
			typedError = '';
		} else {
			reason = '';
		}
	}

	// What a completion sends: in JSON mode the text VERBATIM, once it parses
	// as an object — the stored values.json is the caller's exact bytes — and
	// in Fields mode the typed value, serialized.
	function valuesToSend(): { value: string } | { error: string } {
		if (mode === 'fields') {
			if (typedError) return { error: typedError };
			return { value: JSON.stringify(fieldsValue) };
		}
		try {
			const parsed: unknown = JSON.parse(jsonText);
			if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
				return { error: 'The values must be one JSON object.' };
			}
		} catch (err) {
			return { error: `The values are not valid JSON: ${err instanceof Error ? err.message : String(err)}` };
		}
		return { value: jsonText };
	}

	const resolveCommand = $derived.by(() => {
		const values = valuesToSend();
		return resolveCli({
			namespace,
			runId,
			taskId: task.id,
			valuesJson: 'value' in values ? values.value : undefined
		});
	});
	const rejectCommand = $derived(
		resolveCli({ namespace, runId, taskId: task.id, reject: true, reason: reason.trim() || '…' })
	);

	async function submitResolve() {
		const values = valuesToSend();
		if ('error' in values) {
			error = values.error;
			return;
		}
		busy = true;
		error = '';
		try {
			const res = await runClient(fetch).resolveAwait({
				runId,
				namespace,
				taskId: task.id,
				valuesJson: values.value
			});
			delivered = res.repeat ? 'repeat' : 'delivered';
			dialog = null;
		} catch (err) {
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}

	async function submitReject() {
		busy = true;
		error = '';
		try {
			const res = await runClient(fetch).resolveAwait({
				runId,
				namespace,
				taskId: task.id,
				reject: true,
				reason: reason.trim()
			});
			delivered = res.repeat ? 'repeat' : 'delivered';
			dialog = null;
		} catch (err) {
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}

	function fmtDeadline(ts: Timestamp): string {
		return new Date(Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6)).toLocaleString();
	}

	function modeClass(active: boolean): string {
		const tone = active ? 'font-semibold text-fg' : 'text-fg-secondary hover:text-fg';
		return `rounded px-1.5 py-0.5 transition-colors duration-150 ${tone}`;
	}
</script>

<div
	class="mt-3 rounded-md border border-awaiting-border bg-awaiting-bg/40 px-3 py-2 text-sm"
	data-testid="task-{task.id}-await"
>
	<div class="flex flex-wrap items-center justify-between gap-2">
		<p class="font-medium text-fg" data-testid="task-{task.id}-await-state">{stateLabel}</p>
		{#if awaiting && !runTerminal}
			<div class="flex flex-wrap items-center gap-2">
				<button
					type="button"
					data-testid="task-{task.id}-resolve"
					onclick={() => open('resolve')}
					class="inline-flex h-8 items-center rounded-md bg-brand-600 px-3 text-xs font-medium text-white shadow-xs hover:bg-brand-700"
				>Resolve</button>
				<button
					type="button"
					data-testid="task-{task.id}-reject"
					onclick={() => open('reject')}
					class="inline-flex h-8 items-center rounded-md border border-danger-200 bg-surface px-3 text-xs font-medium text-danger-700 shadow-xs hover:bg-danger-50"
				>Reject</button>
				<CliEquivalent
					command={resolveCli({ namespace, runId, taskId: task.id })}
					testid="task-{task.id}-resolve-cli"
					label="CLI equivalent of resolving this await"
				/>
			</div>
		{/if}
	</div>
	<dl class="mt-2 flex flex-wrap gap-x-6 gap-y-1 text-xs text-fg-muted">
		<div>
			<dt class="inline">Deadline</dt>
			<dd class="ml-1 inline text-fg-secondary" data-testid="task-{task.id}-await-deadline">
				{deadline ? fmtDeadline(deadline) : timeoutSeconds ? `${timeoutSeconds}s after it starts waiting` : '—'}
			</dd>
		</div>
		{#if awaiting}
			<div>
				<dt class="inline">Remaining</dt>
				<dd class="ml-1 inline font-medium text-awaiting-fg" data-testid="task-{task.id}-await-remaining">
					{awaitRemaining(deadline, now)}
				</dd>
			</div>
		{/if}
		{#if task.await?.resolvedBy}
			<div title="Asserted by the resolving request's gateway, never authenticated by OrkStack.">
				<dt class="inline">Resolved by</dt>
				<dd class="ml-1 inline text-fg-secondary" data-testid="task-{task.id}-await-resolved-by">
					{task.await.resolvedBy} (asserted)
				</dd>
			</div>
		{/if}
	</dl>
	{#if delivered && awaiting}
		<p class="mt-2 text-xs font-medium text-fg-secondary" data-testid="task-{task.id}-await-delivered">
			{delivered === 'repeat'
				? 'Already resolved with these values — nothing was sent'
				: 'Delivered — waiting for the run to apply it'}
		</p>
	{/if}
	{#if callback}
		<div class="mt-2 flex flex-wrap items-center gap-2 text-xs text-fg-muted">
			<span>Accepts a callback at <span class="font-mono break-all">POST {callbackUrl}</span></span>
			<CliEquivalent
				command={awaitCurl({ url: callbackUrl })}
				testid="task-{task.id}-await-callback-cli"
				label="Callback command for this await"
			/>
		</div>
		<p class="mt-1 text-xs text-fg-muted">
			The token was shown once, to whoever submitted the run; only its digest is kept. If the
			engine's API is published at another address than this page, post there instead.
		</p>
	{/if}
</div>

{#if dialog === 'resolve'}
	<Dialog
		testid="await-resolve-dialog"
		title="Resolve await {task.id}"
		widthClass="max-w-xl"
		onclose={() => (dialog = null)}
	>
		{#snippet body()}
			<div class="space-y-4">
				<p>
					The values become this task's <span class="font-mono">values.json</span>, stored before they are
					delivered to the run. The run applies them next.
				</p>
				<div class="flex gap-2 text-xs">
					<button
						type="button"
						data-testid="await-resolve-fields-mode"
						onclick={() => (mode = 'fields')}
						class={modeClass(mode === 'fields')}>Fields</button
					>
					<button
						type="button"
						data-testid="await-resolve-json-mode"
						onclick={() => {
							if (mode === 'fields' && !typedError) jsonText = JSON.stringify(fieldsValue, null, 2);
							mode = 'json';
						}}
						class={modeClass(mode === 'json')}>JSON</button
					>
				</div>
				{#if mode === 'fields'}
					{#if hasSchema}
						<DeclaredInputs
							inputs={spec?.schema}
							value={fieldsValue}
							onchange={(next) => (fieldsValue = next)}
							oninvalid={(message) => (typedError = message)}
							resetKey="await-{task.id}"
						/>
					{:else}
						<p class="text-xs text-fg-muted">This await declares no schema. Use JSON to send values.</p>
					{/if}
				{:else}
					<textarea
						data-testid="await-resolve-values"
						bind:value={jsonText}
						rows="8"
						spellcheck="false"
						class="w-full rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg shadow-xs"
					></textarea>
				{/if}
				{#if error}
					<ErrorPanel testid="await-resolve-error" subject="the resolution of {task.id}" message={error} />
				{/if}
				<div class="flex items-center justify-between gap-3">
					<CliEquivalent command={resolveCommand} testid="await-resolve-dialog-cli" label="CLI equivalent" />
					<button
						type="button"
						data-testid="await-resolve-submit"
						disabled={busy}
						onclick={() => void submitResolve()}
						class="inline-flex h-9 items-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs hover:bg-brand-700 disabled:opacity-50"
					>{busy ? 'Delivering…' : 'Resolve'}</button>
				</div>
			</div>
		{/snippet}
	</Dialog>
{/if}

{#if dialog === 'reject'}
	<Dialog testid="await-reject-dialog" title="Reject await {task.id}?" onclose={() => (dialog = null)}>
		{#snippet body()}
			<div class="space-y-3">
				<p>
					A rejection fails this task with AWAIT_REJECTED and the run with it. If a "no" should
					branch the run instead, resolve with a value a downstream condition reads.
				</p>
				<label class="block text-sm font-medium text-fg" for="await-reject-reason-{task.id}">Reason</label>
				<textarea
					id="await-reject-reason-{task.id}"
					data-testid="await-reject-reason"
					bind:value={reason}
					rows="3"
					class="w-full rounded-md border border-border bg-surface px-3 py-2 text-sm text-fg"
					placeholder="Why is this await rejected?"
				></textarea>
				<p class="overflow-x-auto rounded bg-surface-sunken p-2 font-mono text-xs text-fg-muted">{rejectCommand}</p>
				{#if error}
					<ErrorPanel testid="await-reject-error" subject="the rejection of {task.id}" message={error} />
				{/if}
				<div class="flex justify-end">
					<button
						type="button"
						data-testid="await-reject-submit"
						disabled={busy || reason.trim() === ''}
						onclick={() => void submitReject()}
						class="inline-flex h-9 items-center rounded-md bg-danger-600 px-3.5 text-sm font-medium text-white shadow-xs hover:bg-danger-700 disabled:opacity-50"
					>{busy ? 'Delivering…' : 'Reject'}</button>
				</div>
			</div>
		{/snippet}
	</Dialog>
{/if}
