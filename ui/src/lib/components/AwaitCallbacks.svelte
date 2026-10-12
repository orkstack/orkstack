<script lang="ts">
	// THE ONE-TIME DISCLOSURE of a submission's callback tokens.
	//
	// SubmitRun hands each `callback: true` await's token back exactly once;
	// the engine keeps only a salted digest. So the tokens live in this
	// component's memory and nowhere else — never a URL, never localStorage,
	// never the run page — exactly like the Triggers tab's one-time secret.
	// "Continue to the run" is the only way past this panel, so a submitter
	// cannot lose the token by a navigation they did not choose.
	import CliEquivalent from './CliEquivalent.svelte';
	import { awaitCallbackUrl, awaitCurl } from '$lib/awaits';
	import type { AwaitCallback } from '$lib/gen/orkstack/v1/run_service_pb';

	interface Props {
		runId: string;
		callbacks: AwaitCallback[];
		/** The engine address the page was opened at. */
		origin: string;
		oncontinue: () => void;
	}

	let { runId, callbacks, origin, oncontinue }: Props = $props();
</script>

<section class="space-y-4" data-testid="await-callbacks">
	<p class="text-sm text-fg">
		Run <span class="font-mono" data-testid="await-callbacks-run">{runId}</span> was submitted. Its
		awaited {callbacks.length === 1 ? 'task accepts' : 'tasks accept'} a callback over plain HTTP:
	</p>
	<p
		class="rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-sm font-medium text-warning-800"
		data-testid="await-callbacks-once"
	>
		Shown once: only a digest of each token is kept. Copy it now.
	</p>
	<ul class="space-y-2">
		{#each callbacks as callback (callback.taskId)}
			{@const url = awaitCallbackUrl(origin, runId, callback.taskId)}
			<li
				class="flex items-start justify-between gap-3 rounded-md border border-border bg-surface px-3 py-2"
				data-testid="await-callback-{callback.taskId}"
			>
				<div class="min-w-0">
					<p class="text-sm font-medium text-fg">Await {callback.taskId}</p>
					<p class="font-mono text-xs break-all text-fg-muted">POST {url}</p>
				</div>
				<CliEquivalent
					command={awaitCurl({ url, token: callback.token })}
					testid="await-callback-{callback.taskId}-cli"
					label="Callback command for {callback.taskId}"
				/>
			</li>
		{/each}
	</ul>
	<p class="text-xs text-fg-muted">
		The commands address <span class="font-mono">{origin}</span>, the address this page was opened at.
		If the engine's API is published somewhere else, post to that address instead. Reject with the same
		URL plus <span class="font-mono">/reject</span> and a body of
		<span class="font-mono">{'{"reason": "…"}'}</span>.
	</p>
	<div class="flex justify-end">
		<button
			type="button"
			data-testid="await-callbacks-continue"
			onclick={() => oncontinue()}
			class="inline-flex h-9 items-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs hover:bg-brand-700"
		>Continue to the run</button>
	</div>
</section>
