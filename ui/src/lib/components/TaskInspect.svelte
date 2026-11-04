<script lang="ts">
	// What ran and what it produced, for the selected task: the definition half
	// (image, command, env, secret NAMES) comes from the Orkflow this run pinned,
	// the attempt half (exit code, commit) from the task's record, every try from
	// the attempt store, and its artifacts from the run's artifact list. A secret's
	// value never reaches this page: only names are rendered.
	import type { CacheResult, CommitRef } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { CacheOutcome } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import type { Payload, ArtifactRef } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { Task } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
	import type { TaskAttempt, TaskChild } from '#lib/gen/orkstack/v1/run_pb.ts';
	import { ArtifactKind, type RunArtifact } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { pinnedOrReference, shortDigest } from '#lib/activity.ts';
	import { cacheOutcomeLabel, cacheSourceHref, cacheSpecLabel, memoWriteSentence, missReasonSentence, sourceAttemptDuration } from '#lib/cache.ts';
	import { childLabel } from '#lib/compose.ts';
	import { artifactMetadata, inlinePayload, payloadJson } from '#lib/runDetail.ts';
	import { fmtTimestamp, isTerminal } from '#lib/status.ts';
	import type { TaskTiming } from '#lib/timing.ts';
	import ArtifactProvenance from './ArtifactProvenance.svelte';
	import AttemptTimeline from './AttemptTimeline.svelte';

	interface Props {
		runId: string;
		namespace: string;
		task: {
			id: string;
			activity: string;
			status: Status;
			attempt: number;
			pool: string;
			exitCode?: number;
			commit?: CommitRef;
			cache?: CacheResult;
			child?: TaskChild;
			output?: Payload;
			valuesRef?: ArtifactRef;
		};
		def?: Task;
		isAwait: boolean;
		isComposed: boolean;
		attempts: TaskAttempt[];
		attemptsSeeded: boolean;
		attemptsError: string | null;
		timing: TaskTiming;
		artifacts: RunArtifact[];
		downloading: string;
		onDownload: (record: RunArtifact) => void;
		onViewLogs: (attempt: number) => void;
		onCopyValues: () => void;
	}

	let {
		runId,
		namespace,
		task,
		def,
		isAwait,
		isComposed,
		attempts,
		attemptsSeeded,
		attemptsError,
		timing,
		artifacts,
		downloading,
		onDownload,
		onViewLogs,
		onCopyValues
	}: Props = $props();
</script>

<div class="p-4 pt-3" data-testid="task-{task.id}-inspect">
		<p class="text-xs break-all text-ink-muted" data-testid="task-{task.id}-activity">{task.activity || 'activity not yet known'}</p>
		<!--
			WHAT RAN, AND WHAT IT PRODUCED.

			The definition half (image, command, env, secret NAMES)
			comes from the Orkflow this run pinned; the attempt half
			(exit code, commit) comes from the TaskRun. They are shown
			together because a reader debugging a task needs both, and
			the pin — the resolved index digest — is the point: it is
			what makes this run explainable after a tag moves.

			SECRET VALUES ARE NEVER HERE. A SecretRef carries a name
			and a SOURCE; the value is resolved on the Orker, into the
			container, and never travels to the engine or this page.
			Only names are rendered, and nothing renders `source`.
		-->
		{#if isComposed}
			<dl
				class="mt-3 grid grid-cols-1 gap-x-6 gap-y-2 border-t border-rule-faint pt-3 text-xs"
				data-testid="task-{task.id}-detail"
			>
				<div>
					<dt class="caps text-ink-muted">Child Orkflow</dt>
					<dd class="mt-0.5 font-mono break-all text-ink-secondary" data-testid="task-{task.id}-child-ref">
						{childLabel(task.child)}
					</dd>
				</div>
				<div>
					<dt class="caps text-ink-muted">Definition digest</dt>
					<dd class="mt-0.5 font-mono break-all text-ink-secondary" data-testid="task-{task.id}-child-digest">
						{task.child?.orkflowDefinitionDigest || '—'}
					</dd>
				</div>
			</dl>
		{:else if !isAwait && (def || task.exitCode !== undefined || task.commit)}
			<dl
				class="mt-3 grid grid-cols-1 gap-x-6 gap-y-2 border-t border-rule-faint pt-3 text-xs sm:grid-cols-2"
				data-testid="task-{task.id}-detail"
			>
				{#if def?.image}
					<div class="sm:col-span-2">
						<dt class="caps text-ink-muted">Image</dt>
						<dd
							class="mt-0.5 font-mono break-all text-ink-secondary"
							data-testid="task-{task.id}-image"
						>
							{pinnedOrReference(def.image) || '—'}
						</dd>
						{#if def.image.indexDigest}
							<dd class="mt-0.5">
								<span
									class="font-mono text-blue"
									data-tip={def.image.indexDigest}
									data-digest={def.image.indexDigest}
									data-testid="task-{task.id}-image-digest"
									>{shortDigest(def.image.indexDigest)}</span
								>
							</dd>
						{/if}
					</div>
				{/if}
				{#if def && def.command.length > 0}
					<div class="sm:col-span-2">
						<dt class="caps text-ink-muted">Command</dt>
						<dd
							class="mt-0.5 font-mono break-all text-ink-secondary"
							data-testid="task-{task.id}-command"
						>
							{JSON.stringify(def.command)}
						</dd>
					</div>
				{/if}
				{#if def && Object.keys(def.env).length > 0}
					<div class="sm:col-span-2">
						<dt class="caps text-ink-muted">Environment</dt>
						<!--
							Env is shown as NAME=VALUE. It is
							workload-supplied, non-secret configuration by
							construction: the platform puts nothing here
							(Task.env's own contract), and anything that
							must not be read travels as a `secrets:` entry,
							whose value is resolved on the Orker and never
							reaches the engine at all.
						-->
						<dd
							class="mt-0.5 font-mono break-all text-ink-secondary"
							data-testid="task-{task.id}-env"
						>
							{Object.entries(def.env)
								.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
								.map(([k, v]) => `${k}=${v}`)
								.join('  ')}
						</dd>
					</div>
				{/if}
				{#if def && def.secrets.length > 0}
					<div>
						<dt class="caps text-ink-muted">Secrets</dt>
						<dd
							class="mt-0.5 font-mono text-ink-secondary"
							data-tip="names only — a secret's value is resolved on the Orker and never reaches this page"
							data-testid="task-{task.id}-secrets"
						>
							{def.secrets.map((s) => s.name).join(', ')}
						</dd>
					</div>
				{/if}
				{#if def?.cache}
					<div>
						<dt class="caps text-ink-muted">Cache</dt>
						<dd class="mt-0.5 text-ink-secondary" data-testid="task-{task.id}-cache-spec">
							{cacheSpecLabel(def.cache)}
						</dd>
					</div>
				{/if}
				{#if task.cache}
					<div class="sm:col-span-2" data-testid="task-{task.id}-cache-detail">
						<dt class="caps text-ink-muted">Cache decision</dt>
						<dd class="mt-0.5 text-ink-secondary">{cacheOutcomeLabel(task.cache.outcome)}</dd>
						{#if task.cache.key}
							<dd class="mt-0.5 font-mono break-all text-ink-muted" data-testid="task-{task.id}-cache-key">
								{task.cache.key}
							</dd>
						{/if}
						{#if task.cache.outcome === CacheOutcome.HIT}
							<dd class="mt-0.5">
								Reused the result of run
								<a
									class="link font-mono"
									href={cacheSourceHref(namespace, task.cache.source)}
									data-testid="task-{task.id}-cache-detail-source">{task.cache.source?.runId}</a
								>, produced {fmtTimestamp(task.cache.source?.finishedAt)}{#if sourceAttemptDuration(task.cache.source)}
									(its attempt took {sourceAttemptDuration(task.cache.source)}){/if}.
							</dd>
							<dd
								class="mt-0.5 font-mono break-all text-ink-muted"
								data-testid="task-{task.id}-cache-source-commit"
							>
								{task.cache.source?.commitUri}
							</dd>
							<dd class="mt-0.5 text-ink-muted">
								stdout.log and stderr.log below are the producing run's; this
								attempt's own log is the cache line in the log pane.
							</dd>
						{:else}
							{#if task.cache.outcome === CacheOutcome.MISS}
								<dd class="mt-0.5" data-testid="task-{task.id}-cache-reason">
									{missReasonSentence(task.cache.missReason, task.cache.missDetail)}
								</dd>
							{/if}
							{#if memoWriteSentence(task.cache)}
								<dd class="mt-0.5 text-ink-muted" data-testid="task-{task.id}-cache-memo">
									{memoWriteSentence(task.cache)}
								</dd>
							{/if}
						{/if}
					</div>
				{/if}
				<div>
					<dt class="caps text-ink-muted">Exit code</dt>
					<dd
						class="mt-0.5 font-mono {task.exitCode === undefined
							? 'text-ink-muted'
							: task.exitCode === 0
								? 'text-ink-secondary'
								: 'text-failed-fg'}"
						data-state={task.exitCode === undefined ? 'unknown' : 'measured'}
						data-testid="task-{task.id}-exit-code"
					>
						{task.exitCode === undefined ? 'Not measured' : task.exitCode}
					</dd>
				</div>
				{#if task.commit?.uri}
					<div class="sm:col-span-2">
						<dt class="caps text-ink-muted">Commit</dt>
						<dd
							class="mt-0.5 font-mono break-all text-ink-muted"
							data-testid="task-{task.id}-commit"
						>
							{task.commit.uri}
						</dd>
					</div>
				{/if}
			</dl>
		{/if}
		<!--
			WHAT EVERY TRY DID. The block above describes the
			task's FINAL attempt — its exit code, its commit —
			because that is all a TaskRun carries. The timeline
			is the rest of the story: the tries that failed, why
			they failed, and which Orker was holding each one.
			It is drawn only when the store had something to
			say; a deployment with no attempt store shows the
			page-level notice instead of an empty disclosure in
			every card.
		-->
		{#if attemptsSeeded && (!attemptsError || attempts.length > 0)}
			<AttemptTimeline
				runId={runId}
				taskId={task.id}
				namespace={namespace}
				attempts={attempts}
				pool={task.pool}
				decidedOn={task.attempt}
				{timing}
				onViewLogs={(attempt) => onViewLogs(attempt)}
			/>
		{/if}
		<!--
			Gated on their own presence, not on the definition
			block above: a task that produced artifacts and has
			no pinned definition still lists them.
		-->
		{#if artifacts.length > 0}
			<div class="mt-3 border-t border-rule-faint pt-3" data-testid="task-artifacts-{task.id}">
				<h4 class="caps text-ink-muted">Artifacts</h4>
				{#each artifacts as record (record.kind + ':' + (record.artifact?.name ?? ''))}
					{#if record.artifact}
						{@const key = `${record.taskId}:${record.kind}:${record.artifact.name}`}
						<div class="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-rule-faint py-2 text-xs last:border-b-0">
							<span class="font-medium text-ink">{ArtifactKind[record.kind] ?? 'UNSPECIFIED'}</span>
							<span class="font-mono break-all text-ink-secondary">{record.artifact.name}</span>
							<span class="text-ink-muted">{record.artifact.contentType || 'unknown type'} · {record.artifact.sizeBytes.toString()} bytes</span>
							<button type="button" data-testid="download-artifact-{task.id}-{record.artifact.name}" onclick={() => onDownload(record)} disabled={downloading === key} class="link ml-auto font-medium disabled:opacity-50">{downloading === key ? 'Downloading…' : 'Download'}</button>
							<ArtifactProvenance runId={runId} namespace={namespace} taskId={task.id} kind={record.kind} name={record.artifact.name} />
							<details class="w-full min-w-0 text-ink-muted"><summary>Metadata</summary><pre class="mt-1 overflow-auto font-mono">{JSON.stringify(artifactMetadata(record.artifact), null, 2)}</pre></details>
						</div>
					{/if}
				{/each}
			</div>
		{/if}
	
{#if task.output || task.valuesRef}
	<div class="mt-3 border-t border-rule-faint pt-3">
		<div class="flex items-center justify-between gap-3">
			<span class="text-xs font-semibold text-ink-2">Values</span>
			{#if task.output && inlinePayload(task.output) === null}<button type="button" data-testid="copy-task-values-{task.id}" onclick={() => onCopyValues()} class="link text-xs">Copy JSON</button>{/if}
		</div>
		{#if task.output && inlinePayload(task.output) !== null}
			<code data-testid="task-values-{task.id}" class="mt-1 block font-mono text-sm break-normal text-ink-2">{inlinePayload(task.output)}</code>
		{:else}
			<pre data-testid="task-values-{task.id}" class="mt-1 max-h-40 overflow-auto font-mono text-xs text-ink-2">{task.output ? payloadJson(task.output) : isTerminal(task.status) ? 'This task finished without inline values. Its recorded values artifact, if present, is listed under Inspect.' : 'Task values are not available yet.'}</pre>
		{/if}
	</div>
{/if}
</div>
