<script lang="ts">
	// Access → Explain (UX35): pick a principal, a permission and a resource;
	// the server's decision and the binding behind it, with every binding it
	// weighed and why each did or did not grant.
	import { untrack } from 'svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { accessClient } from '#lib/clients/access.ts';
	import { AccessDecision, BindingMatch } from '#lib/gen/orkstack/v1/access_pb.ts';
	import type { ExplainAccessResponse } from '#lib/gen/orkstack/v1/access_service_pb.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	let principal = $state(untrack(() => data.me?.principalId ?? ''));
	let permission = $state('run.read');
	let namespace = $state('');
	let orkflow = $state('');
	let pool = $state('');
	let busy = $state(false);
	let error = $state('');
	let answer = $state<ExplainAccessResponse | null>(null);

	const matchWords: Record<number, string> = {
		[BindingMatch.GRANTED]: 'Granted it',
		[BindingMatch.SUBJECT_DIFFERS]: 'Names someone else',
		[BindingMatch.SCOPE_DIFFERS]: 'At another scope',
		[BindingMatch.ROLE_LACKS_PERMISSION]: 'Its role lacks the permission',
		[BindingMatch.PINNED_ELSEWHERE]: 'Pinned to another incarnation'
	};

	async function explain() {
		busy = true;
		error = '';
		try {
			answer = await accessClient(fetch).explainAccess({ principalId: principal.trim(), permission, namespace: namespace.trim(), orkflow: namespace.trim() ? orkflow.trim() : '', pool: pool.trim() });
		} catch (err) {
			answer = null;
			error = errorMessage(err);
		} finally {
			busy = false;
		}
	}
	const command = $derived(
		['orkctl admin access explain', principal || 'PRINCIPAL', permission, namespace && `-n ${namespace}`, orkflow && `--orkflow ${orkflow}`, pool && `--pool ${pool}`].filter(Boolean).join(' ')
	);
</script>

<svelte:head><title>Explain · Access · OrkStack</title></svelte:head>

<SheetHeader title="Explain" testid="explain-title" commands={[{ label: 'Explain this decision', command }]} />

{#if data.permissionsError}
	<ErrorPanel message={data.permissionsError} subject="the permissions" testid="explain-error" />
{:else}
	<form class="card mb-5 grid max-w-4xl gap-4 p-5 md:grid-cols-2" onsubmit={(e) => { e.preventDefault(); explain(); }} data-testid="explain-form">
		<label class="flex flex-col gap-1 md:col-span-2">
			<span class="text-sm font-semibold text-ink-2">Principal</span>
			<input class="field font-mono" bind:value={principal} required placeholder="user:corp:ana@example.com" data-testid="explain-principal" />
		</label>
		<label class="flex flex-col gap-1 md:col-span-2">
			<span class="text-sm font-semibold text-ink-2">Permission</span>
			<select class="field font-mono" bind:value={permission} data-testid="explain-permission">
				{#each data.permissions as p (p.name)}<option value={p.name}>{p.name}</option>{/each}
			</select>
		</label>
		<label class="flex flex-col gap-1"><span class="text-sm font-semibold text-ink-2">Namespace</span><input class="field font-mono" bind:value={namespace} placeholder="The deployment" data-testid="explain-namespace" /></label>
		<label class="flex flex-col gap-1"><span class="text-sm font-semibold text-ink-2">Orkflow</span><input class="field font-mono" bind:value={orkflow} disabled={!namespace} data-testid="explain-orkflow" /></label>
		<label class="flex flex-col gap-1"><span class="text-sm font-semibold text-ink-2">Pool</span><input class="field font-mono" bind:value={pool} data-testid="explain-pool" /></label>
		<div class="flex items-end justify-end"><button type="submit" class="btn" disabled={busy || !principal.trim()} data-testid="explain-run">{busy ? 'Asking…' : 'Explain'}</button></div>
	</form>

	{#if error}<p class="text-fail" role="alert" data-testid="explain-refused">{error}</p>{/if}
	{#if answer}
		<section class="card" aria-live="polite" data-testid="explain-answer">
			<div class="card-head">
				<h2>
					{#if answer.decision === AccessDecision.ALLOWED}<span class="tag" data-testid="explain-allowed">Allowed</span>{:else}<span class="stamp stamp-sm pill-outline" data-testid="explain-denied">Denied</span>{/if}
					<span class="ml-2 text-ink-2">against revision <span class="tabular-nums">{answer.accessRevision}</span></span>
				</h2>
			</div>
			<div class="flex flex-col gap-3 px-4 pb-4">
				<p>{answer.explanation}</p>
				{#if answer.reason}<p class="mt-1 text-xs text-ink-3"><code data-testid="explain-reason">{answer.reason}</code></p>{/if}
				{#if answer.considered.length > 0}
					<table class="register">
						<thead><tr><th>Binding</th><th>Role</th><th>Scope</th><th>Subject that matched</th><th>Outcome</th></tr></thead>
						<tbody>
							{#each answer.considered as c (c.binding)}
								<tr data-testid="explain-considered-{c.binding}">
									<td class="font-mono">{c.binding}</td><td>{c.role}</td><td class="font-mono">{c.scope}</td><td class="font-mono">{c.subject || '—'}</td><td>{matchWords[c.match] ?? ''}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				{/if}
			</div>
		</section>
	{/if}
{/if}
