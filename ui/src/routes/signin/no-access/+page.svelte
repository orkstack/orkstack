<script lang="ts">
	// No access (UX35): signed in, bound to nothing. Who OrkStack sees, as it
	// sees it, so the person can tell an administrator exactly what to bind;
	// and the way out.
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Mark from '#lib/components/Mark.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { authClient } from '#lib/clients/auth.ts';
	import { MANUAL_COPY_MESSAGE, copyText, selectByTestid } from '#lib/clipboard.ts';
	import { endSessionTarget } from '#lib/signin.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	let leaving = $state(false);
	let leaveError = $state('');
	let copied = $state('');

	// What an administrator needs to bind this person: the three facts, as
	// lines they can paste into a message.
	// Each value on its own line, whatever the identity provider put in it.
	const one = (v: string) => v.replace(/\s+/g, ' ').trim();
	const details = $derived(
		data.me ? [`Principal: ${one(data.me.principalId)}`, `Signed in with: ${one(data.me.issuer)}`, `Groups: ${data.me.groups.map(one).join(', ') || 'none'}`].join('\n') : ''
	);
	async function copyDetails() {
		if ((await copyText(details)) === 'copied') copied = 'Copied.';
		else {
			selectByTestid('no-access-who');
			copied = MANUAL_COPY_MESSAGE;
		}
	}

	async function signOut() {
		leaving = true;
		leaveError = '';
		try {
			const out = await authClient(fetch).signOut({});
			window.location.assign(endSessionTarget(out.endSessionUrl));
		} catch (err) {
			leaveError = errorMessage(err);
		} finally {
			leaving = false;
		}
	}
</script>

<svelte:head><title>No access · OrkStack</title></svelte:head>

<main class="noaccess" data-testid="no-access-page">
	<div class="noaccess-col">
		<p class="noaccess-brand"><Mark class="size-8" /><span>OrkStack</span></p>
		<h1 class="page-h1">You are signed in, with no access yet</h1>
		<p class="text-ink-2">
			No role in this deployment names you or a group you are in. An administrator of the deployment, or of a namespace, can
			bind one to you. Send them what OrkStack sees:
		</p>

		{#if data.meError}
			<ErrorPanel message={data.meError} subject="who you are signed in as" testid="no-access-error" />
		{:else if data.me}
			<dl class="card noaccess-facts" data-testid="no-access-who">
				<div>
					<dt>Principal</dt>
					<dd class="font-mono">{data.me.principalId}</dd>
				</div>
				<div>
					<dt>Signed in with</dt>
					<dd>{data.me.issuer}</dd>
				</div>
				<div>
					<dt>Groups</dt>
					<dd>
						{#if data.me.groups.length === 0}
							<span class="text-ink-3">None</span>
						{:else}
							<ul class="noaccess-groups">
								{#each data.me.groups as g (g)}<li class="font-mono">{g}</li>{/each}
							</ul>
						{/if}
					</dd>
				</div>
			</dl>
		{/if}

		<div class="flex flex-wrap items-center gap-3">
			{#if data.me}
				<button class="btn" type="button" onclick={copyDetails} data-testid="no-access-copy">Copy these details</button>
			{/if}
			<button class="btn" type="button" onclick={signOut} disabled={leaving} data-testid="no-access-signout">
				{leaving ? 'Signing out…' : 'Sign out, to use another account'}
			</button>
			<span class="text-sm text-ink-2" role="status" data-testid="no-access-copied">{copied}</span>
			{#if leaveError}<p class="text-fail" role="alert">{leaveError}</p>{/if}
		</div>
	</div>
</main>

<style>
	.noaccess {
		display: grid;
		min-height: 100dvh;
		place-items: center;
		padding: var(--spacing-sheet) var(--spacing-gutter);
		background-color: var(--color-bg);
	}
	.noaccess-col {
		display: flex;
		flex-direction: column;
		gap: calc(var(--spacing) * 4);
		width: min(100%, 34rem);
	}
	.noaccess-brand {
		display: flex;
		align-items: center;
		gap: calc(var(--spacing) * 2.5);
		margin-bottom: calc(var(--spacing) * 4);
		font-size: var(--text-md);
		font-weight: var(--font-weight-semibold);
	}
	.noaccess-facts {
		display: grid;
		gap: calc(var(--spacing) * 3);
		padding: calc(var(--spacing) * 4);
	}
	.noaccess-facts dt {
		font-size: var(--text-xs);
		font-weight: var(--font-weight-semibold);
		color: var(--color-ink-3);
	}
	.noaccess-facts dd {
		overflow-wrap: anywhere;
		font-variant-numeric: tabular-nums;
	}
	.noaccess-groups {
		display: flex;
		flex-wrap: wrap;
		gap: calc(var(--spacing) * 1) calc(var(--spacing) * 3);
	}
</style>
