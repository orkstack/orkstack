<script lang="ts">
	// Sign in (UX35): one action per issuer that offers sign-in, each a plain
	// link to the engine's redirect, carrying the page the visitor was going
	// to. Why the last sign-in failed, when it did, comes first. In asserted
	// mode there is nothing to sign in to, and the page says so.
	import { invalidateAll } from '$app/navigation';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import Mark from '#lib/components/Mark.svelte';
	import { loginHref } from '#lib/signin.ts';
	import { issuerHost, problemOf } from '#lib/signinReasons.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const problem = $derived(problemOf(data.error));
	// Only an issuer whose login path makes a link is offered; none left is
	// the same as none at all.
	const offered = $derived(data.mode === 'asserted' ? [] : data.issuers.filter((i) => loginHref(i.loginPath, data.ret) !== ''));
	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}
</script>

<svelte:head><title>Sign in · OrkStack</title></svelte:head>

<main class="signin" data-testid="signin-page">
	<div class="signin-col">
		<p class="signin-brand"><Mark class="size-8" /><span>OrkStack</span></p>

		{#if data.mode === 'asserted'}
			<h1 class="page-h1">Sign-in is off</h1>
			<p class="signin-lede" data-testid="signin-off">
				This deployment does not verify who calls it. Every principal it records was asserted by your gateway.
			</p>
			<a class="btn" href="/">Go to Runs</a>
		{:else}
			<h1 class="page-h1">Sign in</h1>
			<p class="signin-lede">Use your organisation’s identity provider. You return to the page you asked for.</p>

			{#if problem}
				<div class="alert alert-fail" role="alert" data-testid="signin-problem">
					<span class="alert-ic"><Icon name="alert" /></span>
					<div class="min-w-0 flex-1">
						<p class="font-semibold">{problem.title}</p>
						<p class="text-ink-2">{problem.detail}</p>
					</div>
				</div>
			{/if}

			{#if data.configError}
				<ErrorPanel
					message={data.configError}
					subject="the ways to sign in"
					testid="signin-config-error"
					busy={retrying}
					onretry={retry}
				/>
			{:else if offered.length === 0}
				<p class="signin-empty" data-testid="signin-none">
					No identity provider offers sign-in on this deployment yet. An administrator of the deployment can set one up.
				</p>
			{:else}
				<ul class="signin-list" aria-label="Identity providers">
					{#each offered as issuer (issuer.name)}
						<li>
							<a class="btn signin-issuer" href={loginHref(issuer.loginPath, data.ret)} data-testid="signin-issuer-{issuer.name}">
								<span class="signin-issuer-name">Continue with {issuer.name}</span>
								<span class="signin-issuer-host">{issuerHost(issuer.issuerUrl)}</span>
							</a>
						</li>
					{/each}
				</ul>
			{/if}
		{/if}
	</div>
</main>

<style>
	.signin {
		display: grid;
		min-height: 100dvh;
		place-items: center;
		padding: var(--spacing-sheet) var(--spacing-gutter);
		background-color: var(--color-bg);
	}
	.signin-col {
		display: flex;
		flex-direction: column;
		gap: calc(var(--spacing) * 4);
		width: min(100%, 26rem);
	}
	.signin-brand {
		display: flex;
		align-items: center;
		gap: calc(var(--spacing) * 2.5);
		margin-bottom: calc(var(--spacing) * 4);
		font-size: var(--text-md);
		font-weight: var(--font-weight-semibold);
		color: var(--color-ink);
	}
	.signin-lede,
	.signin-empty {
		color: var(--color-ink-2);
		text-wrap: pretty;
	}
	.signin-list {
		display: flex;
		flex-direction: column;
		gap: calc(var(--spacing) * 2.5);
		margin-top: calc(var(--spacing) * 2);
	}
	.signin-issuer {
		display: flex;
		width: 100%;
		height: auto;
		min-height: calc(var(--spacing) * 13);
		flex-direction: column;
		align-items: flex-start;
		justify-content: center;
		gap: calc(var(--spacing) * 0.5);
		padding: calc(var(--spacing) * 2.5) calc(var(--spacing) * 4);
		text-align: left;
	}
	.signin-issuer-name {
		font-weight: var(--font-weight-semibold);
	}
	.signin-issuer-host {
		font-family: var(--font-mono);
		font-size: var(--text-xs);
		font-weight: var(--font-weight-normal);
		color: var(--color-ink-3);
	}
</style>
