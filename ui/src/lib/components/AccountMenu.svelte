<script lang="ts">
	// The account menu (UX35): who you are signed in as, by which issuer,
	// your roles by scope, and Sign out. It names the verified principal with
	// its issuer, never as an asserted one.
	import Popover from './Popover.svelte';
	import { errorMessage } from '#lib/client.ts';
	import { authClient } from '#lib/clients/auth.ts';
	import { endSessionTarget } from '#lib/signin.ts';
	import type { GetSigninResponse } from '#lib/gen/orkstack/v1/auth_service_pb.ts';

	let { me }: { me: GetSigninResponse } = $props();
	let open = $state(false);
	let leaving = $state(false);
	let error = $state('');
	const roles = $derived(me.bindings.map((b) => ({ role: b.role, scope: b.scope })));
	const initial = $derived((me.display || me.principalId).replace(/^[a-z]+:[^:]*:/, '').charAt(0).toUpperCase());

	async function signOut() {
		leaving = true;
		error = '';
		try {
			const out = await authClient(fetch).signOut({});
			window.location.assign(endSessionTarget(out.endSessionUrl));
		} catch (err) {
			error = errorMessage(err);
			leaving = false;
		}
	}
</script>

<Popover {open} onOpenChange={(v) => (open = v)} testid="account-menu" align="start">
	{#snippet trigger()}
		<button
			type="button"
			class="flex w-full items-center gap-2.5 rounded-sm px-2.5 py-1.75 text-left text-side-strong hover:bg-side-hover"
			aria-expanded={open}
			onclick={() => (open = !open)}
			data-testid="account-trigger"
		>
			<span aria-hidden="true" class="grid size-6.5 shrink-0 place-items-center rounded-sm border border-side-field-line bg-side-field text-sm font-bold text-side-strong">{initial}</span>
			<span class="min-w-0 flex-1 leading-tight"
				><b class="block truncate font-semibold">{me.display || me.principalId}</b><small class="block text-sm text-side-ink-2">{me.issuer}</small></span
			>
		</button>
	{/snippet}
	{#snippet content()}
		<div style:width="min(20rem, 80vw)" data-testid="account-panel">
			<p class="text-xs font-semibold text-ink-3">Signed in as</p>
			<p class="font-mono text-sm break-all" data-testid="account-principal">{me.principalId}</p>
			<!-- The same tag every principal carries (Principal.svelte), so a verified sign-in reads alike everywhere. -->
			<p class="mt-1 flex flex-wrap items-center gap-1.5 text-sm text-ink-2">
				<span class="atag atag-verified">verified</span><span>by OrkStack ({me.method}), from {me.issuer}</span>
			</p>
			<p class="mt-3 text-xs font-semibold text-ink-3">Roles</p>
			{#if roles.length === 0}
				<p class="text-sm text-ink-3">None</p>
			{:else}
				<ul class="text-sm" data-testid="account-roles">
					{#each roles as r (r.role + r.scope)}<li>{r.role} <span class="text-ink-3">at</span> <span class="font-mono">{r.scope}</span></li>{/each}
				</ul>
			{/if}
			{#if me.signin}
				<button type="button" class="btn btn-sm mt-3 w-full" onclick={signOut} disabled={leaving} data-testid="account-signout">{leaving ? 'Signing out…' : 'Sign out'}</button>
			{/if}
			{#if error}<p class="mt-2 text-sm text-fail" role="alert">{error}</p>{/if}
		</div>
	{/snippet}
</Popover>
