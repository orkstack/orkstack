<script lang="ts">
	// Image supply-chain policy (proto/orkstack/v1/service.proto's
	// GetImagePolicy/UpdateImagePolicy): the deployment's floor on which
	// images may be applied and run — whether a `uses:` may be a mutable tag,
	// which registries images may come from, and which cosign public keys a
	// namespace may require by name (Namespace.require_signed_by, set on
	// /admin/namespaces). A namespace may tighten this floor and never loosen
	// it; every label here says so.
	//
	// The form sends the WHOLE document, as `orkctl admin image-policy set`
	// does after its read. A refusal (a list that excludes a catalog
	// activity, a key a namespace still names, a key that is not P-256) is
	// the engine's own sentence, shown verbatim.
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage } from '$lib/client';
	import { systemClient } from '$lib/clients/system';
	import {
		fingerprint,
		parseRegistryLines,
		registryPrefixesError,
		signingKeyNameError,
		signingKeyPemError,
		MAX_SIGNING_KEYS
	} from '$lib/imagePolicyForm';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);

	const policy = $derived(data.imagePolicy);
	const keyNames = $derived(Object.keys(policy?.signingKeys ?? {}).sort());

	// Fingerprints are computed in the browser (crypto.subtle), rendered the
	// way orkctl renders them: "sha256:<64 hex>" over the key's DER.
	let fingerprints = $state<Record<string, string>>({});
	$effect(() => {
		const keys = policy?.signingKeys ?? {};
		const next: Record<string, string> = {};
		void Promise.all(
			Object.entries(keys).map(async ([name, pem]) => {
				next[name] = (await fingerprint(pem)) ?? '(unparseable key)';
			})
		).then(() => {
			fingerprints = next;
		});
	});

	// ── the form ────────────────────────────────────────────────────────
	// Prefilled from what is stored every time the page (re)loads, so Save
	// always sends an edit of the current document.
	let requireDigest = $state(false);
	let registriesText = $state('');
	let formKeys = $state<Record<string, string>>({});
	let newKeyName = $state('');
	let newKeyPem = $state('');
	let formError = $state('');
	let setBusy = $state(false);
	let setError = $state('');
	let setSuccess = $state('');

	$effect(() => {
		requireDigest = policy?.requireDigest ?? false;
		registriesText = (policy?.allowedRegistries ?? []).join('\n');
		formKeys = { ...(policy?.signingKeys ?? {}) };
	});
	const formKeyNames = $derived(Object.keys(formKeys).sort());

	function removeKey(name: string) {
		const next = { ...formKeys };
		delete next[name];
		formKeys = next;
	}

	function addKey() {
		formError = '';
		const name = newKeyName.trim();
		const err = signingKeyNameError(name) ?? signingKeyPemError(newKeyPem);
		if (err) {
			formError = err;
			return;
		}
		if (!(name in formKeys) && Object.keys(formKeys).length >= MAX_SIGNING_KEYS) {
			formError = `a deployment holds at most ${MAX_SIGNING_KEYS} signing keys`;
			return;
		}
		formKeys = { ...formKeys, [name]: newKeyPem.trim() + '\n' };
		newKeyName = '';
		newKeyPem = '';
	}

	async function save() {
		formError = '';
		setError = '';
		setSuccess = '';
		const registries = parseRegistryLines(registriesText);
		const err = registryPrefixesError(registries);
		if (err) {
			// A prefilter: no request is made for an entry the engine would
			// refuse anyway.
			formError = err;
			return;
		}
		if (newKeyName.trim() !== '' || newKeyPem.trim() !== '') {
			formError = 'finish adding the signing key (Add key) or clear its fields before saving';
			return;
		}
		setBusy = true;
		try {
			const res = await systemClient(fetch).updateImagePolicy({
				policy: { requireDigest, allowedRegistries: registries, signingKeys: formKeys }
			});
			const stored = res.policy;
			setSuccess =
				`Image policy saved: require digest ${stored?.requireDigest ? 'yes' : 'no'}, ` +
				`${stored?.allowedRegistries.length ? stored.allowedRegistries.length + ' registry prefix(es)' : 'every registry'}, ` +
				`${Object.keys(stored?.signingKeys ?? {}).length} signing key(s).`;
			await invalidateAll();
		} catch (e) {
			setError = errorMessage(e);
		} finally {
			setBusy = false;
		}
	}

	const inputClass =
		'rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none';
</script>

<svelte:head><title>Admin — Image policy — OrkStack</title></svelte:head>

<p class="mb-4 text-sm text-fg-muted" data-testid="image-policy-scope-caption">
	Deployment-wide: the namespace selector above does not scope it. A namespace may tighten this on
	<code>/admin/namespaces</code> and never loosen it.
</p>

{#if data.imagePolicyError}
	<ErrorPanel
		testid="image-policy-error"
		retryTestid="image-policy-retry"
		subject="this deployment's image policy"
		message={data.imagePolicyError}
		busy={retrying}
		onretry={retry}
	/>
{:else if busy}
	<p class="text-sm text-fg-muted" data-testid="image-policy-loading">Loading image policy…</p>
{:else}
	<section class="mb-8" data-testid="image-policy-info">
		<div class="overflow-hidden rounded-lg border border-border bg-surface shadow-xs">
			<dl class="divide-y divide-border-subtle text-sm">
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Require digest</dt>
					<dd class="text-fg" data-testid="image-policy-require-digest">
						{policy?.requireDigest ? 'Yes' : 'No'}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Allowed registries</dt>
					<dd class="font-mono text-xs text-fg" data-testid="image-policy-allowed-registries">
						{#if policy?.allowedRegistries.length}
							{#each policy.allowedRegistries as registry (registry)}
								<div>{registry}</div>
							{/each}
						{:else}
							<span class="font-sans text-sm">Unrestricted</span>
						{/if}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Signing keys</dt>
					<dd class="font-mono text-xs text-fg" data-testid="image-policy-signing-keys">
						{#if keyNames.length}
							{#each keyNames as name (name)}
								<div data-testid="image-policy-signing-key-{name}">
									{name}&nbsp;&nbsp;{fingerprints[name] ?? '…'}
								</div>
							{/each}
						{:else}
							<span class="font-sans text-sm">None</span>
						{/if}
					</dd>
				</div>
				<div class="grid grid-cols-[10rem_1fr] px-4 py-2.5">
					<dt class="text-fg-muted">Updated</dt>
					<dd class="text-fg" data-testid="image-policy-updated">
						{policy?.updatedAt
							? fmtTimestamp(policy.updatedAt)
							: 'Never — no policy stored; every image is permitted'}
					</dd>
				</div>
			</dl>
		</div>
		<p class="mt-2 text-xs text-fg-muted">
			Stored with the deployment, so every replica answers the same — see
			<code>orkctl admin image-policy get</code>. Refused at validate, apply and submit, and by the
			Orker before it pulls; a run keeps the policy it was submitted under.
		</p>
	</section>

	<section class="rounded-lg border border-border bg-surface p-5 shadow-xs">
		<h2 class="mb-3 text-sm font-semibold text-fg">Change the policy</h2>
		<form
			class="grid gap-4 sm:grid-cols-2"
			data-testid="image-policy-form"
			onsubmit={(e) => {
				e.preventDefault();
				void save();
			}}
		>
			<label class="flex items-center gap-2 text-sm sm:col-span-2">
				<input
					type="checkbox"
					data-testid="image-policy-form-require-digest"
					bind:checked={requireDigest}
				/>
				<span class="font-medium text-fg">Require digest</span>
				<span class="text-xs text-fg-muted"
					>— refuse a <code>uses:</code> written as a mutable tag; catalog names are exempt</span
				>
			</label>
			<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
				<span class="font-medium text-fg"
					>Allowed registries (one prefix per line, blank = unrestricted)</span
				>
				<textarea
					data-testid="image-policy-form-allowed-registries"
					bind:value={registriesText}
					rows="3"
					placeholder="ghcr.io/acme/"
					spellcheck="false"
					class="{inputClass} py-2 font-mono text-xs"
				></textarea>
			</label>
			<div class="flex flex-col gap-1.5 text-sm sm:col-span-2">
				<span class="font-medium text-fg">Signing keys</span>
				{#if formKeyNames.length === 0}
					<span class="text-xs text-fg-muted">No keys.</span>
				{/if}
				{#each formKeyNames as name (name)}
					<div class="flex items-center gap-3 font-mono text-xs">
						<span>{name}</span>
						<button
							type="button"
							data-testid="image-policy-form-remove-key-{name}"
							onclick={() => removeKey(name)}
							class="rounded-md border border-border px-2 py-0.5 font-sans text-xs text-fg-secondary hover:bg-surface-sunken"
						>
							Remove
						</button>
					</div>
				{/each}
			</div>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Add signing key — name</span>
				<input
					data-testid="image-policy-form-key-name"
					bind:value={newKeyName}
					placeholder="release"
					class="{inputClass} h-9"
				/>
			</label>
			<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
				<span class="font-medium text-fg">Public key (PEM, cosign.pub)</span>
				<textarea
					data-testid="image-policy-form-key-pem"
					bind:value={newKeyPem}
					rows="4"
					placeholder="-----BEGIN PUBLIC KEY-----"
					spellcheck="false"
					class="{inputClass} py-2 font-mono text-xs"
				></textarea>
			</label>
			<div class="flex gap-2 sm:col-span-2">
				<button
					type="button"
					data-testid="image-policy-form-add-key"
					onclick={addKey}
					class="inline-flex h-9 items-center justify-center rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs hover:bg-surface-sunken"
				>
					Add key
				</button>
				<button
					type="submit"
					data-testid="image-policy-form-submit"
					disabled={setBusy}
					class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
				>
					{setBusy ? 'Saving…' : 'Save'}
				</button>
			</div>
		</form>
		{#if formError}
			<p
				class="mt-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
				data-testid="image-policy-form-error"
			>
				{formError}
			</p>
		{/if}
		{#if setError}
			<p
				class="mt-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
				data-testid="image-policy-set-error"
			>
				{setError}
			</p>
		{/if}
		{#if setSuccess}
			<p class="mt-3 text-sm text-succeeded-fg" data-testid="image-policy-set-success">
				{setSuccess}
			</p>
		{/if}
		<p class="mt-3 text-xs text-fg-muted">
			A list that would exclude an activity of the engine's catalog is refused, and so is removing a
			key a namespace still names. Definitions already stored are re-checked at their next submit.
		</p>
	</section>
{/if}
