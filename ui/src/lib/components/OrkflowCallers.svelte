<script lang="ts">
	// Callers: what in this namespace starts or composes this orkflow, and the
	// version each one pins — `orkctl orkflow callers`, in the same columns and
	// words. It is read here, on mount and when "every version" changes, not
	// in the page loader: the engine answers it with a scan of the namespace,
	// which not every tab should pay for (interface-compatibility-gate §7.5).
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import {
		callerHref,
		callerKindWord,
		callerLabel,
		callersCommand,
		callersFooter,
		pinsLabel
	} from '$lib/compat';
	import { OrkflowCallerKind, type ListOrkflowCallersResponse } from '$lib/gen/orkstack/v1/service_pb';
	import { namespaceLabel } from '$lib/namespace';

	interface Props {
		name: string;
		namespace: string;
	}

	let { name, namespace }: Props = $props();

	let allVersions = $state(false);
	let loading = $state(true);
	let error = $state('');
	let response = $state<ListOrkflowCallersResponse | null>(null);
	let sequence = 0;

	async function load() {
		const mine = ++sequence;
		loading = true;
		error = '';
		try {
			const res = await orkstack(fetch).orkflows.listOrkflowCallers({ name, namespace, allVersions });
			if (mine !== sequence) return;
			response = res;
		} catch (err) {
			if (mine !== sequence) return;
			error = errorMessage(err);
			response = null;
		} finally {
			if (mine === sequence) loading = false;
		}
	}

	$effect(() => {
		name;
		namespace;
		allVersions;
		void load();
	});

	const rowTestid = (c: ListOrkflowCallersResponse['callers'][number]) => {
		switch (c.kind) {
			case OrkflowCallerKind.ORKFLOW:
				return `caller-orkflow-${c.name}-${c.callerVersion}-${c.taskId}`;
			case OrkflowCallerKind.TRIGGER:
				return `caller-trigger-${c.id}`;
			default:
				return `caller-schedule-${c.name}`;
		}
	};
</script>

<section class="mt-10" data-testid="orkflow-callers">
	<div class="mb-3 flex items-center justify-between gap-4">
		<div class="flex items-center gap-4">
			<h3 class="text-sm font-semibold text-fg">Callers</h3>
			<label class="inline-flex items-center gap-2 text-xs text-fg-secondary">
				<input
					type="checkbox"
					data-testid="callers-all-versions"
					bind:checked={allVersions}
					class="accent-brand-600"
				/>
				Include every version of each Orkflow
			</label>
		</div>
		<CliEquivalent
			command={callersCommand(namespace, name, allVersions)}
			testid="callers-cli"
			label="List the callers of {name} from the CLI"
		/>
	</div>

	{#if loading}
		<p data-testid="callers-loading" class="text-sm text-fg-muted">Scanning the namespace for callers…</p>
	{:else if error}
		<ErrorPanel
			testid="callers-error"
			retryTestid="callers-retry"
			subject="the callers of {name} in namespace {namespaceLabel(namespace)}"
			message={error}
			onretry={load}
		/>
	{:else if response}
		{#if response.storedVersions === 0}
			<p data-testid="callers-deleted" class="mb-3 text-sm text-fg-secondary">
				No version of {name} is stored in namespace {namespaceLabel(namespace)}; these still reference it.
			</p>
		{/if}
		{#if response.callers.length === 0}
			<p data-testid="callers-empty" class="text-sm text-fg-muted">
				Nothing in namespace {namespaceLabel(namespace)} references {name}: no orkflow composes it and
				no trigger or schedule starts it.
			</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
				<table class="w-full min-w-[720px] border-collapse text-left text-sm" data-testid="callers-table">
					<thead class="bg-surface-sunken">
						<tr>
							<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Kind</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Caller</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Version</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Task</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Pins</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Reads</th>
							<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Supplies</th>
						</tr>
					</thead>
					<tbody>
						{#each response.callers as caller, index (index)}
							<tr class="border-t border-border-subtle" data-testid={rowTestid(caller)}>
								<td class="h-11 px-4 pl-5 align-middle text-fg-secondary">{callerKindWord(caller)}</td>
								<td class="h-11 px-4 align-middle">
									<a
										href={callerHref(caller, name, namespace)}
										class="font-medium text-brand-700 hover:underline"
										title={caller.description || undefined}>{callerLabel(caller)}</a
									>
								</td>
								<td class="h-11 px-4 align-middle text-fg-secondary">
									{caller.callerVersion
										? `${caller.callerVersion}${response.allVersions && caller.callerVersionIsCurrent ? '*' : ''}`
										: '—'}
								</td>
								<td class="h-11 px-4 align-middle font-mono text-xs text-fg-secondary"
									>{caller.taskId || '—'}</td
								>
								<td
									class="h-11 px-4 align-middle {pinsLabel(caller) === 'current'
										? 'font-medium text-fg'
										: 'text-fg-secondary'}"
									title={caller.unavailableReason || undefined}>{pinsLabel(caller)}</td
								>
								<td class="h-11 px-4 align-middle font-mono text-xs text-fg-secondary"
									>{caller.outputsRead.length ? caller.outputsRead.join(', ') : '—'}</td
								>
								<td class="h-11 px-4 pr-5 align-middle font-mono text-xs text-fg-secondary"
									>{caller.inputsSupplied.length ? caller.inputsSupplied.join(', ') : '—'}</td
								>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
			<div class="mt-3 space-y-0.5 text-xs text-fg-muted" data-testid="callers-scanned">
				{#each callersFooter(response) as line, index (index)}
					<p>{line}</p>
				{/each}
			</div>
		{/if}
	{/if}
</section>
