<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import { canonicalTexts, type FieldFilter } from '$lib/listFilter';
	import { ListWebhooksRequestSchema } from '$lib/gen/orkstack/v1/service_pb';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import Dialog from '$lib/components/Dialog.svelte';
	import { webhookUpdateCli } from '$lib/automation';
	import type { Webhook } from '$lib/gen/orkstack/v1/service_pb';
	import WebhookDeliveries from '$lib/components/WebhookDeliveries.svelte';
	import { deliveriesHref } from '$lib/deliveries';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { EventType } from '$lib/gen/orkstack/v1/run_pb';
	import { fmtTimestamp } from '$lib/status';
	import { RUN_EVENTS, SCOPE_SLUG_PATTERN, TASK_EVENTS, webhookScope } from '$lib/webhooks';
	import type { PageProps } from './$types';
	let { data }: PageProps = $props();

	// The subscriptions table's filters (ListFilter). The address is built by
	// deliveriesHref, so the Deliveries panel's own coordinates survive a
	// change of filters, and its paging keeps them.
	function applyFilters(filters: FieldFilter[]) {
		void goto(deliveriesHref(data.coordinates, { subscriptionFilter: canonicalTexts(filters) }), { keepFocus: true, noScroll: true });
	}
	const clearHref = $derived(deliveriesHref(data.coordinates, { subscriptionFilter: [] }));
	let url = $state(''); let selected = $state<EventType[]>([]); let secret = $state(''); let shownSecret = $state(''); let formError = $state(''); let busy = $state(false); let deleting = $state(''); let deleteError = $state('');
	// The scope is free text, not a Select over existing namespaces or orkflows:
	// registration checks shape, never existence, so a subscription may be
	// registered before the orkflow it names is applied.
	let namespace = $state(''); let orkflowName = $state(''); let taskId = $state('');
	// A name is how an automation document addresses a subscription; it needs a
	// namespace scope, because a name is unique within one namespace and a
	// deployment-wide subscription belongs to none.
	let hookName = $state('');
	// The edit dialog: name, URL, events and scope change in place; the id and
	// the signing secret never do (UpdateWebhook).
	let editing = $state<Webhook | null>(null); let editName = $state(''); let editUrl = $state(''); let editEvents = $state<EventType[]>([]); let editNamespace = $state(''); let editOrkflow = $state(''); let editTask = $state(''); let editError = $state(''); let saving = $state(false);
	function startEdit(hook: Webhook) { editing = hook; editName = hook.name; editUrl = hook.url; editEvents = [...hook.events]; editNamespace = hook.namespace; editOrkflow = hook.orkflowName; editTask = hook.taskId; editError = ''; }
	function toggleEdit(event: EventType, checked: boolean) { editEvents = checked ? [...editEvents, event] : editEvents.filter((value) => value !== event); }
	async function saveEdit() { if (!editing) return; saving = true; editError = ''; try { await orkstack(fetch).webhooks.updateWebhook({ id: editing.id, name: editName.trim(), url: editUrl.trim(), events: editEvents, namespace: editNamespace.trim(), orkflowName: editOrkflow.trim(), taskId: editTask.trim() }); editing = null; await invalidateAll(); } catch (err) { editError = errorMessage(err); } finally { saving = false; } }
	const label = (event: EventType) => EventType[event];
	function toggle(event: EventType, checked: boolean) { selected = checked ? [...selected, event] : selected.filter((value) => value !== event); }
	async function register() { busy = true; formError = ''; shownSecret = ''; try { const response = await orkstack(fetch).webhooks.registerWebhook({ url, events: selected, secret, namespace: namespace.trim(), orkflowName: orkflowName.trim(), taskId: taskId.trim(), name: namespace.trim() ? hookName.trim() : '' }); shownSecret = response.secret; url = ''; secret = ''; selected = []; namespace = ''; orkflowName = ''; taskId = ''; hookName = ''; await invalidateAll(); } catch (err) { formError = errorMessage(err); } finally { busy = false; } }
	async function remove() { if (!deleting) return; busy = true; deleteError = ''; try { await orkstack(fetch).webhooks.deleteWebhook({ id: deleting }); deleting = ''; await invalidateAll(); } catch (err) { deleteError = errorMessage(err); } finally { busy = false; } }
	const slugTitle = 'lowercase letters, digits and hyphens, starting with a letter';
	const inputClass = 'h-9 w-full rounded-md border border-border bg-surface px-3 text-sm';
</script>
<svelte:head><title>Admin — Webhooks — OrkStack</title></svelte:head>
<p class="mb-5 text-sm text-fg-muted" data-testid="webhooks-scope">Subscriptions are deployment-wide unless they name a scope. The namespace selector does not filter this list; each row states what it covers.</p>
<section class="mb-6 rounded-lg border border-border bg-surface p-4 shadow-xs"><h2 class="text-sm font-semibold text-fg">Register webhook</h2>
	<form class="mt-3 space-y-3" onsubmit={(event) => { event.preventDefault(); void register(); }}>
		<label class="block text-sm font-medium" for="webhook-url">HTTP or HTTPS URL</label><input id="webhook-url" data-testid="webhook-url" bind:value={url} required placeholder="https://example.com/hooks/orkstack" class={inputClass} />
		<div class="grid gap-3 sm:grid-cols-3">
			<div><label class="block text-sm font-medium" for="webhook-namespace">Namespace <span class="font-normal text-fg-muted">empty means every</span></label><input id="webhook-namespace" data-testid="webhook-namespace" bind:value={namespace} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} placeholder="analytics" class={inputClass} /></div>
			<div><label class="block text-sm font-medium" for="webhook-orkflow">Orkflow <span class="font-normal text-fg-muted">empty means every</span></label><input id="webhook-orkflow" data-testid="webhook-orkflow" bind:value={orkflowName} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} placeholder="nightly-etl" class={inputClass} /></div>
			<div><label class="block text-sm font-medium" for="webhook-task">Task <span class="font-normal text-fg-muted">empty means every</span></label><input id="webhook-task" data-testid="webhook-task" bind:value={taskId} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} placeholder="extract" class={inputClass} /></div>
		</div>
		<div><label class="block text-sm font-medium" for="webhook-name">Name <span class="font-normal text-fg-muted">(optional — how an automation document refers to it)</span></label><input id="webhook-name" data-testid="webhook-name" bind:value={hookName} disabled={!namespace.trim()} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} placeholder="etl-loader" class="{inputClass} disabled:opacity-50" />{#if !namespace.trim()}<p class="mt-1 text-xs text-fg-muted" data-testid="webhook-name-hint">a name needs a namespace scope</p>{/if}</div>
		<fieldset><legend class="text-sm font-medium">Events <span class="font-normal text-fg-muted">(none means every run transition; task events are opt-in)</span></legend>
			<div class="mt-2 flex flex-wrap gap-3" data-testid="webhook-run-events">{#each RUN_EVENTS as event (event)}<label class="flex items-center gap-1.5 text-sm"><input type="checkbox" data-testid="webhook-event-{label(event)}" checked={selected.includes(event)} onchange={(e) => toggle(event, e.currentTarget.checked)} />{label(event)}</label>{/each}</div>
			<div class="mt-2 flex flex-wrap gap-3" data-testid="webhook-task-events">{#each TASK_EVENTS as event (event)}<label class="flex items-center gap-1.5 text-sm"><input type="checkbox" data-testid="webhook-event-{label(event)}" checked={selected.includes(event)} onchange={(e) => toggle(event, e.currentTarget.checked)} />{label(event)}</label>{/each}</div>
			<p class="mt-1.5 text-xs text-fg-muted" data-testid="webhook-task-caveat">Task events are best-effort hints with a short retry budget. A subscriber that must not miss one should reconcile with GetRun.</p>
		</fieldset>
		<label class="block text-sm font-medium" for="webhook-secret">Signing secret <span class="font-normal text-fg-muted">(optional)</span></label><input id="webhook-secret" data-testid="webhook-secret" bind:value={secret} class={inputClass} />
		{#if formError}<ErrorPanel testid="webhook-register-error" message={formError} />{/if}
		<button type="submit" data-testid="register-webhook" disabled={busy} class="inline-flex h-9 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white disabled:opacity-50">{busy ? 'Registering…' : 'Register webhook'}</button>
	</form>
	{#if shownSecret}<div data-testid="webhook-secret-once" class="mt-4 rounded-md border border-warning-200 bg-warning-50 p-3 text-sm text-warning-800"><p class="font-medium">Copy this signing secret now. It will not be shown again.</p><code class="mt-1 block break-all font-mono text-xs">{shownSecret}</code></div>{/if}
</section>
<ListFilter request={ListWebhooksRequestSchema} applied={data.applied} testid="webhooks" command={['orkctl', 'webhook', 'list']} onapply={applyFilters} {clearHref} />
{#if data.error}<ErrorPanel testid="webhooks-error" message={data.error} />{:else if data.webhooks.length === 0 && data.applied.length > 0}<p data-testid="webhooks-empty" class="text-sm text-fg-muted">No webhooks match these filters.</p>{:else if data.webhooks.length === 0}<p data-testid="webhooks-empty" class="text-sm text-fg-muted">No webhooks are registered.</p>{:else}<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs"><table class="w-full min-w-[820px] text-left text-sm" data-testid="webhook-list"><thead class="bg-surface-sunken"><tr><th class="p-3">ID</th><th class="p-3">Name</th><th class="p-3">URL</th><th class="p-3">Events</th><th class="p-3">Scope</th><th class="p-3">Created</th><th class="p-3"></th></tr></thead><tbody>{#each data.webhooks as webhook (webhook.id)}<tr class="border-t border-border-subtle" data-testid="webhook-{webhook.id}"><td class="p-3 font-mono text-xs">{webhook.id}</td><td class="p-3 font-mono text-xs" data-testid="webhook-name-{webhook.id}">{webhook.name || '—'}</td><td class="p-3 break-all">{webhook.url}</td><td class="p-3 text-xs">{webhook.events.length ? webhook.events.map(label).join(', ') : 'ALL'}</td><td class="p-3 font-mono text-xs" data-testid="webhook-scope-{webhook.id}">{webhookScope(webhook)}</td><td class="p-3 text-xs text-fg-muted">{fmtTimestamp(webhook.createdAt)}</td><td class="p-3 whitespace-nowrap"><a data-testid="webhook-deliveries-{webhook.id}" href={deliveriesHref(data.coordinates, { webhook: webhook.id, cursor: '', page: 1 }, 'deliveries')} class="mr-3 text-sm font-medium text-brand-700 hover:underline">Deliveries</a><button type="button" data-testid="edit-webhook-{webhook.id}" onclick={() => startEdit(webhook)} class="mr-3 text-sm font-medium text-brand-700 hover:underline">Edit</button><button type="button" data-testid="delete-webhook-{webhook.id}" onclick={() => { deleting = webhook.id; deleteError = ''; }} class="text-sm font-medium text-danger-700 hover:underline">Delete</button></td></tr>{/each}</tbody></table></div>{/if}
<WebhookDeliveries coordinates={data.coordinates} response={data.deliveries} error={data.deliveriesError} errorCode={data.deliveriesErrorCode} webhooks={data.panelWebhooks} />
{#if deleting}<ConfirmDialog testid="delete-webhook-confirm" title="Delete webhook {deleting}?" acceptLabel="Delete" acceptTestid="delete-webhook-accept" cancelTestid="delete-webhook-cancel" busy={busy} error={deleteError} destructive onaccept={remove} oncancel={() => { deleting = ''; deleteError = ''; }}>{#snippet body()}<p>Future matching events will no longer be delivered. In-flight delivery is not cancelled.</p>{/snippet}</ConfirmDialog>{/if}

{#if editing}
	<Dialog title="Edit webhook {editing.id}" testid="webhook-edit-dialog" onclose={() => (editing = null)} widthClass="max-w-xl">
		{#snippet body()}
			<p class="mb-3 text-sm text-fg-secondary">Editing keeps this subscription's id and signing secret.</p>
			<form class="space-y-3" onsubmit={(event) => { event.preventDefault(); void saveEdit(); }}>
				<div><label class="block text-sm font-medium" for="webhook-edit-name">Name <span class="font-normal text-fg-muted">(needs a namespace; empty makes it unnamed)</span></label><input id="webhook-edit-name" data-testid="webhook-edit-name" bind:value={editName} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} class={inputClass} /></div>
				<div><label class="block text-sm font-medium" for="webhook-edit-url">URL</label><input id="webhook-edit-url" data-testid="webhook-edit-url" bind:value={editUrl} required class={inputClass} /></div>
				<div class="grid gap-3 sm:grid-cols-3">
					<div><label class="block text-sm font-medium" for="webhook-edit-namespace">Namespace</label><input id="webhook-edit-namespace" data-testid="webhook-edit-namespace" bind:value={editNamespace} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} class={inputClass} /></div>
					<div><label class="block text-sm font-medium" for="webhook-edit-orkflow">Orkflow</label><input id="webhook-edit-orkflow" data-testid="webhook-edit-orkflow" bind:value={editOrkflow} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} class={inputClass} /></div>
					<div><label class="block text-sm font-medium" for="webhook-edit-task">Task</label><input id="webhook-edit-task" data-testid="webhook-edit-task" bind:value={editTask} pattern={SCOPE_SLUG_PATTERN} maxlength={200} title={slugTitle} class={inputClass} /></div>
				</div>
				<fieldset><legend class="text-sm font-medium">Events <span class="font-normal text-fg-muted">(none means every run transition)</span></legend>
					<div class="mt-2 flex flex-wrap gap-3">{#each [...RUN_EVENTS, ...TASK_EVENTS] as event (event)}<label class="flex items-center gap-1.5 text-sm"><input type="checkbox" data-testid="webhook-edit-event-{label(event)}" checked={editEvents.includes(event)} onchange={(e) => toggleEdit(event, e.currentTarget.checked)} />{label(event)}</label>{/each}</div>
				</fieldset>
				{#if editError}<ErrorPanel testid="webhook-edit-error" message={editError} />{/if}
				<div class="flex items-center justify-between gap-3">
					<button type="submit" data-testid="webhook-edit-submit" disabled={saving} class="inline-flex h-9 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white disabled:opacity-50">{saving ? 'Saving…' : 'Save'}</button>
					<CliEquivalent command={webhookUpdateCli(editing?.id ?? "", { name: editName.trim(), url: editUrl.trim(), events: editEvents, namespace: editNamespace.trim(), orkflowName: editOrkflow.trim(), taskId: editTask.trim() })} testid="webhook-edit-cli" />
				</div>
			</form>
		{/snippet}
	</Dialog>
{/if}
