<script lang="ts">
	// What changed since the run a reader would compare this one with: the last
	// success for a run that failed, the last finished run otherwise. RunDiff's
	// fields and nothing else: one list of recent runs, one DiffRuns, after the
	// page has drawn. A run with too many tasks to compare in passing offers the
	// comparison instead of reading it.
	import { errorMessage } from '#lib/client.ts';
	import { runClient } from '#lib/clients/runs.ts';
	import type { Run } from '#lib/gen/orkstack/v1/run_pb.ts';
	import type { DiffRunsResponse } from '#lib/gen/orkstack/v1/run_service_pb.ts';
	import { runWindowFilters } from '#lib/browsing.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { isTerminal, statusLabel } from '#lib/status.ts';
	import { shortRunId } from '#lib/runDetail.ts';
	import { withNamespace } from '#lib/namespace.ts';
	import { relativeTime } from '#lib/when.ts';

	interface Props {
		namespace: string;
		runId: string;
		orkflow: string;
		createdAt: number | null;
		/** Compare with the last success rather than the last finished run. */
		sinceSuccess: boolean;
		/** Whether this page may read the comparison in passing. */
		read: boolean;
		/** Whether this run has ended: a live run's unfinished sides are no differences. */
		finished: boolean;
	}

	let { namespace, runId, orkflow, createdAt, sinceSuccess, read, finished }: Props = $props();

	const SHOWN = 6;
	let other = $state.raw<Run | null>(null);
	let diff = $state.raw<DiffRunsResponse | null>(null);
	let settled = $state(false);
	// The newest page of earlier runs held none that finished, and there are more.
	let unfinishedWindow = $state(false);
	let failure = $state('');

	$effect(() => {
		const [ns, id, name, wantSuccess, may, created] = [namespace, runId, orkflow, sinceSuccess, read, createdAt];
		let live = true;
		void (async () => {
			try {
				const client = runClient(fetch);
				// The runs before this one, newest first: for a last success, only successes.
				const before = created === null ? '' : new Date(created).toISOString();
				const res = await client.listRuns({ namespace: ns, filters: runWindowFilters(name, wantSuccess ? 'SUCCEEDED' : '', '', before), pageSize: 20 });
				if (!live) return;
				const found = res.runs.find((r) => r.id !== id && isTerminal(r.status)) ?? null;
				other = found;
				unfinishedWindow = !found && res.nextPageToken !== '';
				if (found && may) {
					const answer = await client.diffRuns({ namespace: ns, fromRunId: found.id, toRunId: id, only: [] });
					if (live) diff = answer;
				}
				if (live) settled = true;
			} catch (err) {
				if (live) {
					failure = errorMessage(err);
					settled = true;
				}
			}
		})();
		return () => {
			live = false;
		};
	});

	// What a difference says, in sentences. A side with nothing on it means "not
	// finished yet" on a live run and is no difference to tell; who ran it, and
	// where, changes every run and is what the comparison page is for.
	const QUIET = new Set(['orker', 'pool', 'platform_digest', 'pin']);
	const HIDDEN_WHILE_LIVE = new Set(['status', 'exit_code', 'error', 'duration']);
	const statusWord = (value: string) => statusLabel(Status[value as keyof typeof Status]) || value;
	const seconds = (go: string): number | null => {
		const m = /^(?:(\d+)h)?(?:(\d+)m)?(?:([\d.]+)s)?$/.exec(go);
		if (m && go) return Number(m[1] ?? 0) * 3600 + Number(m[2] ?? 0) * 60 + Number(m[3] ?? 0);
		const ms = /^([\d.]+)ms$/.exec(go);
		return ms ? Number(ms[1]) / 1000 : null;
	};
	const human = (s: number) => (s >= 60 ? `${Math.floor(s / 60)}m ${Math.round(s % 60)}s` : `${s < 10 ? s.toFixed(1) : Math.round(s)}s`);
	interface Said {
		key: string;
		what: string;
		now: string;
		was: string;
	}
	function inputRows(from: string, to: string): Said[] {
		try {
			const [a, b] = [JSON.parse(from || '{}') as Record<string, unknown>, JSON.parse(to || '{}') as Record<string, unknown>];
			const keys = [...new Set([...Object.keys(a), ...Object.keys(b)])].filter((k) => JSON.stringify(a[k]) !== JSON.stringify(b[k]));
			return keys.map((k) => ({ key: `input/${k}`, what: `Input ${k}`, now: JSON.stringify(b[k]) ?? 'unset', was: `was ${JSON.stringify(a[k]) ?? 'unset'}` }));
		} catch {
			return [{ key: 'input', what: 'Input', now: brief(to), was: `was ${brief(from)}` }];
		}
	}
	function sayRow(row: DiffRunsResponse['differences'][number], live: boolean): Said[] {
		const who = row.taskId + (row.item === undefined ? '' : `[${row.item}]`);
		const key = `${who}/${row.field}`;
		const [to, from] = [row.toValue, row.fromValue];
		const versionOf = (value: string) => /@(\S+)/.exec(value)?.[1] ?? brief(value);
		if (QUIET.has(row.field)) return [];
		if (!row.toAvailable || !row.fromAvailable) return [];
		if (live && (to === '' || to === '0' || HIDDEN_WHILE_LIVE.has(row.field))) return [];
		switch (row.field) {
			case 'definition':
				return [{ key, what: 'Version', now: versionOf(to), was: `was ${versionOf(from)}` }];
			case 'input':
				return inputRows(from, to);
			case 'attempt':
				return [{ key, what: who, now: `needed ${to} ${to === '1' ? 'attempt' : 'attempts'}`, was: `${from} ${from === '1' ? 'attempt' : 'attempts'} last run` }];
			case 'status':
				return [{ key, what: who, now: statusWord(to), was: `${statusWord(from)} last run` }];
			case 'duration': {
				const [a, b] = [seconds(from), seconds(to)];
				// A task always takes a little more or less: only half as long again is worth a line.
				if (a === null || b === null || Math.max(a, b) < 1.5 * Math.min(a, b)) return [];
				return [{ key, what: who, now: `took ${human(b)}`, was: `took ${human(a)} last run` }];
			}
			case 'image':
				return [{ key, what: who, now: 'ran another image', was: `was ${brief(from)}` }];
			case 'exit_code':
				return [{ key, what: who, now: `exit code ${to}`, was: `exit code ${from || 'none'} last run` }];
			case 'presence':
				return [{ key, what: who, now: to || 'not in this run', was: from ? `${from} last run` : 'not in the last run' }];
			default:
				return to && from ? [{ key, what: who ? `${who} ${fieldWord(row.field)}` : fieldWord(row.field), now: brief(to), was: `was ${brief(from)}` }] : [];
		}
	}
	const rows = $derived.by(() => {
		const all = diff?.differences ?? [];
		// One line for a fan-out, however many of its items differ.
		const items = new Map<string, Set<number>>();
		const said: Said[] = [];
		for (const row of all) {
			if (row.item !== undefined) {
				if (!QUIET.has(row.field) && !(!finished && (row.toValue === '' || row.toValue === '0' || HIDDEN_WHILE_LIVE.has(row.field)))) {
					const known = items.get(row.taskId) ?? new Set<number>();
					known.add(row.item);
					items.set(row.taskId, known);
				}
				continue;
			}
			said.push(...sayRow(row, !finished));
		}
		for (const [task, differ] of items) {
			said.push({ key: `${task}/items`, what: task, now: `${differ.size} ${differ.size === 1 ? 'item differs' : 'items differ'}`, was: 'the comparison lists them' });
		}
		return said;
	});
	const sameImages = $derived(!(diff?.differences ?? []).some((d) => d.field === 'image'));
	const sameInput = $derived(!(diff?.differences ?? []).some((d) => d.field === 'input'));
	const when = $derived(other?.createdAt ? relativeTime(new Date(Number(other.createdAt.seconds) * 1000)) : '');
	const fieldWord = (field: string) => field.replaceAll('_', ' ');
	// A digest in a sentence is its first twelve characters; the comparison page has it whole.
	const brief = (value: string) => value.replace(/sha256:([0-9a-f]{12})[0-9a-f]+/g, 'sha256:$1…');
</script>

<section class="card" aria-labelledby="run-since-heading" data-testid="run-since">
	<div class="card-head">
		<h2 id="run-since-heading">{sinceSuccess ? 'Since the last success' : 'Since the last run'}</h2>
		{#if other}<span class="card-sub"><span class="font-mono">{shortRunId(other.id)}</span> · {statusLabel(other.status)} {when}</span>{/if}
	</div>
	<div class="difflist">
		{#if failure}
			<p data-testid="run-since-error">It could not be read: {failure}</p>
		{:else if !settled}
			<p class="text-ink-3">Reading the last run…</p>
		{:else if !other}
			<p class="text-ink-3" data-testid="run-since-none">{unfinishedWindow ? 'None of the 20 runs before this one has finished.' : sinceSuccess ? 'No earlier run of this Orkflow succeeded.' : 'No earlier run of this Orkflow has finished.'}</p>
		{:else}
			{#each rows.slice(0, SHOWN) as row (row.key)}
				<div data-testid="run-since-row">
					<b>{row.what}</b> <span class="break-all">{row.now}</span>
					<div class="was"><span class="break-all">{row.was}</span></div>
				</div>
			{/each}
			{#if rows.length > SHOWN}<p class="same">and {rows.length - SHOWN} more.</p>{/if}
			{#if diff}
				{#if diff.sameDefinitionDigest && sameInput && sameImages}<p class="same" data-testid="run-since-same">Same version and definition digest, same input, same image digests.</p>
				{:else if diff.sameDefinitionDigest}<p class="same" data-testid="run-since-same">Same version and definition digest.</p>{/if}
			{/if}
			<div class="flex"><a class="btn btn-sm" data-testid="run-since-compare" href={withNamespace(`/runs/${encodeURIComponent(runId)}/compare?to=${encodeURIComponent(other.id)}`, namespace)}>Compare the two runs</a></div>
		{/if}
	</div>
</section>
