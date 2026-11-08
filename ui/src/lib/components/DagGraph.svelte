<script lang="ts">
	// An Orkflow's tasks drawn as the DAG they form: one node per task, one
	// edge per need, routed across, down, across. On the Orkflow page a node is
	// a link to its row in the task register; on the run page it is a toggle
	// button that selects the task, the same selection its row and the task map
	// hold, and everything outside that selection is greyed.
	//
	// The contract a spec reads (join-policy.spec.ts): a failure handler's dash
	// is set by ATTRIBUTE (DAG_DASH) on every edge into it, and on its outline
	// where no status is drawn, never by the CSS property, so the attribute is the
	// dash drawn. On a run page a dashed outline means a task that has not run.
	//
	// The caller bounds what is drawn (DrawingFrame: at most 80 tasks, 300 on
	// request); this component lays out whatever it is handed. The drawing
	// takes its container's width, never wider than its natural size, and never
	// narrower than DAG_MIN_SCALE of it unless the reader asks to fit it.
	import type { Task } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
	import {
		DAG_DASH,
		DAG_ID_CHARS,
		DAG_LINE_CHARS,
		DAG_MIN_SCALE,
		DAG_NODE_H,
		DAG_NODE_W,
		edgePath,
		fitDagLine,
		dagNodeName,
		fitDagRef,
		layoutDag,
		type DagDirection,
		type DagEdge,
		type DagNode,
		type NodeState
	} from '#lib/dag.ts';
	import type { DeclaredIcon } from '#lib/activityIcon.ts';
	import { activityLabel } from '#lib/activity.ts';
	import { activityKind } from '#lib/activityIcon.ts';
	import { isFailureHandler, joinTitle, joinWord } from '#lib/join.ts';
	import { statusLabel } from '#lib/status.ts';
	import { shortVersion } from '#lib/version.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { statusClassWord, svgMark } from '#lib/statusMarks.ts';
	import { MediaQuery } from 'svelte/reactivity';
	import type { JoinPolicy } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import ActivityIcon from './ActivityIcon.svelte';

	interface Props {
		tasks: Task[];
		/**
		 * Which page draws it, which names its testids: the Orkflow page's
		 * `orkflow-dag` / `dag-node-{id}`, the run page's `run-dag` /
		 * `run-dag-node-{id}` (two drawings on one page never share an id).
		 */
		on?: 'orkflow' | 'run';
		label?: string;
		/** The run page's live state per task; absent on the Orkflow page. */
		stateOf?: (id: string) => NodeState | undefined;
		selected?: string;
		hrefOf?: (id: string) => string;
		onselect?: (id: string) => void;
		/** Left to right (the default) or top to bottom. */
		direction?: DagDirection;
		/** What a task's image declares as its icon. */
		iconOf?: (id: string) => DeclaredIcon | undefined;
		/** Scale the whole drawing into its frame, however small that makes it. */
		fit?: boolean;
	}

	let {
		tasks,
		on = 'orkflow',
		label = 'Orkflow task DAG',
		stateOf = undefined,
		selected = undefined,
		hrefOf = (id: string) => `#task-row-${id}`,
		onselect = undefined,
		direction = 'LR',
		iconOf = undefined,
		fit = false
	}: Props = $props();

	const layout = $derived(layoutDag(tasks, direction));
	// A task's join, by id: an edge INTO a joined task carries its word, and
	// is drawn dashed when that task is a failure handler.
	const joinById = $derived(new Map(tasks.map((task) => [task.id, task.join])));
	// UNSPECIFIED (0) is all_succeeded: no join to mark.
	const joinAttr = (policy: JoinPolicy | undefined) => (policy ? joinWord(policy) : undefined);

	const run = $derived(on === 'run');
	const MARK = 10;
	// On a phone the drawing keeps its words readable and scrolls instead.
	const phone = new MediaQuery('(max-width: 640px)');
	const PHONE_MIN_SCALE = 0.78;
	const TEXT_X = 12;
	const ID_X = 29;
	const CACHED_TAG = 'cached';

	// What the selection keeps lit: the task, what it needs, what needs it.
	const lit = $derived.by(() => {
		if (!selected) return null;
		const keep = new Set<string>([selected]);
		for (const task of tasks) {
			if (task.id === selected) for (const need of task.needs) keep.add(need);
			else if (task.needs.includes(selected)) keep.add(task.id);
		}
		return keep;
	});

	function edgeClass(edge: DagEdge): string {
		if (!run) return '';
		const to = stateOf?.(edge.toId);
		const from = stateOf?.(edge.fromId);
		// Red only out of the failed task into a handler; what it held back is dashed.
		if (from?.status === Status.FAILED) return isFailureHandler(joinById.get(edge.toId)) ? 'is-fail' : 'is-held';
		const touches = edge.fromId === selected || edge.toId === selected;
		if (selected && !touches) return 'is-dim';
		if (from?.status === Status.RUNNING) return 'is-live';
		if (from?.status === Status.SUCCEEDED && (touches || to?.status === Status.RUNNING)) return 'is-done';
		return '';
	}

	// The lane's name: to the left of its first task where nothing stands
	// there, else above it.
	const handlerLabel = $derived.by(() => {
		const handlers = layout.nodes.filter((n) => isFailureHandler(n.task.join));
		if (handlers.length === 0) return null;
		const first = handlers.reduce((a, b) => (b.x < a.x || (b.x === a.x && b.y < a.y) ? b : a));
		const room = 110;
		const blocked = layout.nodes.some(
			(n) => n !== first && n.x < first.x && n.x + DAG_NODE_W > first.x - room && n.y < first.y + DAG_NODE_H && n.y + DAG_NODE_H > first.y
		);
		return blocked
			? { x: first.x, y: first.y - 8, anchor: 'start' as const }
			: { x: first.x - 14, y: first.y + DAG_NODE_H / 2 + 4, anchor: 'end' as const };
	});

	function nodeName(task: Task, state: NodeState | undefined): string {
		const room = DAG_LINE_CHARS - (task.cache ? CACHED_TAG.length + 1 : 0);
		const line = state ? state.line : task.orkflow ? `orkflow ${task.orkflow.name}@${shortVersion(task.orkflow.version)}` : activityLabel(task) || '—';
		const shownLine = !state && task.orkflow ? fitDagRef(task.orkflow.name, task.orkflow.version, room) : fitDagLine(line, room);
		return dagNodeName({
			id: task.id,
			shownId: fitDagLine(task.id, DAG_ID_CHARS + (state ? 0 : 2)),
			line,
			shownLine,
			tags: [...(!state && task.each ? [`≤ ${task.each.maxItems}`] : []), ...(task.cache ? [CACHED_TAG] : [])],
			status: state ? statusLabel(state.status) : undefined
		});
	}

	function select(event: MouseEvent, id: string): void {
		if (!onselect || event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) return;
		event.preventDefault();
		onselect(id);
	}
	function selectByKey(event: KeyboardEvent, id: string): void {
		if (event.key !== 'Enter' && event.key !== ' ') return;
		event.preventDefault();
		onselect?.(id);
	}
</script>

{#snippet line(node: DagNode, text: string)}
	{@const fitted = fitDagLine(text, DAG_LINE_CHARS - (node.task.cache ? CACHED_TAG.length + 1 : 0))}
	<text x={node.x + TEXT_X} y={node.y + 44} class="dag-line"
		>{#if fitted !== text}<title>{text}</title>{/if}{fitted}</text
	>
{/snippet}

{#snippet body(node: DagNode, state: NodeState | undefined)}
	{@const textX = state ? node.x + ID_X : node.x + TEXT_X}
	{@const id = fitDagLine(node.task.id, DAG_ID_CHARS + (state ? 0 : 2))}
	{@const declared = iconOf?.(node.task.id)}
	<!-- One <g> root: a snippet's namespace is decided by what stands at its top. -->
	<g>
	<rect
		x={node.x}
		y={node.y}
		width={DAG_NODE_W}
		height={DAG_NODE_H}
		rx="10"
		class="dag-outline"
		stroke-dasharray={!run && isFailureHandler(node.task.join) ? DAG_DASH : undefined}
	/>
	<rect class="dag-focus" x={node.x - 5} y={node.y - 5} width={DAG_NODE_W + 10} height={DAG_NODE_H + 10} rx="14" />
	{#if state?.items && state.items.total > 0}
		{@const ok = (DAG_NODE_W * state.items.succeeded) / state.items.total}
		{@const going = (DAG_NODE_W * state.items.running) / state.items.total}
		<clipPath id="{on}-clip-{node.task.id}"><rect x={node.x} y={node.y} width={DAG_NODE_W} height={DAG_NODE_H} rx="10" /></clipPath>
		<g clip-path="url(#{on}-clip-{node.task.id})">
			<rect class="dag-track" x={node.x} y={node.y + DAG_NODE_H - 6} width={DAG_NODE_W} height="6" />
			<rect class="dag-bar-ok" x={node.x} y={node.y + DAG_NODE_H - 6} width={ok} height="6" />
			<rect class="dag-bar-run" x={node.x + ok} y={node.y + DAG_NODE_H - 6} width={going} height="6" />
		</g>
	{/if}
	{#if declared}
		<ActivityIcon icon={declared} class="dag-icon" x={node.x + DAG_NODE_W - 25} y={node.y + 11} width={15} height={15} />
	{:else}
		<use
			href="#i-a-{node.task.orkflow ? 'orkflow' : activityKind(node.task.uses, { awaited: !!node.task.await })}"
			x={node.x + DAG_NODE_W - 25}
			y={node.y + 11}
			width="15"
			height="15"
			class="dag-icon"
		/>
	{/if}
	{#if state}
		{#each svgMark(state.status, node.x + TEXT_X, node.y + 13, MARK) as part, i (i)}
			{#if part.el === 'circle'}
				<circle cx={part.cx} cy={part.cy} r={part.r} class={part.cls} />
			{:else}
				<path d={part.d} class={part.cls} stroke-dasharray={part.dash} />
			{/if}
		{/each}
	{/if}
	<text x={textX} y={node.y + 24} class="dag-id"
		>{#if id !== node.task.id}<title>{node.task.id}</title>{/if}{id}</text
	>{' '}
	{#if state}
		{@render line(node, state.line)}
	{:else if node.task.orkflow}
		<!-- A composed task: it runs another Orkflow as a child run. Its
		     name gives way to fit the node; its version never does. -->
		{@const ref = `orkflow ${node.task.orkflow.name}@${node.task.orkflow.version}`}
		{@const shown = fitDagRef(
			node.task.orkflow.name,
			node.task.orkflow.version,
			DAG_LINE_CHARS - (node.task.cache ? CACHED_TAG.length + 1 : 0)
		)}
		<text
			data-testid={run ? `run-dag-node-${node.task.id}-orkflow` : `dag-node-${node.task.id}-orkflow`}
			x={node.x + TEXT_X}
			y={node.y + 44}
			class="dag-tag">{#if shown !== ref}<title>{ref}</title>{/if}{shown}</text
		>
	{:else}
		{@render line(node, activityLabel(node.task) || '—')}
	{/if}
	{#if !run && node.task.each}
		<!-- A fan-out: one node, run as up to max_items items. -->
		<text
			data-testid="dag-node-{node.task.id}-each"
			x={node.x + DAG_NODE_W - TEXT_X - 22}
			y={node.y + 22}
			text-anchor="end"
			class="dag-tag">≤ {node.task.each.maxItems}</text
		>
	{/if}
	{#if run && node.task.each}<title>A fan-out of up to {node.task.each.maxItems} items</title><desc data-testid="run-dag-node-{node.task.id}-each">each ≤ {node.task.each.maxItems}</desc>{/if}
	{#if joinAttr(node.task.join)}<desc data-testid={run ? `run-dag-node-${node.task.id}-join` : `dag-node-${node.task.id}-join`}>join {joinWord(node.task.join)}</desc>{/if}
	{#if node.task.cache}
		<!-- A cached task: memoized across runs (docs/DSL.md "Caching"). -->
		<text
			data-testid={run ? `run-dag-node-${node.task.id}-cache` : `dag-node-${node.task.id}-cache`}
			x={node.x + DAG_NODE_W - TEXT_X}
			y={node.y + 44}
			text-anchor="end"
			class="dag-tag">{CACHED_TAG}</text
		>
	{/if}
	</g>
{/snippet}

{#if tasks.length === 0}
	<p class="p-4 text-sm text-ink-muted">No tasks.</p>
{:else}
	<svg
		data-testid={run ? 'run-dag' : 'orkflow-dag'}
		data-direction={layout.direction}
		viewBox="0 0 {layout.width} {layout.height}"
		style="width:100%;max-width:{layout.width}px;min-width:{fit ? 0 : Math.round(layout.width * (phone.current ? PHONE_MIN_SCALE : DAG_MIN_SCALE))}px"
		class="dag mx-auto block h-auto"
		role="group"
		aria-label={label}
	>
		{#each layout.edges as edge (edge.fromId + '->' + edge.toId)}
			<path
				data-testid={run ? `run-dag-edge-${edge.fromId}-${edge.toId}` : `dag-edge-${edge.fromId}-${edge.toId}`}
				d={edgePath(edge, 0)}
				class="dag-edge {edgeClass(edge)}"
				stroke-dasharray={isFailureHandler(joinById.get(edge.toId)) ? DAG_DASH : undefined}
				data-join={joinAttr(joinById.get(edge.toId))}
			/>
		{/each}
		{#if handlerLabel}
			<text class="dag-lane" x={handlerLabel.x} y={handlerLabel.y} text-anchor={handlerLabel.anchor} data-testid={run ? 'run-dag-handlers' : 'dag-handlers'}>Failure handlers</text>
		{/if}
		{#each layout.nodes as node (node.task.id)}
			{@const state = stateOf?.(node.task.id)}
			{@const word = state ? statusClassWord(state.status) : ''}
			{@const pending = state?.status === Status.PENDING || state?.status === Status.SKIPPED}
			<g
				data-testid={run ? `run-dag-node-${node.task.id}` : `dag-node-${node.task.id}`}
				data-join={joinAttr(node.task.join)}
				data-status={state ? Status[state.status ?? Status.UNSPECIFIED] : undefined}
				class="dag-node {run ? 'dag-hit' : ''} {word ? `is-${word}` : ''} {state?.neverStarted ? 'is-never' : ''} {pending ? 'is-pend' : ''} {selected === node.task.id
					? 'is-selected'
					: ''} {lit && !lit.has(node.task.id) ? 'is-dim' : ''}"
				{...run
					? {
							role: 'button',
							tabindex: 0,
							'aria-label': nodeName(node.task, state),
							'aria-pressed': selected === node.task.id,
							onclick: (e: MouseEvent) => select(e, node.task.id),
							onkeydown: (e: KeyboardEvent) => selectByKey(e, node.task.id)
						}
					: {}}
			>
				{#if joinAttr(node.task.join)}
					<title>{joinTitle(node.task.join, node.task.needs)}</title>
				{/if}
				{#if run}
					{@render body(node, state)}
				{:else}
					<a href={hrefOf(node.task.id)} class="dag-hit" aria-label={nodeName(node.task, state)} onclick={(e) => select(e, node.task.id)}>
						{@render body(node, state)}
					</a>
				{/if}
			</g>
		{/each}
	</svg>
{/if}

<style>
	/* Harbour's graph: white rounded nodes on the dotted ground, the id in
	   the sans, an activity icon top right. Every colour is a token; the
	   markup carries geometry only. */
	.dag-outline {
		fill: var(--surface);
		stroke: var(--line-strong);
		stroke-width: 1.25;
	}
	.dag-edge {
		fill: none;
		stroke: var(--line-strong);
		stroke-width: 1.5;
	}
	.dag-edge.is-done {
		stroke: var(--ok-mark);
	}
	.dag-edge.is-live {
		stroke: var(--run);
		stroke-dasharray: 5 4;
	}
	.dag-edge.is-fail {
		stroke: var(--fail-mark);
	}
	.dag-edge.is-held {
		stroke-dasharray: 4 3;
	}
	.dag-edge.is-dim {
		stroke: var(--pend-mark);
		stroke-dasharray: 1.5 3.5;
		stroke-linecap: round;
	}
	/* A drawing scaled to its frame keeps its rules at their drawn weight. */
	.dag-outline,
	.dag-edge,
	.dag-focus {
		vector-effect: non-scaling-stroke;
	}
	.dag-id {
		fill: var(--ink);
		font-family: var(--font-ui);
		font-size: var(--text-base);
		font-weight: var(--font-weight-semibold);
	}
	.dag-line {
		fill: var(--ink-3);
		font-family: var(--font-ui);
		font-size: var(--text-sm);
		font-variant-numeric: tabular-nums;
	}
	.dag-tag {
		fill: var(--ink-2);
		font-family: var(--font-ui);
		font-size: var(--text-2xs);
	}
	.dag-lane {
		fill: var(--ink-2);
		font-family: var(--font-ui);
		font-size: var(--text-sm);
		font-weight: var(--font-weight-semibold);
	}
	:global(.dag-icon) {
		color: var(--ink-3);
	}
	.dag-track {
		fill: var(--surface-3);
	}
	.dag-bar-ok {
		fill: var(--ok-mark);
	}
	.dag-bar-run {
		fill: var(--run);
	}
	.dag-focus {
		fill: none;
		stroke: transparent;
		stroke-width: 2;
	}
	/* Selection is the node's own border, in ink; focus draws its own ring
	   outside the box, so a selected node still shows focus. */
	.dag-node.is-selected .dag-outline {
		stroke: var(--select);
		stroke-width: 2;
	}
	.dag-hit {
		cursor: pointer;
	}
	.dag-hit:focus {
		outline: none;
	}
	.dag-hit:focus-visible .dag-focus {
		stroke: var(--focus);
	}
	.dag-hit:hover .dag-outline {
		fill: var(--hover);
	}
	.dag-node.is-pend .dag-outline:not([stroke-dasharray]),
	.dag-node.is-never .dag-outline:not([stroke-dasharray]) {
		stroke-dasharray: 4 3;
	}
	.dag-node.is-failed .dag-outline {
		fill: var(--fail-tint);
		stroke: var(--fail-mark);
		stroke-width: 2;
	}
	.dag-node.is-failed.is-selected .dag-outline {
		stroke: var(--select);
	}
	.dag-node.is-running .dag-outline {
		fill: var(--run-tint);
		stroke: var(--run);
		stroke-width: 2;
	}
	.dag-node.is-running.is-selected .dag-outline {
		stroke: var(--select);
	}
	.dag-node.is-awaiting .dag-outline {
		stroke: var(--await);
	}
	/* Outside the selection: greyed, not faded, so its words keep 4.5:1. */
	.dag-node.is-dim .dag-outline {
		fill: var(--dim-fill);
		stroke: var(--dim-line);
		stroke-width: 1.25;
	}
	.dag-node.is-dim .dag-id {
		fill: var(--ink-2);
		font-weight: var(--font-weight-medium);
	}

	/* The status marks (statusMarks.svgMark). */
	.mark-fill-succeeded {
		fill: var(--ok-mark);
	}
	.mark-fill-running {
		fill: var(--run);
	}
	.mark-fill-failed {
		fill: var(--fail-mark);
	}
	.mark-line-awaiting,
	.mark-line-canceled,
	.mark-line-skipped,
	.mark-line-pending,
	.mark-line-unspecified {
		fill: none;
		stroke-width: 1.5;
	}
	.mark-line-awaiting {
		stroke: var(--await);
	}
	.mark-line-canceled,
	.mark-line-skipped {
		stroke: var(--cancel);
	}
	.mark-line-pending,
	.mark-line-unspecified {
		stroke: var(--pend-mark);
	}
	@media (forced-colors: active) {
		.dag-outline,
		.dag-edge {
			stroke: CanvasText;
		}
		.dag-node.is-selected .dag-outline,
		.dag-hit:focus-visible .dag-focus {
			stroke: Highlight;
		}
	}
</style>
