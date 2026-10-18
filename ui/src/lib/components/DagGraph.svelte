<script lang="ts">
	// An Orkflow's tasks drawn as the DAG they form: one node per task, one
	// edge per need, routed across, down, across. A node is a link — on the
	// Orkflow page to its row in the task register, on the run page to the task
	// itself (selecting it, the same coordinate as the task map).
	//
	// The contract a spec reads (join-policy.spec.ts): exactly ONE <rect> per
	// node, and a failure handler's dash set by ATTRIBUTE (DAG_DASH) on that
	// rect and on every edge into it — never by the CSS property, so the
	// attribute is the dash drawn. Every other mark of a node (selection,
	// focus, status, strike, tags) is a path, a circle, a line or text.
	//
	// The caller bounds what is drawn (DrawingFrame: at most 80 tasks, 300 on
	// request); this component lays out whatever it is handed.
	import type { Task } from '$lib/gen/orkstack/v1/orkflow_pb';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
	import {
		DAG_DASH,
		DAG_LINE_CHARS,
		DAG_NODE_H,
		DAG_NODE_W,
		edgePath,
		fitDagLine,
		layoutDag,
		type DagNode,
		type NodeState
	} from '$lib/dag';
	import { activityLabel } from '$lib/activity';
	import { isFailureHandler, joinTitle, joinWord } from '$lib/join';
	import { statusLabel } from '$lib/status';
	import { statusClassWord, svgMark } from '$lib/statusMarks';
	import type { JoinPolicy } from '$lib/gen/orkstack/v1/activity_pb';

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
	}

	let {
		tasks,
		on = 'orkflow',
		label = 'Orkflow task DAG',
		stateOf = undefined,
		selected = undefined,
		hrefOf = (id: string) => `#task-row-${id}`,
		onselect = undefined
	}: Props = $props();

	const layout = $derived(layoutDag(tasks));
	// A task's join, by id: an edge INTO a joined task carries its word, and
	// is drawn dashed when that task is a failure handler.
	const joinById = $derived(new Map(tasks.map((task) => [task.id, task.join])));
	// UNSPECIFIED (0) is all_succeeded: no join to mark.
	const joinAttr = (policy: JoinPolicy | undefined) => (policy ? joinWord(policy) : undefined);

	const run = $derived(on === 'run');
	const MARK = 10;
	const TEXT_X = 12;
	const CACHED_TAG = 'cached';

	/** A frame `pad` units outside the node's outline: the selection and focus marks. */
	function frame(x: number, y: number, pad: number): string {
		return `M ${x - pad} ${y - pad} h ${DAG_NODE_W + 2 * pad} v ${DAG_NODE_H + 2 * pad} h ${-(DAG_NODE_W + 2 * pad)} Z`;
	}

	function nodeName(task: Task, state: NodeState | undefined): string {
		if (!state) return `Task ${task.id}`;
		return `Task ${task.id}, ${statusLabel(state.status)}${state.line ? `, ${state.line}` : ''}`;
	}

	function select(event: MouseEvent, id: string): void {
		if (!onselect || event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) return;
		event.preventDefault();
		onselect(id);
	}
</script>

{#snippet line(node: DagNode, text: string)}
	{@const fitted = fitDagLine(text, DAG_LINE_CHARS - (node.task.cache ? CACHED_TAG.length + 1 : 0))}
	<text x={node.x + TEXT_X} y={node.y + 44} class="dag-line"
		>{#if fitted !== text}<title>{text}</title>{/if}{fitted}</text
	>
{/snippet}

{#if tasks.length === 0}
	<p class="p-4 text-sm text-ink-muted">No tasks.</p>
{:else}
	<svg
		data-testid={run ? 'run-dag' : 'orkflow-dag'}
		width={layout.width}
		height={layout.height}
		viewBox="0 0 {layout.width} {layout.height}"
		class="dag block"
		role="group"
		aria-label={label}
	>
		<defs>
			<marker
				id={run ? 'run-dag-arrow' : 'dag-arrow'}
				viewBox="0 0 10 10"
				refX="9"
				refY="5"
				markerWidth="7"
				markerHeight="7"
				orient="auto-start-reverse"
			>
				<path d="M 0 1 L 9 5 L 0 9 z" class="dag-arrowhead" />
			</marker>
		</defs>
		{#each layout.edges as edge (edge.fromId + '->' + edge.toId)}
			<path
				data-testid={run ? `run-dag-edge-${edge.fromId}-${edge.toId}` : `dag-edge-${edge.fromId}-${edge.toId}`}
				d={edgePath({ ...edge, x2: edge.x2 - 2 })}
				class="dag-edge"
				stroke-dasharray={isFailureHandler(joinById.get(edge.toId)) ? DAG_DASH : undefined}
				data-join={joinAttr(joinById.get(edge.toId))}
				marker-end={run ? 'url(#run-dag-arrow)' : 'url(#dag-arrow)'}
			/>
		{/each}
		{#each layout.nodes as node (node.task.id)}
			{@const state = stateOf?.(node.task.id)}
			{@const word = state ? statusClassWord(state.status) : ''}
			{@const textX = node.x + TEXT_X + (state ? MARK + 6 : 0)}
			<g
				data-testid={run ? `run-dag-node-${node.task.id}` : `dag-node-${node.task.id}`}
				data-join={joinAttr(node.task.join)}
				data-status={state ? statusLabel(state.status) : undefined}
				class="dag-node {word ? `is-${word}` : ''} {state?.neverStarted ? 'is-never' : ''} {selected === node.task.id
					? 'is-selected'
					: ''}"
			>
				{#if joinAttr(node.task.join)}
					<title>{joinTitle(node.task.join, node.task.needs)}</title>
				{/if}
				<a
					href={hrefOf(node.task.id)}
					aria-label={nodeName(node.task, state)}
					aria-current={selected === node.task.id ? 'true' : undefined}
					onclick={(e) => select(e, node.task.id)}
				>
					<rect
						x={node.x}
						y={node.y}
						width={DAG_NODE_W}
						height={DAG_NODE_H}
						rx="2"
						class="dag-outline"
						stroke-dasharray={isFailureHandler(node.task.join) ? DAG_DASH : undefined}
					/>
					<path class="dag-select" d={frame(node.x, node.y, 3)} />
					<path class="dag-focus" d={frame(node.x, node.y, 6)} />
					{#if state}
						{#each svgMark(state.status, node.x + TEXT_X, node.y + 12, MARK) as part, i (i)}
							{#if part.el === 'circle'}
								<circle cx={part.cx} cy={part.cy} r={part.r} class={part.cls} />
							{:else}
								<path d={part.d} class={part.cls} stroke-dasharray={part.dash} />
							{/if}
						{/each}
						{#if state.status === Status.CANCELED}
							<line
								class="dag-strike"
								x1={node.x + 4}
								y1={node.y + DAG_NODE_H - 4}
								x2={node.x + DAG_NODE_W - 4}
								y2={node.y + 4}
							/>
						{/if}
					{/if}
					<text x={textX} y={node.y + 22} class="dag-id">{node.task.id}</text>
					{#if state}
						{@render line(node, state.line)}
					{:else if node.task.orkflow}
						<!-- A composed task: it runs another Orkflow as a child run. -->
						<text
							data-testid={run ? `run-dag-node-${node.task.id}-orkflow` : `dag-node-${node.task.id}-orkflow`}
							x={node.x + TEXT_X}
							y={node.y + 44}
							class="dag-tag">orkflow {node.task.orkflow.name}@{node.task.orkflow.version}</text
						>
					{:else}
						{@render line(node, activityLabel(node.task) || '—')}
					{/if}
					{#if node.task.each}
						<!-- A fan-out: one node, run as up to max_items items. -->
						<text
							data-testid={run ? `run-dag-node-${node.task.id}-each` : `dag-node-${node.task.id}-each`}
							x={node.x + DAG_NODE_W - TEXT_X}
							y={node.y + 22}
							text-anchor="end"
							class="dag-tag">each ≤ {node.task.each.maxItems}</text
						>
					{:else if joinAttr(node.task.join)}
						<!-- A join: when the task starts, decided from how its needs ended. -->
						<text
							data-testid={run ? `run-dag-node-${node.task.id}-join` : `dag-node-${node.task.id}-join`}
							x={node.x + DAG_NODE_W - TEXT_X}
							y={node.y + 22}
							text-anchor="end"
							class="dag-tag">join {joinWord(node.task.join)}</text
						>
					{/if}
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
				</a>
			</g>
		{/each}
	</svg>
{/if}

<style>
	/* Ink on paper: a 1.5 px outline, the id in the sans, the second line in
	   the mono. Every colour is a token; the markup carries geometry only. */
	.dag-outline {
		fill: var(--color-paper);
		stroke: var(--color-ink);
		stroke-width: var(--rule-region);
	}
	.dag-edge {
		fill: none;
		stroke: var(--color-ink);
		stroke-width: var(--rule-region);
	}
	.dag-arrowhead {
		fill: var(--color-ink);
	}
	.dag-id {
		fill: var(--color-ink);
		font-family: var(--font-sans);
		font-size: var(--text-sm);
		font-weight: var(--font-weight-semibold);
	}
	.dag-line {
		fill: var(--color-ink-muted);
		font-family: var(--font-mono);
		font-size: var(--text-2xs);
	}
	.dag-tag {
		fill: var(--color-blue);
		font-family: var(--font-mono);
		font-size: var(--text-2xs);
	}
	.dag-select,
	.dag-focus {
		fill: none;
		stroke: transparent;
		stroke-width: var(--rule-frame);
	}
	.dag-node.is-selected .dag-select {
		stroke: var(--color-blue);
	}
	.dag-node a {
		cursor: pointer;
	}
	.dag-node a:focus {
		outline: none;
	}
	.dag-node a:focus-visible .dag-focus {
		stroke: var(--color-focus);
	}
	.dag-node a:hover .dag-outline {
		fill: var(--color-hover);
	}

	/* Run-page status, as the drawing draws it. */
	.dag-node.is-failed .dag-outline {
		stroke: var(--color-red);
		stroke-width: var(--rule-alarm);
	}
	.dag-node.is-failed .dag-id,
	.dag-node.is-failed .dag-line {
		fill: var(--color-red);
	}
	.dag-node.is-running .dag-outline,
	.dag-node.is-awaiting .dag-outline {
		stroke: var(--color-blue);
	}
	.dag-node.is-never {
		opacity: var(--opacity-faded);
	}
	.dag-strike {
		stroke: var(--color-ink-muted);
		stroke-width: var(--rule-region);
	}

	/* The status marks (statusMarks.svgMark). */
	.mark-fill-paper {
		fill: var(--color-paper);
	}
	.mark-line-paper {
		fill: none;
		stroke: var(--color-paper);
		stroke-width: var(--rule-region);
	}
	.mark-fill-succeeded {
		fill: var(--color-succeeded-solid);
	}
	.mark-fill-running {
		fill: var(--color-running-solid);
	}
	.mark-fill-failed {
		fill: var(--color-failed-solid);
	}
	.mark-line-awaiting,
	.mark-line-canceled,
	.mark-line-skipped,
	.mark-line-pending,
	.mark-line-unspecified {
		fill: none;
		stroke-width: var(--rule-region);
	}
	.mark-line-awaiting {
		stroke: var(--color-awaiting-solid);
	}
	.mark-line-canceled {
		stroke: var(--color-canceled-solid);
	}
	.mark-line-skipped {
		stroke: var(--color-skipped-solid);
	}
	.mark-line-pending {
		stroke: var(--color-pending-solid);
	}
	.mark-line-unspecified {
		stroke: var(--color-unspecified-solid);
	}
	@media (forced-colors: active) {
		.dag-outline,
		.dag-edge {
			stroke: CanvasText;
		}
		.dag-node.is-selected .dag-select,
		.dag-node a:focus-visible .dag-focus {
			stroke: Highlight;
		}
	}
</style>
