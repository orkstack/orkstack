<script lang="ts">
	import type { Task } from '$lib/gen/orkstack/v1/orkflow_pb';
	import { DAG_NODE_H, DAG_NODE_W, layoutDag } from '$lib/dag';
	import { activityLabel } from '$lib/activity';

	let { tasks }: { tasks: Task[] } = $props();

	const layout = $derived(layoutDag(tasks));
</script>

{#if tasks.length === 0}
	<p class="text-sm text-fg-muted">No tasks.</p>
{:else}
	<div class="overflow-x-auto rounded-lg border border-border bg-surface p-3 shadow-xs">
		<svg
			data-testid="orkflow-dag"
			width={layout.width}
			height={layout.height}
			viewBox="0 0 {layout.width} {layout.height}"
			class="block"
			role="img"
			aria-label="Orkflow task DAG"
		>
			<defs>
				<marker
					id="dag-arrow"
					viewBox="0 0 10 10"
					refX="9"
					refY="5"
					markerWidth="7"
					markerHeight="7"
					orient="auto-start-reverse"
				>
					<path d="M 0 1 L 9 5 L 0 9 z" fill="var(--color-border-strong)" />
				</marker>
			</defs>
			{#each layout.edges as edge (edge.fromId + '->' + edge.toId)}
				<path
					data-testid="dag-edge-{edge.fromId}-{edge.toId}"
					d="M {edge.x1} {edge.y1} C {edge.x1 + 48} {edge.y1}, {edge.x2 - 48} {edge.y2}, {edge.x2 -
						2} {edge.y2}"
					fill="none"
					stroke="var(--color-border-strong)"
					stroke-width="1.5"
					marker-end="url(#dag-arrow)"
				/>
			{/each}
			{#each layout.nodes as node (node.task.id)}
				<g data-testid="dag-node-{node.task.id}">
					<rect
						x={node.x}
						y={node.y}
						width={DAG_NODE_W}
						height={DAG_NODE_H}
						rx="8"
						fill="var(--color-surface)"
						stroke="var(--color-border-strong)"
						stroke-width="1.5"
					/>
					<text
						x={node.x + 12}
						y={node.y + 24}
						fill="var(--color-fg)"
						font-size="13"
						font-weight="600"
						font-family="var(--font-sans)">{node.task.id}</text
					>
					{#if node.task.orkflow}
						<!-- A composed task: it runs another Orkflow as a child run. -->
						<text
							data-testid="dag-node-{node.task.id}-orkflow"
							x={node.x + 12}
							y={node.y + 43}
							fill="var(--color-brand-700)"
							font-size="11"
							font-family="var(--font-mono)">orkflow {node.task.orkflow.name}@{node.task.orkflow.version}</text
						>
					{:else}
						<text
							x={node.x + 12}
							y={node.y + 43}
							fill="var(--color-fg-muted)"
							font-size="11"
							font-family="var(--font-mono)">{activityLabel(node.task) || '—'}</text
						>
					{/if}
					{#if node.task.each}
						<!-- A fan-out: one node, run as up to max_items items. -->
						<text
							data-testid="dag-node-{node.task.id}-each"
							x={node.x + DAG_NODE_W - 12}
							y={node.y + 24}
							text-anchor="end"
							fill="var(--color-brand-700)"
							font-size="11"
							font-family="var(--font-mono)">each ≤ {node.task.each.maxItems}</text
						>
					{/if}
					{#if node.task.cache}
						<!-- A cached task: memoized across runs (docs/DSL.md "Caching"). -->
						<text
							data-testid="dag-node-{node.task.id}-cache"
							x={node.x + DAG_NODE_W - 12}
							y={node.y + 43}
							text-anchor="end"
							fill="var(--color-brand-700)"
							font-size="11"
							font-family="var(--font-mono)">cached</text
						>
					{/if}
				</g>
			{/each}
		</svg>
	</div>
{/if}
