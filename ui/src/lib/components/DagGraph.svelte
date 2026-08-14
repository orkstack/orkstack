<script lang="ts">
	import type { Task } from '$lib/gen/orkstack/v1/workflow_pb';
	import { DAG_NODE_H, DAG_NODE_W, layoutDag } from '$lib/dag';

	let { tasks }: { tasks: Task[] } = $props();

	const layout = $derived(layoutDag(tasks));
</script>

{#if tasks.length === 0}
	<p class="text-sm text-zinc-500">No tasks.</p>
{:else}
	<div class="overflow-x-auto rounded-lg border border-zinc-800 bg-zinc-900/40 p-2">
		<svg
			data-testid="workflow-dag"
			width={layout.width}
			height={layout.height}
			viewBox="0 0 {layout.width} {layout.height}"
			class="block"
			role="img"
			aria-label="Workflow task DAG"
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
					<path d="M 0 1 L 9 5 L 0 9 z" fill="#71717a" />
				</marker>
			</defs>
			{#each layout.edges as edge (edge.fromId + '->' + edge.toId)}
				<path
					data-testid="dag-edge-{edge.fromId}-{edge.toId}"
					d="M {edge.x1} {edge.y1} C {edge.x1 + 48} {edge.y1}, {edge.x2 - 48} {edge.y2}, {edge.x2 -
						2} {edge.y2}"
					fill="none"
					stroke="#71717a"
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
						fill="#18181b"
						stroke="#3f3f46"
						stroke-width="1.5"
					/>
					<text x={node.x + 12} y={node.y + 24} fill="#f4f4f5" font-size="13" font-weight="600">
						{node.task.id}
					</text>
					<text x={node.x + 12} y={node.y + 43} fill="#a1a1aa" font-size="11">
						plugin: {node.task.plugin}
					</text>
				</g>
			{/each}
		</svg>
	</div>
{/if}
