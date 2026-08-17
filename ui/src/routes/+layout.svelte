<script lang="ts">
	import '../app.css';

	let { children } = $props();
</script>

<div class="min-h-screen">
	<header class="border-b border-zinc-800 bg-zinc-900/60">
		<nav class="mx-auto flex max-w-6xl items-center gap-6 px-6 py-3">
			<a href="/orkflows" class="text-lg font-bold tracking-tight text-zinc-50">
				Ork<span class="text-emerald-400">Stack</span>
			</a>
			<a href="/orkflows" class="text-sm text-zinc-300 hover:text-white">Orkflows</a>
			<a href="/runs" class="text-sm text-zinc-300 hover:text-white">Runs</a>
			<a href="/submit" class="text-sm text-zinc-300 hover:text-white">Submit run</a>
			<a href="/apply" class="text-sm text-zinc-300 hover:text-white">Apply orkflow</a>
		</nav>
	</header>
	<main class="mx-auto max-w-6xl px-6 py-8">
		{@render children()}
	</main>
</div>
