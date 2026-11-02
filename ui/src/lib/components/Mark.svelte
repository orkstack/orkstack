<script lang="ts">
	// The OrkStack mark, "Strata": three bars joined by round turns inside a
	// ring, with a dot where each end meets the ring. Ring and S take
	// --mark-ring and --mark-s, so the mark follows the theme.
	let { class: className = 'size-6.5' }: { class?: string } = $props();
</script>

<svg viewBox="0 0 32 32" aria-hidden="true" focusable="false" class="shrink-0 {className}"
	><circle cx="16" cy="16" r="11.5" fill="none" stroke="var(--mark-ring)" stroke-width="3" /><path
		d="M25.81 10 H13.6 A3 3 0 0 0 13.6 16 H18.4 A3 3 0 0 1 18.4 22 H6.19"
		fill="none"
		stroke="var(--mark-s)"
		stroke-width="3"
		stroke-linejoin="round"
	/><circle cx="25.81" cy="10" r="2.3" fill="var(--mark-s)" /><circle cx="6.19" cy="22" r="2.3" fill="var(--mark-s)" /></svg
>
