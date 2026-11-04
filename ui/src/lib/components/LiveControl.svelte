<script lang="ts">
	// The live-updates control (brief §7): a state chip (role="status") and a
	// toggle. Live: a navy dot with a quiet pulse ring, "Live", then "Pause
	// updates". Paused: a hollow dot, "Paused", then "Resume updates", the
	// whole pill dashed and grey. The toggle's name is its visible label,
	// swapped between states; no aria-pressed, because the label already says
	// the state. Its tooltip says exactly what pausing does.
	import Icon from './Icon.svelte';

	interface Props {
		paused: boolean;
		ontoggle: () => void;
		/** What it is about: "updates" (a page) or "log" (the log pane). */
		what?: 'updates' | 'log';
		testid?: string;
	}

	let { paused, ontoggle, what = 'updates', testid }: Props = $props();

	const subject = $derived(what === 'log' ? 'Log' : 'Updates');
	const label = $derived(paused ? (what === 'log' ? 'Resume log' : 'Resume updates') : what === 'log' ? 'Pause log' : 'Pause updates');
	const tip = $derived(
		paused
			? what === 'log'
				? 'The log is paused; nothing is lost. Resume to catch up.'
				: 'Updates to this page are paused; nothing is lost. Resume to catch up.'
			: what === 'log'
				? 'Pause the live log. The task keeps going; resume to catch up.'
				: 'Pause live updates to this page. The run keeps going; resume to catch up.'
	);
</script>

<span class="livectl" class:paused data-testid={testid}
	><span class="live-state" role="status"
		><i class="live-dot" aria-hidden="true"></i><span class="sr">{subject}: </span>{paused ? 'Paused' : 'Live'}</span
	><button class="live-tg" type="button" data-tip={tip} data-testid={testid ? `${testid}-toggle` : undefined} onclick={ontoggle}
		><Icon name={paused ? 'play' : 'pause'} />{label}</button
	></span
>
