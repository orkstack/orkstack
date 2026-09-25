<script lang="ts">
	// A namespace's hold, stated where a person is about to try a submit.
	//
	// It is a snapshot of the server's own state — the layout's namespace
	// read, or the submit panel's fresh one — never a rule the UI invented:
	// the server is the gate, and a submit forced through by any other means
	// is refused by the same check (docs/UI_CLI_PARITY.md). The release
	// command is the CLI's, so the fix is one copy away.
	import CliEquivalent from './CliEquivalent.svelte';
	import type { NamespaceHold } from '$lib/gen/orkstack/v1/namespace_pb';
	import { fmtTimestamp } from '$lib/status';

	interface Props {
		namespace: string;
		hold: NamespaceHold;
		testid: string;
	}

	let { namespace, hold, testid }: Props = $props();
</script>

<div
	role="status"
	data-testid={testid}
	class="mb-4 flex items-start gap-3 rounded-lg border border-danger-200 bg-danger-50 px-4 py-3 text-sm text-danger-800"
>
	<div class="min-w-0 flex-1">
		<p class="font-semibold" data-testid="{testid}-title">
			On hold since {fmtTimestamp(hold.since)} — "{hold.reason}"
		</p>
		<p class="mt-1 text-danger-700">
			New runs of namespace <span class="font-mono">{namespace}</span> are refused until the hold is
			released. Runs already accepted finish untouched.
		</p>
	</div>
	<CliEquivalent
		testid="{testid}-cli"
		label="Command that releases this hold"
		command={`orkctl namespace update ${namespace} --release-hold`}
	/>
</div>
