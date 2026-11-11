// Sharing across namespaces (docs/DSL.md "Sharing across namespaces"): a
// child run of another namespace's Orkflow runs in the parent's namespace and
// names its owner in Run.orkflowNamespace. Every rendering mirrors
// cmd/orkctl/compose.go.
import type { OrkflowShare } from './gen/orkstack/v1/orkflow_pb';
import type { Run } from './gen/orkstack/v1/run_pb';

/** The Orkflow a run ran: its name, or "namespace/name" for a shared one. */
export function runOrkflowLabel(run: Pick<Run, 'orkflowName' | 'orkflowNamespace'> | undefined): string {
	if (!run) return '';
	return run.orkflowNamespace ? `${run.orkflowNamespace}/${run.orkflowName}` : run.orkflowName;
}

/**
 * A grouped row's Orkflow (stats, failures, flakes, usage, cache usage): its
 * name, or "namespace/name" for another namespace's shared Orkflow, whose
 * runs are grouped apart from this namespace's own of that name.
 */
export function ownedLabel(namespace: string, name: string): string {
	return namespace && name ? `${namespace}/${name}` : name;
}

/** The namespace whose Orkflow a run ran, where its definition is read. */
export function orkflowOwner(run: Pick<Run, 'orkflowNamespace'> | undefined, namespace: string): string {
	return run?.orkflowNamespace || namespace;
}

/** An Orkflow's shared_with, one phrase per entry, in its own order. */
export function sharedWithLabel(shares: readonly Pick<OrkflowShare, 'namespace' | 'orkflow'>[]): string[] {
	return shares.map((share) => {
		if (share.namespace === '*') return 'every namespace';
		if (share.namespace) return `namespace ${share.namespace}`;
		return `Orkflow ${share.orkflow}`;
	});
}
