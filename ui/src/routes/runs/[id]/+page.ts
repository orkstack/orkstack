import { Code, ConnectError } from '@connectrpc/connect';
import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import type { RunArtifact } from '$lib/gen/orkstack/v1/service_pb';
import type { TaskAttempt } from '$lib/gen/orkstack/v1/run_pb';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params, url }) => {
	const client = orkstack(fetch);
	// A run id addresses a run inside a namespace, not across the deployment:
	// the engine answers NotFound for a run of another one, so the page has to
	// say which it is asking about.
	const namespace = namespaceFrom(url);
	try {
		const res = await client.runs.getRun({ runId: params.id, namespace });
		const run = res.run ?? null;
		let artifacts: RunArtifact[] = [];
		let artifactsError: string | null = null;
		let attempts: TaskAttempt[] = [];
		let attemptsError: string | null = null;
		// The engine has no attempt store at all (FailedPrecondition): not a
		// transient read failure, but "no record will ever exist" — the timing
		// derivation names it as its own reason.
		let attemptStoreAbsent = false;
		let orkflow = null;
		if (run) {
			try {
				const artifactRes = await client.runs.listRunArtifacts({ runId: run.id, namespace, taskId: '' });
				artifacts = artifactRes.artifacts;
			} catch (err) {
				artifactsError = errorMessage(err);
			}
			// The retry history of EVERY task in one call — `taskId: ''` selects
			// them all, and the response already arrives in the order this page
			// draws them. It is read as tolerantly as the artifacts above and for
			// the same reason: an engine with no attempt store configured answers
			// FailedPrecondition, and a deployment that cannot say what a task's
			// earlier tries did must still render the run, its statuses and its
			// logs.
			try {
				const attemptRes = await client.runs.listTaskAttempts({
					runId: run.id,
					namespace,
					taskId: ''
				});
				attempts = attemptRes.attempts;
			} catch (err) {
				attemptsError = errorMessage(err);
				attemptStoreAbsent = err instanceof ConnectError && err.code === Code.FailedPrecondition;
			}
			try {
				const wfRes = await client.orkflows.getOrkflow({
					name: run.orkflowName,
					version: run.orkflowVersion,
					namespace
				});
				orkflow = wfRes.orkflow ?? null;
			} catch {
				orkflow = null; // orkflow may have been superseded; statuses still render
			}
		}
		return {
			runId: params.id,
			run,
			orkflow,
			artifacts,
			artifactsError,
			attempts,
			attemptsError,
			attemptStoreAbsent,
			namespace,
			error: null as string | null
		};
	} catch (err) {
		return {
			runId: params.id,
			run: null,
			orkflow: null,
			artifacts: [],
			artifactsError: null,
			attempts: [] as TaskAttempt[],
			attemptsError: null,
			attemptStoreAbsent: false,
			namespace,
			error: errorMessage(err)
		};
	}
};
