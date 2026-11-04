import { errorMessage } from '#lib/client.ts';
import { orkflowClient } from '#lib/clients/orkflows.ts';
import { runClient } from '#lib/clients/runs.ts';
import { namespaceFrom } from '#lib/namespace.ts';
import type { RunArtifact } from '#lib/gen/orkstack/v1/run_service_pb.ts';
import type { PageLoad } from './$types';

// The run page renders in the browser only. Server-rendered, a run at the
// task ceiling was sent as one HTML document of every task card plus the same
// rows again as hydration data (15 MB at 4,000 tasks), and the browser then
// parsed it and hydrated every card: twice the work of rendering it here,
// before the page answered a click. Everything on it is live state the
// browser keeps anyway.
export const ssr = false;

interface ArtifactsRead {
	artifacts: RunArtifact[];
	error: string | null;
}
const NO_ARTIFACTS: ArtifactsRead = { artifacts: [], error: null };

export const load: PageLoad = async ({ fetch, params, url }) => {
	// A run id addresses a run inside a namespace, not across the deployment:
	// the engine answers NotFound for a run of another one, so the page has to
	// say which it is asking about.
	const namespace = namespaceFrom(url);
	// This page sets code, and a face that lands after thousands of rows are drawn
	// has the browser re-style all of them: ask for it before there are any.
	void globalThis.document?.fonts?.load('13px "JetBrains Mono Variable"');
	try {
		const res = await runClient(fetch).getRun({ runId: params.id, namespace });
		const run = res.run ?? null;
		let orkflow = null;
		// Why the pinned definition could not be read, when it could not: the
		// page still renders every task, in the run's own order, and says why
		// its DAG is missing.
		let orkflowError: string | null = null;
		// The artifact list does not hold the page up either: at thousands of tasks
		// it is a second of the opening, for what only an opened task shows. The
		// page takes the answer when it lands (RunView).
		const artifactsRead: Promise<ArtifactsRead> = run
			? runClient(fetch)
					.listRunArtifacts({ runId: run.id, namespace, taskId: '' })
					.then(
						(answer) => ({ artifacts: answer.artifacts, error: null }),
						(err) => ({ artifacts: [], error: errorMessage(err) })
					)
			: Promise.resolve(NO_ARTIFACTS);
		if (run) {
			// The retry history of every task is NOT read here: the run, its
			// statuses and its logs need only GetRun, and a run of a thousand
			// fan-out items should not wait on its whole attempt timeline to
			// draw. The page reads it once it has rendered (#lib/attempts.ts
			// readAttemptSeed) and fills timing in when it lands.
			try {
				const wfRes = await orkflowClient(fetch).getOrkflow({
					name: run.orkflowName,
					version: run.orkflowVersion,
					namespace
				});
				orkflow = wfRes.orkflow ?? null;
				if (!orkflow) orkflowError = 'the engine returned no definition';
			} catch (err) {
				orkflow = null; // statuses still render without it
				orkflowError = errorMessage(err);
			}
		}
		return {
			runId: params.id,
			run,
			orkflow,
			orkflowError,
			artifactsRead,
			namespace,
			error: null as string | null
		};
	} catch (err) {
		return {
			runId: params.id,
			run: null,
			orkflow: null,
			orkflowError: null as string | null,
			artifactsRead: Promise.resolve(NO_ARTIFACTS),
			namespace,
			error: errorMessage(err)
		};
	}
};
