import { defineRunPageCeilingSpec } from './runPageCeiling';

// E7 at the deployment's default engine.orkflow_max_tasks: the run page of a
// flat stage of 4,000 tasks is interactive within 5 s, live and finished
// (ORK_SOAK_TASKS selects a fallback size, as it does for the task soak).
// The nightly tier and the pre-release gate run this file
// (scripts/proof-nightly-specs.txt); the merge gate runs
// run-page-reduced-ceiling.spec.ts, the same measurement at 1,000.
defineRunPageCeilingSpec({
	name: 'run-page-ceiling',
	file: 'run-page-ceiling.spec.ts',
	title: 'the run page at the gated task size is interactive within 5 s, live and finished',
	tasks: Number(process.env.ORK_SOAK_TASKS ?? 4000),
	enginePort: 18471,
	orkflowPrefix: 'run-page-gated',
	sizeNote: 'the deployment default engine.orkflow_max_tasks; the engine runs at its defaults'
});
