import { defineRunPageCeilingSpec } from './runPageCeiling';

// E7 at a reduced ceiling, the merge gate's positive control for
// run-page-ceiling.spec.ts, which runs at the 4,000-task default in the
// nightly tier (scripts/proof-nightly-specs.txt): the engine's
// engine.orkflow_max_tasks is lowered to 1,000 and the run page of a flat
// stage of that many tasks is interactive within 5 s, live and finished —
// the same helpers, budget and steps, as the Go side proves its ceilings
// at a reduced size (AGENTS.md §11).
const REDUCED_CEILING = 1000;

defineRunPageCeilingSpec({
	name: 'run-page-reduced',
	file: 'run-page-reduced-ceiling.spec.ts',
	title: 'the run page at a reduced task ceiling is interactive within 5 s, live and finished',
	tasks: REDUCED_CEILING,
	enginePort: 18474,
	orkflowPrefix: 'run-page-reduced',
	engineEnv: { ORK_ORKFLOW_MAX_TASKS: String(REDUCED_CEILING) },
	sizeNote: `engine.orkflow_max_tasks lowered to ${REDUCED_CEILING}, the ceiling this run is at`
});
