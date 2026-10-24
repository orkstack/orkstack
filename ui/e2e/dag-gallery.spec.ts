import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
	noteTo,
	orkerEnv,
	ownBucket,
	proofOut,
	removeBucket,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	temporalAddress,
	temporalReachable,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';
import { DAG_STRESS, type StressTask } from '../test/dagStress';

// The DAG gallery: every stress shape of test/dagStress.ts — wide parallel
// stages, diamonds, long chains, joins and failure handlers, pins, a
// fan-out, a composed task, an await, crossing-prone shapes and DAGs near the
// run page's limit — applied as real Orkflows to a real engine and drawn on
// the Orkflow page as the Direction Rule chooses (the default, `auto`), left
// to right (`dag=lr`) and top to bottom (`dag=tb`), in light and dark at 1440
// and 390 px. Every task is
// a box exactly once and no two boxes overlap, as dagLayout.test.ts proves
// of the layout itself; here the browser's drawing is held to it, and a few
// shapes are run so the run page's DAG and task map are held to it too. The
// screens under ORK_PROOF_DIR are the left-to-right / top-to-bottom
// comparison's evidence.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = '18941';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14941';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-dag-gallery-${STAMP}`;
const BUCKET = ownBucket('dag-gallery');
const CHILD = `gallery-child-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'dag-gallery')
	: path.join(uiDir, 'test-results', 'dag-gallery');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

/** An Orkflow's name for a stress shape in this run. */
const nameOf = (shape: string) => `dag-${shape}-${STAMP}`;

/** A stress shape as an authored Orkflow: every task an `echo`, with its join, pin, fan-out, await or composed child. */
function orkflowYaml(shape: string, spec: StressTask[]): string {
	const pins = [...new Set(spec.flatMap((t) => (t.pin ? [t.pin] : [])))];
	const lines = ['apiVersion: orkstack/v1', 'kind: Orkflow', `name: ${nameOf(shape)}`, 'version: "1.0.0"', `description: The DAG gallery's ${shape}.`];
	if (pins.length) lines.push('pins:', ...pins.map((p) => `  ${p}: {}`));
	lines.push('tasks:');
	for (const t of spec) {
		lines.push(`  - id: ${t.id}`);
		if (t.needs.length) lines.push(`    needs: [${t.needs.join(', ')}]`);
		if (t.join) lines.push(`    join: ${t.join}`);
		if (t.pin) lines.push(`    pin: ${t.pin}`);
		if (t.await) {
			lines.push('    await:', '      timeout_seconds: 3600');
			continue;
		}
		if (t.orkflow) {
			lines.push(`    uses: orkflow:${CHILD}@1.0.0`);
			continue;
		}
		if (t.each) {
			lines.push('    each:', `      over: '{{ .tasks.${t.each.over}.output.result }}'`, '      as: part', `      max_items: ${t.each.maxItems}`);
		}
		if (spec.some((o) => o.each?.over === t.id)) {
			// The list a fan-out reads: a JSON array of three parts.
			lines.push('    uses: json', '    network: none', '    with: {op: query, nullInput: true, vars: {parts: [a, b, c]}, filter: "$vars.parts"}');
			continue;
		}
		lines.push('    uses: shell', '    network: none', '    timeout_seconds: 300', '    with: {op: exec, command: ["echo", "ok"]}');
	}
	return `${lines.join('\n')}\n`;
}

const child = () =>
	[
		'apiVersion: orkstack/v1',
		'kind: Orkflow',
		`name: ${CHILD}`,
		'version: "1.0.0"',
		'description: The composed child.',
		'tasks:',
		'  - id: only',
		'    uses: shell',
		'    network: none',
		'    with: {op: exec, command: ["echo", "ok"]}',
		''
	].join('\n');

async function terminal(runId: string): Promise<string> {
	let s = '';
	await waitFor(`run ${runId} finished`, async () => {
		s = (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId })).run?.status ?? '';
		return ['STATUS_SUCCEEDED', 'STATUS_FAILED', 'STATUS_CANCELED'].includes(s);
	}, 300_000);
	return s;
}

// The shapes run, so the run page's DAG and task map are drawn from a run.
const RUN_SHAPES = ['diamond-chain', 'joins-and-handlers'];
const runs = new Map<string, string>();

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(12 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('object storage is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	ensureActivityImages();
	ensureBucket(BUCKET);
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_S3_BUCKET: BUCKET,
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('dag-gallery') });
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: UI_PORT, ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: child() });
	for (const [shape, spec] of Object.entries(DAG_STRESS)) {
		await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflowYaml(shape, spec) });
	}
	for (const shape of RUN_SHAPES) {
		const id = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: nameOf(shape) })).run.id;
		runs.set(shape, id);
	}
	for (const [shape, id] of runs) note(`${shape}: run ${id} ${await terminal(id)}`);
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}; ${Object.keys(DAG_STRESS).length} shapes applied`);
});

test.afterAll(async () => {
	await killStarted();
	try {
		removeBucket(BUCKET, note);
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

/** Every drawn box, by task id, as the browser laid it out. */
async function drawnBoxes(page: Page, dag: string) {
	return page.evaluate((testid) => {
		const svg = document.querySelector(`[data-testid="${testid}"]`)!;
		return [...svg.querySelectorAll('g.dag-node')].map((g) => {
			const r = g.querySelector('rect')!.getBoundingClientRect();
			return { id: g.getAttribute('data-testid')!.replace(/^(run-)?dag-node-/, ''), x: r.x, y: r.y, w: r.width, h: r.height };
		});
	}, dag);
}

function overlapping(boxes: { id: string; x: number; y: number; w: number; h: number }[]): string[] {
	const found: string[] = [];
	for (let i = 0; i < boxes.length; i++) {
		for (let j = i + 1; j < boxes.length; j++) {
			const a = boxes[i];
			const b = boxes[j];
			if (a.x + a.w > b.x + 0.5 && b.x + b.w > a.x + 0.5 && a.y + a.h > b.y + 0.5 && b.y + b.h > a.y + 0.5) found.push(`${a.id}/${b.id}`);
		}
	}
	return found;
}

/** The appearance switch is in the top bar from 1024 px; set the theme there. */
async function setTheme(page: Page, theme: 'light' | 'dark' | 'system'): Promise<void> {
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${UI_ORIGIN}/orkflows?ns=default`);
	await page.getByTestId(`theme-${theme}`).click();
}

test('every shape is drawn whole, each task once and no box over another, left to right and top to bottom', async ({ page }) => {
	test.setTimeout(15 * 60_000);
	for (const theme of ['light', 'dark'] as const) {
		await setTheme(page, theme);
		for (const viewport of [
			{ label: '1440', width: 1440, height: 900 },
			{ label: '390', width: 390, height: 844 }
		]) {
			await page.setViewportSize({ width: viewport.width, height: viewport.height });
			for (const [shape, spec] of Object.entries(DAG_STRESS)) {
				// `auto` is the default (the Direction Rule): what a page with no
				// `dag=` draws.
				for (const dag of ['auto', 'lr', 'tb'] as const) {
					await page.goto(`${UI_ORIGIN}/orkflows/${nameOf(shape)}?ns=default${dag === 'auto' ? '' : `&dag=${dag}`}`);
					const drawAll = page.getByTestId('dag-draw-all');
					await expect(page.getByTestId('orkflow-dag').or(drawAll).first()).toBeVisible();
					if (await drawAll.isVisible()) await drawAll.click();
					const svg = page.getByTestId('orkflow-dag');
					await expect(svg).toBeVisible();
					const direction = await svg.getAttribute('data-direction');
					if (dag !== 'auto') expect(direction).toBe(dag.toUpperCase());
					const boxes = await drawnBoxes(page, 'orkflow-dag');
					expect(boxes.map((b) => b.id).sort(), `${shape}: every task once`).toEqual(spec.map((t) => t.id).sort());
					expect(overlapping(boxes), `${shape}: no box over another`).toEqual([]);
					await page.getByTestId('orkflow-drawing').screenshot({ path: out(`${shape}-${dag}-${theme}-${viewport.label}.png`) });
					if (theme === 'light') note(`${shape} ${dag} at ${viewport.label}: drawn ${direction}, ${boxes.length} boxes`);
					// The rule's two clear cases hold in the browser too.
					if (dag === 'auto' && shape === 'parallel-1x50') expect(direction).toBe('LR');
					if (dag === 'auto' && shape === 'chain-40') expect(direction).toBe('TB');
				}
			}
		}
	}
	await setTheme(page, 'system');
});

test('a run of each shape draws every task once on its DAG and its task map', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	for (const [shape, id] of runs) {
		const spec = DAG_STRESS[shape];
		for (const dag of ['lr', 'tb'] as const) {
			await page.goto(`${UI_ORIGIN}/runs/${id}?ns=default&view=dag&dag=${dag}`);
			await expect(page.getByTestId('run-dag')).toHaveAttribute('data-direction', dag.toUpperCase());
			const boxes = await drawnBoxes(page, 'run-dag');
			expect(boxes.map((b) => b.id).sort(), shape).toEqual(spec.map((t) => t.id).sort());
			expect(overlapping(boxes), shape).toEqual([]);
			await page.getByTestId('run-drawing').screenshot({ path: out(`run-${shape}-${dag}.png`) });
		}
		await page.getByTestId('run-view-map').click();
		await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
		await expect(page.getByTestId('task-map')).toHaveAttribute('data-cells', String(spec.length));
		await page.getByTestId('run-drawing').screenshot({ path: out(`run-${shape}-map.png`) });
		note(`${shape}: run ${id} drawn whole on its DAG (both ways) and its ${spec.length}-cell map`);
	}
});
