import { expect, test, type Page } from './test';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
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
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('run-page-pins');

// Task pins on the run detail page, in a browser, against the real stack.
//
// A pin is a named group of tasks that execute on ONE Orker process and share
// /orkstack/shared. What is measured here is the CLIENT SURFACE of that, and
// specifically the one claim a reader cannot verify any other way:
//
//   1. a pinned task's card states `Pin <name>` and `Orker <id>`;
//   2. the members of one pin name the SAME Orker — the client-visible proof
//      the pin actually held, not merely that the field is rendered. This is
//      asserted against a real dispatch, so a regression that scheduled the
//      members apart would fail here rather than pass a mocked fixture;
//   3. an unpinned task shows NO pin, and still shows the Orker that ran it,
//      because orker_id is populated for every task that reached one — an
//      absent Orker means "never got there", never "not pinned";
//   4. members of one pin share one visual grouping, and a DIFFERENTLY pinned
//      task does not share it.
//
// Topology: one engine, one PIN-HOST Orker (ORK_ORKER_MAX_PINS > 0), the
// production UI build. Isolation per AGENTS.md §11.1 — a Temporal namespace of
// this spec's own and a stamped orkflow name, so a parallel agent cannot steal
// these activities and two runs of this spec cannot collide.

// Ports of this spec's own, so the whole e2e directory can run in one go.
const ENGINE = 'http://127.0.0.1:18092';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;

const TEMPORAL_NAMESPACE = temporalNamespace('run-pins');
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const STAMP = Date.now().toString(36);
const PINNED_ORKFLOW = `run-page-pins-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-page-pins')
	: path.join(uiDir, 'test-results', 'run-page-pins');
const TRANSCRIPT = 'run-page-pins-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

/**
 * pinnedOrkflow: two pins and one unpinned task, which is the smallest shape
 * that can tell the three cases apart on one page.
 *
 * `write` and `read` share the pin `build` and hand a file over ON DISK
 * through /orkstack/shared — a handoff that is only possible if they really
 * ran on one host, so the run SUCCEEDING is itself part of the claim, not just
 * the labels the page draws afterwards.
 *
 * `stamp` is in a SECOND pin, `verify`. It exists so the grouping assertion
 * has a negative case that is still pinned: a treatment that simply tinted
 * every pinned task would pass with only one pin on the page.
 *
 * `checksum` is unpinned and reaches the pinned group's work the ordinary way,
 * through the artifact `read` committed.
 *
 * Both members of `build` set run_as_orker_user and neither names a pool,
 * because every task of one pin must agree on both (/orkstack/shared is 1777
 * with the sticky bit, and a pin is one process out of one pool's set).
 */
function pinnedOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${PINNED_ORKFLOW}
version: "0.1.0"
description: Two pinned tasks hand a file over on disk; a second pin and an unpinned task stand apart.

pins:
  build:
    timeout_seconds: 600
  verify:
    timeout_seconds: 600

tasks:
  - id: write
    uses: shell
    pin: build
    run_as_orker_user: true
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        mkdir -p /orkstack/shared/src
        echo "handed over on disk" > /orkstack/shared/src/version.txt
        cat /orkstack/shared/src/version.txt

  - id: read
    uses: shell
    pin: build
    needs: [write]
    run_as_orker_user: true
    timeout_seconds: 120
    with:
      op: exec
      command:
        - sh
        - -c
        - cat /orkstack/shared/src/version.txt | tee /orkstack/output/artifacts/version.txt

  - id: stamp
    uses: shell
    pin: verify
    run_as_orker_user: true
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo a second pin"]

  - id: checksum
    uses: file
    needs: [read]
    timeout_seconds: 120
    with:
      op: checksum
      src: '{{ artifact "read/version.txt" }}'
`;
}

/** cardAttr reads one attribute off a task's card. */
function cardAttr(page: Page, taskId: string, attribute: string): Promise<string | null> {
	return page.getByTestId(`task-${taskId}`).getAttribute(attribute);
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# Task pins on the run detail page: Pin and Orker per task, one',
			'# Orker across the members of one pin, and a visual grouping that',
			'# separates two pins.',
			'#',
			'# Written by ui/e2e/run-page-pins.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflow:            ${PINNED_ORKFLOW} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			`# container image:    ${IMAGE}`,
			''
		].join('\n')
	);

	// Prerequisites, stated as failures rather than skips: the caller is
	// expected to probe every one of them BEFORE it runs this spec, so an
	// absence never reaches here and a failure here is never an absence
	// (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18092)) throw new Error('port 18092 already in use (stray process?)');

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);

	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18092',
		...uiEnv()
	});
	// The one Orker is a PIN HOST: ORK_ORKER_MAX_PINS defaults to 0, and an
	// engine refuses a document that pins at SUBMIT time until an operator has
	// opted a pool in — a pinned run is never left waiting for a host that
	// does not exist. Two concurrent pins are declared here because the
	// orkflow has two.
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...cluster,
		...storeEnv(BUCKET),
		...orkerEnv('run-page-pins'),
		ORK_ORKER_MAX_PINS: '2'
	});
	await waitForEngine(ENGINE);

	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);

	// The engine refuses a submission whose pool has nobody serving it, so the
	// Orker has to be polling before anything is started.
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: pinnedOrkflow() });
	note(`applied ${PINNED_ORKFLOW}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('a pinned task states its pin and its orker, and one pin means one orker', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: PINNED_ORKFLOW
	});
	const run = submitted.run.id;
	note(`submitted run ${run}`);

	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	// The run must SUCCEED: `read` cats a file `write` left in /orkstack/shared,
	// so a green run is itself evidence the two shared one host's disk.
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });
	await shot(page, 'screenshot-run-pins.png');

	// 1. Each member of `build` says which pin it is in, in the author's own
	//    word, beside the pool it was routed to.
	await expect(page.getByTestId('task-write-pin')).toHaveText(/Pin build/);
	await expect(page.getByTestId('task-read-pin')).toHaveText(/Pin build/);
	await expect(page.getByTestId('task-stamp-pin')).toHaveText(/Pin verify/);

	// 2. THE PROOF. Both members name an Orker, and it is the SAME one. This
	//    is measured against a real dispatch, so a scheduler regression that
	//    put the members on different hosts fails here.
	const writeOrker = await page.getByTestId('task-write-orker').textContent();
	const readOrker = await page.getByTestId('task-read-orker').textContent();
	expect(writeOrker?.trim()).toMatch(/^Orker \S+/);
	expect(readOrker?.trim()).toBe(writeOrker?.trim());
	note(`pin build held on ${writeOrker?.trim()}`);

	// 3. The unpinned task shows NO pin and STILL shows its Orker: orker_id is
	//    populated for every task that reached one, so its presence is not
	//    evidence of a pin and its absence would mean the task never arrived.
	await expect(page.getByTestId('task-checksum-pin')).toHaveCount(0);
	await expect(page.getByTestId('task-checksum-orker')).toHaveText(/^Orker \S+/);
	await expect(page.getByTestId('task-checksum')).not.toContainText('Pin ');

	// 4. GROUPING. The members of one pin carry one accent; the task in the
	//    OTHER pin carries a different one, and the unpinned task carries
	//    none. The negative case is pinned on purpose: a treatment that tinted
	//    every pinned card alike would pass with a single pin on the page.
	const writeAccent = await cardAttr(page, 'write', 'data-pin-accent');
	const readAccent = await cardAttr(page, 'read', 'data-pin-accent');
	const stampAccent = await cardAttr(page, 'stamp', 'data-pin-accent');
	expect(writeAccent).toBeTruthy();
	expect(readAccent).toBe(writeAccent);
	expect(stampAccent).toBeTruthy();
	expect(stampAccent).not.toBe(writeAccent);
	expect(await cardAttr(page, 'checksum', 'data-pin-accent')).toBeNull();

	// The pin NAME is on the card too, so the colour is never the only signal.
	expect(await cardAttr(page, 'write', 'data-pin')).toBe('build');
	expect(await cardAttr(page, 'read', 'data-pin')).toBe('build');
	expect(await cardAttr(page, 'stamp', 'data-pin')).toBe('verify');
	expect(await cardAttr(page, 'checksum', 'data-pin')).toBeNull();

	// The accent is really PAINTED, not merely announced in an attribute, and
	// it is painted with the pin token rather than with a fallback.
	//
	// This assertion is load-bearing beyond "the two differ". The accent is
	// chosen at runtime and read with var() from an inline style, so the token
	// has to exist in the stylesheet; the first cut of it lived in Tailwind's
	// `@theme`, which tree-shakes variables no utility class references, and
	// every card was painted `currentColor` — a plausible dark edge that still
	// differed from the unpinned border and would have passed a mere
	// inequality. So the exact colour is named, resolved from the token itself
	// rather than hard-coded, and checked against the foreground it must not
	// have fallen back to.
	const edges = await page.evaluate(() => {
		const style = getComputedStyle(document.documentElement);
		const read = (id: string) =>
			getComputedStyle(document.querySelector(`[data-testid="task-${id}"]`)!).borderLeftColor;
		return {
			write: read('write'),
			stamp: read('stamp'),
			checksum: read('checksum'),
			// The tokens the two pinned cards must be wearing, and the colour an
			// unresolved var() would have fallen back to.
			pinF: style.getPropertyValue('--color-pin-f-line').trim(),
			pinE: style.getPropertyValue('--color-pin-e-line').trim(),
			foreground: style.getPropertyValue('--color-fg').trim()
		};
	});
	note(`accents ${JSON.stringify(edges)}`);
	// The tokens resolve at all — an emptied custom property is the failure
	// this whole assertion exists to catch.
	expect(edges.pinF).toBeTruthy();
	expect(edges.pinE).toBeTruthy();
	// `build` hashes to pin-f and `verify` to pin-e (see pinAccent). Comparing
	// through a canvas normalises #rrggbb against the rgb() a computed style
	// reports.
	const asRgb = async (hex: string) =>
		page.evaluate((value) => {
			const probe = document.createElement('span');
			probe.style.color = value;
			document.body.appendChild(probe);
			const resolved = getComputedStyle(probe).color;
			probe.remove();
			return resolved;
		}, hex);
	expect(edges.write).toBe(await asRgb(edges.pinF));
	expect(edges.stamp).toBe(await asRgb(edges.pinE));
	// Not the foreground: that is what an unresolved var() would have given.
	expect(edges.write).not.toBe(await asRgb(edges.foreground));
	// And the unpinned card keeps the ordinary card border.
	expect(edges.checksum).not.toBe(edges.write);
	expect(edges.checksum).not.toBe(edges.stamp);

	// The dark palette is a SECOND definition of the same tokens, so it can be
	// missing on its own. Flip the theme the way theme.ts does and check the
	// edge moved to the dark line rather than falling back.
	await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'dark'));
	const dark = await page.evaluate(() => {
		const style = getComputedStyle(document.documentElement);
		return {
			write: getComputedStyle(document.querySelector('[data-testid="task-write"]')!)
				.borderLeftColor,
			pinF: style.getPropertyValue('--color-pin-f-line').trim()
		};
	});
	note(`dark accents ${JSON.stringify(dark)}`);
	expect(dark.pinF).toBeTruthy();
	expect(dark.pinF).not.toBe(edges.pinF);
	expect(dark.write).toBe(await asRgb(dark.pinF));
	await shot(page, 'screenshot-run-pins-dark.png');
	await page.evaluate(() => document.documentElement.setAttribute('data-theme', 'light'));

	// PARITY. The page renders what the API says, so the same two fields are
	// read straight off GetRun — the response `orkctl run get --json` prints
	// verbatim. A page that agreed with itself but not with the CLI would pass
	// every assertion above.
	const exported = await rpc<{
		run: { taskRuns: { taskId: string; pin?: string; orkerId?: string }[] };
	}>('RunService/GetRun', { runId: run, namespace: 'default' });
	const write = exported.run.taskRuns.find((t) => t.taskId === 'write');
	const readTask = exported.run.taskRuns.find((t) => t.taskId === 'read');
	const checksum = exported.run.taskRuns.find((t) => t.taskId === 'checksum');
	expect(write?.pin).toBe('build');
	expect(readTask?.pin).toBe('build');
	expect(write?.orkerId).toBeTruthy();
	expect(readTask?.orkerId).toBe(write?.orkerId);
	// protojson omits an empty string, which is the CLI's `-` and the page's
	// absent label: an unpinned task carries no pin at all rather than "".
	expect(checksum?.pin).toBeUndefined();
	expect(checksum?.orkerId).toBeTruthy();
});
