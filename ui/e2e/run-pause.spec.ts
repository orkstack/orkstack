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
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('run-pause');

// Pausing a run from its page, in a browser, against the real stack (E103):
//
//   R1  Pause run opens its dialog, which asks the engine first (a dry run)
//       and offers Ends after from the deployment's default to 31 days;
//       Keep running closes it and sends nothing;
//   R2  confirming pauses the run: the pill reads Paused with its own mark
//       and accessible words, the Paused card says who, why and until when,
//       the task still running finishes and the one that needs it stays
//       Pending, and Unpause run replaces Pause run;
//   R3  the Runs list shows the run Paused, its Paused tile filters to it,
//       and the sidebar says it is paused;
//   R4  Unpause run lets the held task run and the run succeed; the card
//       goes, Last pause says it was unpaused, and neither control is left.
//
// Ports and a Temporal namespace of this spec's own (AGENTS.md §11.1).

const ENGINE = 'http://127.0.0.1:18353';
const UI_ORIGIN = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('run-pause');
const STAMP = Date.now().toString(36);
const ORKFLOW = `run-pause-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-pause')
	: path.join(uiDir, 'test-results', 'run-pause');
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, 'run-pause-session.txt'));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// slow runs long enough to be paused while it runs; after needs it, so the
// pause holds after while slow finishes.
function orkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
version: "0.1.0"
description: A run to pause while a task runs.

tasks:
  - id: slow
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "sleep 12"]
  - id: after
    uses: shell
    needs: [slow]
    with:
      op: exec
      command: ["sh", "-c", "echo after"]
`;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	// Prerequisites are failures, never skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18353)) throw new Error('port 18353 already in use (stray process?)');
	ensureActivityImages();
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18353',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv('run-pause'),
		ORK_ORKER_ID: 'orker-e2e-pause'
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow() });
	note(`applied ${ORKFLOW}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('a run is paused and unpaused from its page', async ({ page }) => {
	// The Orker may still be booting: the run waits for it rather than being
	// refused, and the wait for slow's Running below bounds that.
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: ORKFLOW,
		allowEmptyPool: true
	});
	const run = submitted.run.id;
	note(`submitted ${run}`);
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('task-slow-status')).toHaveText('Running', { timeout: 120_000 });

	// The header keeps one line: the name, cut short beside a live run's
	// actions, stays whole as the link's name and in the tooltip, on hover
	// and on keyboard focus.
	const title = page.getByTestId('pinned-orkflow-link');
	await expect(title).toHaveAccessibleName(ORKFLOW);
	expect(await title.evaluate((e) => e.scrollWidth > e.clientWidth)).toBe(true);
	await title.hover();
	await expect(page.getByRole('tooltip')).toHaveText(ORKFLOW);
	await page.mouse.move(0, 0);
	await title.focus();
	await page.keyboard.press('Shift+Tab');
	await page.keyboard.press('Tab');
	await expect(page.getByRole('tooltip')).toHaveText(ORKFLOW);
	await page.keyboard.press('Escape');

	// R1 — the dialog asks the engine first, and its back button sends nothing.
	const pauseButton = page.getByTestId('pause-run');
	await expect(pauseButton).toHaveText('Pause run');
	await expect(pauseButton).not.toHaveAttribute('aria-disabled', 'true');
	await pauseButton.click();
	const dialog = page.getByTestId('pause-run-confirm');
	await expect(dialog).toBeVisible();
	const ends = page.getByTestId('pause-ends-after');
	await expect(ends.locator('option')).toHaveCount(7);
	await expect(ends.locator('option').first()).toHaveText("The deployment's default (7 days unless set)");
	await expect(ends.locator('option').last()).toHaveText('31 days');
	await expect(page.getByTestId('pause-run-dismiss')).toBeFocused();
	await shot(page, 'pause-dialog.png');
	await page.getByTestId('pause-run-dismiss').click();
	await expect(dialog).toHaveCount(0);
	await expect(page.getByTestId('run-status')).toHaveText('Running');

	// R2 — pause it for a day, with a reason.
	await pauseButton.click();
	await page.getByTestId('pause-reason').fill('db maintenance');
	await ends.selectOption({ label: '1 day' });
	await expect(page.getByTestId('pause-run-accept')).not.toHaveAttribute('aria-disabled', 'true');
	await page.getByTestId('pause-run-accept').click();
	const pill = page.getByTestId('run-status');
	await expect(pill).toHaveText('Paused', { timeout: 30_000 });
	await expect(pill).toHaveClass(/mark-pause/);
	await expect(page.getByRole('heading', { level: 1 })).toContainText('Paused: no new tasks start');
	const card = page.getByTestId('run-paused');
	await expect(card).toBeVisible({ timeout: 15_000 });
	await expect(page.getByTestId('run-paused-reason')).toHaveText('db maintenance');
	await expect(page.getByTestId('run-paused-by')).toContainText('asserted');
	await expect(page.getByTestId('run-paused-ends-at')).toHaveText(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
	await expect(page.getByTestId('pause-run')).toHaveCount(0);
	await expect(page.getByTestId('unpause-run')).toHaveText('Unpause run');
	note(`R2 paused; card: ${await card.innerText()}`);
	await shot(page, 'paused-running.png');

	// The running task finishes; the one that needs it is held Pending. Its
	// end is the progress the held window is observed against.
	await expect(page.getByTestId('task-slow-status')).toHaveText('Succeeded', { timeout: 60_000 });
	await expect(page.getByTestId('run-paused-line')).toHaveText('Nothing is running; nothing new starts', { timeout: 15_000 });
	const heldUntil = Date.now() + 3_000;
	while (Date.now() < heldUntil) {
		await expect(page.getByTestId('task-after-status')).toHaveText('Pending');
		await page.waitForTimeout(500);
	}
	await shot(page, 'paused-held.png');

	// R3 — the list, its tile and the sidebar.
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	const tile = page.getByTestId('runs-tile-paused');
	await expect(tile).toBeVisible({ timeout: 30_000 });
	await expect(page.getByTestId('nav-running')).toContainText('1 paused', { timeout: 30_000 });
	// What a screen reader announces: the link's accessible name, not its text.
	await expect(page.getByTestId('nav-runs')).toHaveAccessibleName(/\b1 paused\b/);
	await tile.click();
	await expect(page.getByRole('row').filter({ hasText: run.slice(-6) }).getByText('Paused')).toBeVisible({ timeout: 30_000 });
	await shot(page, 'runs-paused.png');

	// R4 — unpause; the held task runs and the run succeeds.
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await page.getByTestId('unpause-run').click();
	await expect(page.getByTestId('run-status')).toHaveText(/Running|Succeeded/, { timeout: 30_000 });
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 120_000 });
	await expect(page.getByTestId('task-after-status')).toHaveText('Succeeded');
	await expect(page.getByTestId('run-paused')).toHaveCount(0);
	await expect(page.getByTestId('run-last-pause')).toContainText('Unpaused');
	await expect(page.getByTestId('pause-run')).toHaveCount(0);
	await expect(page.getByTestId('unpause-run')).toHaveCount(0);
	await shot(page, 'unpaused-succeeded.png');
	note(`R4 ${run} succeeded after its unpause`);
});
