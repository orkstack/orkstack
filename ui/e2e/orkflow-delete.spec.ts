import { expect, test, type Page } from "./test";
import { mkdirSync } from "node:fs";
import path from "node:path";
import {
  bin,
  catalogEnv,
  clusterEnv,
  ensureBucket,
  killStarted,
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
  waitForEngine,
} from "./harness";

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('orkflow-delete');

// Orkflow deletion through the real built UI and engine. The UI-only safety
// states use targeted ListRuns responses; successful deletion uses the real
// Temporal-backed ListRuns check and the real S3-backed DeleteOrkflow RPC.
const ENGINE = "http://127.0.0.1:18091";
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('orkflow-delete');
const NS = "ui-delete";
const STAMP = Date.now().toString(36);
const DELETABLE = `deletable-${STAMP}`;
const BLOCKED = `blocked-${STAMP}`;
const UNCHECKED = `unchecked-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
  ? path.join(process.env.ORK_PROOF_DIR, "orkflow-delete")
  : path.join(uiDir, "test-results", "orkflow-delete");
const outPath = proofOut(PROOF_DIR);
const rpc = <T>(method: string, body: unknown): Promise<T> =>
  callEngine<T>(ENGINE, method, body);

function orkflowYaml(name: string): string {
  return `apiVersion: orkstack/v1
kind: Orkflow
name: ${name}
version: "0.1.0"
description: deletion browser proof
tasks:
  - id: greet
    uses: shell
    with:
      op: exec
      command: ["echo", "hello"]
`;
}

const versionsUrl = (name: string) =>
  `${UI_ORIGIN}/orkflows/${name}?ns=${NS}&tab=versions`;

/** The header's More menu, open: Delete Orkflow lives in it, on every tab. */
async function openMore(page: Page) {
  await page.getByTestId("orkflow-more").click();
  return page.getByTestId("orkflow-delete");
}

/** Type the Orkflow's name, which is what lets the confirm go. */
async function typeName(page: Page, name: string) {
  await page.getByTestId("delete-confirm-name").fill(name);
}

test.describe.configure({ mode: "serial" });

test.beforeAll(async () => {
  ensureBucket(BUCKET);
  mkdirSync(PROOF_DIR, { recursive: true });
  if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
  if (!(await tcpOpen(9000)))
    throw new Error("MinIO is not reachable on 127.0.0.1:9000");
  if (await tcpOpen(18091))
    throw new Error("port 18091 already in use (stray process?)");

  spawnLogged(outPath("engine.log"), bin("ork"), [], {
    ...catalogEnv,
    ...clusterEnv(TEMPORAL_NAMESPACE),
    ...storeEnv(BUCKET),
    ORK_HTTP_ADDR: "127.0.0.1:18091",
    ...uiEnv(),
  });
  await waitForEngine(ENGINE);
  await waitFor(
    "built ui",
    async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok,
  );

  await rpc("NamespaceService/CreateNamespace", {
    namespace: { name: NS, description: "Orkflow deletion UI proof" },
  });
  for (const name of [DELETABLE, BLOCKED, UNCHECKED]) {
    await rpc("OrkflowService/ApplyOrkflow", {
      yaml: orkflowYaml(name),
      namespace: NS,
    });
  }
});

test.afterAll(async () => {
  await killStarted();
  removeBucket(BUCKET);
});

test("the delete control confirms irreversible deletion and cancel changes nothing", async ({
  page,
}) => {
  await page.goto(versionsUrl(DELETABLE));
  const button = await openMore(page);
  await expect(button).toBeVisible();
  await expect(button).toBeEnabled();
  await expect(button).toHaveText("Delete Orkflow");
  await expect(button.locator("svg use")).toHaveAttribute("href", "#i-trash");

  await button.click();
  const dialog = page.getByTestId("delete-confirm");
  await expect(dialog).toBeVisible();
  await expect(page.getByTestId("delete-accept").locator("svg use")).toHaveAttribute("href", "#i-trash");
  await expect(dialog).toContainText(
    "This action will delete all versions and cannot be reversed.",
  );
  // The confirm is held until the name is typed, whatever else was pressed.
  const accept = page.getByTestId("delete-accept");
  await expect(accept).toHaveAttribute("aria-disabled", "true");
  await typeName(page, `${DELETABLE}x`);
  await expect(accept).toHaveAttribute("aria-disabled", "true");
  await typeName(page, DELETABLE);
  await expect(accept).not.toHaveAttribute("aria-disabled", "true");
  await dialog.screenshot({ path: outPath("confirmation-dialog.png") });
  await page.getByTestId("delete-cancel").click();
  await expect(dialog).toHaveCount(0);
  await expect(page.getByTestId("version-row-0.1.0")).toBeVisible();
  await expect(
    rpc("OrkflowService/GetOrkflow", { name: DELETABLE, namespace: NS }),
  ).resolves.toBeTruthy();
});

test("a server refusal is shown verbatim in the open confirmation dialog", async ({
  page,
}) => {
  const refusal = `cannot delete orkflow "${DELETABLE}" in namespace "${NS}": a run appeared`;
  await page.route(
    "**/orkstack.v1.OrkflowService/DeleteOrkflow",
    async (route) => {
      await route.fulfill({
        status: 400,
        contentType: "application/json",
        body: JSON.stringify({ code: "failed_precondition", message: refusal }),
      });
    },
  );
  await page.goto(versionsUrl(DELETABLE));
  await expect(await openMore(page)).toBeEnabled();
  await page.getByTestId("orkflow-delete").click();
  await typeName(page, DELETABLE);
  await page.getByTestId("delete-accept").click();
  await expect(page.getByTestId("delete-confirm")).toContainText(refusal);
  await expect(page).toHaveURL(versionsUrl(DELETABLE));
  await page.unroute("**/orkstack.v1.OrkflowService/DeleteOrkflow");
  await page.getByTestId("delete-cancel").click();
});

test("an active run behind 200 old terminal runs disables deletion through an exact status query", async ({
  page,
}) => {
  type WireFilter = { field: string; op: string; values: string[] };
  const requests: Array<{ filters?: WireFilter[]; pageSize?: number }> = [];
  await page.route("**/orkstack.v1.RunService/ListRuns", async (route) => {
    const body = route.request().postDataJSON() as {
      filters?: WireFilter[];
      pageSize?: number;
    };
    requests.push(body);
    // The status the check asks about is a filter now, never a typed field.
    const status = body.filters?.find((f) => f.field === "status")?.values[0];
    let runs: unknown[] = [];
    if (status === "RUNNING") {
      runs = [{ id: "ork-older-active", orkflowName: BLOCKED, status: "STATUS_RUNNING" }];
    } else if (!status) {
      const finishedAt = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();
      runs = Array.from({ length: 200 }, (_, index) => ({
        id: `ork-newer-old-${index}`,
        orkflowName: BLOCKED,
        status: "STATUS_SUCCEEDED",
        finishedAt,
      }));
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ runs }),
    });
  });
  await page.goto(versionsUrl(BLOCKED));
  await expect(await openMore(page)).toBeDisabled();
  await expect(page.getByTestId("orkflow-delete-status")).toContainText(
    "At least 1 run(s) block deletion (1 active status(es), 0 finished within 24h",
  );
  const orkflowFilter = { field: "orkflow_name", op: "FILTER_OPERATOR_EQUALS", values: [BLOCKED] };
  expect(requests).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        filters: [orkflowFilter, { field: "status", op: "FILTER_OPERATOR_EQUALS", values: ["RUNNING"] }],
        pageSize: 1,
      }),
      expect.objectContaining({
        filters: [orkflowFilter, { field: "status", op: "FILTER_OPERATOR_EQUALS", values: ["PENDING"] }],
        pageSize: 1,
      }),
      expect.objectContaining({ filters: [orkflowFilter], pageSize: 200 }),
    ]),
  );
  await page.screenshot({
    path: outPath("deletion-blocked.png"),
    fullPage: true,
  });
  await page.unroute("**/orkstack.v1.RunService/ListRuns");
});

test("Delete Orkflow is in the header's More menu on every tab, and no longer on the Versions tab", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  for (const tab of ["overview", "versions", "edit", "schedules", "triggers"]) {
    await page.goto(`${UI_ORIGIN}/orkflows/${DELETABLE}?ns=${NS}&tab=${tab}`);
    const header = page.locator("main header").first();
    await expect(header.getByTestId("orkflow-more")).toBeVisible();
    await expect(await openMore(page)).toBeVisible();
    await page.keyboard.press("Escape");
  }
  await page.goto(versionsUrl(DELETABLE));
  await expect(page.getByTestId("versions-delete")).toHaveCount(0);
  await openMore(page);
  await page.screenshot({ path: outPath("delete-in-more-menu.png") });
});

test("a failed run check keeps deletion disabled and names the failed check", async ({
  page,
}) => {
  await page.route("**/orkstack.v1.RunService/ListRuns", (route) =>
    route.abort(),
  );
  await page.goto(versionsUrl(UNCHECKED));
  await expect(await openMore(page)).toBeDisabled();
  await expect(page.getByTestId("orkflow-delete-status")).toContainText(
    "Deletion is disabled because runs could not be checked",
  );
  await page.unroute("**/orkstack.v1.RunService/ListRuns");
});

test("accepting deletion returns to the namespace-scoped list and removes the orkflow", async ({
  page,
}) => {
  await page.goto(versionsUrl(DELETABLE));
  await expect(await openMore(page)).toBeEnabled();
  await page.getByTestId("orkflow-delete").click();
  await typeName(page, DELETABLE);
  await page.getByTestId("delete-accept").click();
  await page.waitForURL(new RegExp(`/orkflows\\?ns=${NS}$`));
  await expect(page.getByTestId(`orkflow-row-${DELETABLE}`)).toHaveCount(0);
  await expect(
    rpc("OrkflowService/GetOrkflow", { name: DELETABLE, namespace: NS }),
  ).rejects.toThrow(/not_found|404|not found/i);
});
