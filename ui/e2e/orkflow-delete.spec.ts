import { expect, test } from "./test";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import {
  catalogEnv,
  clusterEnv,
  killStarted,
  proofOut,
  rpc as callEngine,
  s3Env,
  spawnLogged,
  tcpOpen,
  uiDir,
  repoRoot,
  waitFor,
  waitForEngine,
} from "./harness";

// Orkflow deletion through the real built UI and engine. The UI-only safety
// states use targeted ListRuns responses; successful deletion uses the real
// Temporal-backed ListRuns check and the real S3-backed DeleteOrkflow RPC.
const ENGINE = "http://127.0.0.1:18089";
const ENGINE_WS = "ws://127.0.0.1:18089";
const UI_ORIGIN = "http://127.0.0.1:14182";
const TEMPORAL_NAMESPACE = "ork-ui-orkflow-delete";
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
  return `name: ${name}
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

test.describe.configure({ mode: "serial" });

test.beforeAll(async () => {
  mkdirSync(PROOF_DIR, { recursive: true });
  if (!(await tcpOpen(7233)))
    throw new Error("Temporal is not reachable on 127.0.0.1:7233");
  if (!(await tcpOpen(9000)))
    throw new Error("MinIO is not reachable on 127.0.0.1:9000");
  for (const port of [18089, 14182]) {
    if (await tcpOpen(port))
      throw new Error(`port ${port} already in use (stray process?)`);
  }

  execFileSync("go", ["build", "-o", "bin/ork", "./cmd/ork"], {
    cwd: repoRoot,
  });
  execFileSync("bun", ["run", "build"], { cwd: uiDir, stdio: "inherit" });
  if (!existsSync(path.join(uiDir, "build", "index.js"))) {
    throw new Error("UI build output missing: ui/build/index.js");
  }

  spawnLogged(outPath("engine.log"), "./bin/ork", [], {
    ...catalogEnv,
    ...clusterEnv(TEMPORAL_NAMESPACE),
    ...s3Env,
    ORK_HTTP_ADDR: "127.0.0.1:18089",
    ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN,
  });
  await waitForEngine(ENGINE);
  spawnLogged(
    outPath("ui.log"),
    "node",
    ["build"],
    {
      HOST: "127.0.0.1",
      PORT: "14182",
      ENGINE_API_URL: ENGINE,
      PUBLIC_ENGINE_WS_URL: ENGINE_WS,
    },
    uiDir,
  );
  await waitFor(
    "built ui",
    async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok,
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
});

test("the delete control confirms irreversible deletion and cancel changes nothing", async ({
  page,
}) => {
  await page.goto(versionsUrl(DELETABLE));
  const button = page.getByTestId("versions-delete");
  await expect(button).toBeVisible();
  await expect(button).toBeEnabled();

  await button.click();
  const dialog = page.getByTestId("delete-confirm");
  await expect(dialog).toBeVisible();
  await expect(dialog).toContainText(
    "This action will delete all versions and cannot be reversed.",
  );
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
  await expect(page.getByTestId("versions-delete")).toBeEnabled();
  await page.getByTestId("versions-delete").click();
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
  await expect(page.getByTestId("versions-delete")).toBeDisabled();
  await expect(page.getByTestId("versions-delete-status")).toContainText(
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

test("a failed run check keeps deletion disabled and names the failed check", async ({
  page,
}) => {
  await page.route("**/orkstack.v1.RunService/ListRuns", (route) =>
    route.abort(),
  );
  await page.goto(versionsUrl(UNCHECKED));
  await expect(page.getByTestId("versions-delete")).toBeDisabled();
  await expect(page.getByTestId("versions-delete-status")).toContainText(
    "Deletion is disabled because runs could not be checked",
  );
  await page.unroute("**/orkstack.v1.RunService/ListRuns");
});

test("accepting deletion returns to the namespace-scoped list and removes the orkflow", async ({
  page,
}) => {
  await page.goto(versionsUrl(DELETABLE));
  await expect(page.getByTestId("versions-delete")).toBeEnabled();
  await page.getByTestId("versions-delete").click();
  await page.getByTestId("delete-accept").click();
  await page.waitForURL(new RegExp(`/orkflows\\?ns=${NS}$`));
  await expect(page.getByTestId(`orkflow-row-${DELETABLE}`)).toHaveCount(0);
  await expect(
    rpc("OrkflowService/GetOrkflow", { name: DELETABLE, namespace: NS }),
  ).rejects.toThrow(/not_found|404|not found/i);
});
