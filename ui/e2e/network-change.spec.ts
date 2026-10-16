import { test as unguarded } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { createServer, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { noteTo, uiDir, waitFor } from "./harness";
import { expect, NETWORK_CHANGED, test, type Page } from "./test";

// The failure ./test.ts guards every navigation against, made on purpose:
//
//   N1  (the cause) a page load during which the host's network interfaces
//       change loses the code it was still waiting for: Chromium aborts every
//       request queued behind its six-connections-per-host limit with
//       net::ERR_NETWORK_CHANGED, and the page is left without it. This is
//       the 500 screen several specs met mid-proof whenever a container
//       started or stopped under a page load;
//   N2  (the guard) the same load through the `test` the specs use arrives
//       whole: the navigation is made again, exactly once, and every one of
//       the page's scripts ran.
//   N3  (the cause, after `load`) a page that imports its code dynamically —
//       as a page rendered in the browser only does — has its navigation
//       resolve while that code is still in flight, and a network change in
//       that window loses it after the navigation was over;
//   N4  (the guard, after `load`) the same load through the guarded `test`
//       waits for that code, sees the loss, and navigates again: the page the
//       spec gets ran every script.
//
// The page is this spec's own — a document and CHUNKS module scripts from a
// local server that can hold its responses — so the window is deterministic
// rather than a race against the suite's containers. The network change is
// a real one, made the way the suite makes them: Docker adding a network
// interface to the host and removing it again (a bridge network rather than a
// container, so the spec needs no image).

const CHUNKS = 16;
/** Chromium's HTTP/1.1 connections per host: requests past it wait in the pool. */
const PER_HOST = 6;

const PROOF_DIR = process.env.ORK_PROOF_DIR
  ? path.join(process.env.ORK_PROOF_DIR, "network-change")
  : path.join(uiDir, "test-results", "network-change");
const note = noteTo(path.join(PROOF_DIR, "network-change-session.txt"));

let server: Server | undefined;
let origin = "";
let holding = false;
let documents = 0;
const held: ServerResponse[] = [];

function sendChunk(res: ServerResponse) {
  res.writeHead(200, {
    "content-type": "text/javascript",
    "cache-control": "no-store",
  });
  res.end("globalThis.__chunks = (globalThis.__chunks ?? 0) + 1;\n");
}

unguarded.beforeAll(async () => {
  mkdirSync(PROOF_DIR, { recursive: true });
  server = createServer((req, res) => {
    if (req.url === "/") {
      documents++;
      const scripts = Array.from(
        { length: CHUNKS },
        (_, i) => `<script type="module" src="/chunk/${i}.js"></script>`,
      ).join("\n");
      res.writeHead(200, {
        "content-type": "text/html",
        "cache-control": "no-store",
      });
      res.end(
        `<!doctype html><html><head>${scripts}</head><body>chunks</body></html>`,
      );
      return;
    }
    if (req.url === "/dynamic") {
      documents++;
      // Imported from a module script: Chromium fires `load` without waiting
      // for these, so the navigation resolves while they are held.
      res.writeHead(200, {
        "content-type": "text/html",
        "cache-control": "no-store",
      });
      res.end(
        `<!doctype html><html><head><script type="module">` +
          `for (let i = 0; i < ${CHUNKS}; i++) import("/chunk/" + i + ".js");` +
          `</script></head><body>dynamic chunks</body></html>`,
      );
      return;
    }
    if (req.url?.startsWith("/chunk/")) {
      if (holding) held.push(res);
      else sendChunk(res);
      return;
    }
    res.writeHead(404).end();
  });
  await new Promise<void>((resolve) => server!.listen(0, "127.0.0.1", resolve));
  origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

unguarded.afterAll(async () => {
  for (const res of held.splice(0)) res.destroy();
  await new Promise<void>((resolve) =>
    server ? server.close(() => resolve()) : resolve(),
  );
});

/** changeHostNetwork adds an interface to the host and takes it away again. */
function changeHostNetwork(tag: string) {
  const name = `orkstack-e2e-netchange-${Date.now().toString(36)}-${tag}`;
  execFileSync("docker", ["network", "create", name], { stdio: "pipe" });
  execFileSync("docker", ["network", "rm", name], { stdio: "pipe" });
}

/**
 * loadAcrossNetworkChange starts a load of the spec's page with the chunk
 * responses held, changes the host's network once Chromium has filled its
 * connections and queued the rest, and lets the held responses go once
 * Chromium has aborted something. It returns the requests Chromium aborted.
 */
async function loadAcrossNetworkChange(
  page: Page,
  tag: string,
  path = "/",
): Promise<{ aborted: string[]; resolvedWhileHeld: boolean }> {
  const aborted: string[] = [];
  let resolvedWhileHeld = false;
  page.on("requestfailed", (request) => {
    if (request.failure()?.errorText === NETWORK_CHANGED)
      aborted.push(request.url());
  });
  held.length = 0;
  holding = true;
  documents = 0;
  const navigation = page.goto(`${origin}${path}`).then((response) => {
    if (holding) resolvedWhileHeld = true;
    return response;
  });
  try {
    await waitFor(
      `${PER_HOST} chunk requests held open`,
      async () => held.length >= PER_HOST,
      30_000,
    );
    changeHostNetwork(tag);
    await waitFor(
      `Chromium to abort with ${NETWORK_CHANGED}`,
      async () => aborted.length > 0,
      30_000,
    );
  } finally {
    holding = false;
    for (const res of held.splice(0)) {
      if (!res.destroyed) sendChunk(res);
    }
  }
  await navigation;
  return { aborted, resolvedWhileHeld };
}

unguarded(
  "N1: a page load across a host network change loses the code it was waiting for",
  async ({ page }) => {
    const { aborted } = await loadAcrossNetworkChange(page, "n1");
    const ran = await page.evaluate(
      () => (globalThis as { __chunks?: number }).__chunks ?? 0,
    );
    note(
      `N1 unguarded: ${aborted.length} request(s) aborted with ${NETWORK_CHANGED}; ` +
        `${ran} of ${CHUNKS} scripts ran; document requested ${documents} time(s)`,
    );
    expect(aborted.length).toBeGreaterThan(0);
    expect(ran).toBeLessThan(CHUNKS);
    expect(documents).toBe(1);
  },
);

test("N2: the same load through the guarded test navigates again and arrives whole", async ({
  page,
}) => {
  const { aborted } = await loadAcrossNetworkChange(page, "n2");
  const ran = await page.evaluate(
    () => (globalThis as { __chunks?: number }).__chunks ?? 0,
  );
  note(
    `N2 guarded: ${aborted.length} request(s) aborted with ${NETWORK_CHANGED} on the first load; ` +
      `document requested ${documents} time(s); ${ran} of ${CHUNKS} scripts ran on the page the spec got`,
  );
  expect(aborted.length).toBeGreaterThan(0);
  expect(documents).toBe(2);
  expect(ran).toBe(CHUNKS);
});

unguarded(
  "N3: a navigation resolves before the code it imports, and a network change then loses it",
  async ({ page }) => {
    const { aborted, resolvedWhileHeld } = await loadAcrossNetworkChange(
      page,
      "n3",
      "/dynamic",
    );
    const ran = await page.evaluate(
      () => (globalThis as { __chunks?: number }).__chunks ?? 0,
    );
    note(
      `N3 unguarded, dynamic imports: navigation resolved while its code was held: ${resolvedWhileHeld}; ` +
        `${aborted.length} request(s) aborted with ${NETWORK_CHANGED}; ${ran} of ${CHUNKS} scripts ran; ` +
        `document requested ${documents} time(s)`,
    );
    expect(resolvedWhileHeld).toBe(true);
    expect(aborted.length).toBeGreaterThan(0);
    expect(ran).toBeLessThan(CHUNKS);
    expect(documents).toBe(1);
  },
);

test("N4: the guarded test waits for that code, and navigates again when it was lost", async ({
  page,
}) => {
  const { aborted, resolvedWhileHeld } = await loadAcrossNetworkChange(
    page,
    "n4",
    "/dynamic",
  );
  const ran = await page.evaluate(
    () => (globalThis as { __chunks?: number }).__chunks ?? 0,
  );
  note(
    `N4 guarded, dynamic imports: navigation resolved while its code was held: ${resolvedWhileHeld}; ` +
      `${aborted.length} request(s) aborted with ${NETWORK_CHANGED} on the first load; ` +
      `document requested ${documents} time(s); ${ran} of ${CHUNKS} scripts ran on the page the spec got`,
  );
  expect(resolvedWhileHeld).toBe(false);
  expect(aborted.length).toBeGreaterThan(0);
  expect(documents).toBe(2);
  expect(ran).toBe(CHUNKS);
});
