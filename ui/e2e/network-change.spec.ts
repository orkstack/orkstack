import {
  chromium,
  test as unguarded,
  type Browser,
  type Page as RawPage,
} from "@playwright/test";
import { execFileSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { createServer, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { noteTo, uiDir, waitFor } from "./harness";
import { expect, guardPage, NETWORK_CHANGED, type Page } from "./test";

// The failure ./test.ts guards every navigation against, made on purpose, and
// the isolation that keeps it away from the suite's own browser:
//
//   N1  (the cause) a page load during which the host's network interfaces
//       change loses the code it was still waiting for: Chromium aborts every
//       request queued behind its six-connections-per-host limit with
//       net::ERR_NETWORK_CHANGED, and the page is left without it. This is
//       the 500 screen several specs met mid-proof whenever a container
//       started or stopped under a page load;
//   N2  (the guard) the same load through the guarded page arrives whole: the
//       navigation is made again, exactly once, and every one of the page's
//       scripts ran.
//   N3  (the cause, after `load`) a page that imports its code dynamically —
//       as a page rendered in the browser only does — has its navigation
//       resolve while that code is still in flight, and a network change in
//       that window loses it after the navigation was over;
//   N4  (the guard, after `load`) the same load through the guarded page
//       waits for that code, sees the loss, and navigates again: the page the
//       spec gets ran every script.
//   N5  (the isolation) the SUITE's browser runs in a network namespace of
//       its own (browser-server.ts): the same load, made at the same moment
//       on it and on a host-namespace browser, loses nothing on the suite's
//       browser while the host browser — the positive control, proving the
//       change happened and a browser in the host's namespace saw it — loses
//       what N1 loses. Nothing is navigated again, so the proof's ledger may
//       hold the guard to a ceiling of zero repeats.
//   N6  (the isolation, after `load`) the same for a page that imports its
//       code dynamically.
//
// N1–N4 run on a Chromium this spec launches itself, in the HOST's network
// namespace, because that is the only place the cause can be shown; their
// guarded pages report their deliberate repeats to this spec's transcript,
// not to the suite's log the ledger counts (test.ts guardPage). N5 and N6 use
// the suite's own `page`, the one every other spec drives.
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
/** Documents served, by the `who` each page load names itself with. */
const documents = new Map<string, number>();
const held: ServerResponse[] = [];
/** The host-namespace Chromium N1–N4 drive and N5–N6 use as their control. */
let host: Browser | undefined;

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
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    const who = url.searchParams.get("who") ?? "page";
    if (url.pathname === "/") {
      documents.set(who, (documents.get(who) ?? 0) + 1);
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
    if (url.pathname === "/dynamic") {
      documents.set(who, (documents.get(who) ?? 0) + 1);
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
    if (url.pathname.startsWith("/chunk/")) {
      if (holding) held.push(res);
      else sendChunk(res);
      return;
    }
    res.writeHead(404).end();
  });
  await new Promise<void>((resolve) => server!.listen(0, "127.0.0.1", resolve));
  origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  host = await chromium.launch();
  note(`host-namespace control browser: Chromium ${host.version()}, launched by this spec`);
  const container = process.env.ORK_E2E_BROWSER_CONTAINER;
  if (container) {
    const inspected = execFileSync(
      "docker",
      [
        "inspect",
        "-f",
        "{{.Id}} network={{.HostConfig.NetworkMode}} image={{.Image}} {{.Config.Image}}",
        container,
      ],
      { encoding: "utf8" },
    ).trim();
    note(`suite browser server: ${inspected} at ${process.env.ORK_E2E_BROWSER_WS}`);
  } else {
    note(
      `suite browser: ${process.env.ORK_E2E_BROWSER === "host" ? "ORK_E2E_BROWSER=host, the host's own Chromium" : "no browser server recorded"}`,
    );
  }
});

unguarded.afterAll(async () => {
  for (const res of held.splice(0)) res.destroy();
  await host?.close();
  await new Promise<void>((resolve) =>
    server ? server.close(() => resolve()) : resolve(),
  );
});

/** changeHostNetwork adds an interface to the host and takes it away again. */
function changeHostNetwork(tag: string) {
  const name = `orkstack-e2e-netchange-${process.pid}-${Date.now().toString(36)}-${tag}`;
  execFileSync("docker", ["network", "create", name], { stdio: "pipe" });
  execFileSync("docker", ["network", "rm", name], { stdio: "pipe" });
}

interface Loaded {
  /** The requests Chromium aborted with net::ERR_NETWORK_CHANGED. */
  aborted: string[];
  /** Whether `goto` resolved while the page's code was still held. */
  resolvedWhileHeld: boolean;
  /** How many of the CHUNKS scripts ran on the page the load left. */
  ran: number;
  /** How many times the document was requested under this load's name. */
  documents: number;
}

/**
 * loadAcrossNetworkChange starts a load of the spec's page on each of `pages`
 * with the chunk responses held, changes the host's network once every page
 * has filled its connections and queued the rest, and lets the held responses
 * go once the page named `control` has aborted something. It returns what
 * each page saw.
 */
async function loadAcrossNetworkChange(
  pages: { page: RawPage; who: string }[],
  control: string,
  tag: string,
  pathname = "/",
): Promise<Map<string, Loaded>> {
  const seen = new Map<string, { aborted: string[]; resolvedWhileHeld: boolean }>();
  for (const { page, who } of pages) {
    const s = { aborted: [] as string[], resolvedWhileHeld: false };
    seen.set(who, s);
    page.on("requestfailed", (request) => {
      if (request.failure()?.errorText === NETWORK_CHANGED)
        s.aborted.push(request.url());
    });
    documents.delete(who);
  }
  held.length = 0;
  holding = true;
  const navigations = pages.map(({ page, who }) =>
    page.goto(`${origin}${pathname}?who=${who}`).then((response) => {
      if (holding) seen.get(who)!.resolvedWhileHeld = true;
      return response;
    }),
  );
  try {
    await waitFor(
      `${PER_HOST * pages.length} chunk requests held open`,
      async () => held.length >= PER_HOST * pages.length,
      30_000,
    );
    changeHostNetwork(tag);
    await waitFor(
      `${control} to abort with ${NETWORK_CHANGED}`,
      async () => seen.get(control)!.aborted.length > 0,
      30_000,
    );
  } finally {
    holding = false;
    for (const res of held.splice(0)) {
      if (!res.destroyed) sendChunk(res);
    }
  }
  await Promise.all(navigations);
  const out = new Map<string, Loaded>();
  for (const { page, who } of pages) {
    const ran = await page.evaluate(
      () => (globalThis as { __chunks?: number }).__chunks ?? 0,
    );
    const s = seen.get(who)!;
    out.set(who, { ...s, ran, documents: documents.get(who) ?? 0 });
  }
  return out;
}

/** hostPage opens a page on the host-namespace control browser; `guarded` wraps it as every spec's page is. */
async function hostPage(guarded: boolean): Promise<RawPage> {
  const page = await host!.newPage();
  if (guarded) guardPage(page as Page, note);
  return page;
}

unguarded(
  "N1: a page load across a host network change loses the code it was waiting for",
  async () => {
    const page = await hostPage(false);
    const got = (await loadAcrossNetworkChange([{ page, who: "n1" }], "n1", "n1")).get("n1")!;
    await page.close();
    note(
      `N1 unguarded, host namespace: ${got.aborted.length} request(s) aborted with ${NETWORK_CHANGED}; ` +
        `${got.ran} of ${CHUNKS} scripts ran; document requested ${got.documents} time(s)`,
    );
    expect(got.aborted.length).toBeGreaterThan(0);
    expect(got.ran).toBeLessThan(CHUNKS);
    expect(got.documents).toBe(1);
  },
);

unguarded(
  "N2: the same load through the guarded test navigates again and arrives whole",
  async () => {
    const page = await hostPage(true);
    const got = (await loadAcrossNetworkChange([{ page, who: "n2" }], "n2", "n2")).get("n2")!;
    await page.close();
    note(
      `N2 guarded, host namespace: ${got.aborted.length} request(s) aborted with ${NETWORK_CHANGED} on the first load; ` +
        `document requested ${got.documents} time(s); ${got.ran} of ${CHUNKS} scripts ran on the page the spec got`,
    );
    expect(got.aborted.length).toBeGreaterThan(0);
    expect(got.documents).toBe(2);
    expect(got.ran).toBe(CHUNKS);
  },
);

unguarded(
  "N3: a navigation resolves before the code it imports, and a network change then loses it",
  async () => {
    const page = await hostPage(false);
    const got = (
      await loadAcrossNetworkChange([{ page, who: "n3" }], "n3", "n3", "/dynamic")
    ).get("n3")!;
    await page.close();
    note(
      `N3 unguarded, host namespace, dynamic imports: navigation resolved while its code was held: ${got.resolvedWhileHeld}; ` +
        `${got.aborted.length} request(s) aborted with ${NETWORK_CHANGED}; ${got.ran} of ${CHUNKS} scripts ran; ` +
        `document requested ${got.documents} time(s)`,
    );
    expect(got.resolvedWhileHeld).toBe(true);
    expect(got.aborted.length).toBeGreaterThan(0);
    expect(got.ran).toBeLessThan(CHUNKS);
    expect(got.documents).toBe(1);
  },
);

unguarded(
  "N4: the guarded test waits for that code, and navigates again when it was lost",
  async () => {
    const page = await hostPage(true);
    const got = (
      await loadAcrossNetworkChange([{ page, who: "n4" }], "n4", "n4", "/dynamic")
    ).get("n4")!;
    await page.close();
    note(
      `N4 guarded, host namespace, dynamic imports: navigation resolved while its code was held: ${got.resolvedWhileHeld}; ` +
        `${got.aborted.length} request(s) aborted with ${NETWORK_CHANGED} on the first load; ` +
        `document requested ${got.documents} time(s); ${got.ran} of ${CHUNKS} scripts ran on the page the spec got`,
    );
    expect(got.resolvedWhileHeld).toBe(false);
    expect(got.aborted.length).toBeGreaterThan(0);
    expect(got.documents).toBe(2);
    expect(got.ran).toBe(CHUNKS);
  },
);

/**
 * isolationCase runs N5/N6: the suite's page (unguarded, so a loss would show
 * as a loss) and a host-namespace control page load the same held page while
 * the host's network changes. The control must lose code — the change
 * happened, and a host-namespace browser saw it — and the suite's page must
 * lose none, request its document once and run every script.
 */
async function isolationCase(
  suite: RawPage,
  label: string,
  tag: string,
  pathname: string,
) {
  if (process.env.ORK_E2E_BROWSER === "host") {
    throw new Error(
      `${label}: the suite's browser is the host's own (ORK_E2E_BROWSER=host), so it shares the host's network namespace and this isolation cannot hold; run without ORK_E2E_BROWSER to prove it`,
    );
  }
  const control = await hostPage(false);
  const got = await loadAcrossNetworkChange(
    [
      { page: control, who: `${tag}-control` },
      { page: suite, who: `${tag}-suite` },
    ],
    `${tag}-control`,
    tag,
    pathname,
  );
  await control.close();
  const c = got.get(`${tag}-control`)!;
  const s = got.get(`${tag}-suite`)!;
  // A page that imports its code after `load` is still fetching it when its
  // navigation resolves (N3), so the suite's page is given until every
  // script has run — through the connection, a little later than on a host
  // browser — before it is judged. A lost chunk never arrives, so a loss
  // would still fail here, by the deadline, and by `aborted` below.
  await waitFor(
    `${label}: every script to run on the suite's page`,
    async () =>
      (await suite.evaluate(
        () => (globalThis as { __chunks?: number }).__chunks ?? 0,
      )) === CHUNKS,
    15_000,
  );
  s.ran = await suite.evaluate(
    () => (globalThis as { __chunks?: number }).__chunks ?? 0,
  );
  note(
    `${label}: control (host namespace) aborted ${c.aborted.length} request(s) with ${NETWORK_CHANGED}, ${c.ran} of ${CHUNKS} scripts ran; ` +
      `suite browser aborted ${s.aborted.length}, ${s.ran} of ${CHUNKS} scripts ran, document requested ${s.documents} time(s)`,
  );
  // The positive control: the network did change, and a browser in the
  // host's namespace lost code to it.
  expect(c.aborted.length).toBeGreaterThan(0);
  // The claim: the suite's browser saw nothing of it.
  expect(s.aborted).toEqual([]);
  expect(s.documents).toBe(1);
  expect(s.ran).toBe(CHUNKS);
}

unguarded(
  "N5: the suite's browser, in its own network namespace, loses nothing to a host network change its control loses code to",
  async ({ page }) => {
    await isolationCase(page, "N5", "n5", "/");
  },
);

unguarded(
  "N6: the same holds for a page that imports its code after load",
  async ({ page }) => {
    await isolationCase(page, "N6", "n6", "/dynamic");
  },
);
