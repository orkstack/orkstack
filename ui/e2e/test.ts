import {
  test as base,
  type BrowserContext,
  type Page,
  type Request,
  type Response,
} from "@playwright/test";

export { expect, type Page } from "@playwright/test";

// The `test` every spec in this directory uses: Playwright's own, with one
// change to how a page NAVIGATES, and nothing else.
//
// WHY. These specs run activity containers on the same host as the browser,
// and Docker gives every container a veth interface in the host's network
// namespace — added when the container starts, removed when it stops, each
// with an IPv6 link-local address of its own. Chromium watches the host's
// addresses over netlink, and on any change it flushes its socket pools with
// net::ERR_NETWORK_CHANGED: every request still waiting for a connection
// fails. A page load of the built UI asks for some forty chunks at once
// against a six-connection-per-host limit, so most of them are waiting; when
// a container starts or stops in that window, the chunks waiting die, the
// module graph cannot link, SvelteKit cannot start, and the page is its
// error screen. That is the browser reporting the host, not the UI reporting
// anything, and it failed a different spec in a different place in half the
// proofs of this tree (browsing-operations, admission, run origin,
// logs-dashboard), always at a navigation, always with this error code in the
// trace. network-change.spec.ts reproduces it on demand.
//
// WHAT. A navigation (`goto`, `reload`) during which Chromium aborted a
// document, script or stylesheet with net::ERR_NETWORK_CHANGED is made again,
// up to NAVIGATION_ATTEMPTS times, and each repeat is printed so it appears in
// the suite's log. "During" lasts until the page's own code has arrived, not
// only until the navigation resolves: the `load` event does not wait for a
// script the page imports dynamically, and every page renders in the
// browser only (the UI is a static single-page app), so it is still fetching
// most of its chunks when `goto` returns — a network change in that window left the run-origin
// spec a 500 screen after a navigation the guard had already passed. So the
// guard waits, after the navigation, until no script or stylesheet the page
// requested is still in flight (and the page has had a turn to request the
// next), bounded by CODE_SETTLE_MS, and judges the whole window. That is all:
//
//   - only that error code. Every other failure — an error from the engine
//     that serves the page, a chunk that 404s, a refused connection — reaches
//     the spec exactly as it did, because those are things the UI did;
//   - only the page's own code. A fetch or XHR the app makes is the app's
//     business; how it reports a lost request is behavior a spec may assert,
//     so it is never retried here;
//   - only the navigation. Nothing a spec asserts afterwards is retried, and
//     a navigation that keeps failing fails, naming the requests and the
//     cause, rather than passing.
//
// Contexts a spec opens itself with `browser.newContext()` get the same
// guard, so a second browser session standing in for a colleague is not the
// one left exposed.

/** How many times one navigation is made before its failure is reported. */
export const NAVIGATION_ATTEMPTS = 3;

export const NETWORK_CHANGED = "net::ERR_NETWORK_CHANGED";

/**
 * How long, after a navigation resolves, the guard waits for the page's own
 * code still in flight. Past it the navigation is judged on what it saw; the
 * spec's own assertions decide the rest.
 */
export const CODE_SETTLE_MS = 30_000;

/**
 * How long, after a navigation resolves, the guard waits for the app to draw
 * the page it opened (appDrawn). Past it the spec's own assertions see
 * whatever is there.
 */
export const APP_DRAWN_MS = 30_000;

/** The resource types whose loss leaves a page that cannot start. */
const PAGE_CODE = new Set(["document", "script", "stylesheet"]);

const guardedPages = new WeakSet<Page>();
const guardedContexts = new WeakSet<BrowserContext>();

/**
 * navigateThroughNetworkChanges runs `navigate`, and runs it again when
 * Chromium aborted any of the page's own code with net::ERR_NETWORK_CHANGED
 * while it ran. It returns what the last navigation returned.
 */
export async function navigateThroughNetworkChanges(
  page: Page,
  label: string,
  navigate: () => Promise<Response | null>,
  report: (line: string) => void = console.log,
): Promise<Response | null> {
  for (let attempt = 1; ; attempt++) {
    const aborted: string[] = [];
    const inFlight = new Set<Request>();
    const onRequest = (request: Request) => {
      if (PAGE_CODE.has(request.resourceType())) inFlight.add(request);
    };
    const onFinished = (request: Request) => {
      inFlight.delete(request);
    };
    const onFailed = (request: Request) => {
      inFlight.delete(request);
      if (
        request.failure()?.errorText === NETWORK_CHANGED &&
        PAGE_CODE.has(request.resourceType())
      ) {
        aborted.push(request.url());
      }
    };
    page.on("request", onRequest);
    page.on("requestfinished", onFinished);
    page.on("requestfailed", onFailed);
    let response: Response | null = null;
    try {
      response = await navigate();
      await pageCodeSettled(page, inFlight, Date.now() + CODE_SETTLE_MS);
      await appDrawn(page, Date.now() + APP_DRAWN_MS);
    } catch (err) {
      // The document itself can be the request that was waiting. Any
      // other navigation error is the spec's to see.
      if (!String(err).includes(NETWORK_CHANGED)) throw err;
      aborted.push(`${label} (the document)`);
    } finally {
      page.off("request", onRequest);
      page.off("requestfinished", onFinished);
      page.off("requestfailed", onFailed);
    }
    if (aborted.length === 0) return response;
    const what =
      `${label}: Chromium aborted ${aborted.length} request(s) with ${NETWORK_CHANGED} ` +
      `(the host's network interfaces changed during the load)`;
    if (attempt >= NAVIGATION_ATTEMPTS) {
      throw new Error(
        `${what} on every one of ${NAVIGATION_ATTEMPTS} attempts:\n  ${aborted.join("\n  ")}`,
      );
    }
    report(
      `e2e: ${what}; navigating again (attempt ${attempt + 1} of ${NAVIGATION_ATTEMPTS})`,
    );
  }
}

/**
 * pageCodeSettled returns once none of the page's scripts or stylesheets in
 * `inFlight` is still loading and the page has had a turn to request more
 * (its module graph imports the next chunks only once the last ones ran), or
 * at `deadline`. It waits on request events, never on a clock of its own,
 * and a page that closed or navigated away has settled.
 */
async function pageCodeSettled(
  page: Page,
  inFlight: Set<Request>,
  deadline: number,
): Promise<void> {
  while (Date.now() < deadline) {
    if (inFlight.size === 0) {
      try {
        await page.evaluate(() => 0);
      } catch {
        return;
      }
      if (inFlight.size === 0) return;
    }
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(timer);
        page.off("requestfinished", done);
        page.off("requestfailed", done);
        page.off("close", done);
        resolve();
      };
      const timer = setTimeout(done, Math.max(0, deadline - Date.now()));
      page.on("requestfinished", done);
      page.on("requestfailed", done);
      page.on("close", done);
    });
  }
}

/**
 * appDrawn returns once the UI has drawn the page a navigation opened, or at
 * `deadline`. The UI is a static single-page app: the engine answers every
 * route with the same index.html, and the page is drawn in the browser, into
 * the element app.html gives SvelteKit, only once the root layout's and the
 * route's loads have answered — after `goto` has resolved on the document's
 * `load` event. Every spec was written against a page that was already drawn
 * when the navigation returned, as it was when a server rendered it, and
 * acting on the empty document in between finds nothing, measures nothing
 * and screenshots nothing. So a navigation is not over until the app has
 * drawn. A document that is not the app's (a JSON or YAML file of the build,
 * the placeholder page, an error from the engine) is drawn as it arrives,
 * and a page that never draws is left to the spec's own assertions, which
 * then see it.
 */
async function appDrawn(page: Page, deadline: number): Promise<void> {
  try {
    await page.waitForFunction(
      () => {
        const app = document.querySelector('link[href*="/_app/immutable/entry/start."]');
        if (!app) return true;
        const root = document.querySelector('body > div[style*="display: contents"]');
        return !!root && [...root.children].some((el) => el.tagName !== "SCRIPT");
      },
      undefined,
      { timeout: Math.max(1, deadline - Date.now()) },
    );
  } catch {
    // Closed, navigated away, or never drawn: the spec's assertions decide.
  }
}

/**
 * guardPage makes `page.goto` and `page.reload` navigate through network
 * changes. Each repeat is reported through `report` — the suite's log by
 * default, which the proof's ui-e2e ledger counts against
 * NETWORK_CHANGE_RETRY_CEILING (playwright.config.ts). A spec that provokes a
 * repeat on purpose, on a host-namespace browser of its own
 * (network-change.spec.ts), passes its transcript instead, so a deliberate
 * repeat is never counted as churn the suite's browser saw.
 */
export function guardPage(
  page: Page,
  report: (line: string) => void = console.log,
): void {
  if (guardedPages.has(page)) return;
  guardedPages.add(page);
  const goto = page.goto.bind(page);
  const reload = page.reload.bind(page);
  page.goto = (url, options) =>
    navigateThroughNetworkChanges(
      page,
      `goto ${url}`,
      () => goto(url, options),
      report,
    );
  page.reload = (options) =>
    navigateThroughNetworkChanges(
      page,
      `reload ${page.url()}`,
      () => reload(options),
      report,
    );
}

/** guardContext guards every page of `context`, the ones it has and the ones it opens. */
export function guardContext(context: BrowserContext): void {
  if (guardedContexts.has(context)) return;
  guardedContexts.add(context);
  for (const page of context.pages()) guardPage(page);
  context.on("page", guardPage);
}

export const test = base.extend({
  context: async ({ context }, use) => {
    guardContext(context);
    await use(context);
  },
  browser: [
    async ({ browser }, use) => {
      const newContext = browser.newContext.bind(browser);
      browser.newContext = async (options) => {
        const context = await newContext(options);
        guardContext(context);
        return context;
      };
      await use(browser);
    },
    { scope: "worker" },
  ],
});
