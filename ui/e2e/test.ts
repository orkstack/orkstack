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
// module graph cannot link, SvelteKit cannot hydrate, and the page is its
// 500 screen. That is the browser reporting the host, not the UI reporting
// anything, and it failed a different spec in a different place in half the
// proofs of this tree (browsing-operations, admission, run-origin,
// logs-dashboard), always at a navigation, always with this error code in the
// trace. network-change.spec.ts reproduces it on demand.
//
// WHAT. A navigation (`goto`, `reload`) during which Chromium aborted a
// document, script or stylesheet with net::ERR_NETWORK_CHANGED is made again,
// up to NAVIGATION_ATTEMPTS times, and each repeat is printed so it appears in
// the suite's log. That is all:
//
//   - only that error code. Every other failure — a 500 from the server, a
//     chunk that 404s, a refused connection — reaches the spec exactly as it
//     did, because those are things the UI did;
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

/** The resource types whose loss leaves a page that cannot hydrate. */
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
): Promise<Response | null> {
  for (let attempt = 1; ; attempt++) {
    const aborted: string[] = [];
    const onFailed = (request: Request) => {
      if (
        request.failure()?.errorText === NETWORK_CHANGED &&
        PAGE_CODE.has(request.resourceType())
      ) {
        aborted.push(request.url());
      }
    };
    page.on("requestfailed", onFailed);
    let response: Response | null = null;
    try {
      response = await navigate();
    } catch (err) {
      // The document itself can be the request that was waiting. Any
      // other navigation error is the spec's to see.
      if (!String(err).includes(NETWORK_CHANGED)) throw err;
      aborted.push(`${label} (the document)`);
    } finally {
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
    console.log(
      `e2e: ${what}; navigating again (attempt ${attempt + 1} of ${NAVIGATION_ATTEMPTS})`,
    );
  }
}

/** guardPage makes `page.goto` and `page.reload` navigate through network changes. */
export function guardPage(page: Page): void {
  if (guardedPages.has(page)) return;
  guardedPages.add(page);
  const goto = page.goto.bind(page);
  const reload = page.reload.bind(page);
  page.goto = (url, options) =>
    navigateThroughNetworkChanges(page, `goto ${url}`, () =>
      goto(url, options),
    );
  page.reload = (options) =>
    navigateThroughNetworkChanges(page, `reload ${page.url()}`, () =>
      reload(options),
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
