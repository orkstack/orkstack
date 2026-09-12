# List pagination and the dashboard time window: URL hygiene and a UI pass

Status: **design, for review**. Nothing here is implemented. Every claim
about the UI below was read from the tree at the commit this document was
written against (file:line cited), and every claim marked **reproduced** was
observed in a browser driving the SvelteKit dev server against a stand-in
engine (§1). Claims that could not be observed are marked **inferred** and
repeated in §8.

## 0. The one-paragraph version

Three complaints, two of which turn out to be the same design decision seen
from two sides. (1) The Orkflows page-size links append `&size=N` to a query
string that already carries `size=25`, so the URL reads `size=25&size=50` and
the click does nothing — a plain bug, one line. (2) Next/Previous on both
lists do *replace* the query string, but they serialise the whole stack of
earlier cursors into a `history=` parameter, so the address bar grows by one
opaque, URL-escaped cursor per page (202 → 339 → 476 characters over three
clicks, measured) — which is what "it appends instead of replacing" looks
like to a person reading the bar. The fix is one URL builder per list that
only ever `set`s keys, and moving the cursor stack out of the URL into
`sessionStorage` keyed by the list's coordinates, leaving the URL with just
`cursor` and a client-counted `page`. (3) The dashboard's custom window is a
detached form beside the preset group; when it is active nothing in the
control is marked and the only trace of the range is the escaped ISO string
in the URL. It becomes the fourth segment of the same group, marked current
like the presets and labelled with the applied instant in the same format the
page header already uses. A shared `Pagination.svelte` replaces the two
inline footers. The engine is not touched.

## 1. What was reproduced, and how

The engine stack (Temporal, MinIO, Docker) was not available on the machine
this was written on, and none of the three problems depends on engine
behaviour: they are href construction and rendering. So the UI was run with
`bun run dev` (`ENGINE_API_URL` pointed at a ~90-line Bun script speaking
Connect/JSON for `ListNamespaces`, `ListRuns`, `ListOrkflows` and `GetStats`,
with 47 runs and 33 orkflows and cursor tokens of the real engine's shape —
base64url of a JSON struct, `internal/api/service.go:1637-1678`). The proxy
in `ui/src/hooks.server.ts` forwards to it unchanged, so what the browser did
is what it does against a real engine.

| # | Observation | Status |
|---|---|---|
| R1 | `/orkflows?ns=default`: the four page-size links are `/orkflows?ns=default&size=25&size=10`, `…&size=25&size=25`, `…&size=25&size=50`, `…&size=25&size=100`. Clicking **50** lands on `?ns=default&size=25&size=50`; `searchParams.getAll('size')` is `["25","50"]`; 25 rows render and **25** stays bold. Clicking **100** afterwards lands on `…&size=25&size=100`. | **reproduced** |
| R2 | `/runs?ns=default&size=10`: Next → URL 202 chars, keys `ns,size,cursor,history`, `history=%5B%22%22%5D`. Next → 339 chars, `history` holds two entries. Next → 476 chars, three entries. Keys never duplicate. Previous returns to the 339-char URL. | **reproduced** |
| R3 | Runs "Page size" `Select`: choosing 50 changes only the draft (URL unchanged, 10 rows); **Apply filters** then navigates to `?ns=default&size=50` cleanly (one `size` key, 47 rows). | **reproduced** — not a bug, but see §3.4 |
| R4 | `/?ns=default&window=custom&since=2026-09-10T08%3A30%3A00.000Z`: no segment of the preset group carries `aria-current`; the `Since` input is empty; the only rendering of the range is the header line `9/10/2026, 2:30:00 AM → 9/11/2026, 4:12:40 PM` (`dashboard-window`) and the CLI snippet. | **reproduced** |
| R5 | The real engine's run cursor is `base64url(JSON{v,n,o,s,a?,b?,c,i})` (`service.go:1637-1646`, `:1673-1678`); with a namespace, orkflow name and two RFC3339Nano bounds it is 160–240 characters before URL-escaping. The stand-in's tokens were ~140. | **inferred** from source |
| R6 | SvelteKit's client router intercepts the `<a>` click and calls `goto(href)`; it does not merge the current query into the new one. The keys observed in R2 are exactly the keys the page builds. | **reproduced** (R2) and read (`node_modules/@sveltejs/kit/src/runtime/client/client.js`) |

## 2. Root causes

### 2.1 Orkflows page size: string concatenation onto a URL that already has the key

`ui/src/routes/orkflows/+page.svelte:62`:

```svelte
<a href={pageHref('', []) + `&size=${size}`} class:font-semibold={data.size === size}>{size}</a>
```

`pageHref` (`:9-14`) always writes `size: String(data.size)` into its
`URLSearchParams`, so the concatenation produces a second `size` key.
`ui/src/routes/orkflows/+page.ts:8` reads it with `url.searchParams.get('size')`,
which returns the **first** value, so the page reloads at the old size, the
bold marker stays where it was, and the duplicate key stays in the address
bar. This is the literal "appends instead of replaces" and it is confirmed
(R1).

### 2.2 Next/Previous: the cursor stack lives in the URL

Both lists implement Previous by remembering every cursor they have passed
through, as a JSON array in a `history` query parameter:

- Runs: `ui/src/routes/runs/+page.svelte:79` (`nextHref`) writes
  `history: JSON.stringify([...data.history, data.cursor])`; `:80`
  (`previousHref`) pops it. `ui/src/routes/runs/+page.ts:12` parses it with
  `cursorHistory` (`ui/src/lib/browsing.ts:28-30`).
- Orkflows: `ui/src/routes/orkflows/+page.svelte:101`, same shape, inline.

The builders (`runs/+page.svelte:69-74` `href()`, `orkflows/+page.svelte:9-14`
`pageHref()`) construct a fresh `URLSearchParams` and return an absolute
path, so the query string is replaced on every click — R2 shows the key set
stays `ns,size,cursor,history`. But `history` grows by one full cursor per
page, and each cursor is R5's 160–240 characters, escaped again inside the
JSON array (`%5B%22…%22%2C%22…%22%5D`). Five pages in, the address bar holds
roughly a kilobyte of base64 with `%22%2C%22` between the pieces. That is
the "garbage piling up".

Two smaller defects ride along:

- The first entry pushed is the empty string (page 1 has no cursor), so the
  first Next click already writes `history=%5B%22%22%5D` (R2). The helper
  that would avoid it, `pushCursor` (`browsing.ts:31`), exists and is
  imported by nothing. It could not simply be switched on, because both
  pages decide whether Previous is enabled from `data.history.length`
  (`runs/+page.svelte:229`, `orkflows/+page.svelte:101`), and with the
  sentinel gone page 2 would have an empty stack and a disabled Previous.
  The enabling condition is the wrong fact: "there is an earlier page" is
  `data.cursor !== ''`, not "the stack is non-empty".
- The URL exposes the engine's cursor internals (namespace, orkflow,
  bounds) N times over, once per stack entry, for no reader's benefit.

### 2.3 What is *not* the cause (checked)

- `withNamespace` (`ui/src/lib/namespace.ts:69-74`) parses the query it is
  given and `set`s `ns`; it cannot duplicate a key. Neither list calls it for
  pagination hrefs anyway — both write `ns` into their own params
  (`runs/+page.svelte:71`, `orkflows/+page.svelte:10`).
- `Select.svelte` calls `onchange` exactly once per committed choice
  (`:87-92`); on the Runs page that only assigns `draft.size` (`:133`) and
  navigation happens in `filter()` (`:75-78`) through `href()`, which
  replaces (R3).
- The layout's namespace-restore effect (`ui/src/routes/+layout.svelte:59-82`)
  only fires for URLs with no `ns`; every pagination href carries one.
- No stale closure: `href()` reads `data` at call time and the anchors are
  re-rendered on every navigation.

### 2.4 Dashboard custom window: state without a face

`ui/src/routes/+page.svelte:136-145` renders the three presets with
`aria-current={data.window === preset ? 'page' : undefined}`; `:146` renders
a separate `<form>` with an unbound `datetime-local` and a **Custom** submit
button. `ui/src/routes/+page.ts:27` returns `window: 'custom'` whenever a
valid `since` is present, which matches no preset, so nothing is current (R4).
The form's input has no `value`, so a reload shows an empty field beside a
window that is in force. The applied instant is rendered only by the header
(`:117-120`, via `fmtInstant`, `ui/src/lib/stats.ts:58-61`) and by the CLI
snippet (`:105`, raw `data.sinceParam`).

## 3. Fix: the URL

### 3.1 One builder per list, and it only ever `set`s

Each list keeps one function that turns *the page's current coordinates plus
a set of changes* into an absolute href. It is what `runs/+page.svelte:69-74`
already is; the Orkflows page gets the same shape instead of `pageHref(cursor,
history)` plus string concatenation. The rule the builder enforces:

- start from an empty `URLSearchParams`;
- `set` (never `append`) every coordinate that has a value, `ns` first
  (spelled out for `default`, as `withNamespace` does);
- omit empty values, so a cleared filter disappears from the bar rather than
  lingering as `since=`;
- `page` is omitted when it is 1 and `cursor` when it is empty.

To keep the two from drifting, the parameter-writing half moves into
`ui/src/lib/browsing.ts` as a pure function the pages call:

```
listHref(path: '/runs' | '/orkflows', coordinates: Record<string, string>): string
```

where `coordinates` already includes `ns`. The pages keep a thin `href(changes)`
that spreads `changes` over `data`'s coordinates and calls `listHref`. The
Orkflows size links call `href({ size, cursor: '', page: '' })`. The page-size
control on Orkflows becomes the same `Select` the Runs page uses (§4.4), so
the duplicated-key path no longer exists in any form.

### 3.2 The cursor stack leaves the URL

**What stays in the URL:** everything that is a coordinate of *what the page
shows* — `ns`, `orkflow`, `status`, `size`, `since`, `until`, `cursor` — plus
`page`, a 1-based number the client counts, so the footer can say "Page 3"
and a reload or a pasted link says it too. A pasted link still opens the
exact page it was copied from; that property (the design's "selection is
canonical in the URL on every route", quoted at `namespace.ts:66-68`) is
kept in full.

**What leaves:** the list of cursors *behind* the current one. It is not a
coordinate of the page; it is the path this reader took to get here, and it
is only ever consulted by Previous. It moves to `sessionStorage`:

- key: `orkstack.pages:` + `path` + `?` + the canonical filter query — the
  builder's output with `cursor` and `page` removed — so that a stack belongs
  to one filter set and can never be applied to another;
- value: a JSON array of cursors indexed by page: `stack[0] === ''` (page 1),
  `stack[i]` is the cursor of page `i+1`;
- written on every Next click, before navigating: `stack[page-1] = cursor;
  stack[page] = nextPageToken`;
- read on render: Previous is enabled when `page > 1` and
  `stack[page-1] === data.cursor` (the stack describes *this* page); its href
  is `href({ cursor: stack[page-2], page: String(page-1) })`.

When the stack is missing or does not describe this page — a pasted link, a
new tab, a stack written under a different filter set — Previous is disabled
and the footer offers **First page** instead (§4.2). That is an honest
degradation: the browser genuinely does not know the earlier cursor, and the
first page is the only earlier page it can name.

Why `sessionStorage` and not the alternatives considered:

- *Keep the stack in the URL, drop the sentinel* (§2.2): fixes
  `history=%5B%22%22%5D` but leaves R5 × pages in the bar, which is the
  complaint. Rejected.
- *`goto(url, { state })` / `page.state`*: SvelteKit keeps `page.state` per
  history entry and restores it on Back/Forward
  (`node_modules/@sveltejs/kit/src/runtime/client/client.js:2882`), which
  would make the stack ride the history entry with no key to manage. But on
  a full reload it is reset to `{}` (`client.js:3099`), so F5 on page 4
  would lose Previous. A reload of a list is an ordinary gesture. Rejected
  for that reason alone; otherwise the better fit.
- *`localStorage`*: survives the tab, which is the wrong lifetime — a stack
  from yesterday would be trusted today because its first entry still
  matches (§8, staleness). Session scope is the right one: the engine's
  cursor is a position, not a snapshot (`docs/UI_CLI_PARITY.md:58`), so a
  stack only means anything for as long as the reader who built it is still
  paging.
- *Ask the engine for a `previous_page_token`*: out of scope; the engine is
  not touched here, and reverse cursors over a union of live and archived
  runs are their own design.

The storage access is wrapped the way `rememberNamespace` already is
(`namespace.ts:79-86`): private-mode browsers throw, and a stack that cannot
be written simply means Previous is disabled and First page is offered.

### 3.3 Parsing

`ui/src/lib/browsing.ts`:

- `pageNumber(value: string): number` — a positive integer, else 1.
- `cursorHistory` and `pushCursor` are deleted; nothing reads `history` any
  more. A URL that still carries `history=` (an old bookmark) is simply
  ignored, since the loaders no longer look for it.
- `pageStackKey(path, coordinates)`, `readPageStack(key): string[]`,
  `writePageStack(key, stack)`, guarded with try/catch, taking the storage
  as a parameter with `globalThis.sessionStorage` as the default so the unit
  test can hand in a `Map`-backed fake.

`runs/+page.ts` and `orkflows/+page.ts` read `page` alongside `cursor` and
return it; a `page > 1` with an empty `cursor` is normalised to 1 (the number
is a label for a cursor, not a coordinate on its own).

### 3.4 The Runs page-size control

R3 shows the `Select` on Runs is correct but the user experience is that
picking a size does nothing until **Apply filters**. That is by design
(`runs/+page.svelte:23-31`: the form is a draft of the URL) and this document
does not change it, because page size is a filter-form field there and a
form that applied one field on change and the rest on submit would be two
mechanisms. The Orkflows page has no filter form, so its size control applies
on change (§4.4). The two are different on purpose and the difference is
stated in each component's comment.

## 4. Pagination UI

### 4.1 A shared component, and why

The two footers (`runs/+page.svelte:228-232`, `orkflows/+page.svelte:101`)
are the same three elements — Previous, a label, Next — with the same
`aria-disabled`/`opacity-50` rule and the same testid pattern, differing only
in the prefix (`runs-`/`orkflows-`) and in how their hrefs are built. The
hrefs are the pages' business (they know the coordinates); everything
rendered is not. That is the line for `ui/src/lib/components/Pagination.svelte`:
**presentational, given hrefs, owning no navigation state**. It is the same
split `ErrorPanel` makes (given `message` and `onretry`, owning neither).

### 4.2 Props

```ts
interface Props {
	/** testid prefix: 'runs' → runs-previous, runs-next, runs-first, runs-page-status */
	testid: string;
	/** 1-based, client-counted (§3.2) */
	page: number;
	/** rows on this page */
	count: number;
	/** singular/plural noun for the status line: 'run' | 'orkflow' */
	noun: string;
	/** undefined = disabled */
	previousHref?: string;
	nextHref?: string;
	/** shown in place of Previous when page > 1 and previousHref is undefined (§3.2) */
	firstHref?: string;
}
```

The page-size `Select` is deliberately not a prop: on Runs it belongs to the
filter form, on Orkflows it is a sibling in the same row (§4.4).

### 4.3 What it shows, and what it honestly cannot

The API is cursor-based (`ListRunsResponse.next_page_token`,
`proto/orkstack/v1/service.proto:380-383`; `ListOrkflowsResponse`,
`:151-154`) and returns no total. The component therefore never shows a
total or a page count; the status line states only what is known:

- page > 1, more pages: **Page 3 · 10 runs**
- page 1, more pages: **Page 1 · 10 runs**
- last page: **Page 5 · 7 runs · last page**
- page 1 and last page (everything fits): **7 runs** — no "Page 1", because
  there is nothing to page.

`data-testid="{testid}-page-status"`. The noun is pluralised by count
(`1 run`, `10 runs`). If a total is wanted later it is an engine change
(`total_size` on both list responses, with the archive/live union counted
server-side) and the component gains one optional prop; nothing here
pre-empts it.

### 4.4 How it looks

The footer is one row, `mt-4 flex items-center justify-between gap-3`, the
status line on the left in `text-sm text-fg-muted tabular-nums`, the buttons
on the right as a pair. The buttons take the app's secondary-button recipe
verbatim — the one on `OrkflowVersions.svelte:264` and the dashboard's
Custom button (`+page.svelte:146`):

```
inline-flex h-9 items-center justify-center gap-1.5 rounded-md border border-border
bg-surface px-3.5 text-sm font-medium text-fg shadow-xs transition-colors duration-150
hover:border-border-strong hover:bg-surface-hover
focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:outline-none
```

- A disabled direction is rendered as an `<a>` with no `href`,
  `aria-disabled="true"`, and `pointer-events-none opacity-50` — the same
  pair every disabled button in the app uses (`Select.svelte:229`,
  `OrkflowVersions.svelte:274`). The testids `runs-previous`/`runs-next`
  and the `aria-disabled` values the existing e2e asserts on
  (`ui/e2e/browsing-operations.spec.ts:66,72`) are kept exactly.
- Previous carries a leading chevron, Next a trailing one, drawn inline as
  the same `size-4` stroked SVG `Select.svelte:232-243` uses, `aria-hidden`.
- When Previous is unknown but page > 1, the slot renders **First page**
  (`{testid}-first`) in the same recipe, pointing at `firstHref`.
- Tokens only: `border-border`, `bg-surface`, `text-fg`, `text-fg-muted`,
  `border-border-strong`, `bg-surface-hover`, `brand-500` for the ring —
  all from `ui/src/app.css` `@theme`, all with dark values (`:207-332`), so
  nothing is added to the palette.

Orkflows' page-size control moves into the same row: `Page size` label +
`Select` (`testid="orkflow-page-size"`, options 10/25/50/100) at the left of
the footer, before the status line, with `onchange` navigating to
`href({ size, cursor: '', page: '' })` and clearing that filter set's stack.
The four bold-or-not links at `orkflows/+page.svelte:62` are removed.

## 5. The dashboard window control

### 5.1 One group, four segments

The custom range joins the group it belongs to. The `role="group"` at
`+page.svelte:136` gains a fourth segment after `24h`:

- The three presets stay `<a>` elements with `aria-current="page"` and the
  `current:` variant (`app.css:382`), unchanged.
- The fourth is a `<button type="button" data-testid="dashboard-window-custom">`
  that is the **trigger** of a `Popover` (`ui/src/lib/components/Popover.svelte`,
  `align="end"` so the panel stays on screen next to the CLI button). It
  shares the segment recipe (`:142`) so the group reads as one control, and
  it takes `aria-current="page"` when `data.window === 'custom'` — a button
  can carry `aria-current` and the `current:` variant keys off the attribute,
  not the element, so it paints identically. It also carries
  `aria-expanded` and `aria-haspopup="dialog"` for the popover.
- Its label is **Custom** while a preset is active, and the applied instant
  when the custom window is: `Since 9/10/2026, 2:30:00 AM`, formatted with
  the same `fmtInstant` the header uses (`stats.ts:58-61`), so the header
  line and the segment never disagree. `fmtInstant` takes a proto Timestamp;
  a one-line `fmtInstantMs(ms: number)` split out of it (and `fmtInstant`
  calling it) renders `data.sinceParam` without inventing a second format.

The detached `<form>` at `:146` and `setCustom` at `:104` are removed; what
they did moves inside the popover.

### 5.2 The popover

Content (`data-testid="dashboard-window-custom-popover"`):

- A label **Show runs created since**, a `datetime-local` input
  (`dashboard-window-custom-since`) pre-filled from `data.sinceParam` when
  the custom window is active — converted to the input's local `YYYY-MM-DDTHH:mm`
  form by a `sinceInputValue(iso: string): string` helper in `browsing.ts`
  (the inverse of the `new Date(v).toISOString()` at `:104`, and unit-tested
  as a round trip). `max` is set to now, since a window starting in the
  future is empty by definition.
- An **Apply** button (`dashboard-window-custom-apply`) in the primary
  recipe (`bg-brand-600 …`, `:165` on the Runs page). Apply navigates with
  `goto(windowHref('custom', new Date(value).toISOString()))` — the URL
  keeps `since=` as an ISO instant because that is the value
  `orkctl stats --since` takes and the CLI snippet at `:105` prints; the
  encoding the user saw is the URL's, and the URL is no longer the only
  place the value is shown. Enter in the input applies too (the content is
  a `<form onsubmit>`), and Apply is disabled while the input is empty or
  invalid. The popover closes on Apply, Escape or an outside press
  (`Popover.svelte:34-58` already does the last two).
- No "until": the API's custom window is `since` to now
  (`+page.ts:26`, `GetStats{since}`), and the header states the `until` the
  engine actually used. Offering an end the request cannot carry would be a
  control that does nothing.

### 5.3 Behaviour matrix

| URL | Group | Custom segment label | Header `dashboard-window` |
|---|---|---|---|
| `?window=24h` (or none) | `24h` current | Custom | since → until from the response |
| `?window=custom&since=<iso>` | Custom current | `Since <fmtInstant(since)>` | same |
| `?window=custom` (no since) | Custom current | Custom | absent; `ErrorPanel` says "Custom dashboard windows require a valid since timestamp." (`browsing.ts:35`) |
| `?window=bogus` | nothing current | Custom | absent; `ErrorPanel` "Unknown dashboard window" |

The third row is the one case where a segment is current without a value,
and the error panel beneath it is the explanation; it is preferable to
snapping back to `24h`, which would silently disagree with the address bar.

## 6. Test plan

### 6.1 Unit (`bun test test/`)

`ui/test/browsing.test.ts` — extend:

- `listHref` never emits a duplicate key: build with `size: '25'`, change to
  `size: '50'`, assert `getAll('size')` is `['50']`; assert empty values are
  omitted and `ns` is first; assert `page: '1'` and empty `cursor` are
  omitted.
- `pageNumber('3') === 3`, `pageNumber('0') === 1`, `pageNumber('x') === 1`.
- Page stack: with a `Map`-backed fake storage, `writePageStack` then
  `readPageStack` round-trips; a stack whose `stack[page-1] !== cursor` is
  reported as not describing the page; a throwing storage yields `[]` and
  does not throw.
- `sinceInputValue(new Date(v).toISOString()) === v` for a fixed local
  value.
- Delete the `cursorHistory` test (`:16-20`) with the function.

`ui/test/stats.test.ts` — `fmtInstantMs` agrees with `fmtInstant` for the
same instant.

### 6.2 End-to-end (`ui/e2e`, live stack)

`ui/e2e/browsing-operations.spec.ts` — the runs test (`:59-81`) gains the
regression this document exists for, and the fixture grows from 11 to 21
runs so that at `size=10` there are three pages:

- After each of two Next clicks: `new URL(page.url()).searchParams` has
  exactly the keys `ns, orkflow, size, cursor, page`; `getAll('size')` has
  length 1; `has('history')` is false; `page.url().length` is below a fixed
  bound (400 is generous for one cursor) and, decisively, the URL length
  after the second Next is not greater than after the first by more than a
  cursor's worth — i.e. does not grow with depth.
- `runs-page-status` reads `Page 2 · 10 runs`, then `Page 3 · 1 run · last
  page`.
- Previous twice returns to page 1 with no `cursor` and no `page`
  (`:74` already asserts the first half).
- Reload on page 2 (`page.reload()`): Previous is still enabled (the
  session stack survives), and one more Previous lands on page 1.
- A pasted link: `page.goto(<the page-2 URL>)` in a **new context**;
  `runs-previous` is `aria-disabled="true"`, `runs-first` is visible and
  leads to page 1.
- Orkflows: choose **50** from `orkflow-page-size`; assert
  `getAll('size')` is `['50']` and the trigger reads `50`; choose **100**;
  assert `['100']`. (The fixture has one orkflow; the assertion is about the
  URL, not the row count.)

The dashboard test (`:83-94`): replace `:88-89` (fill the detached input,
click the Custom submit) with: click `dashboard-window-custom`, fill
`dashboard-window-custom-since`, click `dashboard-window-custom-apply`; then
assert `dashboard-window-custom` has `aria-current="page"` and its text
starts with `Since`, no preset has `aria-current`, and after `page.reload()`
the same holds and the popover's input is pre-filled. Keep `:90-92`.

`ui/e2e/logs-dashboard.spec.ts` drives the dashboard (`:768-881`) but never
the window controls: **no change**. `ui/e2e/run-page.spec.ts` and
`run-operations.spec.ts` do not touch either list's footer: **no change**.
`ui/e2e/uiHelpers.ts` gains `selectOption(page, testid, value)` — the
trigger-then-row gesture `selectNamespace` (`:27-36`) already spells out —
so the Orkflows size step and any future `Select` are driven from one place.

A lighter, engine-free Playwright spec (routing `/orkstack.v1.*` to fixtures
with `page.route`) was considered and is **not** proposed: the first render
is server-side through `hooks.server.ts` and cannot be intercepted from the
browser, so such a spec would exercise client navigation only after an SSR
error. The stand-in engine used for §1 is a development aid, not a test.

### 6.3 `svelte-check`

`bun run check` must stay clean; the `Pagination` props interface and the
narrowed `PageLoad` return types (`page` added, `history` removed) are the
places it will complain if a page is missed.

## 7. File-by-file

| File | Change |
|---|---|
| `ui/src/lib/browsing.ts` | Add `listHref`, `pageNumber`, `pageStackKey`, `readPageStack`, `writePageStack`, `sinceInputValue`. Delete `cursorHistory` (`:28-30`) and `pushCursor` (`:31`). `statsWindowError` unchanged. |
| `ui/src/lib/stats.ts` | Split `fmtInstantMs(ms)` out of `fmtInstant` (`:58-61`); `fmtInstant` calls it. |
| `ui/src/lib/components/Pagination.svelte` | **New.** Props per §4.2, rendering per §4.3–4.4. Head comment states the one rule: given hrefs, owns no state, never shows a total the API did not send. |
| `ui/src/routes/runs/+page.ts` | Read `page` with `pageNumber`; stop reading `history` (`:12`); return `page` in all three branches; normalise `page > 1 && !cursor` to 1. |
| `ui/src/routes/runs/+page.svelte` | `href()` (`:69-74`) delegates to `listHref` and includes `page`. Replace `nextHref`/`previousHref` (`:79-80`) with a `$state` stack read in an `$effect` keyed on the stack key, a `next()` click handler that writes the stack then `goto`s, and derived `previousHref`/`firstHref`. `filter()` (`:77`) and Clear (`:169`) drop `history: ''`, set `page: ''`. Replace `:228-232` with `<Pagination testid="runs" noun="run" …/>`. |
| `ui/src/routes/orkflows/+page.ts` | Same as the runs loader (`:8-10`). |
| `ui/src/routes/orkflows/+page.svelte` | Replace `pageHref` (`:9-14`) with `href(changes)` over `listHref`. Delete the size links (`:62`); add the `Select` per §4.4 with `onchange` navigating. Replace `:101` with `<Pagination testid="orkflows" noun="orkflow" …/>`. Import `Select`. |
| `ui/src/routes/+page.ts` | No logic change. `window`/`sinceParam` already come back for both branches (`:24,27,29`). |
| `ui/src/routes/+page.svelte` | Delete `setCustom` (`:104`) and the `<form>` (`:146`). Add `customOpen` state, the fourth segment + `Popover` per §5.1–5.2, `fmtInstantMs(Date.parse(data.sinceParam))` for the label, `sinceInputValue(data.sinceParam)` for the input's initial value. Update the comment at `:129-135` to say the group has four members and why the fourth is a button. |
| `ui/test/browsing.test.ts` | Per §6.1. |
| `ui/test/stats.test.ts` | Per §6.1. |
| `ui/e2e/browsing-operations.spec.ts` | Per §6.2; fixture loop `:53` to 21 runs and `:54` to expect 21. |
| `ui/e2e/uiHelpers.ts` | Add `selectOption`. |
| `docs/UI_CLI_PARITY.md` | Row `:251`: "URL preserves state" → "URL preserves filters, cursor and page number; the path back is per-tab". Row `:253`: add "the active window, custom included, is marked in the control and shown as a formatted instant". |

Nothing under `proto/`, `internal/`, `cmd/` or `gen/` changes.

## 8. Not verified, and other things to know

- **R5 cursor length** is computed from `service.go:1637-1646`, not
  measured against a live engine. The direction of the argument does not
  depend on the exact figure.
- **Stack staleness within a session.** A reader who pages to 3, waits an
  hour, and presses Previous will be handed the cursor of page 2 *as it was*
  — the engine's cursor is a position, so the page is correct, merely
  shifted by whatever was created since. That is the same behaviour the
  `history` parameter has today and the same the CLI has with
  `--page-token`; it is stated, not fixed.
- **Bookmarks with `history=`** from before this change open page 1 of the
  right filter set with `cursor` honoured and `history` ignored. They do not
  break; they lose Previous, which the `First page` affordance covers.
- **`aria-current` on a `<button>`** is valid ARIA and the `current:`
  variant is attribute-keyed (`app.css:382`), so the fourth segment paints
  like the other three. Not exercised in a browser here; the e2e in §6.2
  asserts the attribute, and the screenshot the spec already takes
  (`browsing-operations.spec.ts:93`) is the visual check.
- **Private-mode storage.** `sessionStorage` can throw on write in some
  private modes; the helpers swallow it and Previous degrades to First
  page. Not exercised.
