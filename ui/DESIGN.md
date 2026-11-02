---
name: OrkStack
description: The OrkStack UI, drawn as Harbour. A calm slate ground, white cards, harbour navy for the brand and every selection, one container orange for the one action that starts or commits work.
colors:
  bg: "#F6F8FA"
  surface: "#FFFFFF"
  surface-2: "#F1F4F7"
  surface-3: "#E4E9EE"
  hover: "#F1F4F7"
  scrim: "rgb(14 28 40 / 0.42)"
  ink: "#0E1C28"
  ink-2: "#3A4C5C"
  ink-3: "#4D5F70"
  line: "#E1E6EC"
  line-2: "#CBD3DC"
  line-strong: "#74879A"
  navy: "#0F2B3D"
  navy-tint: "#E5EDF4"
  navy-text: "#163A52"
  meter: "#2B4A63"
  accent: "#DA8757"
  accent-hover: "#E29463"
  accent-edge: "#9F5021"
  on-accent: "#0E1C28"
  focus: "#163A52"
  run: "#1A62C4"
  run-tint: "#E8F0FC"
  ok: "#1D7442"
  ok-mark: "#2E8B57"
  ok-tint: "#E6F4EB"
  fail: "#B42F27"
  fail-mark: "#D13B30"
  fail-tint: "#FBE9E7"
  wait: "#855400"
  wait-mark: "#A87300"
  wait-tint: "#FCF3DD"
  await: "#6B45BF"
  await-tint: "#F0EBFB"
  cancel: "#4D5F70"
  pend-mark: "#788797"
  destroy-bg: "#0F2B3D"
  destroy-ink: "#FFFFFF"
  log-bg: "#0F1B26"
  log-ink: "#DCE5EC"
  side: "#FBFCFD"
  side-active: "#0F2B3D"
  sb-thumb: "#6A7D8F"
  bg-dark: "#0B141C"
  surface-dark: "#111D27"
  surface-2-dark: "#0E1922"
  surface-3-dark: "#1A2834"
  hover-dark: "#15232F"
  scrim-dark: "rgb(3 7 11 / 0.66)"
  ink-dark: "#E2E9EF"
  ink-2-dark: "#B2C0CB"
  ink-3-dark: "#8C9DAA"
  line-dark: "#1F2F3D"
  line-2-dark: "#2C3E4E"
  line-strong-dark: "#687D8F"
  navy-dark: "#11293C"
  navy-tint-dark: "#172C3D"
  navy-text-dark: "#B9CCDB"
  meter-dark: "#7E9CB4"
  accent-dark: "#E39967"
  accent-hover-dark: "#EBA879"
  accent-edge-dark: "#E39967"
  on-accent-dark: "#0B141C"
  focus-dark: "#B9CCDB"
  run-dark: "#6EA8F2"
  run-tint-dark: "#132A44"
  ok-dark: "#5DBF82"
  ok-tint-dark: "#12301F"
  fail-dark: "#F27A70"
  fail-tint-dark: "#3A1A18"
  wait-dark: "#E4B04E"
  wait-tint-dark: "#33270F"
  await-dark: "#AE93F2"
  await-tint-dark: "#261E3D"
  cancel-dark: "#8C9DAA"
  pend-mark-dark: "#64798B"
  destroy-bg-dark: "#E2E9EF"
  destroy-ink-dark: "#0B141C"
  log-bg-dark: "#08111A"
  log-ink-dark: "#D3DDE5"
  side-dark: "#0D1720"
  side-active-dark: "#1C3A52"
  sb-thumb-dark: "#6B8091"
typography:
  headline:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif"
    fontSize: "1.5rem"
    fontWeight: 700
    lineHeight: "2rem"
    letterSpacing: "-0.018em"
  figure:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.625rem"
    fontWeight: 650
    lineHeight: "1.875rem"
    letterSpacing: "-0.02em"
  title:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.9375rem"
    fontWeight: 650
    lineHeight: "1.375rem"
  body:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: "1.3125rem"
  body-dense:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.84375rem"
    fontWeight: 400
    lineHeight: "1.3125rem"
  label:
    fontFamily: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 600
    lineHeight: "1rem"
  code:
    fontFamily: "JetBrains Mono Variable, JetBrains Mono, ui-monospace, SF Mono, Menlo, Consolas, monospace"
    fontSize: "0.8125rem"
    fontWeight: 400
    lineHeight: "1.25rem"
rounded:
  xs: "4px"
  sm: "6px"
  md: "10px"
  full: "9999px"
spacing:
  step: "0.25rem"
  gutter: "1rem"
  sheet: "1.75rem"
  bar: "3.5rem"
  side: "15.5rem"
  aside: "21.25rem"
  content: "92.5rem"
components:
  button:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.sm}"
    height: "34px"
    padding: "0 14px"
  button-primary:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.on-accent}"
    rounded: "{rounded.sm}"
    height: "34px"
  button-primary-hover:
    backgroundColor: "{colors.accent-hover}"
  button-destroy:
    backgroundColor: "{colors.destroy-bg}"
    textColor: "{colors.destroy-ink}"
    rounded: "{rounded.sm}"
  card:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.md}"
  stamp:
    rounded: "{rounded.full}"
    height: "24px"
    typography: "{typography.label}"
  plate:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.sm}"
    typography: "{typography.code}"
  tooltip:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.surface}"
    rounded: "{rounded.sm}"
---

# Design System: OrkStack

This file records the design system the UI ships: Harbour. Every value is defined in `ui/src/tokens.css`; if this file and `tokens.css` ever differ, **`tokens.css` wins** and this file is stale. `ui/src/tailwind-tokens.css` registers the token names with Tailwind and removes Tailwind's own palette, scale, radii and shadows, so no class can reach a value outside `tokens.css`. `ui/src/app.css` holds the primitives and writes no value of its own. The site (`site/`) imports `tokens.css` as it is.

## Overview

**Creative North Star: "The Harbour Chart"**

A calm, legible working surface for operators who need to see what ran, what failed and what to do next. A slate ground carries white cards; harbour navy marks the brand, the active page and every selection; one container orange appears only on the single action that starts or commits work on the screen's own object. Status hues appear only where a status is drawn, and every status also has its own shape and word. Dark is a navy-tinted, low-glare night chart with no pure white text.

Density is moderate: 14 px body, 13.5 px in tables and controls, tabular numerals for every count, time and duration. Identifiers sit in plates; facts sit in strips; the command line lives only behind a shell icon.

**Key Characteristics:**
- One action colour (orange), one brand colour (navy), status colours for status only.
- Status is a mark shape plus a word plus a hue, never hue alone.
- Flat cards with a hairline border and a barely-there shadow.
- Inter for the interface, JetBrains Mono for ids, digests, logs and plates.
- Every text pair 4.5:1 and every non-text part 3:1, in both themes (`ui/test/tokenContrast.test.ts`).

## Colors

Cool slate neutrals, a deep harbour navy, one warm orange, and a closed set of status hues. Each role below is `light / dark`.

### Primary
- **Harbour Navy** (`--navy` #0F2B3D / #11293C; `--navy-text` #163A52 / #B9CCDB; `--navy-tint` #E5EDF4 / #172C3D): the brand, the sidebar's active pill (`--side-active`), tags, the retry mark, the live dot, links and the focus ring (`--link` = `--focus` = navy-text). `--meter` (#2B4A63 / #7E9CB4) is the progress ring's running and succeeded stroke.

### Secondary
- **Container Orange** (`--accent` #DA8757 / #E39967, hover #E29463 / #EBA879, edge #9F5021 / #E39967): the primary button and the Strata mark's S (`--mark-s`). Its label is dark ink (`--on-accent` #0E1C28 / #0B141C, 6.2:1 and 8.0:1), never white.

### Neutral
- **Ground and surfaces**: `--bg` #F6F8FA / #0B141C; `--surface` #FFFFFF / #111D27 (cards, fields, buttons); `--surface-2` #F1F4F7 / #0E1922 (plate keys, disabled, pill-plain); `--surface-3` #E4E9EE / #1A2834; `--hover` #F1F4F7 / #15232F.
- **Ink**: `--ink` #0E1C28 / #E2E9EF; `--ink-2` #3A4C5C / #B2C0CB (secondary, quiet buttons); `--ink-3` #4D5F70 / #8C9DAA (labels, dt, placeholders, captions).
- **Lines**: `--line` #E1E6EC / #1F2F3D (card borders, row rules); `--line-2` #CBD3DC / #2C3E4E (button borders); `--line-strong` #74879A / #687D8F (field borders, hover borders, the live control).
- **Sidebar** (`--side-*`): its own ground (#FBFCFD / #0D1720), inks and field; the active item is navy with white ink (dark: #1C3A52 with #E2E9EF).
- **Log pane** (`--log-*`): always dark, #0F1B26 / #08111A ground, #DCE5EC / #D3DDE5 ink, `--log-err` #F27A70.
- **Scrollbars** (`--sb-*`): track #EEF2F6 / #0E1922, thumb #6A7D8F / #6B8091, hover #4E6070 / #94A8B8; logs have their own (`--sb-log-*`).
- **Scrim**: rgb(14 28 40 / 0.42) / rgb(3 7 11 / 0.66) behind every modal.

### Status
Each status has a word colour, a mark colour and a tint (pill ground). Light / dark:
- **Running** `--run` #1A62C4 / #6EA8F2, tint #E8F0FC / #132A44; mark: filled circle.
- **Succeeded** `--ok` #1D7442 / #5DBF82, mark #2E8B57, tint #E6F4EB / #12301F; mark: filled square.
- **Failed** `--fail` #B42F27 / #F27A70, mark #D13B30, tint #FBE9E7 / #3A1A18; mark: triangle.
- **Waiting for capacity** `--wait` #855400 / #E4B04E, mark #A87300, fill #E3A21A, tint #FCF3DD / #33270F; mark: ring.
- **Awaiting** `--await` #6B45BF / #AE93F2, tint #F0EBFB / #261E3D; mark: diamond.
- **Canceled** `--cancel` #4D5F70 / #8C9DAA; mark: slashed square.
- **Pending / Skipped** `--pend-mark` #788797 / #64798B; outlined pill, outlined or dotted square.

The `--color-*` names Tailwind and the site read (`--color-running-bg`, `--color-failed-solid`, and so on) are aliases of these roles. Pins have six fixed accents (`--color-pin-a…f-line/-fill`); the editor and JSON views use `--syn-*`.

### Named Rules
**The One Action Colour Rule.** Orange appears on at most one button per screen: the action that creates work or commits a change on the screen's own object (Submit a run, Run again, Apply version). The object decides, never the state. Nothing on Pools or Activities is orange.

**The Red Is Status Rule.** Red, green, blue, amber and violet describe a status and nothing else. No button, border or banner action takes a status hue.

**The Shape Before Hue Rule.** Every status carries its mark and its word; colour is the third signal, never the only one (WCAG 1.4.1).

## Typography

**Interface Font:** Inter Variable (fallback `ui-sans-serif, system-ui, -apple-system, 'Segoe UI'`)
**Code Font:** JetBrains Mono Variable (fallback `ui-monospace, 'SF Mono', Menlo, Consolas`)

Both are self-hosted `@fontsource-variable` 5.3.0, imported by `app.css`. Inter carries every word of the interface; JetBrains Mono carries run ids, digests, versions, image references, logs, YAML and plate values.

### Hierarchy
- **Page h1** (700, 24/32 px, −0.018em, `.page-h1`): one per page, `text-wrap: balance`.
- **Figure** (650, 26/30 px, −0.02em): a tile's count.
- **Card heading** (650, 15/22 px): `.card-head h2/h3`.
- **Body** (400, 14/21 px): the default.
- **Dense** (13.5/21 px): tables, chips, buttons, key/value lists.
- **Secondary** (13/20 px): plates, card subtitles, small buttons, table heads.
- **Label** (600, 12/16 px): dt, facts-strip labels, the asserted tag.
- **Inline code** (0.89em; 12.5 px in tooltips and small plates).

### Named Rules
**The Sentence Case Rule.** Labels, table heads and headings are sentence case at weight 600. Harbour never sets tracked capitals.

**The Tabular Figures Rule.** Every count, time, duration and percentage uses `font-variant-numeric: tabular-nums`; ids and digests use the code face.

## Layout

A fixed light sidebar (248 px, `--spacing-side`) beside a sticky top bar (56 px, `--spacing-bar`) and a content column capped at 1480 px (`--spacing-content`) with 28 px sheet padding (`--spacing-sheet`) and 16 px gutters. Below 900 px the sidebar becomes a drawer opened from the top bar's menu button. Spacing is Tailwind's 4 px step (`--spacing`). The editor uses layout C: the YAML editor beside a 340 px docs rail (`--spacing-aside`). A log pane is bounded at `min(60dvh, 30rem)`; the graph card at `clamp(15rem, 42vh, 30rem)` (tall: `clamp(15rem, 65svh, 40rem)`).

Responsive rules, all in components: the facts strip goes to two columns with no rules below 640 px and one below 360 px; registers stack into labelled rows below 640 px; buttons and chips reach a 40 px minimum on phones; everything reflows at 320 px. `scroll-padding-top` keeps a focused control clear of the sticky bar.

## Elevation & Depth

Flat with a whisper. Depth comes from tonal layers (bg, surface, surface-2) and hairlines; shadows are small and structural.

### Shadow Vocabulary
- **Card** (`--shadow`: `0 1px 2px rgb(15 43 61 / 0.06), 0 1px 3px rgb(15 43 61 / 0.04)`; dark `0 1px 2px rgb(0 0 0 / 0.35)`): cards and neutral buttons at rest.
- **Overlay** (`--shadow-lg`: `0 28px 56px -28px rgb(15 43 61 / 0.38), 0 2px 8px -2px rgb(15 43 61 / 0.08)`; dark `0 28px 56px -24px rgb(0 0 0 / 0.75)`): popovers, dialogs and the tooltip.

### Named Rules
**The Two Shadows Rule.** There are exactly two shadows, card and overlay. Hover changes a border or ground, never a shadow.

## Shapes

Gently rounded: 10 px (`--r`) for cards, tiles, banners, popovers; 6 px (`--r-sm`) for buttons, fields, plates and the tooltip; 4 px for keyboard keys; full for pills, chips, tags, the live control and dots. Borders are 1 px hairlines; the focus ring is 2 px with a 2 px offset; a failing banner has a 3 px left alarm edge (`--rule-alarm`).

Icons are one family (`ui/src/lib/icons.ts`): a 16-unit grid, 1.4 stroke, round caps and joins, `currentColor`, drawn once as `<symbol>`s by `IconSprite` and referenced by `Icon` with `<use>`. An icon is always decoration beside a word or an accessible name.

The mark is **Strata**: three bars joined by round turns inside a ring, a dot where each end meets the ring (`Mark.svelte`); ring `--mark-ring` (#0F2B3D / #DCE5EC), S `--mark-s` (orange in both themes).

## Components

### Buttons
- **Shape:** 34 px high (30 px `.btn-sm`), 6 px radius, 13.5 px semibold, with a transparent `::after` giving a 40 px hit area.
- **Neutral `.btn`:** surface ground, `--line-2` border, card shadow; hover takes `--hover` and `--line-strong`; active scales to 0.98 unless reduced motion.
- **Primary `.btn-primary`:** orange ground, orange-edge border, dark ink label. See the One Action Colour Rule.
- **Quiet `.btn-quiet`:** no border, no ground, `--ink-2`; hover takes `--hover`.
- **Destroy `.btn-destroy`:** solid ink (navy with white in light, near-white with dark ink in dark). Only the confirm inside a reason dialog, never a trigger on a page.
- **Icon `.btn-icon`:** square, `--ink-2`, named by `aria-label`; only terminal, copy and More.
- **Disabled:** surface-2, line-2, ink-3, no shadow; `aria-disabled` with its reason visible and wired by `aria-describedby`.
- **Icons on buttons:** 16 px, leading, only on actions that start or commit a change (play, check, ban, pause, and their confirms).

### Status pills `.stamp`
24 px (20 px `.stamp-sm`) full pills: mark, word, then tint ground and word colour. The mark is CSS on the pill itself (`.mark-dot`, `.mark-square`, `.mark-tri`, `.mark-ring`, `.mark-diamond`, `.mark-strike`, `.mark-hollow`, `.mark-dotted`), because a run page holds thousands. Pending and Skipped are outlined (`.pill-outline`). A running dot breathes three times then rests. `.pill-plain` is a markless label; `.tag` is a navy-tint count or kind.

### Plates `.plate`
A key cell (surface-2, ink-3, semibold) and a mono value, 6 px radius: the one way a run id, version or digest is set. A run id has full and short forms and a copy button; a version never copies and looks the same on every page. `.plate-sm` in rows.

### Facts strip `.fstrip`
A `<dl>` of 12 px labels over 14 px tabular values, separated by vertical rules; on the run, failed run, Runs and editor headers. Principals always carry the **asserted tag** (`.atag`, `Asserted`/`Principal`): "asserted by your gateway, not verified by OrkStack".

### Progress ring `.pring`
20 px (16 px in rows) with the percentage beside it; counts in the tooltip and sr text. Basis (`lib/progress.ts`): ended tasks plus fan-out items over all of them, Skipped and Canceled counting as ended. Stroke `--meter` while running or succeeded, `--fail-mark` when failed, `--ink-3` when canceled.

### Retry mark `.retry`
A 22 px navy circular arrow with the count (10 px bold) inside; drawn only for two or more attempts.

### Live control `.livectl`
One full pill: a state chip (`role="status"`) and a toggle. Live: navy dot with a 2.4 s pulse ring, "Live", "Pause updates". Paused: hollow dot, "Paused", "Resume updates", the pill grey on surface-2. The toggle's name is its visible label; no `aria-pressed`. On Runs, the run page and the log pane.

### Chips, tiles and registers
- **Chip `.chip`:** 34 px full pill, surface, `--ink-2` label with a semibold value and a trailing chevron; a listbox trigger.
- **Tile `.tile`:** 10 px card with a label and a figure; filters a list (`aria-pressed`, selected border `--select`).
- **Register `.register`:** the table. 13 px semibold ink-3 heads, 14 px rows, row hover `--hover`, hairline rules; stacks below 640 px.

### Cards and banners
- **Card `.card`:** surface, 1 px `--line`, 10 px radius, card shadow; `.card-head` is 52 px minimum with a 15 px heading and actions at the end (`.card-end`).
- **Banner `.alert`:** a card-like row with an icon cell; `.alert-fail` adds the 3 px fail-mark left edge. `.capline` is the waiting-for-capacity line with a wait-mark border.

### Inputs
`.field` and `.search`: surface, 1 px `--line-strong`, 6 px radius; `.search` shows the 2 px focus ring on `:focus-within`. Labels 13 px semibold `--ink-2`, hints 13 px `--ink-3`.

### Navigation
**Sidebar:** brand, namespace switch, Runs (with a count of runs going now), Orkflows and Apply, then an Operate group; the OrkStack plate and the appearance switch at its foot. The active page is a navy pill (`aria-current`). **Top bar:** menu button below 900 px, breadcrumb, and Jump (Ctrl K / ⌘K, a native modal search over pages and recent runs).

### Overlays
- **Tooltip `.tt`:** the one tooltip (`lib/tooltip.ts`), for any `data-tip`; no native `title`. Ink ground, surface text, 12.5 px, 6 px radius, overlay shadow, max 300 px; 300 ms hover delay, instant on keyboard focus; 8 px below, flips above, clamped 12 px inside the viewport. Its nine rules: a 10 px bridge over the gap; timers cleared before set, hide after 200 ms; opening one closes the last and releases its `aria-describedby`; hover-opened closes on pointer-out, focus-opened on blur; joins `aria-describedby` only when it adds words; Escape closes only the tip first; any click closes it; a relative time (`TimeAgo`) is a tab stop carrying its exact instant as tip and sr text; a dismissed tip re-arms only after the pointer leaves.
- **Dialogs:** native modals (`Dialog`, `ConfirmDialog`) with a glyph in the header; focus returns to the opener. A destructive confirm names the safe choice on the back button, which takes focus first.
- **CLI popover:** behind the terminal icon only (`CliPopover`), listing the screen's `orkctl` commands with Copy; closes on Escape and focus-out.
- **Popover `.pop`:** surface, line-2 border, 10 px radius, overlay shadow; enters with a 180 ms rise unless reduced motion.

### Signature: graph card and nodes
`DrawingFrame` holds `DagGraph` over a dotted ground (8 px minor, 40 px major pitch). The graph scrolls horizontally and centres the selected node on load (the failed one on a failed run). A node is `role="button"`, `aria-pressed`, with an activity icon and its own focus ring outside the box. Activity icons come from `icons.ts` (`a-*`) or from an image's declared icon, rendered through the closed allowlist in `lib/activityIcon.ts`; anything unknown draws the default icon, never a partial one.

### Motion
80 ms instant feedback, 180 ms state change, 320 ms entrance; `--ease-in` cubic-bezier(.22,1,.36,1), `--ease-out` cubic-bezier(.4,0,.2,1). Under reduced motion: no pulse, no transforms.

## Named Rules

**The One Action Colour Rule.** At most one orange button per screen, on the action that creates work or commits a change on the screen's own object.

**The No Hue For Destruction Rule.** A destructive trigger is neutral, ends in an ellipsis and carries its glyph ("Cancel run…" with ban, "Cordon…" with pause), set apart by a divider (`.act-gap`). It opens a reason dialog whose confirm is solid ink (`--destroy-*`) with the same glyph.

**The Icons Earn Their Place Rule.** Leading icons only on actions that start or commit a change; icon-only buttons only for terminal, copy and More; one 16-unit, 1.4-stroke family everywhere.

**The Shell Icon Rule.** No `orkctl` text on an app screen; every screen's commands live in its CLI popover.

**The Plate Rule.** Run ids, versions and digests are always plates, identical on every page.

**The One Tooltip Rule.** One shared tooltip element obeys the nine rules above; no native `title`.

**The Scrollbar Rule.** Every scrolling panel draws `--sb-*` thin scrollbars under `forced-colors: none`, with `scrollbar-gutter: stable` on the page; forced colours get the system's.

**The Words Rule.** Run states are Running, Succeeded, Failed, Canceled; tasks add Pending, Skipped, Waiting for capacity, Awaiting. Fan-out items are named by index (`render[10]`).

**The Both Themes Rule.** Every pair holds 4.5:1 for text and 3:1 for non-text in light and dark, held by `tokenContrast.test.ts`; no value lives outside `tokens.css`.
