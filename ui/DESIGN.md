---
name: OrkStack
description: The OrkStack UI, drawn as an engineering drawing office. Every Orkflow is a controlled drawing with a version, a digest and a title block.
colors:
  paper: "#fbfbf8"
  paper-sunken: "#f3f3ee"
  sheet: "#fefefc"
  hover: "#eff0ee"
  scrim: "rgb(20 33 61 / 0.42)"
  ink: "#14213d"
  ink-secondary: "#3a4661"
  ink-muted: "#5d6a85"
  ink-placeholder: "#5d6a85"
  on-ink: "#fbfbf8"
  mark: "#6b7690"
  rule: "rgb(20 33 61 / 0.16)"
  rule-faint: "rgb(20 33 61 / 0.09)"
  rule-strong: "rgb(20 33 61 / 0.62)"
  grid: "#e6e9ef"
  grid-major: "#c8cfdc"
  blue: "#2b59c3"
  blue-strong: "#1f4596"
  blue-tint: "#e8ecf5"
  green: "#2b7a4b"
  green-strong: "#1d5e39"
  green-tint: "#e5f2e8"
  red: "#c2362b"
  red-strong: "#9e2a21"
  red-tint: "#fde9e6"
  focus: "#2b59c3"
  paper-dark: "#13161c"
  paper-sunken-dark: "#0f1217"
  sheet-dark: "#1a1e26"
  hover-dark: "#1d222b"
  scrim-dark: "rgb(5 6 9 / 0.6)"
  ink-dark: "#e7e5dc"
  ink-secondary-dark: "#c3c2ba"
  ink-muted-dark: "#9ba0ab"
  ink-placeholder-dark: "#9ba0ab"
  on-ink-dark: "#13161c"
  mark-dark: "#8f95a2"
  rule-dark: "rgb(231 229 220 / 0.14)"
  rule-faint-dark: "rgb(231 229 220 / 0.08)"
  rule-strong-dark: "rgb(231 229 220 / 0.5)"
  grid-dark: "rgb(231 229 220 / 0.06)"
  grid-major-dark: "rgb(231 229 220 / 0.11)"
  blue-dark: "#86a4ec"
  blue-strong-dark: "#a9c0f5"
  blue-tint-dark: "#1c2741"
  green-dark: "#7cc795"
  green-strong-dark: "#a5dcb5"
  green-tint-dark: "#16281e"
  red-dark: "#ef7b6e"
  red-strong-dark: "#f6a196"
  red-tint-dark: "#3a1f1d"
  focus-dark: "#86a4ec"
typography:
  headline:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.375rem"
    fontWeight: 600
    lineHeight: "1.75rem"
    letterSpacing: "-0.01em"
  title:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.125rem"
    fontWeight: 600
    lineHeight: "1.5rem"
  subtitle:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.9375rem"
    fontWeight: 600
    lineHeight: "1.5rem"
  body:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 450
    lineHeight: "1.375rem"
  body-sm:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.8125rem"
    fontWeight: 450
    lineHeight: "1.25rem"
  control:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.8125rem"
    fontWeight: 600
    lineHeight: "1.25rem"
  caption:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 450
    lineHeight: "1rem"
  label:
    fontFamily: "Archivo Narrow Variable, Arial Narrow, Roboto Condensed, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.6875rem"
    fontWeight: 600
    lineHeight: "1rem"
    letterSpacing: "0.12em"
  figure:
    fontFamily: "Spline Sans Mono Variable, ui-monospace, SF Mono, Menlo, Consolas, monospace"
    fontSize: "0.8125rem"
    fontWeight: 450
    lineHeight: "1.25rem"
    fontFeature: "tnum"
rounded:
  none: "0"
  xs: "1px"
  sm: "2px"
  md: "3px"
  cloud: "18px"
  full: "9999px"
spacing:
  unit: "0.25rem"
  gutter: "1rem"
  sheet: "1.5rem"
  bar: "2.75rem"
  titleblock: "4rem"
  aside: "22.5rem"
  drawing: "clamp(15rem, 42vh, 30rem)"
components:
  button:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.control}"
    rounded: "{rounded.sm}"
    height: "36px"
    padding: "0 14px"
  button-hover:
    backgroundColor: "{colors.hover}"
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.on-ink}"
    typography: "{typography.control}"
    rounded: "{rounded.sm}"
    height: "36px"
    padding: "0 14px"
  button-primary-hover:
    backgroundColor: "{colors.ink-secondary}"
  button-danger:
    backgroundColor: "{colors.red}"
    textColor: "{colors.on-ink}"
    typography: "{typography.control}"
    rounded: "{rounded.sm}"
    height: "36px"
    padding: "0 14px"
  button-danger-hover:
    backgroundColor: "{colors.red-strong}"
  button-sm:
    typography: "{typography.caption}"
    height: "28px"
    padding: "0 10px"
  button-icon:
    textColor: "{colors.ink-secondary}"
    size: "32px"
    padding: "0"
  field:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.body-sm}"
    rounded: "{rounded.sm}"
    height: "36px"
    padding: "0 12px"
  top-bar:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    height: "44px"
  title-block:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    height: "64px"
  title-block-label:
    textColor: "{colors.ink-muted}"
    typography: "{typography.label}"
  title-block-value:
    textColor: "{colors.ink}"
    typography: "{typography.title}"
  register-head:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink-muted}"
    typography: "{typography.label}"
    padding: "8px 12px"
  register-cell:
    textColor: "{colors.ink}"
    typography: "{typography.body-sm}"
    padding: "8px 12px"
  stamp-pending:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink-secondary}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    padding: "0 6px"
  stamp-running:
    backgroundColor: "{colors.green-tint}"
    textColor: "{colors.green-strong}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    padding: "0 6px"
  stamp-awaiting:
    backgroundColor: "{colors.blue-tint}"
    textColor: "{colors.blue-strong}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    padding: "0 6px"
  stamp-succeeded:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    padding: "0 6px"
  stamp-failed:
    backgroundColor: "{colors.red-tint}"
    textColor: "{colors.red}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    padding: "0 6px"
  stamp-canceled:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink-secondary}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    padding: "0 6px"
  stamp-skipped:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink-muted}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
    padding: "0 6px"
  note:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink}"
    typography: "{typography.body-sm}"
    rounded: "{rounded.sm}"
    padding: "8px 12px"
  failure-panel:
    backgroundColor: "{colors.red-tint}"
    textColor: "{colors.ink}"
    typography: "{typography.body-sm}"
    rounded: "{rounded.sm}"
    padding: "8px 12px"
  revision-cloud:
    textColor: "{colors.ink}"
    rounded: "{rounded.cloud}"
    padding: "16px 20px"
  overlay:
    backgroundColor: "{colors.sheet}"
    textColor: "{colors.ink}"
    rounded: "{rounded.md}"
    padding: "24px"
  palette-option-active:
    backgroundColor: "{colors.blue-tint}"
    textColor: "{colors.ink}"
    typography: "{typography.body-sm}"
    padding: "8px 12px"
  kbd:
    textColor: "{colors.ink-muted}"
    typography: "{typography.label}"
    rounded: "{rounded.xs}"
    padding: "0 4px"
---

# Design System: OrkStack

`ui/src/tokens.css` is the one source of every value below; this file records what shipped and the rules for using it. The frontmatter mirrors `tokens.css`. If the two ever differ, `tokens.css` wins and this file is stale. Light values carry the plain key; the dark value of the same token carries the `-dark` suffix (in CSS both are one name, switched by `:root[data-theme]`).

## Overview

**Creative North Star: "The Drawing Office"**

Every Orkflow is a controlled drawing: a sheet with a version, a digest and a title block that always says what you are looking at. The ground is drafting paper, the text is a blue-black ink, and hue is rationed. One drafting blue marks selection, links and waiting on the outside world. One survey green marks work running now. One revision-cloud red marks failure and attention, and nothing else. Hairline rules divide rows and heavier rules divide regions, so structure comes from line weight rather than from boxes, cards or fills.

The density is a working engineer's: 14 px body text in a narrow sans, 13 px registers, tracked 11 px caps for every label, and a mono for anything a person copies or compares (ids, digests, durations, commands, logs). Every page has the same three bands: a 44 px top bar, the page content, and a 64 px title block stuck to the bottom edge that states the page's facts the way a drawing's title block does. On a run, the content is a drawing frame (the DAG on a drafting grid, or the task map) beside an aside holding the version register, the revision cloud with the reason when the run failed, and the run's timing register.

Dark mode is the same drawing on ink-toned paper with pale rules. It is never a cyan-on-navy blueprint. The system rejects the category default of a grey sidebar console with a strip of stat cards: navigation is two modes in a top bar, and counts are a ruled band of title-block cells, never hero numbers.

**Key Characteristics:**
- Drafting paper and blue-black ink; three accent hues, each with one job.
- Line weight carries structure: 1 px hairline, 1.5 px region, 2 px frame, 3 px alarm.
- Square corners (2 px at most on controls); a radius is a softened corner, never a pill.
- Tracked caps for labels, mono for figures, narrow sans for everything else.
- Status is told by a drawn mark and its word, and only secondarily by colour.
- Flat at rest; shadows belong to overlays alone.
- One authored moment: the revision cloud drawing itself in when a watched run fails; and one state that breathes: work running now.

## Colors

A warm off-white paper and a deep blue-black ink, with one blue, one green and one red that are never used decoratively.

### Primary
- **Drafting Blue** (`blue`): selection, links, the current mode's 2 px underline, the focus ring (`focus` is the same value), work waiting on the outside world (the AWAITING stamp's ring, an awaiting DAG node's outline), the caret and the native accent colour. `blue-strong` is its text-on-tint form (AWAITING ink, the selected window button); `blue-tint` is the ground of a selected palette option, a selected Select option, an awaiting stamp and the diff register's new value.
- **Survey Green** (`green`): work running now, and nothing else. The RUNNING stamp's dot, a running DAG node's outline and breathing ground, a running cell of the task map and of a fan-out's item grid, the LIVE badge while the page receives a run's events, and RUNNING in the title block. `green-strong` is its text-on-tint form (RUNNING ink); `green-tint` is the ground of a running stamp, node and item. It is a survey green, warmer than the teal pin (c) and never confused with SUCCEEDED, which is ink.

### Secondary
- **Revision-Cloud Red** (`red`): failure and attention only. A FAILED stamp and mark, a failed DAG node's 3 px outline and text, the revision cloud's outline, the held-run delta, the danger button, a namespace hold. `red-strong` is red text on a tint and the hover of the danger button; `red-tint` is the ground of a task's failure panel, a FAILED stamp and a held FAILED row.

### Neutral
- **Drafting Paper** (`paper`): the page ground and the ground of every control, so a button reads as drawn on the sheet rather than raised from it.
- **Sunken Paper** (`paper-sunken`): register heads, NOTE grounds, SUCCEEDED and CANCELED stamps, a held finished row.
- **Sheet** (`sheet`): the ground of overlays only (dialogs, the palette, popovers, Select listboxes), one step lighter than paper in light mode and one step lifted in dark.
- **Hover Wash** (`hover`): the hover ground of rows, buttons and options.
- **Blue-Black Ink** (`ink`): text, frame and region rules, the primary button's fill, the DAG's outlines and edges. `ink-secondary` is supporting text and the primary button's hover; `ink-muted` is labels, captions and inactive navigation; `ink-placeholder` equals `ink-muted` so placeholders keep 4.5:1. `on-ink` is text on an ink or red fill.
- **Finished Mark** (`mark`): the filled square of a SUCCEEDED task, lighter than ink so a task map of thousands of finished cells is not a black field, and still 3:1 on every ground.
- **Rules** (`rule`, `rule-faint`, `rule-strong`): ink at 16 %, 9 % and 62 % alpha. `rule` is the hairline between rows; `rule-faint` is the quietest line, for detail nested inside a task (attempt rows, provenance entries, the timing bar); `rule-strong` is a control's outline and the PENDING/SKIPPED stamp border, at 3:1.
- **Drafting Grid** (`grid`, `grid-major`): the 8 px minor and 40 px major lines behind the DAG, and nowhere else.
- **Scrim** (`scrim`): ink at 42 % (black at 60 % in dark) behind a modal and the menu drawer.

### Pins
Six coloured-pencil pairs (`--color-pin-a-line` / `-fill` through `pin-f`: blue, violet, teal, ochre, slate, olive, each with a dark twin in `tokens.css`). A pin's member task rows carry its 1 px line all round and its fill. They are identity, never status: the pin's name is always written beside it, and each line is at least 3:1 on paper and on its own fill. They are runtime tokens (read with `var()`), not Tailwind utilities.

### Status
Status tokens (`--color-{status}-bg / -fg / -border / -solid`) are aliases of the colours above, never new hues: PENDING is paper with a strong rule; RUNNING is green-strong on green-tint with a green border and mark; AWAITING is blue-strong on blue-tint; SUCCEEDED is ink on sunken paper with the `mark` fill; FAILED is red on red-tint; CANCELED is ink-secondary on sunken paper; SKIPPED and UNSPECIFIED are ink-muted on paper with a dashed or dotted border.

### Aliases
`--color-fg` is the one permanent alias, of `ink`: `run-page-pins.spec.ts` reads it by name, and it has always meant the ink. Every legacy name from before this design was deleted; no other alias may be added.

### Named Rules
**The Three Accents Rule.** Blue means selected, linked or waiting on the outside world. Green means running now. Red means failed or needs attention. None is ever used for emphasis, branding or decoration, and a canceled run's reason is an ink NOTE, not red. (Until 2026-10-22 this was the Two Accents Rule, with RUNNING in blue. It became three because blue then meant both "selected" and "running" — a selected running node was a blue ring round a blue outline — and because the person who runs these Orkflows reads running work as green. A fourth accent needs the same kind of case, written here.)

**The One File Rule.** Every colour, in both themes, is declared in `ui/src/tokens.css`. No component, route or stylesheet declares a raw colour; Tailwind's own palette is reset away, so `bg-red-500`, `text-white` or `shadow-md` emit nothing. `ui/test/tokens.test.ts` enforces it on the sources and on the compiled CSS.

**The Contrast Floor Rule.** Every text token reads at 4.5:1 on every ground, and focus, strong rules, status marks and pin lines reach 3:1, in both themes. `ui/test/tokenContrast.test.ts` checks every pair; a new token joins that test before it ships.

## Typography

**Body Font:** Archivo Narrow Variable (with Arial Narrow, Roboto Condensed, the system sans)
**Label/Mono Font:** Spline Sans Mono Variable (with ui-monospace, SF Mono, Menlo, Consolas)

**Character:** A narrow humanist sans, the face of a drawing's lettering, that fits a lot on a line without going small. A plain mono sits beside it for things that are compared character by character. Both are self-hosted variable faces from `@fontsource-variable` (weights 400–700 and 300–700); only the two Latin files are preloaded, 18.7 KB and 36.5 KB.

### Hierarchy
- **Headline** (600, 22 px / 28 px, −0.01em): the page title in `SheetHeader`, one per page; a run id or digest title is set in the mono and breaks anywhere.
- **Title** (600, 18 px / 24 px): title-block values and count-band figures; the largest type a value ever gets.
- **Subtitle** (600, 15 px / 24 px): section heads within a page ("Running now"); 15 px is also the prose size where a paragraph needs room.
- **Body** (450, 14 px / 22 px): the default text of `<body>`. Weight 450 rather than 400 so the narrow strokes hold on paper.
- **Body small** (450, 13 px / 20 px): registers, fields, task rows. The most-used size in the app.
- **Control** (600, 13 px / 20 px): button labels.
- **Caption** (450, 12 px / 16 px): timestamps, secondary facts, small buttons.
- **Label** (600, 11 px / 16 px, 0.12em, uppercase): table heads, title-block labels, field labels, section caps, status stamps, sheet tabs, the wordmark. The `.caps` primitive.
- **Figure** (mono, tabular figures, inherits the size it sits in): ids, digests, counts, durations, versions, commands. The `.figure` primitive.

Paragraphs that explain are held to `max-w-prose` (65ch).

### Named Rules
**The Mono Is Data Rule.** The mono marks text a person copies, compares or reads in a terminal: ids, digests, durations, commands, logs. It is never a "technical" costume for headings or labels.

**The No Hero Number Rule.** No figure is set above 18 px. A count is a fact in a title-block cell, not a display.

**Notes on faces.** No italic file is loaded; the rare emphasis (an unavailable count) renders a synthetic oblique, accepted. Cyrillic and Greek text falls back to the system sans and mono, since only the Latin subsets are declared for preload and the others load only when their characters appear.

## Layout

The page is a vertical stack of three bands. The **top bar** is 44 px (`--spacing-bar`), sticky, with a 1.5 px ink rule under it. The **content** is centred, at most 1440 px wide, with a 16 px gutter below 640 px and a 24 px sheet margin from 640 px up, and 24 px above and below. The **title block** is 64 px (`--spacing-titleblock`), sticky at the bottom with a 2 px ink rule above it. `scroll-padding` on `<html>` equals both, so a focused control is never hidden under either (WCAG 2.4.11).

A page opens with its `SheetHeader`: the headline and a line of facts at the left, the page's one primary action at the top right, a hairline under it and 24 px below. A run's line of facts is one line — `orkflow@version · origin submit · [STATUS]`, the origin in words, never a stamp — and says LIVE only while the run is; a finished run's status already says it ended. Below 1024 px a run's secondary actions (Evidence, Compare, Export JSON, Show events, retry history, the copied command) fold into one **More** disclosure beside Run again, and below 640 px its id is drawn short in the middle (`ork-…873153`), the whole id staying the heading for assistive technology; together they start the drawing within 220 px of the top at 390 px. Regions within a page start with a 1.5 px ink rule (`.region`). Registers (tables) are the main content of list pages.

The run page sets a **drawing frame** beside an **aside** of 360 px (`--spacing-aside`) with a 24 px gap from 1280 px (`xl`) up, and the run's body (its values NOTE, events and task list) flows in the drawing's column under the frame, the aside beside it; below 1280 px the aside follows the drawing and the body follows the aside. The frame is its content plus the 32 px patch and 16 px padding, up to `--spacing-drawing` (clamp(240 px, 42 vh, 480 px)): `min(var(--spacing-drawing), content + 48px)`, so a small run is not a page of empty grid or empty map. The DAG's height is its drawing's own (CSS, from its aspect); the task map's is computed from the frame's width with the map's own geometry, once the frame is laid out and before its first paint, so nothing below the frame moves. The run's timing (Created, Started, Finished, Elapsed, Tasks) is a register in the aside, never a second band across the page, and a run with no input or no aggregated output says so in one NOTE rather than an empty box. The frame's view tabs (DAG, Task map) and caption sit on a 32 px paper patch at its top left. Inside, the task map picks the largest cell pitch of 16, 14, 12, 10 or 8 px (cell = pitch − 2, gap 2) whose rows fit; at 4,000 tasks it fits whole at 1440 × 900 and scrolls inside the frame (never the page) at 1280 × 720. The DAG draws 190 × 58 nodes with 56 px between layers and 34 px between rows, scaled down to the frame's width but never below 0.6; up to 80 tasks it is drawn, up to 300 on request, and beyond that the run page draws only the selected task's neighbourhood.

The DAG is a layered drawing (`$lib/dag` layoutDag), the same Orkflow always drawn the same way: each task one layer past its deepest need; an edge that skips layers gets a 12 px lane in every layer it passes, so no edge ever runs through a box; the order within each layer is chosen by barycentre sweeps from declaration order to cross as few edges as it can (a shape that can be drawn without crossings is); and between two layers each edge runs on a track of its own split or join, the gap widening (to 216 px at most) to keep tracks 8 px apart. It reads left to right. A top-to-bottom drawing of the same layout exists as a prototype (`?dag=tb`, and `?dag=auto` to choose by the frame) while the default is decided; `test/dagStress.ts` holds the shapes both are held to.

The spacing unit is 4 px (`--spacing`); every padding, gap and height is a multiple of it. Typical rhythms: 8 × 12 px register cells, 8 px between the groups of a filter toolbar, 24 px between regions, 40 px after a dashboard section.

Breakpoints, containers, aspect ratios and blurs keep Tailwind's defaults (sm 640, md 768, lg 1024, xl 1280, 2xl 1536 px), because they are not identity and media queries cannot read `var()`. Responsive behaviour:
- Below 1024 px the top bar drops Build's three page links, the namespace picker and the appearance switch into a **Menu** drawer (a right-hand dialog 320 px wide); below 640 px the mode links go too.
- Below 640 px the title block keeps its first cell and STATUS (or its second cell) and folds the rest into a "Page details" disclosure.
- Below 640 px a stacked register (`.register-stacked`) turns each row into a two-column grid of its cells, so nothing hides behind a sideways scroll; every register of the dashboard is one, and a bare figure in it says what it counts ("1 failed", "of 2 finished").
- The dashboard's count band is seven cells in one ruled row from 1024 px up, and below that a 12-column grid: four status cells of three columns over three figure cells of four; below 400 px the four status cells are a 2 × 2 grid. A label never breaks inside a word.
- Below 640 px a run id in a register is drawn short in the middle (`ork-…873153`), its whole id kept for assistive technology and on hover, and the title block's first cell wraps to two lines at 13 px rather than losing its end.

### Named Rules
**The Three Bands Rule.** Every page has the top bar, the content and the title block, and nothing else is fixed to the viewport except an open overlay.

**The Grid Behind the DAG Rule.** The drafting grid appears only behind the DAG. Lists, logs, registers and the task map sit on clean paper.

## Elevation & Depth

The system is flat. Depth comes from line weight and from paper tones (sunken paper for heads and notes, sheet for overlays), not from shadows. Two shadows exist, both ink-tinted in light mode and black in dark, and neither is used at rest on page content.

### Shadow Vocabulary
- **Overlay** (`--shadow-overlay`: `0 8px 24px -6px rgb(20 33 61 / 0.22), 0 2px 6px -2px rgb(20 33 61 / 0.12)`; dark `0 10px 28px -6px rgb(0 0 0 / 0.6), 0 2px 6px -2px rgb(0 0 0 / 0.45)`): dialogs, the command palette, popovers, Select listboxes, the menu drawer and the title block's "Page details" panel.
- **Lift** (`--shadow-lift`: `0 -6px 14px -10px rgb(20 33 61 / 0.32)`; dark `0 -6px 16px -10px rgb(0 0 0 / 0.7)`): the title block's top edge, only while page content runs on beneath it (an IntersectionObserver on a sentinel at the end of `<main>`).

### Named Rules
**The Overlays Only Rule.** A shadow means "this floats above the sheet". Cards, rows, buttons and panels never carry one.

## Shapes

Drawings are square-cornered. Controls, stamps, notes and failure panels take a 2 px corner (`rounded.sm`); overlays take 3 px (`rounded.md`); a `<kbd>` takes 1 px. The full radius exists only for the RUNNING dot and the AWAITING ring. The 18 px `rounded.cloud` is the revision cloud's first-paint form: a dashed 2 px red outline, replaced once the box is measured by a scalloped SVG outline (an arc about every 14 px, 1.5 px red, inset 7 px).

Rules have four weights, each with one job: **hairline** 1 px (`--rule-hair`) between rows and around controls; **region** 1.5 px (`--rule-region`) around a drawing frame, under the top bar and register heads, above regions, and on every DAG outline and edge (non-scaling, so a scaled drawing keeps its weight); **frame** 2 px (`--rule-frame`) above the title block and as the focus outline with its 2 px offset; **alarm** 3 px (`--rule-alarm`) on a failed DAG node only.

Status marks are drawn shapes, 8 px square, in CSS on the stamp itself (no extra element, because a run page holds thousands): SUCCEEDED a filled square, FAILED a red square crossed in paper, CANCELED an outline struck once, SKIPPED a hatched square with a dashed outline, PENDING a hollow square, UNSPECIFIED a dotted outline, RUNNING a filled green dot that pulses, AWAITING a blue ring. The same shapes are drawn as SVG on DAG nodes and on the task map's canvas. Under forced colours every mark keeps a CanvasText border.

The **revision delta** is a small drawn triangle: the drawing's mark for "changed". It sits before a held run's id (red) and before a changed value in the compare register (blue).

## Components

The world's nouns (sheet, title block, drawing, revision cloud, revision delta, superseded) name components and devices in code and in this file. None of them is printed on any surface: the UI says "Page summary", "Page details", "DAG", "Task map", "version", never "sheet", "drawing" or "revision".

### Buttons
Quiet and drawn, not raised.
- **Shape:** 2 px corner, 36 px tall, 14 px inline padding, 13 px semibold, a 1 px `rule-strong` outline on paper.
- **Primary:** ink fill, `on-ink` text; hover moves to `ink-secondary`. One per page, at the top right of the `SheetHeader`; a list's Apply filters is a default button. Where the primary opens a form (Admin → Namespaces' Create namespace), the form is a disclosure under the header and, while it is open, its own submit is the primary.
- **More:** a page's secondary actions folded into one disclosure (`MoreMenu`) where its header has no room; its items are ordinary buttons and links, each doing exactly what the button it stands for does.
- **Danger:** red fill, `on-ink` text, hover `red-strong`. Destructive confirmations only.
- **Hover / Focus:** hover changes the ground to `hover` over 120 ms ease-out; focus is the global 2 px focus-colour outline at 2 px offset.
- **Small:** 28 px, 10 px padding, 12 px. **Icon:** 32 px square (above the 24 px target floor), transparent, `ink-secondary`, ink on hover. **Disabled:** 50 % opacity, no pointer events.
- **Disclosure:** a chevron drawn in CSS on the button itself (no element per task row), turned down when expanded.

### Status stamps
- **Style:** a 1 px frame in the status border colour, the status ground, 11 px tracked caps in the status ink, 6 px inline padding, and the drawn mark 6 px before the word. The text is exactly the status word.
- **Variants:** the eight statuses above. Failure categories reuse the stamp in red and ink treatments: ACTIVITY_FAILED red on red-tint, INFRASTRUCTURE_ERROR red-strong on paper, CONTRACT_VIOLATION on-ink on a red-strong fill, TIMED_OUT and CANCELLED in ink tones. Version marks in the aside are stamps too: "This run" in ink on a row of sunken paper (whatever the run's status: the status is said once, by the run), "Current" in blue, "Head" in `rule-strong`.
- **Never started:** a task the run ended before it reached an Orker is stamped with its lifecycle status (CANCELED, as the wire says) with "never started" under it — on its row, as its DAG node's line, in the map's readout and in the map's key ("CANCELED 1 · never started"). No view says something else about it.

### Registers
The table is the main surface of most pages.
- **Head:** 11 px tracked caps in `ink-muted` on sunken paper, a 1.5 px ink rule beneath, 8 × 12 px padding.
- **Rows:** 13 px, a 1 px `rule` hairline under each, 8 × 12 px padding, `hover` wash on hover. Ids and timestamps in the mono.
- **Held rows:** a run whose status changed since it was last seen keeps its new status's ground (red-tint for FAILED, sunken paper otherwise) and a red revision delta before its id until opened; hover underlines the id instead of replacing the ground.
- **Duration:** the runs register's DURATION is submission to finish; the run page's Elapsed is start to finish, so the two can differ by the run's wait before it started.
- **Task rows:** a run's task rows share one timeline. A task's mark is 4 px tall, `mark` for its run and `rule-strong` for its wait for an Orker, placed at (its start − the run's start) ÷ the run's elapsed and as wide as its duration ÷ that, on a hairline of fixed width, so the rows read together as when each task ran. A value of at most 64 characters is said on its row in the mono (`Values {"exitCode": 0}`); a longer one keeps its block. Each command a row offers is a button that names it in the mono (`run logs`, `run timing`, `task run`), never a row of identical prompts.
- **Commands:** every CLI equivalent quotes only the tokens a shell would read as syntax (`orkctl --namespace default run get ork-1 --json`; `--filter 'action^namespace.'`), and a command written out on the page breaks only at spaces.

### Inputs / Fields
- **Style:** 1 px `rule-strong` outline, 2 px corner, paper ground, ink text, 36 px tall with 12 px padding; placeholder in `ink-placeholder` at full opacity.
- **Label:** 11 px tracked caps in `ink-secondary` above the control, 6 px gap, with an optional hint in 12 px plain case on the same line.
- **Focus:** the global 2 px focus outline. **Select** is a custom combobox with the same trigger as a field and a listbox on `sheet` with the overlay shadow; the selected option is semibold on blue-tint.
- The run input editor is the typed JSON editor; there is no YAML input mode. The Apply page's YAML textarea is an authored document, not input.

### Navigation
- **Top bar:** the wordmark (a title-block-corner SVG mark and "ORKSTACK" in tracked caps), then two modes, **Build** and **Operate**, in caps. The current mode is ink with a 2 px blue underline; Build's three daily pages (Runs, Orkflows, Apply) follow it in 13 px, the current one ink, semibold and underlined. At the right: the namespace picker under a "NAMESPACE" caps label, the palette button ("Jump to…" with ⌘K or Ctrl K in a `<kbd>`), and the appearance switch (system, light, dark as three pressed buttons; the pressed one is ink-filled).
- **Departure from the sketch:** the chosen sketch's top bar held only Build and Operate. The shipped bar keeps Build's three pages visible in Operate too, because the primary-navigation contract the specs hold requires them on every page, and because the daily pages stay one click away from any admin page.
- **Operate tabs:** the admin pages are a row of caps tabs under a 1.5 px ink rule, the current one ink with a 2 px blue underline; the row scrolls sideways when narrow.
- **Command palette:** Ctrl/Cmd+K anywhere, including from inside a text field. A native `<dialog>` on `sheet`, 640 px wide at most, 3 px corner, 1.5 px ink frame, overlay shadow. Results are grouped under caps heads; the active option is semibold on blue-tint with an "Enter" `<kbd>`.

### Title Block (signature)
The persistent 64 px band along the bottom of every page. Cells stack an 11 px caps label in `ink-muted` over an 18 px semibold value, divided by 1 px ink rules; the first cell is twice the width of the rest. STATUS writes its word after its drawn mark, red for FAILED, green for RUNNING, blue for AWAITING, ink otherwise. Below 640 px the first cell wraps to two lines at 13 px rather than losing its end. Digests can be copied from their cell. It never shows a run id: the id is the page's headline. The chosen sketch's static frame used 92 px; the shipped block is 64 px because it is sticky on every page, and at 1280 × 720 the bar and a 92 px block would take 19 % of the viewport for chrome where 64 px takes 15 %, the sketch's own share at 1440 × 900. The difference is cell padding, not type.

### Drawing Frame and Task Map (signature)
A 1.5 px ink frame of fixed height with a paper patch at its top left holding the view tabs (caps, the selected one ink with a 2 px blue underline) and a mono caption ("order-saga @ 0.4.2 · 6 tasks"). The DAG view sits on the drafting grid: nodes are paper boxes with a 1.5 px ink outline, the task id in 13 px semibold sans and a second line in the 11 px mono, orthogonal ink edges with arrowheads, a 2 px blue ring for the selected node, a 3 px red outline and red text for a failed one, a blue outline for awaiting, and 45 % opacity, never a strike, for a task the run never started (its line reads "never started"). A running node is a soft green box: a green outline round a ground that breathes from `green-tint` to paper and back every 2.4 s, resting on the tint under reduced motion. A node's tags (`each ≤ n`, `join …`, `cached`, a composed Orkflow) are `ink-secondary`, never an accent. Where the drawing is wider than the frame at 0.6 scale, a pair of scroll buttons sits on the frame's top band, beside the tabs and never over a node, the one with nothing more to show disabled. The task map is one canvas, one cell per task or fan-out item, drawn with the status marks; a running cell's mark breathes with the DAG's 2.4 s (only those cells are repainted, at most 15 times a second, never while the page is hidden or under reduced motion). A legend under the frame lists each status present with its mark and count.

### Revision Cloud (signature)
The run's failure, once per page, in the aside. A scalloped 1.5 px red outline around a caps tag in `red-strong` ("VERSION 0.4.2 · charge failed"), the reason code in the mono, the plain explanation in ink, the attempt line and the reproducing command. Task and item failures never get a cloud: they use the compact failure panel, red ink on a red-tint ground with a 2 px corner and no rule, the code after a filled mark. Clouds everywhere would stop meaning anything.

### Notes and Holds
- **NOTE:** a caveat that is not an alarm. Sunken paper, 1 px `rule` outline, 2 px corner, 8 × 12 px padding, a "NOTE" caps label in `ink-muted` beside the sentence. Operate pages state their scope with one under the header.
- **Severity:** errors and warnings are `red-strong` text after a drawn 8 px square, filled for an error and outlined for a warning, always with the word.
- **Hold banner:** a namespace on hold is the one full-width red element: 1 px red border on red-tint, the reason, and the CLI command that releases it.

### Overlays
A popover hangs from its trigger's edge but never past the viewport's: opened near the other edge (a header's actions wrapped to the left on a phone), it moves back inside, 8 px clear. Dialogs, popovers and listboxes share one treatment: `sheet` ground, ink frame (1.5 px for dialogs and the palette, 1 px for popovers and listboxes), 3 px corner, overlay shadow, and the `overlay-in` entrance (120 ms, 3 px rise and 0.985 scale, ease-out). The dialog's body scrolls inside the panel; its title sits over a hairline.

### Motion
Motion is state feedback, never decoration, and all of it is in `tokens.css`. Ease-out is `cubic-bezier(0.16, 1, 0.3, 1)`; the one symmetric curve, `--ease-in-out` (`cubic-bezier(0.45, 0, 0.55, 1)`), is for a breath. Durations are 120 ms (hover, colour, overlay entrance), 200 ms (base), 600 ms (the cloud) and 2.4 s (a running ground's breath). Five animations exist: the RUNNING dot's 1.6 s pulse, the running ground's 2.4 s breath (`live-fill` on a DAG node, `live-ground` on a fan-out item, the same curve painted on the task map's canvas), the 2 px navigation progress bar under the top edge, the overlay entrance, and the cloud drawing itself in once when a run turns FAILED while someone is watching it (on load it is simply there). Under `prefers-reduced-motion` the dot, the breath and the cloud stop — a running node rests on its green tint, a running map cell is drawn whole — and the progress bar falls back to a pulse. No animation library is used.

## Do's and Don'ts

### Do:
- **Do** take every colour, size, radius, shadow and duration from `ui/src/tokens.css`, by its token name, in both themes.
- **Do** tell status by its drawn mark and its word first; colour is the third channel (WCAG 1.4.1).
- **Do** divide rows with the 1 px `rule` hairline and regions with the 1.5 px ink rule, instead of boxing content in cards.
- **Do** put the page's one ink-filled primary button at the top right of its `SheetHeader`.
- **Do** set labels in the 11 px tracked caps and ids, digests, durations and commands in the mono with tabular figures.
- **Do** design light and dark together; the dark theme is ink-toned paper (`#13161c`) with pale rules.
- **Do** keep every control at least 24 px and every focus outline the 2 px focus colour at a 2 px offset.
- **Do** write the product's words on every surface: version, origin, pool, pin, "asserted by" for a principal; "Orkflow" is a name and is always capitalised (lowercase only where it is the DSL's or the CLI's own word: `orkflow:name@version`, `orkctl orkflow …`, a cache scope).

### Don't:
- **Don't** use blue, green or red for emphasis, branding or decoration; a canceled run's reason is an ink NOTE.
- **Don't** draw a revision cloud for anything but the run's own failure, and never more than one per page.
- **Don't** put the drafting grid behind anything but the DAG.
- **Don't** add shadows to page content; they belong to overlays and the title block's lift.
- **Don't** round anything into a pill, or past 3 px, except the RUNNING dot, the AWAITING ring and the cloud's first paint.
- **Don't** set a figure above 18 px or build a strip of stat cards; counts are a ruled band of title-block cells.
- **Don't** print the world's device names (sheet, drawing, title block, revision) or any internal mechanism word (queue, session, generation, checkpoint) on a surface.
- **Don't** make it a cyan-on-navy blueprint, a grey sidebar console, or a dashboard of cards.
- **Don't** add a raster, an icon font, a web-font CDN or an animation library; the mark, icons and cloud are SVG and the faces are self-hosted.
