---
name: OrkStack site
description: The landing page and docs, drawn as one more sheet from the UI's drawing office. Every value is a token of ui/src/tokens.css; this file names tokens and never repeats their values.
colors:
  paper: "var(--color-paper)"
  paper-sunken: "var(--color-paper-sunken)"
  sheet: "var(--color-sheet)"
  hover: "var(--color-hover)"
  scrim: "var(--color-scrim)"
  ink: "var(--color-ink)"
  ink-secondary: "var(--color-ink-secondary)"
  ink-muted: "var(--color-ink-muted)"
  on-ink: "var(--color-on-ink)"
  mark: "var(--color-mark)"
  rule: "var(--color-rule)"
  rule-faint: "var(--color-rule-faint)"
  rule-strong: "var(--color-rule-strong)"
  grid: "var(--color-grid)"
  grid-major: "var(--color-grid-major)"
  blue: "var(--color-blue)"
  blue-strong: "var(--color-blue-strong)"
  blue-tint: "var(--color-blue-tint)"
  focus: "var(--color-focus)"
  red: "var(--color-red)"
  red-strong: "var(--color-red-strong)"
  red-tint: "var(--color-red-tint)"
  green: "var(--color-green)"
  green-strong: "var(--color-green-strong)"
  green-tint: "var(--color-green-tint)"
typography:
  display:
    fontFamily: "var(--font-sans)"
    fontSize: "clamp(var(--text-2xl) * 1.3, 2.6vw + 1rem, var(--text-2xl) * 1.9)"
    fontWeight: "var(--font-weight-semibold)"
    lineHeight: "var(--leading-none)"
    letterSpacing: "var(--tracking-tight)"
  headline:
    fontFamily: "var(--font-sans)"
    fontSize: "clamp(var(--text-2xl), 1.6vw + 1.2rem, var(--text-2xl) * 1.5)"
    fontWeight: "var(--font-weight-semibold)"
    lineHeight: "var(--leading-tight)"
    letterSpacing: "var(--tracking-tight)"
  title:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-lg)"
    fontWeight: "var(--font-weight-semibold)"
    lineHeight: "var(--text-lg--line-height)"
  lede:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-lg)"
    fontWeight: "var(--font-weight-body)"
    lineHeight: "calc(var(--text-lg--line-height) * 1.1)"
  body:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-md)"
    fontWeight: "var(--font-weight-body)"
    lineHeight: "var(--text-md--line-height)"
  register:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-base)"
    fontWeight: "var(--font-weight-body)"
    lineHeight: "var(--text-base--line-height)"
  label:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-2xs)"
    fontWeight: "var(--font-weight-semibold)"
    lineHeight: "var(--text-2xs--line-height)"
    letterSpacing: "var(--tracking-caps)"
  code:
    fontFamily: "var(--font-mono)"
    fontSize: "var(--text-sm)"
    lineHeight: "var(--text-sm--line-height)"
  caption:
    fontFamily: "var(--font-mono)"
    fontSize: "var(--text-xs)"
    lineHeight: "var(--text-xs--line-height)"
rounded:
  none: "var(--radius-none)"
  xs: "var(--radius-xs)"
  sm: "var(--radius-sm)"
  md: "var(--radius-md)"
  full: "var(--radius-full)"
spacing:
  unit: "var(--spacing)"
  gutter: "var(--spacing-gutter)"
  sheet: "var(--spacing-sheet)"
  bar: "var(--spacing-bar)"
  section: "calc(var(--spacing) * 24)"
components:
  button:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "{rounded.sm}"
    height: "calc(var(--spacing) * 10)"
    padding: "0 calc(var(--spacing) * 4)"
  button-hover:
    backgroundColor: "{colors.hover}"
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.on-ink}"
    rounded: "{rounded.sm}"
    height: "calc(var(--spacing) * 10)"
    padding: "0 calc(var(--spacing) * 4)"
  button-primary-hover:
    backgroundColor: "{colors.ink-secondary}"
  button-quiet:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "{rounded.sm}"
    height: "calc(var(--spacing) * 8)"
    padding: "0 calc(var(--spacing) * 2.5)"
  topbar:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    height: "{spacing.bar}"
  topbar-link:
    textColor: "{colors.ink-muted}"
    typography: "{typography.label}"
  topbar-link-current:
    textColor: "{colors.ink}"
  theme-switch-pressed:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.on-ink}"
    size: "calc(var(--spacing) * 7)"
  titleblock-cell:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    padding: "calc(var(--spacing) * 2.5) calc(var(--spacing) * 4)"
  stamp:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    rounded: "{rounded.sm}"
  frame-caption:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink-secondary}"
    typography: "{typography.caption}"
    padding: "calc(var(--spacing) * 1.5) calc(var(--spacing) * 3)"
  balloon:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "{rounded.full}"
    size: "calc(var(--spacing) * 6)"
  register-head:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink-muted}"
    typography: "{typography.label}"
    padding: "calc(var(--spacing) * 2) calc(var(--spacing) * 3)"
  terminal:
    backgroundColor: "{colors.paper-sunken}"
    textColor: "{colors.ink}"
    typography: "{typography.code}"
  more-link:
    textColor: "{colors.blue}"
  more-link-hover:
    textColor: "{colors.blue-strong}"
---

# Design System: OrkStack site

The site has no visual system of its own. `ui/src/tokens.css` holds every colour, face, size, radius, rule width and duration the site draws with, in both themes, and `ui/DESIGN.md` holds the reasons ("The Drawing Office"). `site/src/styles/site.css` imports `tokens.css` as it is; nothing under `site/src` repeats a value from it. This file records what is particular to the site: how its pages are built from those tokens, the landing page's drawing conventions, the Starlight mapping, theme and media behaviour, and the check that keeps the site from drifting. The frontmatter is written in token references on purpose; a literal value here would be a second copy of `tokens.css`.

## Overview

**Creative North Star: "One More Sheet From the Drawing Office"**

The site is a drawing set from the same office as the UI. The landing page is one sheet: its first view is a real run of the example Orkflow in the UI, framed as a drawing, and along its bottom edge a title block holds the headline, the two actions and six facts. Below the sheet, each section is a region of the same drawing, opened by a region rule: the model drawn on a drafting grid with balloons keyed to a parts list, a real Orkflow listed line by line with balloons in its margin, the features as registers, durability as an owners register, the limits as a ruled list, the quickstart as a terminal and a recording, and the status as a ruled row of links. The docs are Starlight, restyled only through its custom properties, plain CSS over its public markup and five component overrides, so a docs page carries the same top bar, the same ink rules, the same registers and the same closing title block.

Density follows the UI's: narrow sans body text, tracked caps for every label, the mono only for what a person copies or compares (YAML, commands, ids, captions naming a version). Structure comes from line weight (`--rule-hair`, `--rule-region`, `--rule-frame`), never from cards, fills or shadows. Code is monochrome ink. The landing page renders without JavaScript; scripts only add the appearance switch, the copy button and the pre-paint theme choice.

**Key Characteristics:**
- No colour or face of its own: tokens by name, enforced by `site/scripts/check-tokens.mjs`.
- One drawing sheet in the first viewport: main view above, title block below.
- Regions opened by a `--rule-region` ink rule; rows divided by `--rule-hair` in `--color-rule`.
- Drawing frames with a mono caption patch; the drafting grid behind the architecture drawing only.
- Balloons keyed to parts lists, on the drawing and in the code listing's margin.
- Monochrome code in both themes, generated from the tokens.
- Real screenshots and recordings in light and dark pairs, never autoplaying.

## Colors

Drafting paper and blue-black ink, with hue rationed tighter than in the UI: the site's own drawing uses blue alone, and green appears only inside screenshots and in a mapped Starlight hue.

### Primary
- **Drafting Blue** (`--color-blue`): prose links (`.landing p a`, docs content links), the "more" links under each part and section, the status links, the 2 px underline under the current top-bar place, the current entry of the docs' on-page contents, the caret (`caret-color`) and `accent-color`. Link underlines are the same blue mixed to 40% with `color-mix()` and go solid on hover. `--color-blue-strong` is every link's hover colour. `--color-blue-tint` is the text selection ground, the current docs sidebar entry's ground and the code themes' selection.
- **Focus** (`--color-focus`): the `:focus-visible` outline, `--rule-frame` wide and offset by the same.

### Secondary
- **Revision-Cloud Red** (`--color-red`, `-strong`, `-tint`): in the site's own CSS only through Starlight's mapping: caution and danger asides take the red outline on the red tint, and Starlight's red and orange hues both map to red.
- **Survey Green** (`--color-green`, `-strong`, `-tint`): only through Starlight's green mapping. Elsewhere green appears only inside real UI screenshots, where it means a run is running.

### Neutral
- **Drafting Paper** (`--color-paper`): the page ground of both landing page and docs, every drawing frame, the top bar, the title block, buttons, balloons and the docs' nav and sidebar.
- **Sunken Paper** (`--color-paper-sunken`): register heads, the code listing and terminal grounds, the stamp, inline code-block and Expressive Code editor grounds, docs NOTE asides, the empty frame a missing screenshot draws.
- **Sheet** (`--color-sheet`): overlays only: the mobile menu drawer and the docs' search dialog.
- **Hover Wash** (`--color-hover`): hover ground of buttons, the copy button, status links, menu entries and sidebar links; the ground of inline code in the docs.
- **Ink** (`--color-ink`): text, headings, region and frame rules, the hero sheet's frame, balloon outlines, the architecture drawing's boxes, edges and arrowheads, the primary button's fill and the pressed appearance button.
- **Secondary Ink** (`--color-ink-secondary`): ledes, register cells, part descriptions, frame captions, the primary button's hover fill.
- **Muted Ink** (`--color-ink-muted`): tracked-caps labels, the shell prompt and comments, line references, recording captions, unpressed appearance buttons, idle top-bar links.
- **On Ink** (`--color-on-ink`): text on an ink fill.
- **Rules** (`--color-rule`, `--color-rule-faint`, `--color-rule-strong`): `rule` for hairlines between rows and cells; `rule-faint` for inline code's border; `rule-strong` for control outlines, quiet link underlines and the scrollbar.
- **Grid** (`--color-grid`, `--color-grid-major`): the drafting grid behind the architecture drawing.
- **Mark** (`--color-mark`): Starlight's fourth grey, nothing else on the site.

### Named Rules
**The No Copy Rule.** The site declares no colour. `site/scripts/check-tokens.mjs` (run by the `site` gate) refuses, anywhere under `site/src` and in `astro.config.mjs`, a hex colour, a colour function, a named colour in a colour property or attribute, and a `var()` naming a custom property that neither `tokens.css` nor the site nor Starlight, Expressive Code or Shiki declares. `color-mix()` of tokens is allowed. The two places the site needs literal values derive them at build time from `tokens.css` (`site/scripts/tokens.mjs`): the no-JavaScript dark block and the code themes.

**The Rationed Hue Rule.** The site's own drawing uses one hue, the blue, for links, selection, the current place and focus. Red and green reach the site only through Starlight's mapped hues and inside screenshots. Starlight's blue and purple hues map to rules and ink, so a note or a tip is ink, never a colour.

## Typography

**Body Font:** `--font-sans` (Archivo Narrow, self-hosted)
**Label/Mono Font:** `--font-mono` (Spline Sans Mono, self-hosted)

**Character:** The UI's pairing, unchanged: a narrow humanist sans for prose, headings and tracked-caps labels, and a mono reserved for data. Both faces are installed by `@fontsource-variable` packages at exactly the UI's versions.

### Hierarchy
- **Display** (semibold, fluid from 1.3 to 1.9 times `--text-2xl`, `--leading-none`, `--tracking-tight`, balanced): the landing page's one h1, in the title block's first cell, at most 22ch wide.
- **Headline** (semibold, fluid from `--text-2xl` to 1.5 times it, `--leading-tight`, `--tracking-tight`, balanced): each landing section's h2. The docs use the token ramp directly: h1 `--text-2xl`, h2 `--text-xl`, h3 `--text-lg`, h4 `--text-md`, h5 `--text-base`, all semibold and tight.
- **Title** (semibold, `--text-lg`): a part's h3, a limit's name.
- **Lede** (`--text-lg`, `--color-ink-secondary`): the paragraph under each section heading and the hero lede below 1024 px; at 1024 px and up the hero lede drops to `--text-md`.
- **Body** (`--font-weight-body`, `--text-md`): landing prose, title-block values (semibold); docs paragraphs and list items stop at 72ch.
- **Register** (`--text-base` on the landing page, `--text-sm` in docs tables): register rows.
- **Label** (semibold, `--text-2xs`, `--tracking-caps`, uppercase): the `.caps` class: title-block and fact labels, register heads, the top bar's places, the wordmark (at `--text-xs`), the docs sidebar's group labels and the contents heading, the terminal bar's "Shell".
- **Code** (`--font-mono`, `--text-sm`): the YAML listing, the quickstart commands (medium weight), docs code blocks. Inline code inside prose sets at 0.9em.
- **Caption** (`--font-mono`, `--text-xs`): a drawing frame's caption patch and the quickstart's comments.

### Named Rules
**The Mono Is Data Rule.** As in the UI: the mono marks YAML, commands, paths, versions and line numbers. Headings and labels are never set in it.

**The Two Faces Rule.** A `font-family` under `site/src` is `var(--font-sans)`, `var(--font-mono)` or `inherit`; the check refuses anything else, refuses a `font` shorthand naming no face token, and refuses a site face package whose version differs from `ui/package.json`'s.

## Layout

**The landing sheet.** `main` is centred at a maximum of 90rem, with `--spacing-gutter` side padding below 640 px and `--spacing-sheet` from 640 px. Sections (`.block`) stand `calc(var(--spacing) * 24)` apart; each opens with a `--rule-region` ink rule, then a heading block at most 46rem wide with more space below it (`* 10`) than above (`* 6`).

**The first viewport.** The hero is one grid inside a `--rule-frame` ink border. Below 1024 px it reads in document order: title cell, the framed view, the facts in two columns. From 1024 px the view spans the top and the title block runs along the bottom: the title cell (about 1.15 parts) with a `--rule-frame` ink rule on its right, then six fact cells in two rows of three divided by hairlines in ink. The view's body is cut to the screenshot's top (aspect 2400 / 745) and gives way to the viewport (`max-height: clamp(16rem, calc(100svh - 26rem), 40rem)`) so the actions stay in view. Below 640 px the screenshot is shown at 230% width in a 5 / 4 window, its top left: the run heading and the DAG at a readable size.

**Breakpoints in use.** 640 px (top-bar links fold into the Menu disclosure; sheet padding; registers and owners and status links restack), 768 px (limits become two columns), 900 px (the architecture drawing switches from down to across; the parts list becomes three columns; the title block band becomes one row with the first cell twice as wide), 1024 px (the hero sheet; features as two columns of registers), 1100 px (the code listing beside its parts list, 2 : 1). Starlight's own 50rem switches the docs header.

**The docs.** Starlight's layout with `--sl-nav-height` set to `--spacing-bar` at every width, so the docs top bar is the landing page's height. `scroll-padding-top` clears the bar plus a gutter.

### Named Rules
**The One Sheet Rule.** The first viewport is one framed sheet: the real run as the main view and a title block holding the headline, the actions and the facts. Facts are title-block cells with a caps label over a value, never stat cards or large figures.

**The Region Opens Rule.** Every landing section, every docs h2 (its heading wrapper), every register head and every parts list starts under a `--rule-region` ink rule; rows inside are divided by `--rule-hair` in `--color-rule`.

**The Grid Behind the Drawing Rule.** The drafting grid (`--grid-pitch-minor` under `--grid-pitch-major`, in `--color-grid` and `--color-grid-major`) is drawn only behind the architecture drawing, through `DrawingFrame`'s `grid` option. Screenshots, listings and registers sit on plain paper.

## Elevation & Depth

Flat. Depth is line weight: hairline rows, region rules, frame borders. `--shadow-overlay` appears on the site only where something floats: the landing page's mobile Menu drawer, and Starlight's `--sl-shadow-md` and `--sl-shadow-lg` (search and menus). `--sl-shadow-sm` is `none`, the pagination links and the narrow-screen menu button carry no shadow, and Expressive Code's frames have their shadow removed.

### Shadow Vocabulary
- **Overlay** (`--shadow-overlay`): the Menu drawer and Starlight's floating surfaces. Nothing at rest.

### Named Rules
**The Floats Only Rule.** A shadow means "this floats above the sheet". Buttons, frames, registers, code blocks and pagination cards never carry one.

## Shapes

Square-cornered. Controls (buttons, the appearance switch, the copy button, the docs' GitHub link, search trigger, pagination links, sidebar entries, asides, the stamp, code blocks) take `--radius-sm`; overlays (the Menu drawer, the search dialog) `--radius-md`; inline code `--radius-xs`. Drawing frames, the hero sheet, the listing, the terminal, registers and recordings have no radius. `--radius-full` is only for balloons. Icons are the UI's own (`ui/src/lib/icons.ts`, copied into a generated JSON by `sync-docs.mjs`): a 16-unit grid, one 1.5 stroke, round caps, `currentColor`, hidden from assistive technology. The wordmark is a title-block corner drawn in the ink beside "OrkStack" in tracked caps.

## Components

### Buttons
- **Shape:** softened square corner (`--radius-sm`), a `--rule-hair` outline in `--color-rule-strong`, `calc(var(--spacing) * 10)` tall, semibold `--text-md`.
- **Default:** drawn on the sheet: paper ground, ink text; hover takes `--color-hover`.
- **Primary:** one per view, ink-filled with `--color-on-ink` text; hover fills with `--color-ink-secondary`. Used for "Get started" only.
- **Quiet:** the top bar's GitHub link and Menu summary: `* 8` tall, `--text-sm`, medium weight. The docs' GitHub link draws the same shape in its own rule.
- **Transitions:** colour, ground and border over `--duration-fast` on `--ease-out`.

### Navigation (top bar)
- **Style:** sticky, `--spacing-bar` tall on paper under a `--rule-region` ink rule. Wordmark, a hairline divider, then "Docs" and "Examples" in tracked caps, `--color-ink-muted`, ink on hover. The current place is ink with a `--rule-frame` blue underline at the bar's bottom edge.
- **End:** the quiet GitHub button and the appearance switch.
- **Mobile (below 640 px):** the places and GitHub fold into a `<details>` Menu that needs no JavaScript; its drawer is a sheet-ground overlay with an ink hairline border, `--radius-md`, `--shadow-overlay`, entering with `--animate-overlay-in` (none under reduced motion).
- **Docs:** `Header.astro` draws the same bar around Starlight's search; "Docs" is current (`aria-current="true"`), or "Examples" on an example page; below 50rem the places fold away and Starlight's menu holds the sidebar and the switch.

### Appearance switch
Three icon buttons (system, light, dark) in one `--rule-hair` outlined group, hairline dividers between, `* 7` square. The pressed one is ink-filled with on-ink icon. Hidden until its script runs. It replaces Starlight's theme select in the docs.

### Title block (signature)
A `<dl>` band under a `--rule-frame` ink rule: tracked-caps muted labels over semibold `--text-md` values, cells divided by ink hairlines. From 900 px one row, the first cell twice as wide; below, auto-fit cells of at least 9rem. A value may be a link (ink, `--color-rule-strong` underline) or a stamp. The landing page closes with one (Project, Status, License, Source, Docs) and every docs page closes with one (Source file, Status, License).

### Stamp
The UI's status stamp for a neutral state: sunken ground, `--rule-hair` outline, tracked caps, a small outlined square mark before the word. It carries only "Pre-release".

### Drawing frame (signature)
A `<figure>` in a `--rule-region` ink border on paper; at its top left a paper patch holding a mono `--text-xs` caption in `--color-ink-secondary`, sitting on the frame's own top band so it never covers the contents. Captions name what is drawn ("each-fan-out @ 0.3.0 · 3 tasks", "How a run flows · ARCHITECTURE.md"). An optional `after` slot holds a hairline-ruled note. In the hero its side borders are dropped inside the sheet's frame: region rules above and below it under 1024 px, a single `--rule-frame` rule below it from 1024 px.

### Architecture drawing (signature)
Inline SVG in the UI's drawing language: paper boxes with a `--rule-region` ink outline (non-scaling), the name in the sans semibold and a second line in the mono secondary ink, orthogonal ink edges with filled arrowheads, mono edge labels haloed in paper. Balloons (1 Orkflow, 2 Activity, 3 Orker) sit on box corners and key the boxes to the parts list beside it. Two drawings of one graph: down below 900 px, across from 900 px.

### Balloons and parts lists (signature)
A balloon is a circled figure: `--rule-region` ink ring on paper, bold tabular `--text-xs`. In the Orkflow listing the balloons stand in a margin column left of the code, each placed by its line number times the line height and one line tall, so adjacent balloons never touch. Each parts-list row repeats its balloon, a semibold key, a muted mono line reference and a "Reference" more-link.

### Code listing and code blocks
The listing is a real file from `examples/` rendered by Astro's `Code` with two Shiki themes, on sunken paper inside a `--rule-region` ink border. Docs code blocks are Expressive Code with the same two themes, a `--rule-hair` border in `--color-rule`, `--radius-sm`, tabs and terminal title bars on paper. Both themes come from `codeTheme()`: keys and keywords bold ink, values secondary ink, comments italic muted ink, punctuation muted ink, selection on the blue tint.

### Terminal
The quickstart's commands: a sunken-paper box in a `--rule-region` ink border under a paper bar holding "Shell" in caps and a "Copy all" button that appears only when the Clipboard API exists and announces its result through a polite live region. Each command is mono medium ink after a muted, unselectable prompt, with a muted mono comment line below.

### Registers
The UI's table: tracked-caps muted heads on sunken paper under a `--rule-region` ink rule, `--rule-hair` rows, cells in secondary ink, row heads semibold. Feature row heads are ink links underlined in `--color-rule-strong` that turn blue on hover. Docs markdown tables get the same treatment with no zebra striping. Below 640 px a row restacks into a grid.

### Docs surfaces
Sidebar group labels in tracked caps; links in secondary ink, hover wash; the current page semibold ink on the blue tint. Inline code on the hover wash with a faint hairline. Blockquotes take a `--color-rule-strong` hairline. Asides are sunken paper with a hairline; caution and danger take the red. The search trigger is a quiet outlined control; its dialog has a `--rule-region` ink border on sheet ground. Screenshots in docs take a `--rule-region` ink border and a muted caption.

## Do's and Don'ts

### Do:
- **Do** take every colour, face, size, radius, rule and duration from `ui/src/tokens.css` by name; change the UI's tokens, not the site, to change the look.
- **Do** open every landing section with a `--rule-region` ink rule and divide rows with `--rule-hair` in `--color-rule`.
- **Do** frame screenshots and drawings in `DrawingFrame` with a mono caption naming what is drawn.
- **Do** ship every screenshot and recording as a light and dark pair from `make site-media` (`ThemeShot`, `ThemeClip`), within the media budget `check-tokens.mjs` enforces (900 KiB an image, 2.5 MiB a video, 24 MiB in all), lazy, with alt text, and a caption on every recording.
- **Do** give recordings controls, a poster, `muted`, `playsinline` and `preload="none"`; a terminal recording sits at the top of a box 110 of its pixels taller, in the terminal's own `--color-paper-sunken`, so a paused video's controls band falls below its last line.
- **Do** keep motion to colour and ground transitions over `--duration-fast` on `--ease-out`, and stop the one entrance animation under `prefers-reduced-motion`.
- **Do** keep the landing page working with JavaScript off: commands stay selectable text, the Menu is a `<details>`, and the theme follows the system through the generated dark block.

### Don't:
- **Don't** write a hex, colour function or named colour, or a font stack, anywhere under `site/src`; derive literal values in `site/scripts/tokens.mjs` when a library cannot read `var()`.
- **Don't** colour code: keys, values and comments are ink, secondary ink and muted ink.
- **Don't** use green or red in the site's own drawing; they belong to screenshots and to Starlight's mapped asides.
- **Don't** draw the drafting grid behind anything but the architecture drawing.
- **Don't** put a shadow on anything that does not float.
- **Don't** autoplay a recording or fetch one before the visitor presses play.
- **Don't** patch Starlight's internals; restyle it through `--sl-*`, plain CSS over its markup and component overrides.
