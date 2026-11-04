---
name: OrkStack site
description: The landing page and docs, drawn as Harbour, the same world as the UI. Every colour and face is a token of ui/src/tokens.css; this file names tokens and never repeats their values.
colors:
  bg: "var(--bg)"
  surface: "var(--surface)"
  surface-2: "var(--surface-2)"
  hover: "var(--hover)"
  scrim: "var(--scrim)"
  ink: "var(--ink)"
  ink-2: "var(--ink-2)"
  ink-3: "var(--ink-3)"
  line: "var(--line)"
  line-2: "var(--line-2)"
  line-strong: "var(--line-strong)"
  navy: "var(--navy)"
  navy-tint: "var(--navy-tint)"
  navy-text: "var(--navy-text)"
  link: "var(--link)"
  focus: "var(--focus)"
  accent: "var(--accent)"
  accent-hover: "var(--accent-hover)"
  accent-edge: "var(--accent-edge)"
  on-accent: "var(--on-accent)"
  hero: "var(--hero)"
  hero-2: "var(--hero-2)"
  hero-ink: "var(--hero-ink)"
  hero-ink-2: "var(--hero-ink-2)"
  hero-line: "var(--hero-line)"
  side: "var(--side)"
  side-line: "var(--side-line)"
  side-ink: "var(--side-ink)"
  side-ink-2: "var(--side-ink-2)"
  side-strong: "var(--side-strong)"
  side-hover: "var(--side-hover)"
  side-bar: "var(--side-bar)"
  side-field: "var(--side-field)"
  side-field-line: "var(--side-field-line)"
  side-focus: "var(--side-focus)"
  act: "var(--act)"
  act-ink: "var(--act-ink)"
  run: "var(--run)"
  run-tint: "var(--run-tint)"
  ok: "var(--ok)"
  ok-mark: "var(--ok-mark)"
  ok-tint: "var(--ok-tint)"
  wait: "var(--wait)"
  wait-mark: "var(--wait-mark)"
  wait-tint: "var(--wait-tint)"
  fail: "var(--fail)"
  fail-tint: "var(--fail-tint)"
  pend-mark: "var(--pend-mark)"
  mark-ring: "var(--mark-ring)"
  mark-s: "var(--mark-s)"
  sb-thumb: "var(--sb-thumb)"
  sb-track: "var(--sb-track)"
typography:
  display:
    fontFamily: "var(--font-sans)"
    fontSize: "clamp(2.25rem, 4.2vw, 3.375rem)"
    fontWeight: 750
    lineHeight: 1.05
    letterSpacing: "-0.032em"
  headline:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-figure)"
    fontWeight: 750
    lineHeight: 1.2
    letterSpacing: "var(--tracking-tight)"
  section:
    fontFamily: "var(--font-sans)"
    fontSize: "1.875rem"
    fontWeight: 750
    lineHeight: 1.15
    letterSpacing: "var(--tracking-figure)"
  docs-title:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--sl-text-h1)"
    fontWeight: 750
    lineHeight: 1.12
    letterSpacing: "-0.025em"
  title:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-md)"
    fontWeight: "var(--font-weight-heading)"
  lede:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-lg)"
    fontWeight: "var(--font-weight-body)"
    lineHeight: 1.6
  body:
    fontFamily: "var(--font-sans)"
    fontSize: "1rem"
    fontWeight: "var(--font-weight-body)"
    lineHeight: 1.75
  dense:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-base)"
    fontWeight: "var(--font-weight-body)"
    lineHeight: 1.55
  label:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-sm)"
    fontWeight: "var(--font-weight-semibold)"
  code:
    fontFamily: "var(--font-mono)"
    fontSize: "var(--text-sm)"
    lineHeight: 1.7
  code-inline:
    fontFamily: "var(--font-mono)"
    fontSize: "var(--text-code)"
  caption:
    fontFamily: "var(--font-sans)"
    fontSize: "var(--text-dense)"
rounded:
  r: "var(--r)"
  r-sm: "var(--r-sm)"
  full: "var(--radius-full)"
  showcase: "14px"
  figure: "12px"
spacing:
  page: "1240px"
  gutter: "24px"
  band: "56px"
  head: "64px"
components:
  button:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.r-sm}"
    height: "34px"
    padding: "0 14px"
  button-hover:
    backgroundColor: "{colors.hover}"
  button-primary:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.on-accent}"
    rounded: "{rounded.r-sm}"
    height: "44px"
    padding: "0 20px"
  button-primary-hover:
    backgroundColor: "{colors.accent-hover}"
  button-shell:
    backgroundColor: "transparent"
    textColor: "{colors.side-strong}"
    rounded: "{rounded.r-sm}"
    height: "34px"
    padding: "0 14px"
  button-shell-hover:
    backgroundColor: "{colors.side-hover}"
  site-head:
    backgroundColor: "{colors.side}"
    textColor: "{colors.side-strong}"
    height: "{spacing.head}"
  site-head-on-hero:
    backgroundColor: "{colors.hero}"
    textColor: "{colors.hero-ink}"
    height: "{spacing.head}"
  hero:
    backgroundColor: "{colors.hero}"
    textColor: "{colors.hero-ink}"
  closing-band:
    backgroundColor: "{colors.hero}"
    textColor: "{colors.hero-ink}"
    rounded: "{rounded.r}"
    padding: "28px 32px"
  card:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.r}"
  run-card:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.showcase}"
  feature:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink-2}"
    typography: "{typography.dense}"
    padding: "16px 18px"
  plate-key:
    backgroundColor: "{colors.surface-2}"
    textColor: "{colors.ink-3}"
    typography: "{typography.label}"
    padding: "6px 8px"
  plate-value:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    typography: "{typography.code}"
    padding: "6px 8px"
  status-running:
    backgroundColor: "{colors.run-tint}"
    textColor: "{colors.run}"
    rounded: "{rounded.full}"
    height: "24px"
    padding: "0 10px 0 8px"
  tag:
    backgroundColor: "{colors.navy-tint}"
    textColor: "{colors.navy-text}"
    rounded: "{rounded.full}"
    height: "22px"
    padding: "0 8px"
  search:
    backgroundColor: "{colors.side-field}"
    textColor: "{colors.side-strong}"
    rounded: "{rounded.r-sm}"
    height: "34px"
    padding: "0 10px"
  tooltip:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.surface}"
    rounded: "{rounded.r-sm}"
    padding: "8px 10px"
  setting:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.r}"
  setting-header:
    backgroundColor: "{colors.surface-2}"
    textColor: "{colors.ink}"
    padding: "10px 14px"
  docs-current-page:
    backgroundColor: "{colors.act}"
    textColor: "{colors.act-ink}"
    rounded: "{rounded.r-sm}"
  footer:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink-3}"
    typography: "{typography.dense}"
---

# Design System: OrkStack site

The site is the UI's own world. `ui/src/tokens.css` holds every colour and face it draws with, in both themes, and `ui/DESIGN.md` holds the reasons ("The Harbour Chart"). `site/src/styles/site.css` imports `tokens.css` as it is; nothing under `site/src` repeats a value from it. This file records what is particular to the site: how its pages are built from those tokens, the Starlight mapping, theme, media and motion behaviour, the phone rules, and the checks that keep the site from drifting. The frontmatter names tokens on purpose; a literal colour here would be a second copy of `tokens.css`. The only literals in it are the site's own measures, which no token holds.

## Overview

**Creative North Star: "The Harbour Chart, Opened to Visitors"**

A visitor arrives at the same chart an operator works on: a calm slate ground, white cards, harbour navy, and one container orange. The landing page opens on a navy hero whose right half is a run card, a piece of the product drawn with the product's own primitives: plates, a status pill, real YAML read from `examples/config-from-git.ork.yaml`, and a task map of a run of it. A navy closing band answers the hero at the foot of the page. Between them sit a facts band, three screenshot blocks, the features in one card, three recording slots, the filesystem contract and the deliberate limits.

The docs are Starlight, restyled by custom properties, `customCss` and component overrides, never by patching its internals. They carry the same head, the same search field, the same buttons, the same foot, and a sidebar of ten sections. Reference material earns its own form: each configuration setting is an entry card, filterable on one page.

Density sits between the two surfaces it joins. The landing page speaks at reading sizes, and the UI's dense scale returns wherever the product itself is shown. Sentence case throughout. The mono is for what a person copies or compares. Depth is one card shadow and one overlay shadow. Almost nothing moves.

**Key Characteristics:**
- No colour or face of its own: tokens by name, enforced by `site/scripts/check-tokens.mjs`.
- One orange button on the landing page: Get started.
- A navy hero and a navy closing band, both on `--hero`; everything between is on `--bg`.
- The UI's primitives as they are: plates, status pills, tags, the task map, the one tooltip.
- The features in one card divided by hairlines.
- Ten docs sections; setting entries in place of a settings table.
- No theme switch: the site follows the system.

## Colors

Slate neutrals and navy carry almost everything; orange is rationed to one button and the mark; status hues appear only where a status is drawn.

### Primary
- **Harbour Navy** (`--navy`, in practice through its roles `--hero`, `--act`, `--navy-tint`, `--navy-text`, `--link`): the site reads the brand through these roles, not through `--navy` itself. `--hero` is the hero, the landing page's header and the closing band. `--act` with `--act-ink` is the docs sidebar's current page. `--navy-tint` with `--navy-text` is tags, numbered step badges, generated-page pills and text selection. `--navy-text` alone colours the two-pixel bar on the current contents entry and the square list bullets. `--link` is every prose link and Starlight's accent.
- **Focus** (`--focus`): the `:focus-visible` outline, `--rule-frame` wide and offset by the same. A band redefines it locally: on navy it is `--hero-ink`, in the head `--side-focus`, inside the run card `--ink`.

### Secondary
- **Container Orange** (`--accent`, `--accent-hover`, `--accent-edge`, `--on-accent`): the landing page's Get started button, and nothing else on the page but the S of the mark.

### Tertiary
- **Status hues** (`--run` and `--run-tint`, `--ok`, `--ok-mark`, `--ok-tint`, `--wait`, `--wait-mark`, `--wait-tint`, `--fail`, `--fail-tint`, `--pend-mark`): the run card's Running pill and task map, and the docs' asides and badges through Starlight's named hues. Starlight's red maps to fail, orange to wait, green to ok, blue and purple to the navy roles, so a note or tip takes no hue of its own.

### Neutral
- **Slate Ground** (`--bg`): the page, on both surfaces, and the docs sidebar.
- **White Card** (`--surface`): cards, buttons, plate values, the run card, setting entries, pagination links, code blocks' editor ground, the foot.
- **Sunken Slate** (`--surface-2`): plate keys, the run card's YAML, inline code, setting headers, code tab bars and terminal title bars, posters awaiting a recording.
- **Hover Wash** (`--hover`): the hover ground of buttons, plate buttons and sidebar entries.
- **Ink** (`--ink`, `--ink-2`, `--ink-3`): text in three steps: headings and values; prose under a heading; captions, table heads, breadcrumbs and `dt`s.
- **Lines** (`--line`, `--line-2`, `--line-strong`): `--line` for hairlines between things, `--line-2` for a control's edge, `--line-strong` for a hovered control, table heads and link underlines.
- **Night Ramp** (`--hero-2`, `--hero-ink`, `--hero-ink-2`, `--hero-line`): what stands on navy: the install plate's ground, words, secondary words, and the hero's dotted ground.
- **Head Roles** (`--side`, `--side-line`, `--side-ink`, `--side-ink-2`, `--side-strong`, `--side-hover`, `--side-bar`, `--side-field`, `--side-field-line`): the docs head draws from these directly.
- **Scrollbars** (`--sb-thumb`, `--sb-track`): every scrolling panel, under `forced-colors: none`.
- **Scrim** (`--scrim`): behind Starlight's search dialog.

### Named Rules
**The One Orange Button Rule.** The landing page has exactly one `.btn.primary`: Get started in the hero. The closing band's Get started and the docs carry none. `site/scripts/check-site.mjs` fails the build otherwise. The mark's orange is the mark, and doesn't count.

**The No Copy Rule.** The site declares no colour. `check-tokens.mjs`, run by the `site` gate, refuses under `site/src` and in `astro.config.mjs` a hex colour, a colour function, a named colour in a colour property or attribute, and a `var()` naming a custom property that neither `tokens.css`, the site nor Starlight, Expressive Code or Shiki declares. `color-mix()` of tokens is allowed. Literal values a library needs are derived at build time from `tokens.css` by `site/scripts/tokens.mjs`: the no-JavaScript dark block and the code themes.

**The Local Reroute Rule.** A navy band is not a new palette. `.hero` and `.site-head.on-hero` point the `--side-*` roles at the `--hero-*` ramp, and the closing band redefines the few it uses, so `.btn.shell` and its focus ring work unchanged on navy.

**The Status Only Rule.** A status hue describes a status. Nothing decorative takes `--run`, `--ok`, `--wait` or `--fail`.

## Typography

**Body Font:** `--font-sans` (Inter, self-hosted)
**Label/Mono Font:** `--font-mono` (JetBrains Mono, self-hosted)

**Character:** The UI's pairing, unchanged, with the same character variants on (`cv11`, `ss03`). Both come from `@fontsource-variable` packages at exactly the UI's versions, which the check enforces. The mono is for code, commands and plate values.

### Hierarchy
- **Display** (750, `clamp(2.25rem, 4.2vw, 3.375rem)`, 1.05, -0.032em, at most 15ch): the landing page's one h1, on navy.
- **Headline** (750, `--text-figure`, 1.2, `--tracking-tight`): the h2 of the tour blocks, the features and the recordings.
- **Section** (750, 1.875rem, 1.15, `--tracking-figure`): the h2 of the two-column sections (files in, files out; the limits). The closing band's h2 is smaller, `--text-xl`.
- **Docs Title** (750, `--sl-text-h1`, 2.25rem falling to 1.875rem below 50em, 1.12, -0.025em): one h1 per docs page, under a breadcrumb. Docs h2 is 700 at 1.375rem; h3 is `--font-weight-heading` at 1.0625rem.
- **Title** (`--font-weight-heading`, `--text-md`): a feature's h3.
- **Lede** (`--text-lg`, 1.6): the hero's paragraph, at most 46ch in `--hero-ink-2`, and the first paragraph of a docs page, at most 56ch in `--ink-2`.
- **Body** (1rem, 1.75, at most 60ch): docs prose and list items. Landing prose sits at 1.03125rem, 1.65, in `--ink-2`, at most 46ch in a tour block and 54ch in a section.
- **Dense** (`--text-base`, 1.55): a feature's sentence. Docs tables set at `--text-md`.
- **Label** (600, `--text-sm`): table heads, plate keys, tags, pills and the facts band's `dt`, all in sentence case.
- **Code** (`--font-mono`, `--text-sm`, 1.7): docs code blocks. The run card's YAML steps down to `--text-xs`. Inline code in prose is `--text-code` of its context on `--surface-2` with a hairline.
- **Caption** (`--text-dense`, `--ink-3`): screenshot and figure captions.

### Named Rules
**The Sentence Case Rule.** No tracked capitals and no `text-transform` anywhere on the site. Labels are sentence case at weight 600, as in the UI.

**The Two Faces Rule.** A `font-family` under `site/src` is `var(--font-sans)`, `var(--font-mono)` or `inherit`, and a `font` shorthand names a face token. The check refuses anything else.

## Layout

**The page.** The landing page's content, head and foot share one column: at most 1240px, centred, with `max(24px, env(safe-area-inset-*))` at the sides. Bands stand about 56px apart. The order is fixed: head, hero, facts band, tour, features, recordings, files in and out, limits, closing band, foot.

**The hero.** A two-column grid (0.9 : 1.25, 56px gap) over a dotted ground, `--hero-line` dots on an 18px pitch: headline, lede, the two buttons and the install plate on the left, the run card on the right. The facts band overlaps its foot by 28px, so the hero and the page are joined by a card.

**The middle.** Tour blocks alternate text and screenshot (0.8 : 1.2, every second one flipped). Features are a 3-column grid inside one card. Recordings are three equal columns. Sections are text beside a card of numbered steps or limits (0.9 : 1.1).

**Breakpoints in use.** 1180px: facts become three columns, features two. 900px: the hero, the run card's split, tour blocks, sections and recordings go to one column; facts to two, and the facts band stops overlapping the hero. 820px: the header's links and search fold behind Menu. 640px: the phone rules below; features one column. 480px: a setting's `dl` stacks. 374px: GitHub leaves the head.

**The docs.** Starlight's layout with the head at `--sl-nav-height` (4rem, the landing head's 64px), a 17rem sidebar, a content column of 53.75rem and contents of 13.75rem from 72rem up. Prose stops at 60ch; code, tables and figures take the rest of the column. Anything a link lands on clears the sticky head through `scroll-padding-top`, and clears the settings filter as well where one stands.

**The ten sections.** The sidebar lists, in order: Get started, Build Orkflows, Activities, Run and observe, Operate, Deploy, Configuration, Examples, Reference, Concepts, from `site/docs.manifest.json`. All start collapsed and the current one opens. The docs home repeats them as ten cards in two columns. `check-site.mjs` holds the set and its order. A further group, Contribute, is hidden from the sidebar and reached from the foot.

**The phone rules (640px and below, unless stated).**
- Every link and control a thumb uses is at least 40px tall: buttons, the brand, plate copy buttons (40px square), foot links, feature and limit links, breadcrumbs, docs home links, generated-page pills, the search trigger, a setting's More detail. Sidebar entries get 10px of block padding below 50em.
- The side padding respects the safe area on the head, hero, page and foot.
- Below 820px on the landing page and 50em in the docs the places fold behind the word Menu. On the landing page it opens the links under the head, moves focus to the first, and Escape closes it and returns focus. In the docs it is Starlight's sidebar popover, holding the page still, hiding the foot, and carrying the GitHub link in its own foot. Below 374px the head's GitHub button goes; the hero and the foot still hold the link.
- The install plate wraps its whole command in the hero and never ellipsises it. The run card's YAML wraps too, so it takes no tab stop.
- A docs table scrolls inside its own wrapper, which becomes a named region with a tab stop only when it really scrolls. Its last column keeps at least 14rem, and code in its first column stays whole.
- Where nothing hovers, a code block's copy button stays visible in a 2.75rem cell of its own, and the lines scroll out from under it.
- The settings filter stops sticking, and below 480px a setting's `dt` and `dd` stack.

### Named Rules
**The One Column Rule.** Head, hero content, page and foot line up on one 1240px measure with the same side padding. The docs foot is the exception: it runs beside the fixed sidebar and clear of the contents panel.

## Elevation & Depth

A hybrid: tonal layers and hairlines do most of the work, with two shadows from the tokens. A card sits on the ground with `--shadow`. Something that floats takes `--shadow-lg`. Hover changes a border or a ground, never a shadow.

### Shadow Vocabulary
- **Card** (`--shadow`): cards, default buttons, screenshots, setting entries, the settings filter, pagination links, code frames. `.btn.shell` drops it, since a navy band has no surface to lift from.
- **Overlay** (`--shadow-lg`): the run card standing on the hero, the tooltip, the search dialog, the landing page's open phone menu.

### Named Rules
**The Two Shadows Rule.** There are exactly two shadows, both from the tokens. Starlight's three map onto them.

## Shapes

Softly rounded. Cards and panels take `--r`; controls, plates and sidebar entries take `--r-sm`; pills and tags take `--radius-full`. Two media frames are larger: the run card (14px) and screenshots and docs figures (12px). Diagrams carry no frame. The feature card is a single silhouette: its cells have no corners of their own.

**Status marks are shapes first.** A running mark is a circle, a succeeded one a filled square, a waiting one a ring, a not-yet-started one an outlined square, a cached one a double square. Each still reads when hue is gone, and forced colours keep shape and fill.

**Icons** are the UI's own (`ui/src/lib/icons.ts`, read into a generated JSON by `sync-docs.mjs`): a 16-unit grid, a 1.4 stroke, `currentColor`, hidden from assistive technology. Only the recording icon and the filled GitHub mark are the site's own.

## Components

### Buttons
- **Shape:** `--r-sm`, 34px tall, 600 at `--text-dense`, a hairline in `--line-2`, `--shadow`. The hit area extends 3px above and below the box.
- **Default:** `--surface` and `--ink`; hover takes `--hover` and `--line-strong`.
- **Primary:** `--accent` with an `--accent-edge` edge and `--on-accent` words; hover takes `--accent-hover`. One per landing page.
- **Shell:** transparent with a `--side-field-line` edge, for the head and the navy bands; hover takes `--side-hover`.
- **Small and hero sizes:** 30px at `--text-sm` for Menu, 44px at `--text-md` in the hero.
- **Skip link:** a default button off-screen until focused.

### Head
- **Landing:** `--hero`, with the mark on navy, three places (Docs, Examples, Reference), a search field that hands its words to the docs' search, and the GitHub shell button.
- **Docs:** `--side`, with the mark in colour, the same three places and Starlight's own search drawn as that same field.
- **States:** a place is `--side-ink`, `--side-strong` on hover, and when current 650 with a two-pixel `--side-bar` underline. Forced colours underline it instead.

### The Strata mark (signature)
Three bars joined by round turns inside a ring, with a dot where each end meets the ring. It is drawn in three ways by `Mark.astro`, at 26px by default.
- **Colour:** ring `--mark-ring`, S `--mark-s`, following the theme. The docs head.
- **One colour:** `currentColor`, for any single-colour use (also `public/logo/mark-one-colour.svg`). No page draws it today.
- **On navy:** `public/logo/mark-colour-on-navy.svg`, white and orange in every theme, since no token holds those colours as a pair. The landing head.

### Run card (signature)
The hero's right half: a `--surface` card with `--shadow-lg`. Its bar holds a run plate, the example's name, a version plate and a Running pill pushed to the end. Below, a split (1.45 : 0.75): the example's `render` task on `--surface-2`, set by Shiki in the UI's syntax tokens, then the task map. Only the run's state is illustrative; the YAML is read from the file at build time.

### Task map (signature)
The UI's map of a run. A row per task: its name in `--ink-2`, then one 14px cell per item, wrapping. Cell shapes as in Shapes, with `--ok-mark`, `--run`, `--wait-mark` and `--pend-mark`. The whole map is one `role="img"` whose label says what its marks say.

### Plates and status pills
- **Plate:** the UI's plate for run ids, versions and commands: a `--surface-2` key beside a mono value, parted by a hairline, in a `--line-2` outline at `--r-sm`. `.sm` for the foot's version. A copying plate adds a button once the Clipboard API exists, showing a check for two seconds while a live region says Copied. The hero's install plate is drawn in the night ramp.
- **Status pill:** an 8px mark and a word on the status tint, `--radius-full`, 24px. Running is the one the site draws.
- **Tag:** `--navy-tint` and `--navy-text` at 22px: a setting's `ork` and `orker`.
- **Tooltip:** the UI's one element, used by every `data-tip` control: `--ink` ground, `--surface` words, `--shadow-lg`. No native `title` on the landing page, which `check-site.mjs` holds.

### Facts band
A `dl` in one card overlapping the hero: five cells parted by hairlines, each an `--ink-3` `dt` over a 700 `dd`. Five columns, then three, then two.

### Tour blocks
Three blocks of text beside a real screenshot of the UI from `make site-media`: a headline, one paragraph, three short points with square `--navy-text` bullets. The `figure` is a `--surface` frame at 12px with a captioned foot.

### Features card (signature)
Twelve features in one card. The grid's gap is 1px and its ground is `--line`, so the rules are the card's own colour showing through and the cells stay `--surface`. Each is a linked h3 and one sentence, and each links the docs page that holds the detail. Forced colours draw real borders.

### Video slots (signature)
Three recordings of the real UI, each a slot in a 16 : 10 box on `--surface-2`.
- **Until recorded:** the poster, a `--radius-full` badge saying "Recording planned", its length and that captions are planned, and a heading paragraph that says the same. All three are in this state today.
- **Once recorded:** when `make site-media` names its files in the manifest, the slot is a plain player in both themes, with no change to the component: `controls`, `playsinline`, `preload="none"`, a poster and captions. The paragraph changes with it. `check-site.mjs` holds three slots, each either planned or a player, and that no video anywhere autoplays.
- **In the docs:** recordings are already players, framed at 12px and shown in a figure with a caption.

### Numbered steps and limits
A card of rows parted by hairlines. Steps carry a 28px `--navy-tint` numeral; limits carry none, and each links the page that states it.

### Closing band
A `.card` on `--hero`, inside the page's column, not full-bleed: a heading and the real start command on the left, two shell buttons on the right. No orange here: the hero already has the one.

### Foot
A `--surface` band on every page: a version plate and the license, the links, then one line saying what it is built on.

### Docs surfaces
- **Mapping:** `--sl-*` custom properties mapped once in `starlight.css`, theme-agnostically, because `tokens.css` switches every token on `data-theme`. Plain unlayered rules over Starlight's public markup win over its cascade layers without `!important`. Nine component overrides: PageFrame, Header, SiteTitle, ThemeProvider, ThemeSelect, PageTitle, Footer, MobileMenuToggle, MobileMenuFooter.
- **Sidebar:** group labels in `--ink-3` at 650; entries in `--ink-2`, `--hover` on hover; the current page 650 in `--act-ink` on `--act`.
- **Title:** a breadcrumb naming the section above the h1.
- **Contents:** a column of `--line` ticks; the current entry's tick is `--navy-text` and its words 650 in `--ink`.
- **Tables:** no grid, no zebra, sentence-case heads over a `--line-strong` hairline, `--line` hairlines between rows.
- **Code blocks:** Expressive Code with two themes generated from the UI's syntax tokens, framed in `--line` at `--r` with `--shadow`, and no window dots on a command. Its contrast adjustment is off, because the tokens already meet the floor and an adjusted colour would no longer be the UI's. A `{{ … }}` template in YAML is coloured as in the UI.
- **Home:** ten section cards, then pills linking the generated references.
- **Generated pages:** a pill saying "Generated from the source, always current".
- **Page links:** pagination cards at `--r` with the title in `--link`.

### Setting entries (signature)
Each configuration setting is one card, generated from `docs/DEPLOY.md`'s table. Its header, on `--surface-2`, holds the key in the mono at `--text-base` and 600, with `ork` and `orker` tags. Below, a `dl` of Environment, Default, Accepts and Checked with `--ink-3` terms, then the lead sentence at `--text-md`, capped at the reading measure, then a More detail disclosure when there is more to say. The ring on a disclosure stays inside the card, which clips.

On All settings, a filter card stands above the entries once its script runs, sticky under the head: a labelled search field and a live count.

## Do's and Don'ts

### Do:
- **Do** take every colour and face from `ui/src/tokens.css` by name; change the UI's tokens, not the site, to change the look.
- **Do** build with the UI's primitives as they are: `.btn`, `.card`, `.plate`, `.st`, `.tag`, `.search`, `.tt`.
- **Do** keep the one orange button on the landing page's Get started.
- **Do** point `--side-*` at the `--hero-*` ramp on any navy band, rather than writing new colours for it.
- **Do** follow the system's theme: `ThemeScript.astro` sets `data-theme` before the first paint and follows a change. With JavaScript off the landing page follows the system through the generated dark block, and Starlight's server-rendered docs stay dark.
- **Do** ship screenshots and recordings as light and dark pairs from `make site-media`, one shown per theme by `.theme-pair`, lazy, with alt text, within the media budget `check-tokens.mjs` enforces.
- **Do** keep motion to feedback: a button's ground and border change over `--dur-instant` on `--ease-out`, and the tooltip waits before it shows and hides. That is all the site moves. The global `prefers-reduced-motion` rule in `base.css` still clamps every transition and animation, Starlight's included.
- **Do** give every status a shape and a word as well as a hue.
- **Do** hold the phone rules: 40px targets, safe-area padding, code that wraps or scrolls in its own box.
- **Do** read the example from its file and the commands from the README's Quickstart, so the page shows the real thing.
- **Do** keep forced colours working: the primary button gets a real border, current places an underline, marks an outline.

### Don't:
- **Don't** write a hex, colour function or named colour, or a font stack, anywhere under `site/src`; derive literal values in `site/scripts/tokens.mjs` when a library cannot read `var()`.
- **Don't** add a second `.btn.primary` to the landing page, or use orange for anything but that button and the mark.
- **Don't** add a theme switch; `ThemeSelect.astro` is empty on purpose.
- **Don't** set tracked capitals; labels are sentence case.
- **Don't** put a status hue on anything that isn't a status.
- **Don't** change a shadow on hover, or invent a third shadow.
- **Don't** autoplay a recording, or fetch one before the visitor presses play.
- **Don't** patch Starlight's internals; restyle it through `--sl-*`, plain CSS over its public markup and component overrides.
- **Don't** split the features into separate cards.
