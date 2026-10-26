# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users
Platform and infrastructure engineers who already want durable, multi-step workflows without writing or deploying workflow code, arriving from a link, a search or the repository. Two jobs, in order: decide whether OrkStack's model fits (the landing page), then learn and look things up while they adopt it (the docs). They read code first and copy commands; they distrust claims they cannot check.

## Product Purpose
The site is OrkStack's front door and its manual. The landing page makes the model intelligible in one viewport (an Orkflow in YAML, every step an OCI image, generic Orkers, durability from Temporal) and proves it with a real run of a real example. The docs are written for people: short, task-first pages that lead with an example, each citing the repository docs it summarises, which stay the full reference. Success is an engineer who leaves the landing page knowing what OrkStack is and is not, and who finds any answer in the docs without reading the source.

The product itself is described in `ui/PRODUCT.md`; this file covers the site only.

## Positioning
OrkStack is described on its own terms. The site never names, compares to or positions against any other workflow, orchestration or CI tool ("unlike", "alternative to", "replaces", "better than" are banned). What it can show that no template can: a real run of a real Orkflow from `examples/`, the filesystem contract, the proof discipline, and its deliberate omissions.

## Operating Context
- The landing page is fully static and works without JavaScript; in the docs only search needs it.
- Content is checked against the repository: each page written for the site (`site/content/`) cites the repository docs it summarises, and the sync refuses a citation that names no real file or heading; the generated references (orkctl, configuration, error reasons, API) are ported as they are; the examples come from `examples/`, and the screenshots and recordings from `make site-media` against the real stack.
- Deployed to GitHub Pages at https://orkstack.github.io/orkstack/ on every push to `main`.

## Capabilities and Constraints
- Astro 7 with Starlight for the docs (restyled through custom properties, `customCss` and component overrides only, never its internals) and Landpack's structure for the landing page.
- The whole visual style comes from `ui/src/tokens.css`, imported as it is; `site/scripts/check-tokens.mjs` refuses any colour or face written anywhere else. A change to the UI's look changes the tokens, not the site.
- Vocabulary is the product's (AGENTS.md §7): Orkflow, Orker, pool, pin, release; no internal mechanism names in landing copy.

## Brand Commitments
The name OrkStack and the words Orkflow, Orker, `ork` and `orkctl` are fixed. The product is pre-release: "stable, but breaking changes may be introduced until 1.0", and the site says so plainly. It never claims production readiness, exactly-once execution or Temporal Cloud support. No logo exists beyond the UI's wordmark.

## Evidence on Hand
- Real behaviour: the example Orkflows in `examples/`, the UI and CLI on the local stack (captured by `make site-media`).
- The proof discipline: AGENTS.md §11, `docs/PROOFS.md`, `scripts/proof-section11-coverage.tsv`.
- Absences the site must not fabricate: customers, testimonials, benchmarks, adoption numbers, pricing, a hosted offering, a release (there is none yet).

## Product Principles
- **Show the real thing.** Real YAML read from the file, real runs, real commands that were run.
- **Say what it deliberately does not do.** The limits build trust with this audience.
- **One world.** The site is the UI's drawing office, not a marketing skin over it.
- **Static and light.** No client framework, no autoplay, a media budget.

## Accessibility & Inclusion
Readable in both themes at WCAG AA contrast (the UI's token contrast floors apply), keyboard reachable, no motion that `prefers-reduced-motion` does not stop, alt text on every image and a caption on every recording.
