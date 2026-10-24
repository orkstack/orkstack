import { RunOriginKind, RunOriginKindSchema, type RunOrigin } from './gen/orkstack/v1/run_pb';

// The user-facing spelling of an origin kind in a rendered row: the proto
// value's name after RUN_ORIGIN_KIND_, lowercased, "_" as "-" — "submit",
// "trigger", "schedule", "parent-run", "run-again". It is the one rule
// pkg/artifacts.OriginKindSlug and cmd/orkctl's originKindSlug apply too, so
// the CLI's ORIGIN column, the archive key and this page all spell a kind the
// same way. A filter names a kind by its enum short name (origin.kind=TRIGGER,
// docs/UI_CLI_PARITY.md "Filtering lists"), which also accepts this spelling.
const ORIGIN_KIND_PREFIX = 'RUN_ORIGIN_KIND_';

/** originKindSlug spells kind; UNSPECIFIED (and an undeclared number) is ''. */
export function originKindSlug(kind: RunOriginKind): string {
	if (kind === RunOriginKind.UNSPECIFIED) return '';
	const value = RunOriginKindSchema.values.find((candidate) => candidate.number === kind);
	if (!value) return '';
	return value.name.slice(ORIGIN_KIND_PREFIX.length).toLowerCase().replaceAll('_', '-');
}

/** UNRECORDED is what a run recorded before origins existed renders as. */
export const UNRECORDED = 'unrecorded';

/**
 * originLabel renders an origin in one short phrase — "trigger trg-…",
 * "submit", "run-again ork-…" — or "unrecorded" for a run with none. Never
 * "submit" for a missing origin: nothing supports that claim.
 */
export function originLabel(origin?: RunOrigin): string {
	const slug = origin ? originKindSlug(origin.kind) : '';
	if (!slug) return UNRECORDED;
	return origin?.id ? `${slug} ${origin.id}` : slug;
}

/**
 * originReferenceURL returns the reference as a URL only when it provably is
 * an http(s) one. A reference is caller-supplied text; anything else is shown
 * as text and never interpreted.
 */
export function originReferenceURL(reference: string): string | null {
	if (!reference) return null;
	try {
		const url = new URL(reference);
		return url.protocol === 'http:' || url.protocol === 'https:' ? url.href : null;
	} catch {
		return null;
	}
}

/**
 * originLink is where the origin's id leads on this UI, or null when it leads
 * nowhere: a trigger opens the orkflow's Triggers tab and a schedule its
 * Schedules tab — where the row the description names lives — and a source or
 * parent run opens that run. The namespace is applied by the caller.
 */
export function originLinkPath(origin: RunOrigin | undefined, orkflowName: string): string | null {
	if (!origin || !origin.id) return null;
	switch (origin.kind) {
		case RunOriginKind.TRIGGER:
			return orkflowName ? `/orkflows/${encodeURIComponent(orkflowName)}?tab=triggers` : null;
		case RunOriginKind.SCHEDULE:
			return orkflowName ? `/orkflows/${encodeURIComponent(orkflowName)}?tab=schedules` : null;
		case RunOriginKind.RUN_AGAIN:
		case RunOriginKind.PARENT_RUN:
			return `/runs/${encodeURIComponent(origin.id)}`;
		default:
			return null;
	}
}

/** The caption every rendering of a principal carries. */
export const PRINCIPAL_CAPTION =
	"Ork-Principal as asserted by the deployment's gateway; OrkStack records it and does not authenticate it.";

/** The caption on an unrecorded origin. */
export const UNRECORDED_CAPTION = 'This run started before origins were recorded.';
