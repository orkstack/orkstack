import type { AuditRecord } from './gen/orkstack/v1/audit_pb';
import { listHref, pageStackKey } from './browsing';
import { FilterOperator, equalityOf, filterFlags, type FieldFilter } from './listFilter';

// The Admin → Audit page's pure half: the page's address, what a row reads
// as, and the CLI command for the same listing. Kept out of the component so
// each rule is unit-tested once (test/audit.test.ts) and the page and the
// command can never disagree.
//
// What the trail may be filtered on is not written here: it is the
// ListAuditRecordsRequest allowlist, offered by the one filter component
// (lib/listFilter.ts, components/ListFilter.svelte) and decided by the engine
// (docs/UI_CLI_PARITY.md "Filtering lists"). The trail is a deployment-wide
// listing: a `namespace` filter narrows it like any other property, and
// `namespace=` (the empty value) selects the deployment-wide writes.

/** The caption every rendering carries, empty page included (spec: Attribution). */
export const AUDIT_CAPTION =
	"principal is the identity the deployment's gateway asserted in Ork-Principal; OrkStack records it and does not authenticate it.";

/** An empty principal is an honest gap, never a blank that could pass for a name. */
export const UNATTRIBUTED = 'unattributed';

export const DEFAULT_AUDIT_LIMIT = 50;
export const MAX_AUDIT_LIMIT = 200;

export interface AuditCoordinates {
	/** The switcher's namespace, preserved in every address; it does NOT filter the trail. */
	ns: string;
	filters: FieldFilter[];
	limit: number;
	cursor: string;
	page: number;
}

/** limit from the URL: 1..200, anything else is the default. */
export function auditLimit(value: string | null): number {
	const parsed = Number(value ?? '');
	return Number.isInteger(parsed) && parsed >= 1 && parsed <= MAX_AUDIT_LIMIT ? parsed : DEFAULT_AUDIT_LIMIT;
}

export function auditPrincipalLabel(principal: string): string {
	return principal || UNATTRIBUTED;
}

export function auditResultLabel(rec: Pick<AuditRecord, 'succeeded' | 'errorCode'>): string {
	return rec.succeeded ? 'ok' : `refused ${rec.errorCode || '-'}`;
}

function coordinatesOf(c: AuditCoordinates): Record<string, string> {
	return {
		ns: c.ns || 'default',
		limit: c.limit === DEFAULT_AUDIT_LIMIT ? '' : String(c.limit),
		cursor: c.cursor,
		// A page number without a cursor names no page the engine could return.
		page: c.cursor ? String(c.page) : ''
	};
}

/** The page's address — listHref's rules (lib/browsing.ts), the filters as repeated `filter` keys. */
export function auditHref(c: AuditCoordinates, changes: Partial<AuditCoordinates> = {}): string {
	const next = { ...c, ...changes };
	return listHref('/admin/audit', coordinatesOf(next), { filter: next.filters });
}

/** The sessionStorage key for this filter set's cursor stack: the address without cursor and page. */
export function auditStackKey(c: AuditCoordinates): string {
	return pageStackKey('/admin/audit', coordinatesOf(c), { filter: c.filters });
}

/** `orkctl admin audit …` for the same listing (first page), every value shell-quoted — never -n. */
export function auditCliCommand(c: Pick<AuditCoordinates, 'filters' | 'limit'>): string {
	const parts = ['orkctl', 'admin', 'audit', ...filterFlags(c.filters)];
	if (c.limit !== DEFAULT_AUDIT_LIMIT) parts.push('--limit', String(c.limit));
	return parts.join(' ');
}

/** The scope sentence: every namespace, only the deployment-wide writes, one namespace, or a narrowing. */
export function auditScopeSentence(filters: FieldFilter[]): string {
	if (!filters.some((f) => f.field === 'namespace')) {
		return 'Showing every namespace, deployment-wide writes included. The namespace selector does not filter this page; a namespace filter does.';
	}
	const only = filters.filter((f) => f.field === 'namespace');
	if (only.length === 1 && only[0].values.length === 1 && only[0].values[0] === '' && only[0].op === FilterOperator.EQUALS) {
		return 'Showing only deployment-wide writes (retention, image policy, webhooks, releases, Orker cordons), which carry no namespace.';
	}
	const one = equalityOf(filters, 'namespace');
	return one
		? `Showing namespace ${one}. Deployment-wide writes carry no namespace; the filter namespace= selects them.`
		: 'Showing the namespaces the namespace filter selects.';
}
