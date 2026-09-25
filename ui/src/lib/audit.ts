import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import type { AuditRecord } from './gen/orkstack/v1/audit_pb';
import { shellQuote } from './runDetail';

// The Admin → Audit page's pure half: what the URL says, what the request is,
// what a row reads as, and the CLI command for the same listing. Kept out of
// the component so each rule is unit-tested once (test/audit.test.ts) and the
// page and the command can never disagree about a filter.

/** The caption every rendering carries, empty page included (spec: Attribution). */
export const AUDIT_CAPTION =
	"principal is the identity the deployment's gateway asserted in Ork-Principal; OrkStack records it and does not authenticate it.";

/** An empty principal is an honest gap, never a blank that could pass for a name. */
export const UNATTRIBUTED = 'unattributed';

export const DEFAULT_AUDIT_LIMIT = 50;
export const MAX_AUDIT_LIMIT = 200;

/** The action-prefix filter's choices: every noun the trail records. */
export const AUDIT_ACTION_OPTIONS = [
	{ value: '', label: 'All actions' },
	{ value: 'namespace.', label: 'namespace.' },
	{ value: 'orkflow.', label: 'orkflow.' },
	{ value: 'run.', label: 'run.' },
	{ value: 'webhook.', label: 'webhook.' },
	{ value: 'trigger.', label: 'trigger.' },
	{ value: 'retention.', label: 'retention.' }
];

/**
 * The scope is `all` unless the URL says `namespace`: the trail's natural
 * scope is the deployment, and deployment-wide writes (retention, webhooks)
 * carry no namespace, so a namespace scope would hide them.
 */
export type AuditScope = 'all' | 'namespace';

export interface AuditCoordinates {
	scope: AuditScope;
	/** The switcher's namespace, as a NAME (`default` spelled out). Only used when scope is `namespace`. */
	namespace: string;
	action: string;
	principal: string;
	since: string;
	until: string;
	limit: number;
	cursor: string;
	page: number;
}

export function auditScope(value: string | null): AuditScope {
	return value === 'namespace' ? 'namespace' : 'all';
}

/** limit from the URL: 1..200, anything else is the default. */
export function auditLimit(value: string | null): number {
	const parsed = Number(value ?? '');
	return Number.isInteger(parsed) && parsed >= 1 && parsed <= MAX_AUDIT_LIMIT ? parsed : DEFAULT_AUDIT_LIMIT;
}

// RFC 3339 with seconds and a zone, or a bare date meaning midnight UTC — the
// two spellings `orkctl admin audit --since/--until` accepts, so a window typed
// here is a window the copied command accepts too.
const BOUND = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2}))?$/;

export function auditBound(value: string) {
	if (!value) return undefined;
	const iso = /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00Z` : value;
	const date = new Date(iso);
	return Number.isNaN(date.getTime()) ? undefined : timestampFromDate(date);
}

/** A window the engine would refuse is refused here first, never sent as an unfiltered read. */
export function auditFilterError(since: string, until: string): string | null {
	if (since && (!BOUND.test(since) || !auditBound(since))) return 'Since must be an RFC 3339 instant (or YYYY-MM-DD).';
	if (until && (!BOUND.test(until) || !auditBound(until))) return 'Until must be an RFC 3339 instant (or YYYY-MM-DD).';
	const s = auditBound(since);
	const u = auditBound(until);
	if (s && u && Number(u.seconds) * 1e9 + u.nanos <= Number(s.seconds) * 1e9 + s.nanos) return 'Until must be after since.';
	return null;
}

/** The request namespace: empty means EVERY namespace, the one place in the API it does. */
export function auditRequestNamespace(c: Pick<AuditCoordinates, 'scope' | 'namespace'>): string {
	return c.scope === 'namespace' ? c.namespace || 'default' : '';
}

export function auditPrincipalLabel(principal: string): string {
	return principal || UNATTRIBUTED;
}

export function auditResultLabel(rec: Pick<AuditRecord, 'succeeded' | 'errorCode'>): string {
	return rec.succeeded ? 'ok' : `refused ${rec.errorCode || '-'}`;
}

/**
 * The page's address. Only ever `set`s, omits empties, and leaves `page` out
 * at 1 — listHref's rules (lib/browsing.ts), for this page's own keys.
 */
export function auditHref(c: AuditCoordinates, changes: Partial<AuditCoordinates> = {}): string {
	const next = { ...c, ...changes };
	const params = new URLSearchParams();
	params.set('ns', next.namespace || 'default');
	if (next.scope === 'namespace') params.set('scope', 'namespace');
	if (next.action) params.set('action', next.action);
	if (next.principal) params.set('principal', next.principal);
	if (next.since) params.set('since', next.since);
	if (next.until) params.set('until', next.until);
	if (next.limit !== DEFAULT_AUDIT_LIMIT) params.set('limit', String(next.limit));
	if (next.cursor) params.set('cursor', next.cursor);
	if (next.cursor && next.page > 1) params.set('page', String(next.page));
	return `/admin/audit?${params.toString()}`;
}

/** The sessionStorage key for this filter set's cursor stack: the address without cursor and page. */
export function auditStackKey(c: AuditCoordinates): string {
	return `orkstack.pages:${auditHref(c, { cursor: '', page: 1 })}`;
}

/** `orkctl admin audit …` for the same listing (first page), every value shell-quoted. */
export function auditCliCommand(c: AuditCoordinates): string {
	const parts = ['orkctl'];
	if (c.scope === 'namespace') parts.push('-n', shellQuote(c.namespace || 'default'));
	parts.push('admin', 'audit');
	if (c.since) parts.push('--since', shellQuote(c.since));
	if (c.until) parts.push('--until', shellQuote(c.until));
	if (c.action) parts.push('--action', shellQuote(c.action));
	if (c.principal) parts.push('--principal', shellQuote(c.principal));
	if (c.limit !== DEFAULT_AUDIT_LIMIT) parts.push('--limit', String(c.limit));
	return parts.join(' ');
}
