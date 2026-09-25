import { Code, ConnectError } from '@connectrpc/connect';
import { errorMessage, orkstack } from '$lib/client';
import {
	auditBound,
	auditFilterError,
	auditLimit,
	auditRequestNamespace,
	auditScope,
	type AuditCoordinates
} from '$lib/audit';
import type { ListAuditRecordsResponse } from '$lib/gen/orkstack/v1/audit_pb';
import { pageNumber } from '$lib/browsing';
import { namespaceFrom, namespaceLabel } from '$lib/namespace';
import type { PageLoad } from './$types';

// ListAuditRecords reads the control-plane audit trail. The namespace
// switcher is a FILTER here, and an explicit one: `scope=namespace` narrows to
// the switcher's namespace; without it the page shows every namespace, the
// deployment-wide writes included — the one request in the API where an empty
// namespace means "all of them" (proto/orkstack/v1/audit.proto).
//
// Every coordinate lives in the URL (the runs list's discipline): filters,
// cursor and page number survive a reload. A failed read returns
// { audit: null, auditError } as retention/+page.ts does, never an empty
// table.
export const load: PageLoad = async ({ fetch, url }) => {
	const params = url.searchParams;
	const cursor = params.get('cursor') ?? '';
	const coordinates: AuditCoordinates = {
		scope: auditScope(params.get('scope')),
		namespace: namespaceLabel(namespaceFrom(url)),
		action: params.get('action') ?? '',
		principal: params.get('principal') ?? '',
		since: params.get('since') ?? '',
		until: params.get('until') ?? '',
		limit: auditLimit(params.get('limit')),
		cursor,
		// A page number without a cursor names no page the engine could return.
		page: cursor ? pageNumber(params.get('page') ?? '') : 1
	};
	const filterError = auditFilterError(coordinates.since, coordinates.until);
	if (filterError) {
		return {
			coordinates,
			audit: null as ListAuditRecordsResponse | null,
			auditError: filterError as string | null,
			auditErrorCode: 'invalid_argument' as string | null
		};
	}
	try {
		const audit = await orkstack(fetch).audit.listAuditRecords({
			namespace: auditRequestNamespace(coordinates),
			actionPrefix: coordinates.action,
			principal: coordinates.principal,
			since: auditBound(coordinates.since),
			until: auditBound(coordinates.until),
			pageSize: coordinates.limit,
			pageToken: coordinates.cursor
		});
		return {
			coordinates,
			audit: audit as ListAuditRecordsResponse | null,
			auditError: null as string | null,
			auditErrorCode: null as string | null
		};
	} catch (err) {
		const code =
			err instanceof ConnectError && err.code === Code.ResourceExhausted ? 'resource_exhausted' : 'failed';
		return {
			coordinates,
			audit: null as ListAuditRecordsResponse | null,
			auditError: errorMessage(err) as string | null,
			auditErrorCode: code as string | null
		};
	}
};
