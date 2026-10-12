import { errorMessage } from '$lib/client';
import { auditClient } from '$lib/clients/audit';
import { DEFAULT_AUDIT_LIMIT, auditLimit, type AuditCoordinates } from '$lib/audit';
import type { ListAuditRecordsResponse } from '$lib/gen/orkstack/v1/audit_pb';
import { pageNumber } from '$lib/browsing';
import { filtersFromUrl } from '$lib/listFilter';
import { namespaceFrom, namespaceLabel } from '$lib/namespace';
import type { PageLoad } from './$types';

// ListAuditRecords reads the control-plane audit trail: a deployment-wide
// listing narrowed by filters (the `filter` parameters). The namespace
// switcher does not filter it; a `namespace` filter does, and `namespace=`
// selects the deployment-wide writes, which carry no namespace.
//
// Every coordinate lives in the URL (the runs list's discipline): filters,
// cursor and page number survive a reload. A failed read returns
// { audit: null, auditError } as retention/+page.ts does, never an empty
// table; a filter parameter that does not parse is shown without a request.
export const load: PageLoad = async ({ fetch, url }) => {
	const params = url.searchParams;
	const cursor = params.get('cursor') ?? '';
	const { filters, error: filterError } = filtersFromUrl(url);
	const coordinates: AuditCoordinates = {
		ns: namespaceLabel(namespaceFrom(url)),
		filters,
		limit: auditLimit(params.get('limit')),
		cursor,
		// A page number without a cursor names no page the engine could return.
		page: cursor ? pageNumber(params.get('page') ?? '') : 1
	};
	if (filterError) {
		return {
			coordinates,
			audit: null as ListAuditRecordsResponse | null,
			auditError: filterError as string | null
		};
	}
	try {
		const audit = await auditClient(fetch).listAuditRecords({
			filters,
			pageSize: coordinates.limit === DEFAULT_AUDIT_LIMIT ? 0 : coordinates.limit,
			pageToken: coordinates.cursor
		});
		return {
			coordinates,
			audit: audit as ListAuditRecordsResponse | null,
			auditError: null as string | null
		};
	} catch (err) {
		return {
			coordinates,
			audit: null as ListAuditRecordsResponse | null,
			auditError: errorMessage(err) as string | null
		};
	}
};
