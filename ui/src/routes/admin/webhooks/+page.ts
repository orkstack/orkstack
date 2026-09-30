import { Code, ConnectError } from '@connectrpc/connect';
import { errorMessage, orkstack } from '$lib/client';
import { pageNumber } from '$lib/browsing';
import {
	deliveredFilter,
	deliveriesFilterError,
	deliveriesRequest,
	deliveryLimit,
	type DeliveryCoordinates
} from '$lib/deliveries';
import type { ListDeliveriesResponse } from '$lib/gen/orkstack/v1/service_pb';
import { namespaceFrom, namespaceLabel } from '$lib/namespace';
import type { PageLoad } from './$types';

// Two independent reads: the subscriptions and what their deliveries did.
// Either can fail without taking the other down — a failed deliveries read
// returns { deliveries: null, deliveriesError } and the webhook table still
// renders, and a failed read is never an empty table.
//
// Every coordinate of the Deliveries panel lives in the URL (the runs list's
// discipline), so filters, cursor and page survive a reload. The page's `ns`
// is preserved but does NOT filter the panel: the webhook surface is
// deployment-wide, and the panel's own `namespace` filter is what narrows it.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const params = url.searchParams;
	const cursor = params.get('cursor') ?? '';
	const coordinates: DeliveryCoordinates = {
		ns: namespaceLabel(namespace),
		webhook: params.get('webhook') ?? '',
		run: params.get('run') ?? '',
		event: params.get('event') ?? '',
		delivered: deliveredFilter(params.get('delivered')),
		namespace: params.get('namespace') ?? '',
		since: params.get('since') ?? '',
		until: params.get('until') ?? '',
		limit: deliveryLimit(params.get('limit')),
		cursor,
		// A page number without a cursor names no page the engine could return.
		page: cursor ? pageNumber(params.get('page') ?? '') : 1
	};
	const client = orkstack(fetch);

	const webhooksRead = client.webhooks.listWebhooks({}).then(
		(response) => ({ webhooks: response.webhooks, error: null as string | null }),
		(err) => ({ webhooks: [], error: errorMessage(err) as string | null })
	);
	const filterError = deliveriesFilterError(coordinates);
	const deliveriesRead = filterError
		? Promise.resolve({
				deliveries: null as ListDeliveriesResponse | null,
				deliveriesError: filterError as string | null,
				deliveriesErrorCode: 'invalid_argument' as string | null
			})
		: client.webhooks.listDeliveries(deliveriesRequest(coordinates)).then(
				(deliveries) => ({
					deliveries: deliveries as ListDeliveriesResponse | null,
					deliveriesError: null as string | null,
					deliveriesErrorCode: null as string | null
				}),
				(err) => ({
					deliveries: null as ListDeliveriesResponse | null,
					deliveriesError: errorMessage(err) as string | null,
					deliveriesErrorCode: (err instanceof ConnectError && err.code === Code.ResourceExhausted
						? 'resource_exhausted'
						: err instanceof ConnectError && err.code === Code.InvalidArgument
							? 'invalid_argument'
							: 'failed') as string | null
				})
			);
	const [hooks, deliveries] = await Promise.all([webhooksRead, deliveriesRead]);
	return { webhooks: hooks.webhooks, namespace, error: hooks.error, coordinates, ...deliveries };
};
