import { errorMessage, orkstack } from '$lib/client';
import { pageNumber } from '$lib/browsing';
import { DEFAULT_DELIVERY_LIMIT, DELIVERY_FILTER_PARAM, deliveryLimit, type DeliveryCoordinates } from '$lib/deliveries';
import type { ListDeliveriesResponse } from '$lib/gen/orkstack/v1/service_pb';
import { namespaceFrom, namespaceLabel } from '$lib/namespace';
import { filtersFromUrl } from '$lib/listFilter';
import type { Webhook } from '$lib/gen/orkstack/v1/service_pb';
import type { PageLoad } from './$types';

// Two independent reads: the subscriptions and what their deliveries did.
// Either can fail without taking the other down — a failed deliveries read
// returns { deliveries: null, deliveriesError } and the webhook table still
// renders, and a failed read is never an empty table.
//
// Every coordinate of the Deliveries panel lives in the URL (the runs list's
// discipline), so filters, cursor and page survive a reload. The page holds
// two lists, so each has its own filter parameter: `filter` for the
// subscriptions table, `deliveryFilter` for the panel. The page's `ns` is
// preserved but filters neither: the webhook surface is deployment-wide, and
// a `namespace` filter is what narrows the panel.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const params = url.searchParams;
	const cursor = params.get('cursor') ?? '';
	// A filter parameter that does not parse is shown without a request.
	const { filters: applied, error: subscriptionFilterError } = filtersFromUrl(url);
	const { filters: deliveryApplied, error: deliveryFilterError } = filtersFromUrl(url, DELIVERY_FILTER_PARAM);
	const coordinates: DeliveryCoordinates = {
		ns: namespaceLabel(namespace),
		filters: deliveryApplied,
		limit: deliveryLimit(params.get('limit')),
		cursor,
		// A page number without a cursor names no page the engine could return.
		page: cursor ? pageNumber(params.get('page') ?? '') : 1,
		subscriptionFilters: applied
	};
	const client = orkstack(fetch);

	const webhooksRead = subscriptionFilterError
		? Promise.resolve({ webhooks: [] as Webhook[], error: subscriptionFilterError as string | null })
		: client.webhooks.listWebhooks({ filters: applied }).then(
				(response) => ({ webhooks: response.webhooks, error: null as string | null }),
				(err) => ({ webhooks: [] as Webhook[], error: errorMessage(err) as string | null })
			);
	// The Deliveries panel names EVERY subscription, whatever narrows the
	// table above it; only a filtered table needs the second read.
	const panelRead =
		applied.length === 0 && !subscriptionFilterError
			? webhooksRead.then((read) => read.webhooks)
			: client.webhooks.listWebhooks({}).then(
					(response) => response.webhooks,
					() => [] as Webhook[]
				);
	const deliveriesRead = deliveryFilterError
		? Promise.resolve({
				deliveries: null as ListDeliveriesResponse | null,
				deliveriesError: deliveryFilterError as string | null
			})
		: client.webhooks
				.listDeliveries({
					filters: deliveryApplied,
					pageSize: coordinates.limit === DEFAULT_DELIVERY_LIMIT ? 0 : coordinates.limit,
					pageToken: coordinates.cursor
				})
				.then(
					(deliveries) => ({
						deliveries: deliveries as ListDeliveriesResponse | null,
						deliveriesError: null as string | null
					}),
					(err) => ({
						deliveries: null as ListDeliveriesResponse | null,
						deliveriesError: errorMessage(err) as string | null
					})
				);
	const [hooks, panelWebhooks, deliveries] = await Promise.all([webhooksRead, panelRead, deliveriesRead]);
	return { webhooks: hooks.webhooks, panelWebhooks, applied, namespace, error: hooks.error, coordinates, ...deliveries };
};
