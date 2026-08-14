import type { Handle } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';

// The browser talks to the OrkStack engine through this same-origin proxy so
// the Connect handlers need no CORS configuration. Anything under the Connect
// service path (/{package}.{Service}/{Method}) is forwarded verbatim.
const CONNECT_PATH_PREFIX = '/orkstack.v1.';

function engineApiUrl(): string {
	return (env.ENGINE_API_URL ?? 'http://127.0.0.1:8080').replace(/\/$/, '');
}

export const handle: Handle = async ({ event, resolve }) => {
	if (event.url.pathname.startsWith(CONNECT_PATH_PREFIX)) {
		const target = engineApiUrl() + event.url.pathname + event.url.search;
		const headers = new Headers(event.request.headers);
		headers.delete('host');
		headers.delete('content-length');
		headers.delete('connection');
		const upstream = await fetch(target, {
			method: event.request.method,
			headers,
			body:
				event.request.method === 'GET' || event.request.method === 'HEAD'
					? undefined
					: await event.request.arrayBuffer()
		});
		// undici has already decoded any content-encoding, so the upstream
		// framing headers no longer describe the body we forward.
		const responseHeaders = new Headers(upstream.headers);
		responseHeaders.delete('content-length');
		responseHeaders.delete('content-encoding');
		responseHeaders.delete('transfer-encoding');
		const body = await upstream.arrayBuffer();
		return new Response(body, {
			status: upstream.status,
			statusText: upstream.statusText,
			headers: responseHeaders
		});
	}
	return resolve(event);
};
