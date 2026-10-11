import type { Handle } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import { preloadAsset } from '$lib/preload';

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
					: event.request.body,
			signal: event.request.signal,
			// Node requires duplex when forwarding a streaming request body.
			duplex: 'half'
		} as RequestInit & { duplex: 'half' });
		// undici has already decoded any content-encoding, so the upstream
		// framing headers no longer describe the body we forward.
		const responseHeaders = new Headers(upstream.headers);
		responseHeaders.delete('content-length');
		responseHeaders.delete('content-encoding');
		responseHeaders.delete('transfer-encoding');
		// Preserve the upstream ReadableStream. In particular, ReadArtifact's
		// 64 KiB chunks must not be accumulated in the SvelteKit process before
		// a browser can receive the first one; canceling the browser request also
		// aborts the upstream fetch through event.request.signal above.
		return new Response(upstream.body, {
			status: upstream.status,
			statusText: upstream.statusText,
			headers: responseHeaders
		});
	}
	// A universal load that calls Connect runs on the server first, and
	// SvelteKit serializes that response into the HTML so hydration does not
	// refetch it. It strips every response header not named here, and the
	// Connect client rejects a response whose content-type it cannot read, so
	// without this the server render of every engine-backed page fails with
	// "Failed to get response header content-type" and the browser shows
	// "Could not reach the engine" even though the engine is up.
	return resolve(event, {
		filterSerializedResponseHeaders: (name) =>
			name === 'content-type' || name.startsWith('connect-') || name.startsWith('grpc-'),
		// Scripts and styles as before, plus the two latin font faces every
		// page renders with ($lib/preload).
		preload: preloadAsset
	});
};
