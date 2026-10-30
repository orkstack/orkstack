// Where this browser's WebSockets go: the origin the page came from.
//
// The engine serves the UI, the Connect API and both sockets — the run's
// event stream and a task's log stream — on one listener (internal/webui), so
// a socket opened to the page's own host is same-origin, which the bridge
// admits with no configuration (internal/api/ws.go). In development the Vite
// server proxies /ws/ to the engine (vite.config.ts), the same way it
// proxies the Connect calls, so the rule holds there too.
export function wsBaseUrl(at: Pick<Location, 'protocol' | 'host'> = location): string {
	const scheme = at.protocol === 'https:' ? 'wss' : 'ws';
	return `${scheme}://${at.host}`;
}
