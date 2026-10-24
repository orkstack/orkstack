import { env } from '$env/dynamic/public';

// Where this browser's WebSockets go.
//
// The engine serves both sockets — the run's event stream and a task's log
// stream — on its own HTTP server, not through the UI's same-origin Connect
// proxy: a proxy would have to hold one upstream socket open per browser
// socket for as long as a run lasts, and the bridge already answers browsers
// directly (internal/api/ws.go, ORK_WS_ALLOWED_ORIGINS).
//
// PUBLIC_ENGINE_WS_URL names it when the engine is not on this host's :8080 —
// which is every deployment that is not a laptop, and every test that runs two
// replicas on two ports.
export function wsBaseUrl(): string {
	if (env.PUBLIC_ENGINE_WS_URL) return env.PUBLIC_ENGINE_WS_URL.replace(/\/$/, '');
	const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
	return `${scheme}://${location.hostname}:8080`;
}
