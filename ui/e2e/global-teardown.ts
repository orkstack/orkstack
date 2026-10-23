import { rmSync } from 'node:fs';
import { stopBrowserServer } from './browser-server';

// Stops the browser-server container global-setup.ts started, by its recorded
// id and nothing else, and removes the binaries it built when no proof
// supplied a directory for them. A proof's own directory is the proof's to
// keep.
export default function globalTeardown(): void {
	const container = process.env.ORK_E2E_BROWSER_CONTAINER;
	if (container) stopBrowserServer(container);
	const own = process.env.ORK_E2E_OWN_BIN_DIR;
	if (own) rmSync(own, { recursive: true, force: true });
}
