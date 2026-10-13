import { rmSync } from 'node:fs';

// Removes the binaries global-setup.ts built when no proof supplied a
// directory for them. A proof's own directory is the proof's to keep.
export default function globalTeardown(): void {
	const own = process.env.ORK_E2E_OWN_BIN_DIR;
	if (own) rmSync(own, { recursive: true, force: true });
}
