import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';

// The browser-server image (e2e/browser-image.txt, read by
// e2e/browser-server.ts) must be the Playwright release ui/node_modules
// installs: the server runs THIS tree's playwright-core against the image's
// browsers, and a browser of another release is not what the client's protocol
// expects. It is pinned by digest, because an image tag moves and a proof must
// drive the browser it says it drove.

const ui = `${import.meta.dir}/..`;
const installed = (pkg: string): string =>
	(JSON.parse(readFileSync(`${ui}/node_modules/${pkg}/package.json`, 'utf8')) as { version: string }).version;

/** The one non-comment line of browser-image.txt, as browser-server.ts reads it. */
function imageLine(): string {
	const lines = readFileSync(`${ui}/e2e/browser-image.txt`, 'utf8')
		.split('\n')
		.map((l) => l.trim())
		.filter((l) => l !== '' && !l.startsWith('#'));
	expect(lines).toHaveLength(1);
	return lines[0];
}

test('the browser-server image is the installed Playwright release, pinned by digest', () => {
	const image = imageLine();
	const m = /^mcr\.microsoft\.com\/playwright:v(\d+\.\d+\.\d+)-[a-z]+@sha256:[0-9a-f]{64}$/.exec(image);
	expect(m !== null).toBe(true);
	const version = m ? m[1] : '';
	expect(version).toBe(installed('@playwright/test'));
	expect(version).toBe(installed('playwright-core'));
});
