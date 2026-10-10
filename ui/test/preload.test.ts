import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { inlineAsset, preloadAsset } from '../src/lib/preload';

// F-ui-perf-7: no font face rides inside the render-blocking stylesheet, and
// exactly the two faces every page uses are preloaded. The faces are the ones
// app.css actually imports, read from the packages, with a build hash added
// the way Vite names them.
const faces = ['inter', 'jetbrains-mono'].flatMap((family) => {
	const css = readFileSync(`${import.meta.dir}/../node_modules/@fontsource-variable/${family}/wght.css`, 'utf8');
	return [...css.matchAll(/url\(\.\/files\/([^)]+\.woff2)\)/g)].map(
		(m) => `./_app/immutable/assets/${m[1].replace(/\.woff2$/, '.Ab12_-Cd.woff2')}`
	);
});

test('the imported stylesheets declare every subset', () => {
	expect(faces.length > 8).toBe(true);
	expect(faces.some((face) => face.includes('cyrillic-ext'))).toBe(true);
});

test('no font is ever inlined; everything else keeps the default', () => {
	for (const face of faces) expect(inlineAsset(face)).toBe(false);
	expect(inlineAsset('/x/inter-latin-wght-normal.woff')).toBe(false);
	expect(inlineAsset('/x/logo.svg')).toBeUndefined();
	expect(inlineAsset('/x/icon.png')).toBeUndefined();
});

test('exactly the two latin variable faces are preloaded, with scripts and styles', () => {
	const preloaded = faces.filter((path) => preloadAsset({ type: 'font', path }));
	expect(preloaded.map((path) => path.replace(/^.*\//, '').replace(/\..*$/, '')).sort()).toEqual([
		'inter-latin-wght-normal',
		'jetbrains-mono-latin-wght-normal'
	]);
	expect(preloadAsset({ type: 'js', path: './_app/immutable/entry/start.js' })).toBe(true);
	expect(preloadAsset({ type: 'css', path: './_app/immutable/assets/0.css' })).toBe(true);
	expect(preloadAsset({ type: 'asset', path: './_app/immutable/assets/x.png' })).toBe(false);
});
