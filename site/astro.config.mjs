// @ts-check
// One Astro project: the landing page at the base, Starlight's docs under
// docs/. Both take their whole style from ui/src/tokens.css (src/styles/).
import { readFileSync, existsSync } from 'node:fs';
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';
import { SITE, BASE, BASE_PATH } from './site.config.mjs';
import { repoRoot, readTokens, codeTheme } from './scripts/tokens.mjs';
import { templateInjection } from './src/lib/template-grammar.mjs';

// Read here, at config time, so a missing tokens.css fails the build before
// anything renders (readTokens throws).
const tokens = readTokens();

const manifest = JSON.parse(readFileSync(new URL('./docs.manifest.json', import.meta.url), 'utf8'));
// Written by scripts/sync-docs.mjs, which every script runs first.
const examplesFile = new URL('./src/styles/generated/examples.json', import.meta.url);
if (!existsSync(examplesFile)) throw new Error('src/styles/generated/examples.json is missing: run node scripts/sync-docs.mjs first');
const examples = JSON.parse(readFileSync(examplesFile, 'utf8'));

/**
 * @typedef {{ route: string, title: string, examples?: string, aliases?: string[] }} ManifestPage
 * @typedef {{ label: string, hidden?: boolean, pages: ManifestPage[] }} ManifestGroup
 */

/**
 * The sidebar, in the manifest's order. Examples list every example file after
 * the gallery, the featured ones first (sync-docs.mjs orders them).
 * @param {ManifestPage[]} pages
 * @returns {any[]}
 */
function sidebarItems(pages) {
	return pages.flatMap((p) => {
		const item = { label: p.title, slug: p.route };
		if (!p.examples) return [item];
		return [item, ...examples.map((/** @type {{ title: string, route: string }} */ e) => ({ label: e.title, slug: e.route }))];
	});
}

const olderExamplesFile = new URL('./src/styles/generated/older-examples.json', import.meta.url);
if (!existsSync(olderExamplesFile)) throw new Error('src/styles/generated/older-examples.json is missing: run node scripts/sync-docs.mjs first');
const olderExamples = JSON.parse(readFileSync(olderExamplesFile, 'utf8'));

/**
 * A page's old routes keep working: each redirects to where the page is now. So
 * does an example the docs no longer show, to the gallery.
 */
const redirects = Object.fromEntries([
	...manifest.groups.flatMap((/** @type {ManifestGroup} */ g) =>
		g.pages.flatMap((p) => (p.aliases ?? []).map((a) => [`/${a}`, `${BASE_PATH}/${p.route}/`])),
	),
	...olderExamples.map((/** @type {string} */ id) => [`/docs/examples/${id}`, `${BASE_PATH}/docs/examples/`]),
]);

export default defineConfig({
	site: SITE,
	base: BASE,
	trailingSlash: 'always',
	devToolbar: { enabled: false },
	redirects,
	integrations: [
		starlight({
			title: 'OrkStack',
			description:
				'A thin, Temporal-backed durable execution platform: Orkflows in YAML, every task an OCI container.',
			favicon: '/favicon.svg',
			head: [
				{ tag: 'meta', attrs: { property: 'og:image', content: `${SITE}${BASE_PATH}/og.png` } },
				{ tag: 'meta', attrs: { name: 'twitter:image', content: `${SITE}${BASE_PATH}/og.png` } },
			],
			customCss: ['./src/styles/site.css', './src/styles/starlight.css'],
			components: {
				PageFrame: './src/components/starlight/PageFrame.astro',
				Header: './src/components/starlight/Header.astro',
				SiteTitle: './src/components/starlight/SiteTitle.astro',
				ThemeProvider: './src/components/starlight/ThemeProvider.astro',
				ThemeSelect: './src/components/starlight/ThemeSelect.astro',
				PageTitle: './src/components/starlight/PageTitle.astro',
				Footer: './src/components/starlight/Footer.astro',
				MobileMenuToggle: './src/components/starlight/MobileMenuToggle.astro',
				MobileMenuFooter: './src/components/starlight/MobileMenuFooter.astro',
			},
			sidebar: manifest.groups
				.filter((/** @type {ManifestGroup} */ g) => !g.hidden)
				.map((/** @type {ManifestGroup} */ g) => ({ label: g.label, collapsed: true, items: sidebarItems(g.pages) })),
			expressiveCode: {
				// Two themes generated from tokens.css; the page's data-theme
				// picks one.
				themes: [codeTheme('light', tokens), codeTheme('dark', tokens)],
				themeCssSelector: (theme) => `[data-theme='${theme.type}']`,
				useDarkModeMediaQuery: true,
				// The syntax colours are the UI's own and already meet the text
				// floor on the code ground (ui/test/tokenContrast.test.ts); left
				// on, Expressive Code would shift them, and the docs would no
				// longer match the UI or the landing page's listing.
				minSyntaxHighlightingColorContrast: 0,
				// A code block is a plain card: a title when it has one, and no
				// window dots on a command.
				defaultProps: { frame: 'code' },
				// Languages the repository's docs fence that Shiki does not ship:
				// Go templates read as plain text, Alloy's river as HCL, its parent.
				// The template injection colours {{ … }} in YAML as the UI does.
				shiki: { langAlias: { gotemplate: 'txt', river: 'hcl' }, langs: [templateInjection] },
				styleOverrides: {
					borderRadius: 'var(--r)',
					borderWidth: 'var(--rule-hair)',
					borderColor: 'var(--line)',
					codeFontFamily: 'var(--font-mono)',
					codeFontSize: 'var(--text-sm)',
					codeLineHeight: '1.7',
					uiFontFamily: 'var(--font-sans)',
					uiFontSize: 'var(--text-sm)',
					frames: {
						shadowColor: 'transparent',
						frameBoxShadowCssValue: 'var(--shadow)',
						editorActiveTabIndicatorTopColor: 'transparent',
						editorActiveTabForeground: 'var(--ink-3)',
						editorActiveTabBackground: 'var(--surface-2)',
						editorActiveTabBorderColor: 'var(--line)',
						editorTabBarBackground: 'var(--surface-2)',
						editorTabBarBorderBottomColor: 'var(--line)',
						editorBackground: 'var(--surface)',
						terminalTitlebarBackground: 'var(--surface-2)',
						terminalTitlebarBorderBottomColor: 'var(--line)',
						terminalTitlebarForeground: 'var(--ink-3)',
						terminalTitlebarDotsForeground: 'var(--line-strong)',
						terminalBackground: 'var(--surface)',
						inlineButtonForeground: 'var(--ink-2)',
						inlineButtonBorder: 'var(--line-strong)',
						tooltipSuccessBackground: 'var(--ink)',
						tooltipSuccessForeground: 'var(--surface)',
					},
				},
			},
			editLink: { baseUrl: `https://github.com/orkstack/orkstack/edit/main/` },
			lastUpdated: false,
			credits: false,
			plugins: [
				starlightLinksValidator({
					errorOnFallbackPages: false,
					errorOnInvalidHashes: true,
					// The docs name the local stack's own addresses (localhost:8080 …).
					errorOnLocalLinks: false,
					// The landing page is no docs page; scripts/check-links.mjs covers it.
					exclude: [`${BASE_PATH}/`],
				}),
			],
		}),
	],
	vite: {
		server: { fs: { allow: [repoRoot] } },
	},
});
