// @ts-check
// One Astro project: the landing page at the base, Starlight's docs under
// docs/. Both take their whole style from ui/src/tokens.css (src/styles/).
import { readFileSync, existsSync } from 'node:fs';
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';
import { SITE, BASE, REPO } from './site.config.mjs';
import { repoRoot, readTokens, codeTheme } from './scripts/tokens.mjs';

// Read here, at config time, so a missing tokens.css fails the build before
// anything renders (readTokens throws).
const tokens = readTokens();

const manifest = JSON.parse(readFileSync(new URL('./docs.manifest.json', import.meta.url), 'utf8'));
const examplesFile = new URL('./src/styles/generated/examples.json', import.meta.url);
const examples = existsSync(examplesFile) ? JSON.parse(readFileSync(examplesFile, 'utf8')) : [];

/**
 * @typedef {{ route: string, title: string, examples?: string }} ManifestPage
 * @typedef {{ label: string, pages: Array<ManifestPage | ManifestGroup> }} ManifestGroup
 */

/**
 * The sidebar, in the manifest's order.
 * @param {Array<ManifestPage | ManifestGroup>} pages
 * @returns {any[]}
 */
function sidebarItems(pages) {
	return pages.flatMap((p) => {
		if ('pages' in p) return [{ label: p.label, collapsed: true, items: sidebarItems(p.pages) }];
		const item = { label: p.title, slug: p.route };
		if (!p.examples) return [item];
		return [
			item,
			{
				label: 'Example files',
				collapsed: true,
				items: examples.map((/** @type {{ title: string, route: string }} */ e) => ({ label: e.title, slug: e.route })),
			},
		];
	});
}

export default defineConfig({
	site: SITE,
	base: BASE,
	trailingSlash: 'always',
	devToolbar: { enabled: false },
	integrations: [
		starlight({
			title: 'OrkStack',
			description:
				'A thin, Temporal-backed durable execution platform: Orkflows in YAML, every task an OCI container.',
			social: [{ icon: 'github', label: 'GitHub', href: REPO }],
			customCss: ['./src/styles/site.css', './src/styles/starlight.css'],
			components: {
				Header: './src/components/starlight/Header.astro',
				SiteTitle: './src/components/starlight/SiteTitle.astro',
				ThemeProvider: './src/components/starlight/ThemeProvider.astro',
				ThemeSelect: './src/components/starlight/ThemeSelect.astro',
				Footer: './src/components/starlight/Footer.astro',
			},
			sidebar: manifest.groups.map((/** @type {ManifestGroup} */ g) => ({ label: g.label, items: sidebarItems(g.pages) })),
			expressiveCode: {
				// Two themes generated from tokens.css; the site's data-theme
				// picks one (src/styles/starlight.css covers no-JS visitors).
				themes: [codeTheme('light', tokens), codeTheme('dark', tokens)],
				themeCssSelector: (theme) => `[data-theme='${theme.type}']`,
				useDarkModeMediaQuery: true,
				// Languages the repository's docs fence that Shiki does not ship:
				// Go templates read as plain text, Alloy's river as HCL, its parent.
				shiki: { langAlias: { gotemplate: 'txt', river: 'hcl' } },
				styleOverrides: {
					borderRadius: 'var(--radius-sm)',
					borderWidth: 'var(--rule-hair)',
					borderColor: 'var(--color-rule)',
					codeFontFamily: 'var(--font-mono)',
					codeFontSize: 'var(--text-sm)',
					codeLineHeight: 'var(--text-sm--line-height)',
					uiFontFamily: 'var(--font-sans)',
					uiFontSize: 'var(--text-xs)',
					frames: {
						shadowColor: 'transparent',
						frameBoxShadowCssValue: 'none',
						editorActiveTabIndicatorTopColor: 'var(--color-ink)',
						editorActiveTabBackground: 'var(--color-paper-sunken)',
						editorTabBarBackground: 'var(--color-paper)',
						editorTabBarBorderBottomColor: 'var(--color-rule)',
						terminalTitlebarBackground: 'var(--color-paper)',
						terminalTitlebarBorderBottomColor: 'var(--color-rule)',
						terminalTitlebarForeground: 'var(--color-ink-muted)',
						terminalTitlebarDotsForeground: 'var(--color-rule-strong)',
						terminalBackground: 'var(--color-paper-sunken)',
						inlineButtonForeground: 'var(--color-ink-secondary)',
						inlineButtonBorder: 'var(--color-rule-strong)',
						tooltipSuccessBackground: 'var(--color-ink)',
						tooltipSuccessForeground: 'var(--color-on-ink)',
					},
				},
			},
			editLink: { baseUrl: `${REPO}/edit/main/` },
			lastUpdated: false,
			credits: false,
			plugins: [starlightLinksValidator({
					errorOnFallbackPages: false,
					errorOnInvalidHashes: true,
					// The docs name the local stack's own addresses (localhost:5173 …).
					errorOnLocalLinks: false,
				}),
			],
		}),
	],
	vite: {
		server: { fs: { allow: [repoRoot] } },
	},
});
