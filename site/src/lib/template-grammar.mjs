// A Shiki (TextMate) grammar injection that marks a `{{ … }}` template inside
// any string of a YAML code block, so the site colours an Orkflow's templates
// as the UI's editor does (ui/src/lib/yaml/highlight.ts, the `template` role;
// scripts/tokens.mjs gives the scope its colour). Registered twice, because
// the site highlights YAML twice: Expressive Code's docs blocks
// (astro.config.mjs, shiki.langs) and the landing page's listing
// (components/landing/Example.astro, embeddedLangs).

/** @type {import('shiki').LanguageRegistration} */
export const templateInjection = {
	name: 'orkstack-template',
	scopeName: 'orkstack.template.injection',
	injectTo: ['source.yaml'],
	injectionSelector: 'L:string -comment',
	patterns: [{ name: 'meta.template.expression.orkstack', match: '\\{\\{.*?\\}\\}' }],
};
