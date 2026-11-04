// The configuration pages, generated from the repository so they can never
// drift from it.
//
// docs/DEPLOY.md's "## 1. Configuration" table is the one description of every
// setting (internal/config's completeness tests hold it to the code). This
// module reads it into entries: a setting's key, environment variable, default
// and the binaries that read it, its details as the table words them, what it
// accepts (its shape from internal/config's Schema, sharpened where
// content/configuration/accepts.yaml says more) and when it is checked (only
// where the table says so). Nothing here adds a fact: an accepts line that
// names a value or number its row does not hold is refused.
import GithubSlugger from 'github-slugger';

const KIND_WORDS = {
	KindString: 'text',
	KindInt: 'a whole number',
	KindBool: '`true` or `false`',
	KindDuration: 'a duration, such as `30s` or `5m`',
	KindList: 'a comma-separated list',
	KindMap: 'pairs of `key=value`, comma-separated',
	KindSecret: 'text, or a mapping with a `file:` key naming a file that holds it',
	KindDocument: 'a YAML document',
};

export const AREAS = [
	{ id: 'shared', title: 'Shared by ork and orker', lede: 'The Temporal cluster, the pool and the release: both binaries read these.' },
	{ id: 'engine', title: 'Engine', lede: 'Set on `ork`: the API listener, the limits of a run, its history, cleanup and caches.' },
	{ id: 'orker', title: 'Orker', lede: 'Set on `orker`: the container runtime, resources and capacity, secrets, logs and pins.' },
	{ id: 'storage', title: 'Storage', lede: 'Where artifacts, the run archive and task logs live. Both binaries read these.' },
	{ id: 'telemetry', title: 'Telemetry', lede: 'Process logs, the admin listener, exporters and sampling. Everything is off by default.' },
];

/** The cells of one markdown table row: pipes inside code or escaped stay. */
export function splitRow(line) {
	const cells = [];
	let cur = '';
	let ticks = 0;
	const body = line.trim().replace(/^\|/, '').replace(/\|$/, '');
	for (let i = 0; i < body.length; i++) {
		const c = body[i];
		if (c === '\\' && body[i + 1] === '|') {
			cur += '|';
			i++;
		} else if (c === '`') {
			let run = 1;
			while (body[i + run] === '`') run++;
			if (ticks === 0) ticks = run;
			else if (ticks === run) ticks = 0;
			cur += body.slice(i, i + run);
			i += run - 1;
		} else if (c === '|' && ticks === 0) {
			cells.push(cur.trim());
			cur = '';
		} else cur += c;
	}
	cells.push(cur.trim());
	return cells;
}

const codeSpans = (text) => [...text.matchAll(/`([^`]+)`/g)].map((m) => m[1]);

/** Every row of the configuration table, in its order. */
export function parseSettings(deployMd) {
	const at = deployMd.indexOf('\n## 1. Configuration');
	if (at < 0) throw new Error('docs/DEPLOY.md has no "## 1. Configuration" heading');
	const lines = deployMd.slice(at).split('\n');
	const head = lines.findIndex((l) => /^\|\s*file key\s*\|\s*variable\s*\|/.test(l));
	if (head < 0) throw new Error('docs/DEPLOY.md: no configuration table under "## 1. Configuration"');
	const rows = [];
	for (const line of lines.slice(head + 2)) {
		if (!line.startsWith('|')) break;
		const cells = splitRow(line);
		if (cells.length !== 5) throw new Error(`docs/DEPLOY.md: a configuration row has ${cells.length} cells: ${line.slice(0, 80)}`);
		const [keyCell, envCell, defaultCell, scopeCell, effect] = cells;
		rows.push({
			keys: codeSpans(keyCell).map((k) => k.replace(/:$/, '')),
			envs: codeSpans(envCell),
			envCell,
			defaultCell,
			scope: scopeCell.replace(/[*`]/g, '').trim(),
			effect: effect === '—' ? '' : effect,
		});
	}
	if (rows.length === 0) throw new Error('docs/DEPLOY.md: the configuration table has no rows');
	return rows;
}

/** A setting's id on the page: its key, lower case, every other run a hyphen. */
const idOf = (name) => name.split('{')[0].toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');

export function settingId(row) {
	return idOf(row.keys[0] ?? row.envs[0] ?? '');
}

export function settingArea(row) {
	const key = row.keys[0] ?? '';
	if (/^(temporal|deployment|pool|activities)\b/.test(key)) return 'shared';
	if (key.startsWith('engine.')) return 'engine';
	if (key.startsWith('orker.')) return 'orker';
	if (key.startsWith('storage.')) return 'storage';
	return 'telemetry';
}

/** Schema path -> Kind, read from internal/config/config.go. */
export function schemaKinds(configGo) {
	const kinds = new Map();
	for (const m of configGo.matchAll(/\{Path:\s*"([^"]+)",\s*Env:\s*"[^"]+",\s*Binaries:\s*\w+,\s*Kind:\s*(Kind\w+)/g)) kinds.set(m[1], m[2]);
	if (kinds.size === 0) throw new Error('internal/config/config.go: no Schema entries read');
	return kinds;
}

/** What the table says about when a value is checked, in its words; null when it says nothing. */
export function checkedWhen(effect) {
	const says = [];
	if (/[Rr]efused at boot|refuses boot|stops the Orker at boot|refuses to start|refuse to boot/.test(effect)) says.push('at boot');
	if (/[Cc]hecked at validate, apply and plan, and again at submit/.test(effect)) says.push('against each Orkflow at validate, apply and plan, and again at submit');
	else if (/[Cc]hecked at validate\/apply and again at submit/.test(effect)) says.push('against each Orkflow at validate and apply, and again at submit');
	if (/Read at submit/.test(effect)) says.push('read at submit, so a change applies to new runs');
	return says.length ? says.join('; ') : null;
}

const norm = (s) => s.replace(/[,\s]/g, '').toLowerCase();

/**
 * The problems of an accepts line against its row: every code span and number
 * it names must be in the row, so an edit to the table cannot leave it behind.
 */
export function acceptsProblems(key, text, row) {
	const hay = norm(`${row.defaultCell} ${row.effect} ${row.keys.join(' ')} ${row.envCell}`);
	const out = [];
	for (const span of codeSpans(text)) {
		if (!hay.includes(norm(span))) out.push(`accepts.yaml ${key}: \`${span}\` is not in its row of docs/DEPLOY.md`);
	}
	for (const m of text.replace(/`[^`]*`/g, ' ').matchAll(/\d[\d,.]*\d|\d/g)) {
		if (!hay.includes(norm(m[0]))) out.push(`accepts.yaml ${key}: ${m[0]} is not in its row of docs/DEPLOY.md`);
	}
	return out;
}

/** The default of a size written for people ("256 KiB", "10 MiB/s"), in the digits the setting takes. */
export function bytesDefault(cell) {
	const m = /^`?(\d+(?:\.\d+)?)\s*(KiB|MiB|GiB)(\/s)?`?$/.exec(cell.trim());
	if (!m) return null;
	const unit = { KiB: 1024, MiB: 1024 ** 2, GiB: 1024 ** 3 }[m[2]];
	return { value: Number(m[1]) * unit, perSecond: Boolean(m[3]) };
}

export function acceptsFor(row, kinds, overlay) {
	const key = [...row.keys, ...row.envs].find((k) => overlay[k]);
	const kind = kinds.get(row.keys[0] ?? '');
	// A size's default is printed for people, but only digits parse: say so.
	const size = kind === 'KindInt' ? bytesDefault(row.defaultCell) : null;
	if (size) {
		const words = key ? overlay[key] : `a whole number of bytes${size.perSecond ? ' per second' : ''}`;
		return `${words}, written in digits: the default is \`${size.value}\``;
	}
	if (key) return overlay[key];
	return kind ? KIND_WORDS[kind] : null;
}

/** Split a cell's text at its sentence ends, leaving code spans and abbreviations whole. */
export function sentences(text) {
	const out = [];
	let cur = '';
	let code = false;
	let bracket = 0;
	for (let i = 0; i < text.length; i++) {
		const c = text[i];
		cur += c;
		if (c === '`') code = !code;
		if (!code && (c === '(' || c === '[')) bracket++;
		if (!code && (c === ')' || c === ']')) bracket = Math.max(0, bracket - 1);
		if (code || bracket > 0 || c !== '.') continue;
		if (/(?:e\.g|i\.e|etc|vs)\.$/.test(cur)) continue;
		const next = text.slice(i + 1, i + 3);
		if (/^ [A-Z`*[]/.test(next)) {
			out.push(cur.trim());
			cur = '';
		}
	}
	if (cur.trim()) out.push(cur.trim());
	return out;
}

/** The part of a row's details to show at once, and the part to fold away. */
export function leadAndRest(effect, { lead = 140, foldOver = 120 } = {}) {
	const parts = sentences(effect);
	const shown = [];
	let size = 0;
	while (parts.length && (size === 0 || size + parts[0].length <= lead)) {
		const s = parts.shift();
		shown.push(s);
		size += s.length;
	}
	const rest = parts.join(' ');
	if (rest.length < foldOver) return { lead: effect, rest: '' };
	return { lead: shown.join(' '), rest };
}

const esc = (t) => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

/** A table cell's markdown as HTML: code spans, bold, emphasis and links; nothing else. */
export function inlineHtml(md, link = (t) => t) {
	return md
		.split(/(`[^`]*`)/g)
		.map((part) => {
			if (part.startsWith('`') && part.endsWith('`') && part.length > 1) return `<code>${esc(part.slice(1, -1))}</code>`;
			return esc(part)
				.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, (_, text, target) => `<a href="${esc(link(target.replace(/&amp;/g, '&')))}">${text}</a>`)
				.replace(/\*\*([^*]+)\*\*/g, '<b>$1</b>')
				.replace(/(^|[\s(])\*([^*\s][^*]*)\*/g, '$1<i>$2</i>');
		})
		.join('');
}

/** A markdown link's target pointing at a site route, which the linker adds the base to. */
export const siteRoute = (r) => `route:${r}`;

/**
 * The table's prose speaks of its own document ("§5", "below"). Here a reader
 * is on another page, so each such mention becomes a link, or goes.
 */
export function linkMentions(effect, { deployHeadings, docHeadings, detailHeadings }) {
	const deployAnchor = (name) => deployHeadings.get(name);
	let t = effect;
	t = t.replace(/\s*\(AGENTS\.md §[\d.]+\)/g, '');
	t = t.replace(/AGENTS\.md §[\d.]+/g, 'the project’s engineering contract');
	// How OrkStack's own tests measured a default is no reader's concern here.
	t = t.replace(/\s*The task soak runs in the nightly and pre-release proofs[^.]*\(docs\/PROOFS\.md\)\./g, '');
	t = t.replace(/the task soak measured \([^)]*\)/g, 'measured');
	t = t.replace(/Pool-wide (?:—|-) see below/g, () => {
		const a = detailHeadings.get('Pin hosting is a pool-wide setting');
		return a ? `Pool-wide (see [Pin hosting is a pool-wide setting](${siteRoute('docs/configuration/details')}#${a}))` : 'Pool-wide';
	});
	// §5, "Health checks" and § "The web UI": a named section of DEPLOY.md.
	t = t.replace(/§\s*(?:\d+(?:\.\d+)?\s*,\s*)?"([^"]+)"/g, (all, name) => {
		const a = deployAnchor(name);
		return a ? `[${name}](/docs/DEPLOY.md#${a})` : all;
	});
	// "Declaring an Orker's capacity" below: a section the details page ports.
	t = t.replace(/"([^"]+)" below/g, (all, name) => {
		const a = detailHeadings.get(name);
		return a ? `[${name}](${siteRoute('docs/configuration/details')}#${a})` : all;
	});
	t = t.replace(/(?:—|-) see below/g, `(see [Setting details](${siteRoute('docs/configuration/details')}))`);
	// `docs/DSL.md`, "Pins": a named section of another doc.
	t = t.replace(/`(docs\/[A-Z_]+\.md)`,\s*"([^"]+)"/g, (_, file, name) => {
		const a = docHeadings(file).get(name);
		return a ? `[${name}](/${file}#${a})` : `[${file}](/${file})`;
	});
	t = t.replace(/`(docs\/[A-Z_]+\.md)`/g, (_, file) => `[${file}](/${file})`);
	// §4, §0: a whole section of DEPLOY.md.
	t = t.replace(/§(\d+(?:\.\d+)?)(?![\d"])/g, (all, n) => {
		const a = deployHeadings.get(`§${n}`);
		return a ? `[DEPLOY.md §${n}](/docs/DEPLOY.md#${a})` : all;
	});
	return t;
}

/** A markdown document's headings: text (and "§N" for a numbered section) -> GitHub slug. */
export function headingAnchors(md) {
	const slugger = new GithubSlugger();
	const map = new Map();
	let fence = false;
	for (const line of md.split('\n')) {
		if (/^\s{0,3}(```|~~~)/.test(line)) fence = !fence;
		const m = !fence && /^(#{1,6})\s+(.*?)\s*#*\s*$/.exec(line);
		if (!m) continue;
		const text = m[2].replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1').replace(/\*\*/g, '').trim();
		const slug = slugger.slug(text);
		const plain = text.replace(/`/g, '');
		if (!map.has(plain)) map.set(plain, slug);
		const num = /^(\d+(?:\.\d+)?)\.?\s/.exec(plain);
		if (num && !map.has(`§${num[1]}`)) map.set(`§${num[1]}`, slug);
	}
	return map;
}

/** One setting as the page draws it. One line, so markdown never breaks the block. */
export function renderEntry(row, { accepts, link, mentions = (t) => t }) {
	const id = settingId(row);
	const names = row.keys.length ? row.keys : row.envs.slice(0, 1);
	// A row of several keys: each later key is a link target of its own.
	const title = names.map((k, i) => `<code${i > 0 ? ` id="${idOf(k)}"` : ''}>${esc(k)}</code>`).join(', ');
	const tags = (row.scope === 'both' ? ['ork', 'orker'] : [row.scope]).map((s) => `<span class="tag">${esc(s)}</span>`).join('');
	const dl = [
		['Environment', inlineHtml(row.envCell, link)],
		['Default', inlineHtml(row.defaultCell.replace(/^\*\((.*)\)\*$/, '$1'), link)],
		accepts ? ['Accepts', inlineHtml(accepts, link)] : null,
		checkedWhen(row.effect) ? ['Checked', esc(checkedWhen(row.effect))] : null,
	]
		.filter(Boolean)
		.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`)
		.join('');
	const { lead, rest } = leadAndRest(mentions(row.effect));
	// A table cell starts in lower case; a paragraph does not.
	const lede = lead ? `<p>${inlineHtml(lead.replace(/^[a-z]/, (c) => c.toUpperCase()), link)}</p>` : '';
	const more = rest ? `<details><summary>More detail<span class="sr-only"> on ${esc(names[0])}</span></summary><p>${inlineHtml(rest, link)}</p></details>` : '';
	return `<div class="setting not-content" data-setting><header><h3 id="${id}">${title}</h3>${tags}</header><dl>${dl}</dl>${lede}${more}</div>`;
}

/** An area's settings as a compact table, each key linking to its entry. */
export function renderTable(rows, { allRoute }) {
	const out = ['| Setting | Environment | Default | Set on |', '|---|---|---|---|'];
	for (const row of rows) {
		const name = row.keys[0] ?? row.envs[0];
		const cell = (s) => s.replace(/\|/g, '\\|');
		out.push(`| [\`${name}\`](${allRoute}#${settingId(row)}) | ${cell(row.envCell)} | ${cell(row.defaultCell)} | ${row.scope} |`);
	}
	return out.join('\n');
}
