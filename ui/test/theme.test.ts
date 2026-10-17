import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { rememberTheme, rememberedTheme, resolveTheme, watchSystemTheme, type Theme } from '../src/lib/theme';

// The appearance is decided twice: by app.html's inline boot script before the
// first paint, and by $lib/theme.ts after hydration. They must agree, or the
// page flashes from one to the other. The boot script is read out of
// app.html and executed here against fake storage and a fake matchMedia.
const html = readFileSync(`${import.meta.dir}/../src/app.html`, 'utf8');
const boot = html.slice(html.indexOf('<script>') + '<script>'.length, html.indexOf('</script>'));

interface Fakes {
	stored: string | null;
	osDark: boolean;
	storageThrows?: boolean;
	matchMediaThrows?: boolean;
}

function runBoot(f: Fakes): string | null {
	let attr: string | null = null;
	const storage = {
		getItem: () => {
			if (f.storageThrows) throw new Error('SecurityError');
			return f.stored;
		}
	};
	const matchMedia = () => {
		if (f.matchMediaThrows) throw new Error('no matchMedia');
		return { matches: f.osDark };
	};
	const documentFake = { documentElement: { setAttribute: (_: string, v: string) => (attr = v) } };
	new Function('localStorage', 'matchMedia', 'document', boot)(storage, matchMedia, documentFake);
	return attr;
}

function installGlobals(f: Fakes) {
	const g = globalThis as unknown as Record<string, unknown>;
	const store = new Map<string, string>();
	if (f.stored !== null) store.set('ork-theme', f.stored);
	const listeners: (() => void)[] = [];
	const mql = {
		matches: f.osDark,
		addEventListener: (_: string, fn: () => void) => listeners.push(fn),
		removeEventListener: () => {}
	};
	g.localStorage = {
		getItem: (k: string) => {
			if (f.storageThrows) throw new Error('SecurityError');
			return store.get(k) ?? null;
		},
		setItem: (k: string, v: string) => store.set(k, v),
		removeItem: (k: string) => store.delete(k)
	};
	g.matchMedia = () => mql;
	let attr: string | null = null;
	g.document = { documentElement: { setAttribute: (_: string, v: string) => (attr = v) } };
	return {
		store,
		attr: () => attr,
		osChange: (dark: boolean) => {
			mql.matches = dark;
			for (const fn of listeners) fn();
		},
		restore: () => {
			delete g.localStorage;
			delete g.matchMedia;
			delete g.document;
		}
	};
}

test('the boot script agrees with resolveTheme for every stored choice and OS setting', () => {
	expect(boot.includes('ork-theme')).toBe(true);
	for (const stored of [null, 'light', 'dark'] as const) {
		for (const osDark of [false, true]) {
			const env = installGlobals({ stored, osDark });
			try {
				const choice: Theme = stored ?? 'system';
				expect(rememberedTheme()).toBe(choice);
				expect(runBoot({ stored, osDark })).toBe(resolveTheme(choice));
			} finally {
				env.restore();
			}
		}
	}
});

test('a browser that refuses storage still follows the OS', () => {
	expect(runBoot({ stored: null, osDark: true, storageThrows: true })).toBe('dark');
	expect(runBoot({ stored: null, osDark: false, storageThrows: true })).toBe('light');
	// No matchMedia at all: light, and still an attribute, never none.
	expect(runBoot({ stored: null, osDark: true, matchMediaThrows: true })).toBe('light');
});

test('"system" is never stored', () => {
	const env = installGlobals({ stored: 'dark', osDark: false });
	try {
		rememberTheme('system');
		expect(env.store.has('ork-theme')).toBe(false);
		rememberTheme('light');
		expect(env.store.get('ork-theme')).toBe('light');
	} finally {
		env.restore();
	}
});

test('an OS change is followed only while the choice is "system"', () => {
	const env = installGlobals({ stored: null, osDark: false });
	try {
		let choice: Theme = 'system';
		const stop = watchSystemTheme(() => choice);
		env.osChange(true);
		expect(env.attr()).toBe('dark');
		choice = 'light';
		env.osChange(false);
		env.osChange(true);
		expect(env.attr()).toBe('dark'); // unchanged since the choice became explicit
		stop();
	} finally {
		env.restore();
	}
});
