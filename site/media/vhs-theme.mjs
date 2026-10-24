// Prints a VHS terminal theme for one site theme, made from
// ui/src/tokens.css: the UI's paper and ink, its accents for the terminal's
// red, green and blue, and its inks for the rest. Called by media/run.sh;
// nothing it prints is committed.
import { readTokens, resolve } from '../scripts/tokens.mjs';

const theme = process.argv[2];
if (theme !== 'light' && theme !== 'dark') {
	console.error(`vhs-theme: want light or dark, got ${JSON.stringify(theme)}`);
	process.exit(2);
}
const t = readTokens();
const c = (name, fallback) => {
	try {
		return resolve(t, name, theme);
	} catch (err) {
		if (!fallback) throw err;
		return resolve(t, fallback, theme);
	}
};
const ink = c('--color-ink');
const secondary = c('--color-ink-secondary');
const muted = c('--color-ink-muted');
const blue = c('--color-blue');
const green = c('--color-green', '--color-ink');
console.log(
	JSON.stringify({
		name: `orkstack-${theme}`,
		background: c('--color-paper-sunken'),
		foreground: ink,
		cursor: ink,
		selection: c('--color-blue-tint'),
		black: theme === 'light' ? ink : c('--color-paper'),
		red: c('--color-red'),
		green,
		yellow: secondary,
		blue,
		magenta: secondary,
		cyan: blue,
		white: theme === 'light' ? c('--color-paper') : ink,
		brightBlack: muted,
		brightRed: c('--color-red-strong'),
		brightGreen: green,
		brightYellow: secondary,
		brightBlue: c('--color-blue-strong'),
		brightMagenta: secondary,
		brightCyan: c('--color-blue-strong'),
		brightWhite: ink,
	}),
);
