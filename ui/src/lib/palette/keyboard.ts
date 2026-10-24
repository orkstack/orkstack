// The palette's keyboard model, as a pure reducer over the active option.
//
//   ↑ / ↓          previous / next option, wrapping
//   PageUp/Down    five options back / forward, stopping at the ends
//   Enter          run the active option; Ctrl/Cmd+Enter opens a navigation in a new tab
//   Escape         close
// Home and End stay with the input's caret, and Tab does not leave the input:
// the options are not tab stops.

export const PAGE_STEP = 5;

export type PaletteKeyResult =
	| { kind: 'move'; active: number }
	| { kind: 'run'; active: number; newTab: boolean }
	| { kind: 'close' }
	| { kind: 'stay' }
	| { kind: 'pass' };

export function paletteKeydown(
	key: string,
	mods: { ctrlKey: boolean; metaKey: boolean },
	active: number,
	count: number
): PaletteKeyResult {
	switch (key) {
		case 'ArrowDown':
			return count ? { kind: 'move', active: active < 0 ? 0 : (active + 1) % count } : { kind: 'stay' };
		case 'ArrowUp':
			return count ? { kind: 'move', active: active <= 0 ? count - 1 : active - 1 } : { kind: 'stay' };
		case 'PageDown':
			return count ? { kind: 'move', active: Math.min(count - 1, Math.max(active, 0) + PAGE_STEP) } : { kind: 'stay' };
		case 'PageUp':
			return count ? { kind: 'move', active: Math.max(0, active - PAGE_STEP) } : { kind: 'stay' };
		case 'Enter':
			return active >= 0 && active < count
				? { kind: 'run', active, newTab: mods.ctrlKey || mods.metaKey }
				: { kind: 'stay' };
		case 'Escape':
			return { kind: 'close' };
		case 'Tab':
			return { kind: 'stay' };
		default:
			return { kind: 'pass' };
	}
}
