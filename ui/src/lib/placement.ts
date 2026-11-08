// Where a popover's panel goes, in viewport coordinates. The panel sits in the
// top layer (Popover.svelte), so no scroll container, dialog or stacking
// context can clip it, and the only edge left to respect is the viewport's.

/** Clear space between the trigger and its panel. */
export const PANEL_GAP = 6;
/** The panel never comes closer than this to a viewport edge. */
export const PANEL_EDGE = 8;

export interface AnchorBox {
	left: number;
	right: number;
	top: number;
	bottom: number;
}

export interface PanelPlacement {
	left: number;
	top: number;
	/** What the panel may take of the viewport's height; beyond it, it scrolls. */
	maxHeight: number;
}

const clamp = (value: number, low: number, high: number) => Math.max(low, Math.min(value, Math.max(low, high)));

/**
 * placePanel hangs a panel off its trigger: below it where it fits, else above
 * it where it fits, else on whichever side has more room, scrolling inside
 * what that side offers. It shares its trigger's `align` edge, moved back
 * inside the viewport when that would run past one.
 */
export function placePanel(
	anchor: AnchorBox,
	panel: { width: number; height: number },
	view: { width: number; height: number },
	align: 'start' | 'end' = 'start'
): PanelPlacement {
	const tallest = Math.max(0, view.height - 2 * PANEL_EDGE);
	const roomBelow = clamp(view.height - PANEL_EDGE - anchor.bottom - PANEL_GAP, 0, tallest);
	const roomAbove = clamp(anchor.top - PANEL_GAP - PANEL_EDGE, 0, tallest);
	const below = panel.height <= roomBelow || (panel.height > roomAbove && roomBelow >= roomAbove);
	const maxHeight = below ? roomBelow : roomAbove;
	const height = Math.min(panel.height, maxHeight);
	const top = below ? anchor.bottom + PANEL_GAP : anchor.top - PANEL_GAP - height;
	const left = align === 'end' ? anchor.right - panel.width : anchor.left;
	return {
		left: Math.round(clamp(left, PANEL_EDGE, view.width - PANEL_EDGE - panel.width)),
		// A trigger scrolled out of sight leaves its panel at the nearest edge.
		top: Math.round(clamp(top, PANEL_EDGE, view.height - PANEL_EDGE - height)),
		maxHeight: Math.round(maxHeight)
	};
}

/** What this run itself holds of a pool it waits on, in one sentence; empty when nothing. */
export function waitRoomNote(ownRunning: number, took: string): string {
	const parts = [
		...(ownRunning > 0 ? [`${ownRunning} of the tasks there are this run's`] : []),
		...(took ? [`its finished ones took ${took}`] : [])
	];
	return parts.join('; ');
}
