// The Admin retention forms' local validation, shared by the deployment
// maximum (this page) and a namespace's own run retention
// ($lib/namespaceRules.ts): an hours field must parse to a positive, finite
// number before the browser ever asks UpdateRetentionPolicy or
// CreateNamespace/UpdateNamespace for anything. This mirrors the engine's
// own rule (internal/api/system.go: a non-positive duration is rejected
// with InvalidArgument before any call leaves the process) at the layer
// above it, so a bad value never reaches an RPC.

/**
 * parseRetentionHours turns the form's raw text into a positive number of
 * hours, or the message to show beside the field. It never throws.
 */
export function parseRetentionHours(raw: string): { hours: number } | { error: string } {
	const trimmed = raw.trim();
	if (trimmed === '') {
		return { error: 'Enter a positive number of hours.' };
	}
	const parsed = Number(trimmed);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		return { error: 'Enter a positive number of hours.' };
	}
	return { hours: parsed };
}

/** hoursToMs converts a positive number of hours to milliseconds, for
 * `@bufbuild/protobuf/wkt`'s `durationFromMs`. */
export function hoursToMs(hours: number): number {
	return hours * 60 * 60 * 1000;
}

/**
 * retentionHours is a stored duration in the form's unit, to prefill the
 * field with the value in force: '' when there is none.
 */
export function retentionHours(d: { seconds: bigint; nanos: number } | undefined): string {
	if (!d) return '';
	const hours = (Number(d.seconds) + d.nanos / 1e9) / 3600;
	if (!(hours > 0)) return '';
	return String(Number(hours.toFixed(6)));
}

/**
 * isLowering says whether a new maximum (hours) is below the one in force.
 * Lowering retires stored runs on the next cleanup pass, so it is the one
 * change the page asks to be confirmed; raising, or setting the same value,
 * is not.
 */
export function isLowering(current: { seconds: bigint; nanos: number } | undefined, hours: number): boolean {
	if (!current) return false;
	return hoursToMs(hours) < Number(current.seconds) * 1000 + current.nanos / 1e6;
}

/**
 * lowerConsequence is what the confirmation says lowering does, naming both
 * values: in words, not an "are you sure?".
 */
export function lowerConsequence(currentLabel: string, newHours: number): string {
	return (
		`Lowering the maximum run retention from ${currentLabel} to ${newHours}h reclaims, on the next ` +
		`cleanup pass, the logs, attempt records, snapshots and commits of every finished run older than ` +
		`${newHours}h in every namespace that sets no run retention of its own. This cannot be undone.`
	);
}

/** confirmMatches: the reader typed the new value itself, numerically equal. */
export function confirmMatches(typed: string, hours: number): boolean {
	const trimmed = typed.trim();
	return trimmed !== '' && Number(trimmed) === hours;
}
