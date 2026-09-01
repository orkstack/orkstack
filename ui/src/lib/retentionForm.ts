// The Admin Execution Retention form's local validation: an hours field must
// parse to a positive, finite number before the browser ever asks
// UpdateExecutionRetention for anything. This mirrors the engine's own rule
// (internal/api/system.go: a non-positive duration is rejected with
// InvalidArgument before any call leaves the process) at the layer above it,
// so a bad value never reaches an RPC — the same "refuse locally first" shape
// $lib/namespaceRules.ts uses for the Namespaces form.

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
