// Times in words. A relative time is how long ago, in the coarsest unit that
// still says it ("2 min ago"); its exact instant is always one focus or hover
// away (TimeAgo.svelte), in the reader's own locale and zone.

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

export function relativeTime(at: Date, now: number = Date.now()): string {
	const ms = now - at.getTime();
	if (!Number.isFinite(ms)) return '—';
	if (ms < 0) return 'just now';
	if (ms < 45_000) return 'just now';
	if (ms < HOUR) return `${Math.max(1, Math.round(ms / MINUTE))} min ago`;
	if (ms < DAY) return `${Math.round(ms / HOUR)} h ago`;
	const days = Math.round(ms / DAY);
	if (days < 7) return days === 1 ? 'yesterday' : `${days} days ago`;
	return at.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: at.getFullYear() === new Date(now).getFullYear() ? undefined : 'numeric' });
}

/** "Oct 25, 09:12": an instant in a sentence. */
export function shortInstant(at: { seconds: bigint; nanos: number } | undefined): string {
	if (!at) return '—';
	const d = new Date(Number(at.seconds) * 1000);
	return `${d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}, ${d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false })}`;
}

export function exactTime(at: Date): string {
	return at.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
}
