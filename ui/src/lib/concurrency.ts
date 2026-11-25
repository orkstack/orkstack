// The words of a concurrency limit (E88), shared by the run page's "Queued
// for key" card and the Orkflow page's Concurrency fact. Each one is the
// CLI's, so a person reading `orkctl run get` or `orkctl orkflow get` beside
// the page reads the same thing (docs/UI_CLI_PARITY.md):
//   - queuedHeadline and queuedPlace: cmd/orkctl/run_queued.go, queuedLine;
//   - concurrencyLabel: cmd/orkctl/orkflow.go, concurrencyLabel.
// A limit's note is the server's text and is shown as it comes, never
// composed here.
import type { ConcurrencyQueued } from '#lib/gen/orkstack/v1/run_pb.ts';
import { ConcurrencyQueueCause } from '#lib/gen/orkstack/v1/run_pb.ts';
import type { ConcurrencySpec } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
import { ConcurrencyStrategy } from '#lib/gen/orkstack/v1/orkflow_pb.ts';

/** ordinal is 1st, 2nd, 3rd, 4th…, the teens as 11th, 12th, 13th. */
export function ordinal(n: number): string {
	const teen = n % 100 >= 11 && n % 100 <= 13;
	const suffix = teen ? 'th' : n % 10 === 1 ? 'st' : n % 10 === 2 ? 'nd' : n % 10 === 3 ? 'rd' : 'th';
	return `${n}${suffix}`;
}

/** queuedHeadline says why the run waits for a slot of its key. */
export function queuedHeadline(q: ConcurrencyQueued): string {
	switch (q.cause) {
		case ConcurrencyQueueCause.UNPAUSE:
			return `Waiting again for key ${q.key} after an unpause`;
		case ConcurrencyQueueCause.RESUME:
			return `Queued for key ${q.key} after a resume`;
		case ConcurrencyQueueCause.FAILING:
			return `Waiting for its concurrency slot to run its failure handlers (key ${q.key})`;
		default:
			return `Queued for key ${q.key}`;
	}
}

/** waitsOnArrival says the run waits because it arrived to a full key, not again after an unpause, a resume or a failure. */
export function waitsOnArrival(q: ConcurrencyQueued): boolean {
	return q.cause === ConcurrencyQueueCause.ARRIVAL || q.cause === ConcurrencyQueueCause.UNSPECIFIED;
}

/** queuedPlace is where it stands in its key's line, when it could be read. */
export function queuedPlace(q: ConcurrencyQueued): string {
	return q.positionKnown ? `${ordinal(q.position)} in line` : 'place unknown';
}

/** queuedBehind is the runs holding the key's slots, as far as the run names them; null when it names none. */
export function queuedBehind(q: ConcurrencyQueued): { runs: string[]; more: number; asOfArrival: boolean } | null {
	if (q.holdersAhead.length === 0) return null;
	return { runs: [...q.holdersAhead], more: Math.max(0, q.holdersCount - q.holdersAhead.length), asOfArrival: q.holdersAsOfArrival };
}

/**
 * shortDuration is a whole number of seconds as the CLI prints a timeout
 * (cmd/orkctl, timeoutLabel and shortDuration): Go's Duration form, its
 * trailing zero minutes and seconds trimmed, so 2h, 30m, 1h0m5s and 1m30s.
 */
export function shortDuration(seconds: number): string {
	const h = Math.floor(seconds / 3600);
	const m = Math.floor((seconds % 3600) / 60);
	const sec = seconds % 60;
	let out = `${h > 0 ? `${h}h` : ''}${h > 0 || m > 0 ? `${m}m` : ''}${sec}s`;
	if (out.endsWith('m0s')) out = out.slice(0, -2);
	if (out.endsWith('h0m')) out = out.slice(0, -2);
	return out;
}

/** concurrencyLabel is a declared limit in one sentence. */
export function concurrencyLabel(spec: ConcurrencySpec): string {
	const key = spec.key ? `key ${spec.key}` : "this Orkflow's own key";
	const seconds = Number(spec.queueTimeoutSeconds);
	const upTo = seconds > 0 ? shortDuration(seconds) : "the deployment's default";
	// Only queue makes a run wait; any other strategy is named as the DSL writes it.
	const rest =
		spec.onLimit === ConcurrencyStrategy.QUEUE || spec.onLimit === ConcurrencyStrategy.UNSPECIFIED
			? `the rest wait in line up to ${upTo}${spec.maxQueued > 0 ? `, at most ${spec.maxQueued}` : ''}`
			: `on_limit ${ConcurrencyStrategy[spec.onLimit]?.toLowerCase() ?? String(spec.onLimit)}`;
	return `limit ${spec.limit} on ${key}; ${rest}`;
}
