import type { Timestamp } from '@bufbuild/protobuf/wkt';
import {
	ReleaseDrainState,
	ReleaseRole,
	type ListReleasesResponse,
	type PromoteReleaseResponse,
	type RampReleaseResponse,
	type Release
} from './gen/orkstack/v1/service_pb';
import { shellQuote } from './runDetail';
import { fmtTimestamp } from './status';

// Admin → Releases' rendering rules, shared by the page and its tests
// (orkctl-release §8). They mirror `orkctl release status` (cmd/orkctl/release.go):
// a count the run index could not give is `unknown`, never 0, and "drained"
// is said only when the release may be stopped.

function plural(n: number, one: string, many: string): string {
	return n === 1 ? `1 ${one}` : `${n} ${many}`;
}

/**
 * A ramp percent as `orkctl` prints it: the proto field is a float32, and the
 * CLI formats it with Go's shortest float32 form (strconv.FormatFloat(p, 'g',
 * -1, 32)). A JS number is a double, so a ramp set outside OrkStack at 0.1
 * would otherwise read 0.10000000149011612. The shortest decimal that rounds
 * back to the same float32 is what both surfaces show.
 */
export function percentText(p: number): string {
	if (!Number.isFinite(p)) return String(p);
	const single = Math.fround(p);
	let shortest = single;
	for (let digits = 1; digits <= 9; digits++) {
		const candidate = Number(single.toPrecision(digits));
		if (Math.fround(candidate) === single) {
			shortest = candidate;
			break;
		}
	}
	if (shortest === 0) return '0';
	// Go's 'g' with shortest precision switches to an exponent below 1e-4
	// (and at 1e21 and above, which a percent never reaches), with at least two
	// exponent digits: 1e-05, not JS's 0.00001.
	const exponent = Math.floor(Math.log10(Math.abs(shortest)));
	if (exponent < -4 || exponent >= 21) {
		const [mantissa, exp] = shortest.toExponential().split('e');
		const sign = exp.startsWith('-') ? '-' : '+';
		const digits = exp.replace(/^[+-]/, '').padStart(2, '0');
		return `${mantissa}e${sign}${digits}`;
	}
	return String(shortest);
}

/** ROLE: current, ramping P%, previous, candidate. */
export function roleLabel(release: Release): string {
	switch (release.role) {
		case ReleaseRole.CURRENT:
			return 'current';
		case ReleaseRole.RAMPING:
			return `ramping ${percentText(release.rampPercent)}%`;
		case ReleaseRole.PREVIOUS:
			return 'previous';
		case ReleaseRole.CANDIDATE:
			return 'candidate';
		default:
			return 'unknown';
	}
}

/** RUNS: the count, or `unknown` when the run index could not be asked. */
export function runsLabel(release: Release): string {
	return release.runsUnavailableReason ? 'unknown' : String(release.runsInFlight);
}

export function openPinCount(release: Release): number {
	return release.orkers.reduce((n, o) => n + o.pins.length, 0);
}

/** STATE: the words of `orkctl release status`'s STATE column. */
export function stateLabel(release: Release, fmt: (ts: Timestamp | undefined) => string = fmtTimestamp): string {
	const n = release.blockers.length;
	switch (release.role) {
		case ReleaseRole.CURRENT:
			return 'serving';
		case ReleaseRole.RAMPING:
			return n === 0 ? 'serving' : `at risk (${plural(n, 'blocker', 'blockers')})`;
		case ReleaseRole.CANDIDATE:
			return n === 0 ? 'ready' : `not ready (${plural(n, 'blocker', 'blockers')})`;
	}
	if (release.drained) return 'drained';
	if (release.drainState === ReleaseDrainState.DRAINED) {
		if (openPinCount(release) > 0) return 'drained by the cluster, pins still open';
		if (!release.runsUnavailableReason && release.runsInFlight > 0) return 'drained by the cluster, runs still counted';
		return 'drained by the cluster, Orker list incomplete';
	}
	if (release.drainState === ReleaseDrainState.DRAINING) {
		return release.drainCheckedAt ? `draining (checked ${fmt(release.drainCheckedAt)})` : 'draining';
	}
	return 'not yet checked';
}

/** Promote is offered on a candidate and on the ramping release. */
export function canPromote(release: Release): boolean {
	return release.role === ReleaseRole.CANDIDATE || release.role === ReleaseRole.RAMPING;
}

/** Roll back is offered on a release that was current before. */
export function canRollBack(release: Release): boolean {
	return release.role === ReleaseRole.PREVIOUS;
}

/** Ramp is offered on every release that is not current. */
export function canRamp(release: Release): boolean {
	return (
		release.role === ReleaseRole.CANDIDATE ||
		release.role === ReleaseRole.PREVIOUS ||
		release.role === ReleaseRole.RAMPING
	);
}

export type ReleaseVerb = 'promote' | 'rollback' | 'ramp';

/** The CLI equivalent, every value shell-quoted. */
export function releaseCommand(verb: ReleaseVerb, build: string, percent?: number): string {
	if (verb === 'ramp') return `orkctl release ramp ${shellQuote(build)} --percent ${percent ?? 0}`;
	return `orkctl release ${verb} ${shellQuote(build)}`;
}

/** What still holds a release that takes no new runs: "3 runs, 1 open pin", or "". */
export function holdsSummary(release: Release): string {
	const pins = openPinCount(release);
	const runs = release.runsUnavailableReason ? 0 : release.runsInFlight;
	const parts: string[] = [];
	if (runs > 0) parts.push(plural(runs, 'run', 'runs'));
	if (pins > 0) parts.push(plural(pins, 'open pin', 'open pins'));
	return parts.join(', ');
}

/** A release with runs or pins worth disclosing. */
export function hasHolds(release: Release): boolean {
	return release.runs.length > 0 || openPinCount(release) > 0;
}

/** The `current: none` sentence of `orkctl release status`. */
export function noCurrentSentence(engines: number): string {
	return `none — new runs go to the ${plural(engines, 'engine', 'engines')} started without ORK_BUILD_ID; a release takes nothing until it is promoted (docs/DEPLOY.md §3 step 0)`;
}

export const versioningOffSentence =
	'No release of this deployment has registered with the cluster, so versioning is off: every engine and Orker runs without ORK_BUILD_ID. Set ORK_BUILD_ID on both binaries to start one (docs/DEPLOY.md §1).';

function findRelease(read: ListReleasesResponse | null, build: string): Release | undefined {
	return read?.releases.find((r) => r.buildId === build);
}

const unconfirmed =
	'The change is recorded, but the cluster had not applied it to new runs after 10s; "orkctl release status" shows when it has.';

function runsFinish(previous: Release | undefined, build: string): string {
	if (!previous || previous.runsUnavailableReason) return `the runs executing on ${build} finish there`;
	if (previous.runsInFlight === 1) return `the 1 run executing on ${build} finishes there`;
	return `the ${previous.runsInFlight} runs executing on ${build} finish there`;
}

/** §7.2/§7.3's result sentence for a promote or a rollback. */
export function promoteResult(
	read: ListReleasesResponse | null,
	build: string,
	res: PromoteReleaseResponse,
	rollback: boolean
): string {
	if (!res.changed) return `release ${build} is already current; nothing changed.`;
	const target = findRelease(read, build);
	const ramp = target?.role === ReleaseRole.RAMPING ? ` (its ${percentText(target.rampPercent)}% ramp ends)` : '';
	const verb = rollback ? 'current again' : 'current';
	const prev = res.previousBuildId;
	let text = prev
		? `release ${build} is ${verb} (was ${prev})${ramp}. New runs start on ${build}; ${runsFinish(findRelease(read, prev), prev)}. Keep ${prev} running until "orkctl release wait ${prev}" exits 0.`
		: `release ${build} is ${verb}${ramp}. New runs start on ${build}.`;
	if (!res.routingConfirmed) text += ' ' + unconfirmed;
	return text;
}

/** §7.4's result sentence for a ramp or a stopped ramp. */
export function rampResult(
	read: ListReleasesResponse | null,
	build: string,
	percent: number,
	res: RampReleaseResponse
): string {
	if (!res.changed) return `release ${build} already takes ${percent}% of new runs; nothing changed.`;
	let text: string;
	if (percent === 0) {
		const r = findRelease(read, build);
		const runs =
			!r || r.runsUnavailableReason
				? 'the runs it started during the ramp finish on it'
				: r.runsInFlight === 1
					? 'the 1 run it started during the ramp finishes on it'
					: `the ${r.runsInFlight} runs it started during the ramp finish on it`;
		text = `release ${build} no longer takes new runs; ${runs}.`;
	} else {
		const current = read?.releases.find((r) => r.role === ReleaseRole.CURRENT);
		const rest = current
			? `${current.buildId} (current) takes the rest`
			: 'the engines started without ORK_BUILD_ID take the rest';
		const replaced =
			res.previousBuildId && res.previousBuildId !== build
				? ` (${res.previousBuildId} was ramping at ${percentText(res.previousPercent)}%; its runs finish on it)`
				: '';
		text = `release ${build} now takes ${percent}% of new runs; ${rest}${replaced}.`;
	}
	if (!res.routingConfirmed) text += ' ' + unconfirmed;
	return text;
}

/** A ramp percent the dialog accepts: an integer 1–100. */
export function validRampPercent(raw: string): number | null {
	if (!/^\d+$/.test(raw.trim())) return null;
	const n = Number(raw.trim());
	return n >= 1 && n <= 100 ? n : null;
}
