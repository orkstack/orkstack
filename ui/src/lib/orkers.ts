import type { Timestamp } from '@bufbuild/protobuf/wkt';
import {
	MetadataProbeOutcome,
	ProfileCordonReason,
	ProfileCordon_Cause,
	ProfileKind,
	type MetadataProbe,
	type Orker,
	type OrkerProfile,
	type ProfileCordon
} from './gen/orkstack/v1/activity_pb';
import { shellQuote } from './runDetail';

// The Orker inventory's rendering rules, shared by Admin → Pools and its
// tests (docs/DEPLOY.md "Removing an Orker safely: cordon and drain"). They mirror `orkctl orker list`
// (cmd/orkctl/orker.go): an Orker that could not be asked is unreachable,
// never idle, and none of its numbers are drawn.

export type OrkerState = 'ready' | 'cordoned' | 'profile-drift' | 'profile-out' | 'store-outage' | 'profile-unavailable' | 'stopping' | 'unreachable';

/**
 * An operator's cordon holds: it always carries a reason. An Orker of a release
 * with no store check and no stop grace only ever reports that cause, so a row
 * with neither reads as it always has.
 */
export function operatorCordoned(orker: Orker): boolean {
	if (!orker.cordoned) return false;
	return orker.cordonReason !== '' || (!orker.storeOutage && !orker.stopping && orker.profileCordons.length === 0);
}

/**
 * The row's one state: a stopping Orker never takes work again, so it wins;
 * then an operator's cordon; then a drifted profile, which only a restart
 * ends and which takes the pool out of service on this Orker; then a store
 * outage and an unavailable profile, which both end by themselves. Each
 * cause the state does not name keeps its own sentence on the row.
 */
export function orkerState(orker: Orker): OrkerState {
	if (orker.unavailableReason) return 'unreachable';
	if (orker.stopping) return 'stopping';
	if (operatorCordoned(orker)) return 'cordoned';
	const profile = profileState(orker);
	if (profile && profile !== 'profile-unavailable') return profile;
	if (orker.storeOutage) return 'store-outage';
	return profile ?? 'ready';
}

/**
 * What the Orker's profile causes alone make of it: a drift; else a cause
 * this release cannot read, which keeps the Orker out of its pool as the
 * engine counts it (internal/engine/pool_census.go), never a wait; else an
 * unavailable profile; null when no profile holds it.
 */
export function profileState(orker: Orker): 'profile-drift' | 'profile-out' | 'profile-unavailable' | null {
	const causes = orker.profileCordons;
	if (causes.some((c) => c.cause === ProfileCordon_Cause.DRIFT)) return 'profile-drift';
	if (causes.some((c) => c.cause !== ProfileCordon_Cause.UNAVAILABLE)) return 'profile-out';
	return causes.length > 0 ? 'profile-unavailable' : null;
}

const STATE_LABELS: Partial<Record<OrkerState, string>> = {
	'store-outage': 'object storage unreachable',
	'profile-drift': 'profile drifted',
	'profile-out': 'profile out of service',
	'profile-unavailable': 'profile unavailable'
};

/** The state in words, for the row's stamp. */
export function orkerStateLabel(state: OrkerState): string {
	return STATE_LABELS[state] ?? state;
}

/** What a store outage's stamp says, beside an operator's `cordoned`. */
export const STORE_OUTAGE_LABEL = 'object storage unreachable';

/** PINS: open/max for a pin host, `-` for an Orker that hosts none or was not measured. */
export function orkerPins(orker: Orker): string {
	if (orker.unavailableReason || orker.maxConcurrentPins === 0) return '-';
	return `${orker.openPins}/${orker.maxConcurrentPins}`;
}

/** TASKS: the running count, or `-` when nothing was measured. */
export function orkerTasks(orker: Orker): string {
	return orker.unavailableReason ? '-' : String(orker.runningTasks);
}

function plural(n: number, one: string, many: string): string {
	return n === 1 ? `1 ${one}` : `${n} ${many}`;
}

/** "is running 2 tasks and hosts 1 pin" — the cordon dialog's live counts. */
export function orkerLoad(orker: Orker): string {
	return `is running ${plural(orker.runningTasks, 'task', 'tasks')} and hosts ${plural(orker.openPins, 'pin', 'pins')}`;
}

/**
 * The CLI equivalent of the row's state: the cordon that produced a cordoned
 * row, with its reason shell-quoted so a quote in it cannot become shell
 * syntax, the uncordon that produced a ready one, and, for an Orker a store
 * outage, its own stop or a profile alone holds, which no command produced or
 * can clear, the read of it.
 */
export function orkerCommand(orker: Orker): string {
	if (operatorCordoned(orker)) {
		return `orkctl orker cordon ${shellQuote(orker.orkerId)} --reason ${shellQuote(orker.cordonReason)}`;
	}
	if (orker.storeOutage || orker.stopping || orker.profileCordons.length > 0) return `orkctl orker get ${shellQuote(orker.orkerId)}`;
	return `orkctl orker uncordon ${shellQuote(orker.orkerId)}`;
}

/** The command the cordon dialog is about to run, previewed as the reason is typed. */
export function cordonCommand(orkerId: string, reason: string): string {
	return `orkctl orker cordon ${shellQuote(orkerId)} --reason ${shellQuote(reason)}`;
}

/**
 * The sentence a stopping Orker's row carries: when its stop began and when
 * its grace ends, as `orkctl orker get` prints them. An Orker of a release that
 * reports neither keeps the plain sentence.
 */
export function stoppingNote(orker: Orker, fmt: (ts: Timestamp | undefined) => string): string {
	const stop = orker.stopping;
	const since = stop?.since ? ` since ${fmt(stop.since)}` : '';
	const ends = stop?.graceEndsAt ? `; its grace ends ${fmt(stop.graceEndsAt)}` : '';
	return `Stopping${since}: it takes no new work and finishes what it runs, then exits${ends}. A task it hands back runs again on another Orker of its release.`;
}

/** One address's outcome in words, as `orkctl orker get` prints it: "answered (HTTP 200 to GET /)". */
export function metadataProbeText(probe: MetadataProbe): string {
	const outcome =
		probe.outcome === MetadataProbeOutcome.ANSWERED
			? 'answered'
			: probe.outcome === MetadataProbeOutcome.NO_ANSWER
				? 'no answer'
				: probe.outcome === MetadataProbeOutcome.PROBE_FAILED
					? 'probe failed'
					: 'unknown';
	return probe.detail ? `${outcome} (${probe.detail})` : outcome;
}

/**
 * What the Orker's boot metadata probe found that a reader must see: every
 * address that answered or could not be told about, as `orkctl orker list`
 * names them (docs/DEPLOY.md "Keeping tasks away from the host's identity").
 * An address with no answer is none; an Orker that ran no probe has none.
 */
export function metadataConcerns(orker: Orker): string[] {
	return orker.metadataProbes
		.filter((p) => p.outcome !== MetadataProbeOutcome.NO_ANSWER)
		.map((p) => `${p.address} ${metadataProbeText(p)}`);
}

/** Whether an endpoint answered: a task on this Orker could reach the host's identity. */
export function metadataReachable(orker: Orker): boolean {
	return orker.metadataProbes.some((p) => p.outcome === MetadataProbeOutcome.ANSWERED);
}

/** The stamp beside the state when the probe found an answer, or could not tell. */
export const METADATA_REACHABLE_LABEL = 'host identity reachable';
export const METADATA_UNKNOWN_LABEL = 'host identity unchecked';

// ---- Profiles (docs/DEPLOY.md, "Running a device Orker") ----------------

/** A profile's kind as an operator writes it in orker.profiles. */
export function profileKindLabel(kind: ProfileKind): string {
	return kind === ProfileKind.GPU ? 'gpu' : kind === ProfileKind.DEVICES ? 'devices' : 'unknown kind';
}

/** A cause's machine-readable reason, as the API and `orkctl` name it. */
export function profileReasonCode(reason: ProfileCordonReason): string {
	const name = ProfileCordonReason[reason];
	return name && reason !== ProfileCordonReason.UNSPECIFIED ? name : `REASON_${reason}`;
}

const REASON_WORDS: Partial<Record<ProfileCordonReason, string>> = {
	[ProfileCordonReason.SPEC_CHANGED]: 'its device spec changed',
	[ProfileCordonReason.DEVICE_CHANGED]: 'a device was replaced, or changed its numbers or group',
	[ProfileCordonReason.HOOK_BINARY_CHANGED]: 'a hook binary changed',
	[ProfileCordonReason.SPEC_ABSENT]: 'its device spec is gone',
	[ProfileCordonReason.GPU_HOST_LOCK_HELD]: 'another Orker holds the host’s GPU lock',
	[ProfileCordonReason.HOST_LOCK_MISMATCH]: 'the host’s GPU lock is not the one it audited',
	[ProfileCordonReason.PROBE_IMAGE_UNAVAILABLE]: 'its probe image cannot be pulled or run',
	[ProfileCordonReason.HOST_VIEW_CHANGED]: 'a container no longer sees its devices as the Orker does',
	[ProfileCordonReason.ALL_UNITS_UNHEALTHY]: 'none of its units is healthy',
	[ProfileCordonReason.PROBE_IMAGE_MISMATCH]: 'its probe image is not this Orker’s build',
	[ProfileCordonReason.HOOK_BUILD_UNKNOWN]: 'its hook is a build this release does not list',
	[ProfileCordonReason.FOREIGN_UNITS_HELD]: 'containers it did not start hold its units',
	[ProfileCordonReason.DEVICE_ABSENT]: 'a device is not there',
	[ProfileCordonReason.UNITS_BELOW_CEILING]: 'fewer of its units are usable than the per-task gpu cap (max_request.gpu), so the Orker polls for no task',
	[ProfileCordonReason.PROBE_NOT_STARTED]: 'the container engine does not start its unit probes'
};

/**
 * The causes in the order the row reads them, worst first: those that keep
 * the Orker out of its pool (a drift, or a cause this release cannot read),
 * then an unavailable profile; each by reason, then profile.
 */
export function profileCauses(orker: Orker): ProfileCordon[] {
	const rank = (c: ProfileCordon) => (c.cause === ProfileCordon_Cause.DRIFT ? 0 : c.cause === ProfileCordon_Cause.UNAVAILABLE ? 2 : 1);
	return [...orker.profileCordons].sort((a, b) => rank(a) - rank(b) || a.reason - b.reason || a.profile.localeCompare(b.profile));
}

/** The chip a cause puts on its profile, and whether it is a failure (red, triangle) or a wait. */
export function profileCauseChip(cause: ProfileCordon): { word: string; failure: boolean } {
	if (cause.cause === ProfileCordon_Cause.DRIFT) return { word: 'drifted', failure: true };
	if (cause.cause === ProfileCordon_Cause.UNAVAILABLE) return { word: 'unavailable', failure: false };
	return { word: 'out of service', failure: true };
}

/**
 * One cause in words: what was found, its code, and how it ends. A drift ends
 * only with a restart and keeps this Orker out of its pool; an unavailable
 * profile clears itself, and the pool stays in service on its other Orkers.
 */
export function profileCauseNote(cause: ProfileCordon, fmt: (ts: Timestamp | undefined) => string): string {
	const what = REASON_WORDS[cause.reason] ?? 'the Orker found a fault';
	const since = cause.since ? ` since ${fmt(cause.since)}` : '';
	const code = profileReasonCode(cause.reason);
	if (cause.cause === ProfileCordon_Cause.DRIFT) {
		return `Profile ${cause.profile} drifted${since}: ${what} (${code}). It stays out of service on this Orker until the Orker restarts.`;
	}
	if (cause.cause === ProfileCordon_Cause.UNAVAILABLE) {
		return `Profile ${cause.profile} unavailable${since}: ${what} (${code}). Once that is put right, it returns by itself after consecutive good checks.`;
	}
	return `Profile ${cause.profile} out of service${since}: ${what} (${code}), for a cause this release cannot read. The Orker takes no work from its pool; see orkctl orker get.`;
}

/** The cause that stands on one profile, drift first; none when it is in service. */
export function profileCause(orker: Orker, profile: string): ProfileCordon | undefined {
	return profileCauses(orker).find((c) => c.profile === profile);
}

/**
 * A gpu profile's units, as the bar draws them: bound, then usable but free,
 * then neither (unhealthy). Counts never go below zero, whatever a skewed
 * answer says, and a devices profile has none.
 */
export function profileUnits(p: OrkerProfile): { bound: number; free: number; unusable: number; total: number } | null {
	if (p.kind !== ProfileKind.GPU) return null;
	const total = Math.max(0, p.units);
	if (total === 0) return null;
	const usable = Math.min(total, Math.max(0, p.usableUnits));
	const bound = Math.min(usable, Math.max(0, p.boundUnits));
	return { bound, free: usable - bound, unusable: total - usable, total };
}

/** The bar's words: "1 of 2 units in use; 1 unhealthy". */
export function profileUnitsText(p: OrkerProfile): string {
	const u = profileUnits(p);
	if (!u) return '';
	const usable = u.bound + u.free;
	const out = `${u.bound} of ${usable} usable ${usable === 1 ? 'unit' : 'units'} in use`;
	return u.unusable > 0 ? `${out}; ${u.unusable} unhealthy` : out;
}

/**
 * GPU RESERVED: "1 of 2" units, or null for an Orker that reports no gpu
 * capacity (it offers no gpu profile, or predates them), so the column is
 * drawn only where some Orker of the pool has one.
 */
export function orkerGpu(orker: Orker): string | null {
	if (orker.unavailableReason || orker.capacity?.gpu === undefined) return null;
	return `${orker.reserved?.gpu ?? 0n} of ${orker.capacity.gpu}`;
}

/** The most unit cells the bar draws: a gpu profile declares at most 64. */
export const MAX_UNIT_CELLS = 64;
