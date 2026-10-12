// Interface compatibility (docs/UI_CLI_PARITY.md "Interface compatibility"): the only logic
// behind the verdict chips, the Compare interfaces panel, the promote
// dialog's acknowledgement and the Callers section. Every verdict, finding
// and sentence is the ENGINE's; this file only chooses words and classes for
// them and builds the copyable commands, so the UI and `orkctl` cannot
// disagree about what breaks.
import {
	InterfaceChange,
	InterfaceVerdict,
	OrkflowCallerKind,
	type InterfaceFinding,
	type OrkflowCaller
} from './gen/orkstack/v1/orkflow_service_pb';
import { withNamespace } from './namespace';
import { runCommand } from './runDetail';

/** The verdict in the UI's words; '' for UNSPECIFIED (nothing was compared). */
export function verdictWord(v: InterfaceVerdict): 'compatible' | 'breaking' | 'unknown' | '' {
	switch (v) {
		case InterfaceVerdict.COMPATIBLE:
			return 'compatible';
		case InterfaceVerdict.BREAKING:
			return 'breaking';
		case InterfaceVerdict.UNKNOWN:
			return 'unknown';
		default:
			return '';
	}
}

const chipBase = 'inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium';

/** breaking: danger tokens; unknown: muted; compatible: info. */
export function verdictChipClass(v: InterfaceVerdict): string {
	switch (v) {
		case InterfaceVerdict.BREAKING:
			return `${chipBase} border-danger-200 bg-danger-50 text-danger-700`;
		case InterfaceVerdict.UNKNOWN:
			return `${chipBase} border-border bg-surface-sunken text-fg-muted`;
		case InterfaceVerdict.COMPATIBLE:
			return `${chipBase} border-info-200 bg-info-50 text-info-700`;
		default:
			return chipBase;
	}
}

/** The CLI's CHANGE column: 'ADDED', 'NOW_REQUIRED', …; '' for UNSPECIFIED. */
export function changeWord(c: InterfaceChange): string {
	const name = InterfaceChange[c];
	return name === undefined || c === InterfaceChange.UNSPECIFIED ? '' : name;
}

/** Every finding that is not COMPATIBLE, in the engine's order. */
export function notCompatible(findings: InterfaceFinding[]): InterfaceFinding[] {
	return findings.filter((f) => f.verdict !== InterfaceVerdict.COMPATIBLE);
}

/** The number of BREAKING findings. */
export function breakingCount(findings: InterfaceFinding[]): number {
	return findings.filter((f) => f.verdict === InterfaceVerdict.BREAKING).length;
}

/** "1 breaking change" / "2 breaking changes" — dsl.CountNoun's rendering. */
export function countNoun(n: number, singular: string, plural: string): string {
	return n === 1 ? `1 ${singular}` : `${n} ${plural}`;
}

/**
 * The verdict line the CLI prints last: "breaking — 2 breaking changes",
 * "unknown — <first UNKNOWN message>", or "compatible".
 */
export function verdictLine(verdict: InterfaceVerdict, findings: InterfaceFinding[]): string {
	switch (verdict) {
		case InterfaceVerdict.BREAKING:
			return `breaking — ${countNoun(breakingCount(findings), 'breaking change', 'breaking changes')}`;
		case InterfaceVerdict.UNKNOWN: {
			const first = findings.find((f) => f.verdict === InterfaceVerdict.UNKNOWN);
			return `unknown — ${first?.message ?? ''}`;
		}
		case InterfaceVerdict.COMPATIBLE:
			return 'compatible';
		default:
			return '';
	}
}

/**
 * promoteBlocked holds the promote button: while the comparison is still
 * being read, or while a BREAKING verdict is unacknowledged. A comparison
 * that FAILED does not block — the engine still checks it on promote.
 */
export function promoteBlocked(args: {
	comparing: boolean;
	verdict?: InterfaceVerdict;
	acknowledged: boolean;
}): boolean {
	if (args.comparing) return true;
	return args.verdict === InterfaceVerdict.BREAKING && !args.acknowledged;
}

/** Triggers and schedules that follow the current version — what a promotion moves. */
export function followers(callers: OrkflowCaller[]): OrkflowCaller[] {
	return callers.filter(
		(c) =>
			(c.kind === OrkflowCallerKind.TRIGGER || c.kind === OrkflowCallerKind.SCHEDULE) &&
			c.pinnedVersion === '' &&
			c.unavailableReason === ''
	);
}

/** Schedules whose pinned version could not be read for this listing. */
export function unavailableSchedules(callers: OrkflowCaller[]): OrkflowCaller[] {
	return callers.filter((c) => c.kind === OrkflowCallerKind.SCHEDULE && c.unavailableReason !== '');
}

/** Composing Orkflow rows: they always pin a version and are never moved. */
export function composers(callers: OrkflowCaller[]): OrkflowCaller[] {
	return callers.filter((c) => c.kind === OrkflowCallerKind.ORKFLOW);
}

/** The KIND column: 'orkflow', 'trigger', 'schedule'. */
export function callerKindWord(c: OrkflowCaller): string {
	switch (c.kind) {
		case OrkflowCallerKind.ORKFLOW:
			return 'orkflow';
		case OrkflowCallerKind.TRIGGER:
			return 'trigger';
		case OrkflowCallerKind.SCHEDULE:
			return 'schedule';
		default:
			return '';
	}
}

/** The CALLER column: the name, or the id when the name is empty. */
export function callerLabel(c: OrkflowCaller): string {
	return c.name || c.id;
}

/** The PINS column: a version, 'current', or 'unavailable' — never a guess. */
export function pinsLabel(c: OrkflowCaller): string {
	if (c.unavailableReason) return 'unavailable';
	return c.pinnedVersion || 'current';
}

/** Where a caller's link goes, with the namespace stated. */
export function callerHref(c: OrkflowCaller, target: string, ns: string): string {
	switch (c.kind) {
		case OrkflowCallerKind.ORKFLOW:
			return withNamespace(
				`/orkflows/${encodeURIComponent(c.name)}?version=${encodeURIComponent(c.callerVersion)}`,
				ns
			);
		case OrkflowCallerKind.TRIGGER:
			return withNamespace(`/orkflows/${encodeURIComponent(target)}?tab=triggers`, ns);
		case OrkflowCallerKind.SCHEDULE:
			return withNamespace(`/orkflows/${encodeURIComponent(target)}?tab=schedules`, ns);
		default:
			return withNamespace(`/orkflows/${encodeURIComponent(target)}`, ns);
	}
}

/** `orkctl --namespace NS orkflow compat NAME FROM TO`. */
export function compatCommand(ns: string, name: string, from: string, to: string): string {
	return runCommand(ns, ['orkflow', 'compat', name, from, to]);
}

/** `orkctl --namespace NS orkflow callers NAME [--all-versions]`. */
export function callersCommand(ns: string, name: string, all: boolean): string {
	return runCommand(ns, ['orkflow', 'callers', name, ...(all ? ['--all-versions'] : [])]);
}

/** `orkctl --namespace NS orkflow promote NAME VERSION [--acknowledge-breaking]`. */
export function promoteCommand(ns: string, name: string, version: string, ack: boolean): string {
	return runCommand(ns, ['orkflow', 'promote', name, version, ...(ack ? ['--acknowledge-breaking'] : [])]);
}

/** The callers footer, the CLI's sentences: scanned, moved, not moved. */
export function callersFooter(resp: {
	callers: OrkflowCaller[];
	allVersions: boolean;
	scannedVersions: number;
}): string[] {
	let movedTriggers = 0,
		movedSchedules = 0,
		composing = 0,
		pinnedTriggers = 0,
		pinnedSchedules = 0;
	for (const c of resp.callers) {
		if (c.kind === OrkflowCallerKind.ORKFLOW) composing++;
		else if (c.kind === OrkflowCallerKind.TRIGGER) {
			if (c.pinnedVersion === '') movedTriggers++;
			else pinnedTriggers++;
		} else if (c.kind === OrkflowCallerKind.SCHEDULE && c.unavailableReason === '') {
			if (c.pinnedVersion === '') movedSchedules++;
			else pinnedSchedules++;
		}
	}
	const lines = [
		resp.allVersions
			? `Scanned every stored version of every orkflow: ${resp.scannedVersions} definitions.`
			: `Scanned the current version of ${resp.scannedVersions} orkflows (--all-versions scans every stored version).`,
		`Moved by a promotion (they follow the current version): triggers ${movedTriggers}, schedules ${movedSchedules}.`,
		`Not moved by a promotion (they pin a version): composing orkflows ${composing}, triggers ${pinnedTriggers}, schedules ${pinnedSchedules}.`
	];
	for (const c of unavailableSchedules(resp.callers)) {
		lines.push(`schedule ${c.name}: version unavailable (${c.unavailableReason})`);
	}
	return lines;
}
