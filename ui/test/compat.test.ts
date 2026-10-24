import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import {
	callerHref,
	callerLabel,
	callersCommand,
	callersFooter,
	changeWord,
	compatCommand,
	composers,
	followers,
	notCompatible,
	pinsLabel,
	promoteBlocked,
	promoteCommand,
	verdictChipClass,
	verdictLine,
	verdictWord
} from '../src/lib/compat';
import {
	InterfaceChange,
	InterfaceFindingSchema,
	InterfaceVerdict,
	OrkflowCallerKind,
	OrkflowCallerSchema,
	file_orkstack_v1_orkflow_service
} from '../src/lib/gen/orkstack/v1/orkflow_service_pb';

function enumValues(name: string): number[] {
	const e = file_orkstack_v1_orkflow_service.enums.find((x) => x.name === name);
	if (!e) throw new Error(`enum ${name} not found`);
	return e.values.map((v) => v.number);
}

test('verdictWord covers every declared verdict', () => {
	const words = enumValues('InterfaceVerdict').map((v) => verdictWord(v as InterfaceVerdict));
	expect(words).toEqual(['', 'compatible', 'breaking', 'unknown']);
	expect(verdictChipClass(InterfaceVerdict.BREAKING)).toContain('text-red-strong');
	expect(verdictChipClass(InterfaceVerdict.COMPATIBLE)).toContain('text-ink');
	expect(verdictChipClass(InterfaceVerdict.UNKNOWN)).toContain('text-ink-muted');
});

test('changeWord is the CLI CHANGE column for every declared change', () => {
	const values = enumValues('InterfaceChange');
	for (const v of values) {
		const word = changeWord(v as InterfaceChange);
		if (v === InterfaceChange.UNSPECIFIED) {
			expect(word).toBe('');
		} else {
			expect(/^[A-Z_]+$/.test(word)).toBe(true);
		}
	}
	expect(changeWord(InterfaceChange.NOW_REQUIRED)).toBe('NOW_REQUIRED');
	expect(changeWord(InterfaceChange.OUTPUTS_UNDECLARED)).toBe('OUTPUTS_UNDECLARED');
});

test('promoteBlocked: comparing, breaking unacknowledged, acknowledged, unknown, failed', () => {
	expect(promoteBlocked({ comparing: true, acknowledged: true })).toBe(true);
	expect(promoteBlocked({ comparing: false, verdict: InterfaceVerdict.BREAKING, acknowledged: false })).toBe(true);
	expect(promoteBlocked({ comparing: false, verdict: InterfaceVerdict.BREAKING, acknowledged: true })).toBe(false);
	expect(promoteBlocked({ comparing: false, verdict: InterfaceVerdict.UNKNOWN, acknowledged: false })).toBe(false);
	expect(promoteBlocked({ comparing: false, verdict: InterfaceVerdict.COMPATIBLE, acknowledged: false })).toBe(false);
	// A comparison that could not be read never blocks: the engine still checks.
	expect(promoteBlocked({ comparing: false, verdict: undefined, acknowledged: false })).toBe(false);
});

const caller = (init: Partial<{
	kind: OrkflowCallerKind;
	name: string;
	id: string;
	callerVersion: string;
	pinnedVersion: string;
	unavailableReason: string;
}>) => create(OrkflowCallerSchema, init);

test('followers excludes pinned and unavailable rows and every composer', () => {
	const rows = [
		caller({ kind: OrkflowCallerKind.ORKFLOW, name: 'checkout', callerVersion: '4.2.0', pinnedVersion: '2.1.0' }),
		caller({ kind: OrkflowCallerKind.TRIGGER, id: 'trg-1' }),
		caller({ kind: OrkflowCallerKind.TRIGGER, id: 'trg-2', pinnedVersion: '2.1.0' }),
		caller({ kind: OrkflowCallerKind.SCHEDULE, name: 'nightly' }),
		caller({ kind: OrkflowCallerKind.SCHEDULE, name: 'pinned', pinnedVersion: '2.1.0' }),
		caller({ kind: OrkflowCallerKind.SCHEDULE, name: 'lost', unavailableReason: 'describe failed' })
	];
	expect(followers(rows).map(callerLabel)).toEqual(['trg-1', 'nightly']);
	expect(composers(rows).map(callerLabel)).toEqual(['checkout']);
	expect(rows.map(pinsLabel)).toEqual(['2.1.0', 'current', '2.1.0', 'current', '2.1.0', 'unavailable']);
	expect(
		callersFooter({ callers: rows, allVersions: false, scannedVersions: 3 })
	).toEqual([
		'Scanned the current version of 3 orkflows (--all-versions scans every stored version).',
		'Moved by a promotion (they follow the current version): triggers 1, schedules 1.',
		'Not moved by a promotion (they pin a version): composing orkflows 1, triggers 1, schedules 1.',
		'schedule lost: version unavailable (describe failed)'
	]);
});

test('callerHref keeps the namespace for all three kinds', () => {
	expect(
		callerHref(caller({ kind: OrkflowCallerKind.ORKFLOW, name: 'checkout', callerVersion: '4.2.0' }), 'release-block', 'payments')
	).toBe('/orkflows/checkout?version=4.2.0&ns=payments');
	expect(callerHref(caller({ kind: OrkflowCallerKind.TRIGGER, id: 'trg-1' }), 'release-block', 'payments')).toBe(
		'/orkflows/release-block?tab=triggers&ns=payments'
	);
	expect(callerHref(caller({ kind: OrkflowCallerKind.SCHEDULE, name: 'n' }), 'release-block', '')).toBe(
		'/orkflows/release-block?tab=schedules&ns=default'
	);
});

test('the three commands state the namespace and quote every argument', () => {
	expect(compatCommand('', "it's", '2.1.0', '3.0.0')).toBe(
		`orkctl --namespace 'default' 'orkflow' 'compat' 'it'"'"'s' '2.1.0' '3.0.0'`
	);
	expect(callersCommand('payments', 'release-block', true)).toBe(
		`orkctl --namespace 'payments' 'orkflow' 'callers' 'release-block' '--all-versions'`
	);
	expect(callersCommand('payments', 'release-block', false)).toBe(
		`orkctl --namespace 'payments' 'orkflow' 'callers' 'release-block'`
	);
	expect(promoteCommand('payments', "o'k", '3.0.0', true)).toBe(
		`orkctl --namespace 'payments' 'orkflow' 'promote' 'o'"'"'k' '3.0.0' '--acknowledge-breaking'`
	);
	expect(promoteCommand('payments', 'release-block', '3.0.0', false)).not.toContain('acknowledge');
});

test('notCompatible and verdictLine read the engine findings verbatim', () => {
	const findings = [
		create(InterfaceFindingSchema, { field: 'inputs.image', verdict: InterfaceVerdict.COMPATIBLE }),
		create(InterfaceFindingSchema, { field: 'inputs.region', verdict: InterfaceVerdict.BREAKING, message: 'm1' }),
		create(InterfaceFindingSchema, { field: 'outputs', verdict: InterfaceVerdict.UNKNOWN, message: 'why' })
	];
	expect(notCompatible(findings).map((f) => f.field)).toEqual(['inputs.region', 'outputs']);
	expect(verdictLine(InterfaceVerdict.BREAKING, findings)).toBe('breaking — 1 breaking change');
	expect(verdictLine(InterfaceVerdict.UNKNOWN, findings)).toBe('unknown — why');
	expect(verdictLine(InterfaceVerdict.COMPATIBLE, [])).toBe('compatible');
});
