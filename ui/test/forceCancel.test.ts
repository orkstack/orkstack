import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import {
	ForceCancelCascadeSchema,
	ForceCancelCascadeState,
	ForceCancelExclusionSchema,
	ForceCancelRunBelowSchema,
	ForceCancelRunResponseSchema,
	ForceCancelState,
	ForceCancelSubtreeSchema,
	GetForceCancelResponseSchema
} from '../src/lib/gen/orkstack/v1/run_service_pb';
import { awaited, belowListed, belowWords, cascadeInProgress, cascadeNotes, cascadeWords, forceCancelOffer, graceChoices, inProgress, needsMore, reasonProblem, requestNotes, stateWords } from '../src/lib/forceCancel';

const fc = (init: Parameters<typeof create<typeof GetForceCancelResponseSchema>>[1]) => create(GetForceCancelResponseSchema, init);
const cascade = (init: Parameters<typeof create<typeof ForceCancelCascadeSchema>>[1]) => create(ForceCancelCascadeSchema, init);

test('offered on a run that has not ended, to a caller who may, and held while one is in progress', () => {
	const running = fc({ state: ForceCancelState.REQUESTED });
	expect(forceCancelOffer(Status.RUNNING, { allowed: true })).toEqual({ shown: true, disabled: '' });
	expect(forceCancelOffer(Status.PAUSED, { allowed: true })).toEqual({ shown: true, disabled: '' });
	expect(forceCancelOffer(Status.RUNNING, { allowed: false }).shown).toBe(false);
	expect(forceCancelOffer(Status.CANCELED, { allowed: true }).shown).toBe(false);
	expect(forceCancelOffer(Status.RUNNING, { allowed: true, current: running }).disabled).toBe('Force cancel in progress');
	expect(forceCancelOffer(Status.RUNNING, { allowed: true, current: fc({ state: ForceCancelState.FAILED }) }).disabled).toBe('');
});

test('in progress until its cascade is over too', () => {
	expect(inProgress(undefined)).toBe(false);
	expect(inProgress(fc({ state: ForceCancelState.FORCE_CANCELED }))).toBe(false);
	const r = fc({ state: ForceCancelState.FORCE_CANCELED, cascade: { state: ForceCancelCascadeState.RUNNING } });
	expect([inProgress(r), cascadeInProgress(r)]).toEqual([true, true]);
	expect(cascadeInProgress(fc({ state: ForceCancelState.REQUESTED }))).toBe(false);
});

test('a reason is required, bounded in bytes, and shows every character it holds', () => {
	expect(reasonProblem('stuck after the roll')).toBe('');
	expect(reasonProblem('  ')).toContain('Give a reason');
	expect(reasonProblem('é'.repeat(256))).toBe('');
	expect(reasonProblem('é'.repeat(257))).toContain('514 bytes');
	for (const text of ['a\nb', 'a\u202eb', 'a\u200bb', 'a\u2028b', 'a\u0007b']) expect(reasonProblem(text)).toContain('invisible');
});

test('the least grace is the server\'s, and is the default', () => {
	expect(graceChoices[0]?.seconds).toBeNull();
	expect(graceChoices.slice(1).every((c) => (c.seconds ?? 0) >= 120 && (c.seconds ?? 0) <= 600)).toBe(true);
});

test('needs more when it failed, lacks something, or left runs below', () => {
	const cases: Array<[ReturnType<typeof fc>, boolean]> = [
		[fc({ state: ForceCancelState.REQUESTED, incomplete: true }), false],
		[fc({ state: ForceCancelState.FORCE_CANCELED }), false],
		[fc({ state: ForceCancelState.FORCE_CANCELED, incomplete: true }), true],
		[fc({ state: ForceCancelState.FAILED }), true],
		[fc({ state: ForceCancelState.CANCELED, cascade: { state: ForceCancelCascadeState.COMPLETE } }), false],
		[fc({ state: ForceCancelState.CANCELED, cascade: { state: ForceCancelCascadeState.PARTIAL } }), true],
		[fc({ state: ForceCancelState.CANCELED, cascade: { state: ForceCancelCascadeState.STOPPED } }), true],
		[fc({ state: ForceCancelState.CANCELED, cascade: { state: ForceCancelCascadeState.RUNNING } }), false]
	];
	for (const [r, want] of cases) expect(needsMore(r)).toBe(want);
});

test('every state has words, and what is missing is said', () => {
	for (const state of [ForceCancelState.REQUESTED, ForceCancelState.FORCE_CANCELED, ForceCancelState.CANCELED, ForceCancelState.ENDED, ForceCancelState.ALREADY_ENDED, ForceCancelState.FAILED])
		expect(stateWords(fc({ state }))).not.toBe('Unknown');
	expect(stateWords(fc({ state: ForceCancelState.FORCE_CANCELED, incomplete: true }))).toContain('missing');
	expect(stateWords(fc({ state: ForceCancelState.FAILED }))).toContain('may still be open');
	for (const state of [ForceCancelState.REQUESTED, ForceCancelState.WAITING, ForceCancelState.FORCE_CANCELED, ForceCancelState.CANCELED, ForceCancelState.ENDED, ForceCancelState.ALREADY_ENDED, ForceCancelState.FAILED, ForceCancelState.PENDING])
		expect(belowWords(create(ForceCancelRunBelowSchema, { state }))).not.toBe('Unknown');
	expect(belowWords(create(ForceCancelRunBelowSchema, { state: ForceCancelState.FAILED, cause: 'not_below' }))).toBe('Failed (not_below)');
});

test('what a request did beside starting one is said', () => {
	const said = (init: Parameters<typeof create<typeof ForceCancelRunResponseSchema>>[1]) => requestNotes(create(ForceCancelRunResponseSchema, init)).join(' ');
	expect(said({ state: ForceCancelState.REQUESTED })).toBe('');
	expect(said({ state: ForceCancelState.WAITING, attached: true })).toContain('its reason and grace apply');
	expect(said({ state: ForceCancelState.ALREADY_ENDED, attached: true })).toBe('The run had already ended.');
	expect(said({ state: ForceCancelState.REQUESTED, direct: true, attachUnknown: true })).toContain('ended at once');
	expect(said({ attachUnknown: true })).toContain('may run beside it');
});

test('a run that failed or has not ended is not counted as ended', () => {
	const runs = [ForceCancelState.FAILED, ForceCancelState.PENDING, ForceCancelState.CANCELED].map((state) => ({ state }));
	expect(cascadeWords(cascade({ state: ForceCancelCascadeState.RUNNING, runs }))).toBe('1 of 3 runs below ended');
});

test('a cascade never says done beside what it left', () => {
	const runs = [{ state: ForceCancelState.CANCELED }, { state: ForceCancelState.REQUESTED }, { state: ForceCancelState.WAITING }];
	expect(cascadeWords(cascade({ state: ForceCancelCascadeState.RUNNING, runs }))).toBe('1 of 3 runs below ended');
	expect(cascadeWords(cascade({ state: ForceCancelCascadeState.COMPLETE }))).toBe('Nothing below was open');
	expect(cascadeWords(cascade({ state: ForceCancelCascadeState.COMPLETE, runs: runs.slice(0, 1) }))).toBe('1 run below ended, and none is open');
	expect(cascadeWords(cascade({ state: ForceCancelCascadeState.PARTIAL, runs }))).toContain('may still be open');
	expect(cascadeWords(cascade({ state: ForceCancelCascadeState.STOPPED, runs }))).toContain('may still be open');
	expect(cascadeWords(cascade({ state: ForceCancelCascadeState.RUNNING, unread: true }))).toContain('could not be asked');
	expect(cascadeNotes(cascade({ state: ForceCancelCascadeState.COMPLETE }))).toEqual([]);
	expect(cascadeNotes(cascade({ openAfter: { count: 2, unconfirmedCause: 'too_many_runs' }, cause: 'could_not_read', leftToParents: 1, notStarted: 3, exclusions: [{ cause: 'unreadable' }] }))).toEqual([
		'2 open after it', 'Some runs couldn’t be checked (too_many_runs)', 'The runs below could not be looked for (could_not_read)', '1 deeper run is left to the cancel of the run above',
		'3 not force-canceled, and no run above is open to cancel them', 'Left out: a run (unreadable)'
	]);
});

test('a force cancel is read again while it runs, and until the one asked for is read', () => {
	const done = fc({ state: ForceCancelState.FORCE_CANCELED });
	done.requestId = 'fc-1';
	expect(awaited(undefined, '')).toBe(false);
	expect(awaited(undefined, 'fc-1')).toBe(true);
	expect(awaited(done, 'fc-1')).toBe(false);
	expect(awaited(done, 'fc-2')).toBe(true);
	expect(awaited(fc({ state: ForceCancelState.REQUESTED }), '')).toBe(true);
});

test('a force cancel that has not seen the run end does not say it is being waited for', () => {
	const r = fc({ state: ForceCancelState.REQUESTED });
	expect(stateWords(r)).toContain('asked to cancel');
	expect(stateWords(r, true)).toBe('The run has ended; how is checked once its grace is over');
	expect(stateWords(fc({ state: ForceCancelState.FORCE_CANCELED }), true)).toBe('Ended the run');
});

test('the dialog says where the search below stands, and what it did not see', () => {
	const row = create(ForceCancelRunBelowSchema, { runId: 'ork-b' });
	expect(belowListed(undefined, '')).toBe('Looking for open runs below it…');
	expect(belowListed(undefined, 'no')).toBe('The open runs below it could not be listed: no');
	expect(belowListed(create(ForceCancelSubtreeSchema, {}), '')).toBe('No open run below it.');
	expect(belowListed(create(ForceCancelSubtreeSchema, { runs: [row] }), '')).toBe('1 open run below it.');
	expect(belowListed(create(ForceCancelSubtreeSchema, { truncatedBy: 'scan_limit' }), '')).toBe('No open run below it; not every one could be listed (scan_limit).');
	const out = ['not_below', 'not_below', 'chain_unreadable'].map((cause) => create(ForceCancelExclusionSchema, { cause }));
	expect(belowListed(create(ForceCancelSubtreeSchema, { runs: [row], exclusions: out }), '')).toBe('1 open run below it; 3 left out (not_below, chain_unreadable).');
});
