import { expect, test } from 'bun:test';
import { Code, ConnectError } from '@connectrpc/connect';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { ResumeHintSchema, ResumeHintState } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	ResumeCategory,
	ResumeRefusalClass,
	ResumeRefusalSchema,
	ResumeRunResponseSchema,
	ResumeTaskSchema,
	ResumeWarningKind,
	ResumeWarningSchema
} from '../src/lib/gen/orkstack/v1/run_service_pb';
import {
	ACCEPTANCES,
	acceptanceFor,
	confirmable,
	planSections,
	reasonFits,
	unconfirmableReason,
	refusalLead,
	refusalOf,
	rerunSucceeded,
	resumeCommand,
	resumeOffer,
	sectionKeyOf,
	unknownAttempts
} from '../src/lib/resume';

function plan() {
	return create(ResumeRunResponseSchema, {
		runId: 'ork-r',
		planDigest: 'd',
		terminalSequence: 41n,
		tasks: [
			{ taskId: 'build', category: ResumeCategory.RERUN_FAILED, previousOutcome: Status.FAILED },
			{ taskId: 'publish', category: ResumeCategory.RERUN_DOWNSTREAM, previousOutcome: Status.SUCCEEDED },
			{ taskId: 'fetch', category: ResumeCategory.KEPT, previousOutcome: Status.SUCCEEDED },
			{ taskId: 'convert', item: 3, category: ResumeCategory.RERUN_UNFINISHED, previousOutcome: Status.CANCELED }
		]
	});
}

test('the offer follows the hint, and a force-cancelled run offers only Retry from here', () => {
	expect(resumeOffer(undefined).header).toBe(false);
	const failed = resumeOffer(
		create(ResumeHintSchema, {
			state: ResumeHintState.RESUMABLE,
			taskActions: [{ taskId: 'build', resume: true, retryFrom: true }]
		})
	);
	expect(failed.header).toBe(true);
	expect(failed.tasks.get('build')).toEqual({ resume: true, retryFrom: true });
	const forced = resumeOffer(
		create(ResumeHintSchema, {
			state: ResumeHintState.RESUMABLE_FROM_TASK,
			taskActions: [{ taskId: 'build', resume: true, retryFrom: true }]
		})
	);
	expect(forced.header).toBe(false);
	expect(forced.fromTaskOnly).toBe(true);
	expect(forced.tasks.get('build')).toEqual({ resume: false, retryFrom: true });
	expect(resumeOffer(create(ResumeHintSchema, { state: ResumeHintState.NOT_RESUMABLE })).tasks.size).toBe(0);
});

test('sections list every unit once, in order, and a newer category under Other', () => {
	const p = plan();
	p.tasks.push(create(ResumeTaskSchema, { taskId: 'new', category: 99 as ResumeCategory }));
	const s = planSections(p);
	expect(s.map((x) => x.title)).toEqual(['You asked for these', 'Added because the resume must', 'Kept', 'Other']);
	expect(s[1].units[0].name).toBe('convert[3]');
	for (const v of Object.values(ResumeCategory).filter((v) => typeof v === 'number' && v !== 0)) {
		expect(sectionKeyOf(v as ResumeCategory)).not.toBe('other');
	}
});

test('what an earlier part of the run finished is shown as kept, not run again', () => {
	const p = plan();
	p.tasks.push(create(ResumeTaskSchema, { taskId: 'early', category: ResumeCategory.EARLIER }));
	const earlier = planSections(p).find((x) => x.key === 'earlier');
	expect(earlier?.title).toBe('Kept from an earlier part of the run, not run again');
	expect(earlier?.reruns).toBe(false);
});

test('a confirm asks only when succeeded work would run again', () => {
	expect(rerunSucceeded(plan())).toBe(1);
	const p = plan();
	p.tasks = p.tasks.filter((t) => t.taskId !== 'publish');
	expect(rerunSucceeded(p)).toBe(0);
});

test('unknown attempts need keys, and a plan without its binding is not confirmable', () => {
	const p = plan();
	expect(confirmable(p)).toBe(true);
	p.warnings = [create(ResumeWarningSchema, { kind: ResumeWarningKind.ATTEMPT_OUTCOME_UNKNOWN, acknowledgeKey: 'o/build//2' })];
	expect(unknownAttempts(p)).toEqual({ keys: ['o/build//2'], unkeyed: 0 });
	p.warnings.push(create(ResumeWarningSchema, { kind: ResumeWarningKind.ATTEMPT_OUTCOME_UNKNOWN }));
	expect(confirmable(p)).toBe(false);
	const q = plan();
	q.planDigest = '';
	expect(confirmable(q)).toBe(false);
});

function refused(reason: string, cls: ResumeRefusalClass, cause = '', highestSequence = 0n) {
	const detail = create(ResumeRefusalSchema, {
		reason,
		class: cls,
		cause,
		message: 'sentence',
		retryAfterSeconds: 12n,
		highestSequence,
		diff: { added: [{ taskId: 'lint' }], dropped: [{ taskId: 'convert', item: 2 }] }
	});
	return new ConnectError('refused', Code.FailedPrecondition, undefined, [
		{ desc: ResumeRefusalSchema, value: detail }
	]);
}

test('a refusal reads by its class, and a lost confirm is unconfirmed', () => {
	expect(refusalOf(refused('STILL_STOPPING', ResumeRefusalClass.WAIT), false)?.cls).toBe('wait');
	const replan = refusalOf(refused('PLAN_CHANGED', ResumeRefusalClass.REPLAN), false);
	expect(replan?.cls).toBe('replan');
	expect(replan?.added).toEqual(['lint']);
	expect(replan?.dropped).toEqual(['convert[2]']);
	expect(replan?.retryAfterSeconds).toBe(12);
	expect(refusalOf(refused('RESUME_UNCONFIRMED', ResumeRefusalClass.WAIT), true)?.cls).toBe('unconfirmed');
	expect(refusalOf(refused('COMPOSED_TASK', ResumeRefusalClass.PERMANENT), false)?.cls).toBe('permanent');
	expect(refusalOf(new ConnectError('lost', Code.Unavailable), true)?.cls).toBe('unconfirmed');
	expect(refusalOf(new ConnectError('lost', Code.Unavailable), false)).toBeUndefined();
});

test('the command is the one the CLI would run', () => {
	expect(resumeCommand('default', 'ork-r', '', false)).toBe('orkctl run resume ork-r');
	expect(resumeCommand('payments', 'ork-r', 'build', true)).toBe(
		'orkctl run resume ork-r --from build -n payments --acknowledge-unknown-attempts'
	);
});

test('a preview says why it cannot be confirmed, and a reason is bounded in bytes', () => {
	expect(unconfirmableReason(plan())).toBe('');
	const p = plan();
	p.terminalSequence = 0n;
	expect(unconfirmableReason(p)).toContain('no plan to confirm');
	expect(reasonFits('x'.repeat(1024))).toBe(true);
	expect(reasonFits('é'.repeat(513))).toBe(false);
});

test('an engine that cannot resume says so, at preview and confirm alike', () => {
	for (const atConfirm of [false, true]) {
		const r = refusalOf(new ConnectError('nope', Code.Unimplemented), atConfirm);
		expect(r?.cls).toBe('permanent');
		expect(r?.message).toBe('This engine cannot resume runs.');
	}
});

test('the copied command carries the reason', () => {
	expect(resumeCommand('default', 'ork-r', '', false, "it's back")).toBe(`orkctl run resume ork-r --reason 'it'"'"'s back'`);
});

test('only a replan refusal of a gap that names its event can be accepted', () => {
	const gap = refusalOf(refused('STATE_UNREADABLE', ResumeRefusalClass.REPLAN, 'events_above_terminal', 57n), false);
	expect(gap?.cause).toBe('events_above_terminal');
	expect(gap?.highest).toBe(57n);
	expect(acceptanceFor(gap)).toBe('gap');
	// No event named: there is nothing to accept up to.
	expect(acceptanceFor(refusalOf(refused('STATE_UNREADABLE', ResumeRefusalClass.REPLAN, 'events_above_terminal'), false))).toBeUndefined();
	expect(acceptanceFor(refusalOf(refused('STATE_UNREADABLE', ResumeRefusalClass.PERMANENT, 'events_above_terminal', 57n), false))).toBeUndefined();
	expect(acceptanceFor(refusalOf(refused('PLAN_CHANGED', ResumeRefusalClass.REPLAN, 'toString', 57n), false))).toBeUndefined();
	expect(acceptanceFor(undefined)).toBeUndefined();
	// A refusal this engine makes itself names no event.
	expect(refusalOf(new ConnectError('no', Code.Unimplemented), false)?.highest).toBe(0n);
	expect(refusalOf(new ConnectError('no', Code.Unavailable), true)?.highest).toBe(0n);
});

test('the copied command names the event a gap was accepted up to', () => {
	expect(resumeCommand('default', 'ork-r', '', false, '', new Set(['gap']), 57n)).toBe('orkctl run resume ork-r --accept-sequence-gap 57');
	// No event is known to accept: the command accepts nothing.
	expect(resumeCommand('default', 'ork-r', '', false, '', new Set(['gap']))).toBe('orkctl run resume ork-r');
	expect(ACCEPTANCES.gap.flag).toBe('--accept-sequence-gap');
});

test('an unconfirmed resume says which of its two causes stands', () => {
	const unknown = refusalOf(refused('RESUME_UNCONFIRMED', ResumeRefusalClass.WAIT, 'reset_outcome_unknown'), true);
	expect(unknown?.cls).toBe('unconfirmed');
	expect(refusalLead(unknown!)).toBe('Sent: it is not known yet whether the run was resumed.');
	const late = refusalOf(refused('RESUME_UNCONFIRMED', ResumeRefusalClass.WAIT, 'not_carried_on'), true);
	expect(refusalLead(late!)).toBe('Resumed: the run has not carried on yet.');
	// A confirm whose answer was lost knows neither.
	const lost = refusalOf(new ConnectError('lost', Code.Unavailable), true);
	expect(refusalLead(lost!)).toBe('Sent, outcome not known yet.');
	// RESUME_IN_PROGRESS is a wait, with its time.
	const busy = refusalOf(refused('RESUME_IN_PROGRESS', ResumeRefusalClass.WAIT), false);
	expect(busy?.cls).toBe('wait');
	expect(refusalLead(busy!)).toBe('Not now: try again in 12 s.');
});
