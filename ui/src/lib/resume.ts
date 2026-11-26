// Resume and Retry from here (E85): what the run page offers, how a plan is
// laid out in its dialog, and how a refusal reads. Pure, so the dialog and
// the page share one reading of a ResumeRun answer with `orkctl run resume`
// (cmd/orkctl/resume.go): the same sections, the same rule for when to ask,
// the same exits by class.
import { Code, ConnectError } from '@connectrpc/connect';
import { ResumeHintState, type ResumeHint } from './gen/orkstack/v1/run_pb';
import {
	ResumeCategory,
	ResumeRefusalClass,
	ResumeRefusalSchema,
	ResumeWarningKind,
	type ResumeRefusal,
	type ResumeRunResponse,
	type ResumeUnitRef
} from './gen/orkstack/v1/run_service_pb';
import { Status } from './gen/orkstack/v1/common_pb';
import { shellQuote } from './runDetail';

/** The resume cap a run is counted against (docs/RESUME.md, "Limits"). */
export const MAX_RESUMES = 20;

/** The longest reason a resume records, in bytes (as the CLI's --reason). */
export const MAX_RESUME_REASON = 1024;

/** How many units a section names before it counts the rest, as the CLI does. */
export const SECTION_LIMIT = 20;

/** The deadline of one ResumeRun call, as the CLI's default --timeout. */
export const RESUME_CALL_TIMEOUT_MS = 30_000;

/** The permission that may accept a gap in a run's events (docs/ACCESS.md). */
export const ACCEPT_GAP_PERMISSION = 'run.resume.accept_gap';

/** What an administrator may accept, as `--accept-sequence-gap N`. */
export type Acceptance = 'gap';

/** Each acceptance's control, with the consequence docs/RESUME.md states. */
export const ACCEPTANCES: Record<Acceptance, { label: string; consequence: string; flag: string }> = {
	gap: {
		label: 'Accept the gap in this run’s events',
		consequence:
			'Some of the run’s events were never stored. The resume goes on above the event named here, and the events that were never stored stay missing from the run’s history.',
		flag: '--accept-sequence-gap'
	}
};

/** The causes of the refusals an administrator may accept. */
const ACCEPTABLE_CAUSES: Record<string, Acceptance> = {
	events_above_terminal: 'gap'
};

/** The acceptance that would answer refusal r, or undefined when none can. */
export function acceptanceFor(r: RefusalView | undefined): Acceptance | undefined {
	// A gap is accepted up to an event: one the refusal does not name can't be.
	if (!r || r.cls !== 'replan' || r.highest <= 0n) return undefined;
	return Object.hasOwn(ACCEPTABLE_CAUSES, r.cause) ? ACCEPTABLE_CAUSES[r.cause] : undefined;
}

/** What the page offers for one run, from its advisory hint. */
export interface ResumeOffer {
	/** The header's Resume: a failed run. */
	header: boolean;
	/** A force-cancelled run: only Retry from here, from a task the person picks. */
	fromTaskOnly: boolean;
	/** Per task: Resume (a failed task) and Retry from here. */
	tasks: Map<string, { resume: boolean; retryFrom: boolean }>;
}

const NONE: ResumeOffer = { header: false, fromTaskOnly: false, tasks: new Map() };

/**
 * What a run's hint offers. The hint is advisory (it reads the run's ending
 * alone), so an offer only opens a preview, which may still refuse.
 */
export function resumeOffer(hint: ResumeHint | undefined): ResumeOffer {
	if (!hint) return NONE;
	const resumable = hint.state === ResumeHintState.RESUMABLE;
	const fromTaskOnly = hint.state === ResumeHintState.RESUMABLE_FROM_TASK;
	if (!resumable && !fromTaskOnly) return NONE;
	const tasks = new Map<string, { resume: boolean; retryFrom: boolean }>();
	for (const a of hint.taskActions) {
		tasks.set(a.taskId, { resume: resumable && a.resume, retryFrom: a.retryFrom });
	}
	return { header: resumable, fromTaskOnly, tasks };
}

/** One section of a plan, in the order the dialog shows it (E85 §3.4). */
export interface PlanSection {
	key: string;
	title: string;
	reruns: boolean;
	units: { name: string; why: string; detail: string }[];
}

const SECTIONS: { key: string; title: string; reruns: boolean; categories: ResumeCategory[] }[] = [
	{
		key: 'asked',
		title: 'You asked for these',
		reruns: true,
		categories: [ResumeCategory.RERUN_FAILED, ResumeCategory.RERUN_SELECTED, ResumeCategory.RERUN_DOWNSTREAM]
	},
	{
		key: 'added',
		title: 'Added because the resume must',
		reruns: true,
		categories: [
			ResumeCategory.RERUN_RESULT_GONE,
			ResumeCategory.RERUN_UNFINISHED,
			ResumeCategory.RERUN_PIN_MEMBER,
			ResumeCategory.RERUN_EACH_SOURCE
		]
	},
	{ key: 'waits', title: 'Waits again', reruns: false, categories: [ResumeCategory.WAITS_AGAIN] },
	{
		key: 'record',
		title: 'Answered and on record',
		reruns: false,
		categories: [ResumeCategory.RESOLVED_ON_RECORD, ResumeCategory.CHILD_OUTPUTS_ON_RECORD]
	},
	{ key: 'kept', title: 'Kept', reruns: false, categories: [ResumeCategory.KEPT] },
	{ key: 'done', title: 'Already done', reruns: false, categories: [ResumeCategory.SETTLED, ResumeCategory.SLEEP_KEPT] },
	{ key: 'skipped', title: 'Stays skipped', reruns: false, categories: [ResumeCategory.SKIPPED] },
	{ key: 'earlier', title: 'Kept from an earlier part of the run, not run again', reruns: false, categories: [ResumeCategory.EARLIER] }
];

/** A unit whose category no section claims: a newer engine's, shown, never dropped. */
const OTHER = { key: 'other', title: 'Other', reruns: true };

/** The section a category is listed under. */
export function sectionKeyOf(category: ResumeCategory): string {
	return SECTIONS.find((s) => s.categories.includes(category))?.key ?? OTHER.key;
}

/** A unit's name for a person: `task`, or `task[3]` for a fan-out item. Never parsed. */
export function unitName(taskId: string, item: number | undefined): string {
	return item === undefined ? taskId : `${taskId}[${item}]`;
}

/** The plan's non-empty sections, in order, every unit in exactly one. */
export function planSections(plan: ResumeRunResponse): PlanSection[] {
	const out: PlanSection[] = [];
	for (const s of [...SECTIONS, OTHER]) {
		const units = plan.tasks
			.filter((t) => sectionKeyOf(t.category) === s.key)
			.map((t) => ({ name: unitName(t.taskId, t.item), why: t.why, detail: t.detail }));
		if (units.length) out.push({ key: s.key, title: s.title, reruns: s.reruns, units });
	}
	return out;
}

/** How many tasks that succeeded before would run again: the work a confirm agrees to repeat. */
export function rerunSucceeded(plan: ResumeRunResponse): number {
	const rerun = new Set(SECTIONS.filter((s) => s.reruns).map((s) => s.key).concat(OTHER.key));
	return plan.tasks.filter((t) => rerun.has(sectionKeyOf(t.category)) && t.previousOutcome === Status.SUCCEEDED).length;
}

/** The unknown-outcome attempts a confirm must acknowledge, by their keys. */
export function unknownAttempts(plan: ResumeRunResponse): { keys: string[]; unkeyed: number } {
	const keys: string[] = [];
	let unkeyed = 0;
	for (const w of plan.warnings) {
		if (w.kind !== ResumeWarningKind.ATTEMPT_OUTCOME_UNKNOWN) continue;
		if (w.acknowledgeKey) keys.push(w.acknowledgeKey);
		else unkeyed++;
	}
	return { keys, unkeyed };
}

/**
 * Why a preview cannot be confirmed, or '' when it can: it must name the plan
 * and the ending it sets aside, and every attempt of unknown outcome must be
 * one a confirm can acknowledge.
 */
export function unconfirmableReason(plan: ResumeRunResponse): string {
	if (plan.planDigest === '' || plan.terminalSequence <= 0n) return 'The engine gave no plan to confirm, so nothing can be resumed from this preview.';
	if (unknownAttempts(plan).unkeyed > 0) return 'The engine named an earlier attempt of unknown outcome that cannot be acknowledged, so nothing can be resumed from this preview.';
	return '';
}

/** Whether a preview can be confirmed at all. */
export function confirmable(plan: ResumeRunResponse): boolean {
	return unconfirmableReason(plan) === '';
}

/** Whether a reason fits what a resume records. */
export function reasonFits(reason: string): boolean {
	return new TextEncoder().encode(reason).length <= MAX_RESUME_REASON;
}

/** A refusal as the dialog shows it. */
export interface RefusalView {
	reason: string;
	cls: 'permanent' | 'wait' | 'replan' | 'unconfirmed';
	/** What refines reason (docs/REASONS.md); '' when none. */
	cause: string;
	message: string;
	retryAfterSeconds: number;
	added: string[];
	dropped: string[];
	/** The run's highest stored event, which accepting a gap names; 0n when it is not about one. */
	highest: bigint;
}

function refs(list: ResumeUnitRef[]): string[] {
	return list.map((r) => unitName(r.taskId, r.item));
}

/**
 * The refusal a ResumeRun error carries, or undefined for any other error. A
 * confirm whose answer was lost (a deadline, a dropped connection) reads as
 * "unconfirmed": it may have reached the engine.
 */
export function refusalOf(err: unknown, atConfirm: boolean): RefusalView | undefined {
	const e = ConnectError.from(err);
	const detail: ResumeRefusal | undefined = e.findDetails(ResumeRefusalSchema)[0];
	if (!detail) {
		if (e.code === Code.Unimplemented) {
			return {
				reason: 'UNIMPLEMENTED',
				cls: 'permanent',
				cause: '',
				message: 'This engine cannot resume runs.',
				retryAfterSeconds: 0,
				added: [],
				dropped: [],
				highest: 0n
			};
		}
		if (atConfirm && [Code.DeadlineExceeded, Code.Canceled, Code.Unavailable, Code.Unknown, Code.Internal].includes(e.code)) {
			return {
				reason: 'RESUME_UNCONFIRMED',
				cls: 'unconfirmed',
				cause: '',
				message: 'The resume may have been sent, and its outcome is not known. Check the run before trying again.',
				retryAfterSeconds: 0,
				added: [],
				dropped: [],
				highest: 0n
			};
		}
		return undefined;
	}
	const cls: RefusalView['cls'] =
		detail.reason === 'RESUME_UNCONFIRMED'
			? 'unconfirmed'
			: detail.class === ResumeRefusalClass.WAIT
				? 'wait'
				: detail.class === ResumeRefusalClass.REPLAN
					? 'replan'
					: 'permanent';
	return {
		reason: detail.reason,
		cls,
		cause: detail.cause,
		message: detail.message || detail.reason,
		retryAfterSeconds: Number(detail.retryAfterSeconds),
		added: refs(detail.diff?.added ?? []),
		dropped: refs(detail.diff?.dropped ?? []),
		highest: detail.highestSequence
	};
}

/**
 * The first line of a refusal: what the person should do. RESUME_UNCONFIRMED
 * says which of its causes stands, as the CLI does: a reset that may or may
 * not have been made, or one that was made and whose run has not carried on.
 */
export function refusalLead(r: RefusalView): string {
	switch (r.cls) {
		case 'wait':
			return r.retryAfterSeconds > 0 ? `Not now: try again in ${r.retryAfterSeconds} s.` : 'Not now: try again shortly.';
		case 'replan':
			return 'Look at the plan again before deciding.';
		case 'unconfirmed':
			if (r.cause === 'reset_outcome_unknown') return 'Sent: it is not known yet whether the run was resumed.';
			if (r.cause === 'not_carried_on') return 'Resumed: the run has not carried on yet.';
			return 'Sent, outcome not known yet.';
		default:
			return 'A resume can’t fix this run. Run it again instead.';
	}
}

/** The dialog's one `orkctl` command, as the CLI would make the same resume. */
export function resumeCommand(
	namespace: string,
	runId: string,
	fromTask: string,
	ackUnknown: boolean,
	reason = '',
	accepted: ReadonlySet<Acceptance> = new Set(),
	gapUpTo = 0n
): string {
	const words = ['orkctl run resume', shellQuote(runId)];
	if (fromTask) words.push('--from', shellQuote(fromTask));
	if (reason) words.push('--reason', shellQuote(reason));
	if (namespace && namespace !== 'default') words.push('-n', shellQuote(namespace));
	if (ackUnknown) words.push('--acknowledge-unknown-attempts');
	if (accepted.has('gap') && gapUpTo > 0n) words.push(ACCEPTANCES.gap.flag, String(gapUpTo));
	return words.join(' ');
}
