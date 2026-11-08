import { create } from '@bufbuild/protobuf';
import { DurationSchema, timestampFromDate, type Duration, type Timestamp } from '@bufbuild/protobuf/wkt';
import { instantDate } from './instant';
import { parseInputObject } from './authoring';
import { PayloadSchema, type Payload } from './gen/orkstack/v1/common_pb';
import {
	OverlapPolicy,
	OverlapPolicySchema,
	ScheduleSpecSchema,
	type Schedule,
	type ScheduleFire,
	type ScheduleSpec
} from './gen/orkstack/v1/schedule_pb';

// The user-facing spelling of an overlap policy: the proto value's name after
// OVERLAP_POLICY_, lowercased, "_" as "-" — the rule origin kinds use
// (origin.ts), and the one cmd/orkctl's overlapPolicySlug applies. UNSPECIFIED
// has no slug: it is what a client sends when no policy is given.
const OVERLAP_POLICY_PREFIX = 'OVERLAP_POLICY_';

/** overlapPolicySlug spells a policy; UNSPECIFIED (and an undeclared number) is ''. */
export function overlapPolicySlug(policy: OverlapPolicy): string {
	if (policy === OverlapPolicy.UNSPECIFIED) return '';
	const value = OverlapPolicySchema.values.find((candidate) => candidate.number === policy);
	if (!value) return '';
	return value.name.slice(OVERLAP_POLICY_PREFIX.length).toLowerCase().replaceAll('_', '-');
}

/** overlapPolicyFromSlug is overlapPolicySlug's inverse; anything else is UNSPECIFIED. */
export function overlapPolicyFromSlug(slug: string): OverlapPolicy {
	if (!slug) return OverlapPolicy.UNSPECIFIED;
	const value = OverlapPolicySchema.values.find(
		(candidate) => candidate.number !== OverlapPolicy.UNSPECIFIED && overlapPolicySlug(candidate.number) === slug
	);
	return value ? (value.number as OverlapPolicy) : OverlapPolicy.UNSPECIFIED;
}

/** Every declared policy's slug, in declaration order. A reserved number has no value, so no slug. */
export const OVERLAP_POLICY_SLUGS: string[] = OverlapPolicySchema.values
	.map((value) => overlapPolicySlug(value.number))
	.filter((slug) => slug !== '');

/** What a recorded policy reads as; a read never carries UNSPECIFIED, which is the cluster's skip. */
export function overlapLabel(policy: OverlapPolicy): string {
	return overlapPolicySlug(policy) || 'skip';
}

/** durationLabel spells a duration the way an operator types one: 1h, 15m, 1h30m, 30s. */
export function durationLabel(duration: Duration | undefined): string {
	if (!duration) return '';
	let total = Number(duration.seconds);
	const ms = Math.round(duration.nanos / 1e6);
	if (total === 0 && ms > 0) return `${ms}ms`;
	const hours = Math.floor(total / 3600);
	total -= hours * 3600;
	const minutes = Math.floor(total / 60);
	const seconds = total - minutes * 60;
	let out = '';
	if (hours) out += `${hours}h`;
	if (minutes) out += `${minutes}m`;
	if (seconds || (!out && !ms)) out += `${seconds}s`;
	if (ms) out += `${ms}ms`;
	return out;
}

const DURATION_PART = /(\d+(?:\.\d+)?)(ms|h|m|s)/g;

/**
 * parseDuration reads a Go-style duration ("1h", "15m", "1h30m", "30s",
 * "500ms"). Empty is undefined — the field is unset. Anything else is an error
 * naming the field.
 */
export function parseDuration(field: string, text: string): { value: Duration | undefined } | { error: string } {
	const trimmed = text.trim();
	if (!trimmed) return { value: undefined };
	const parts = [...trimmed.matchAll(DURATION_PART)];
	if (parts.length === 0 || parts.map((part) => part[0]).join('') !== trimmed) {
		return { error: `${field}: "${trimmed}" is not a duration such as 30s, 15m or 1h30m.` };
	}
	let ms = 0;
	for (const [, amount, unit] of parts) {
		const n = Number(amount);
		ms += unit === 'h' ? n * 3_600_000 : unit === 'm' ? n * 60_000 : unit === 's' ? n * 1000 : n;
	}
	const seconds = Math.floor(ms / 1000);
	return { value: create(DurationSchema, { seconds: BigInt(seconds), nanos: Math.round((ms - seconds * 1000) * 1e6) }) };
}

/**
 * whenLabel is a spec's WHEN, spelled as `orkctl schedule list` spells it:
 * cron expressions joined with " | ", then "every <interval>", then the zone.
 */
export function whenLabel(spec: ScheduleSpec | undefined): string {
	if (!spec) return '—';
	const parts = [...spec.cron];
	if (spec.interval) parts.push(`every ${durationLabel(spec.interval)}`);
	if (parts.length === 0) return '—';
	let label = parts.join(' | ');
	if (spec.timezone && spec.cron.length > 0) label += ` (${spec.timezone})`;
	return label;
}

/** isoUTC renders a timestamp as RFC 3339 UTC, second precision. */
export function isoUTC(ts: Timestamp | undefined): string {
	return ts ? instantDate(ts).toISOString().replace(/\.\d{3}Z$/, 'Z') : '—';
}

/**
 * A schedule's fields as a form holds them: text an operator types, one for
 * each mutable field, shared by the create form and the edit dialog. An empty
 * version means current, resolved at each fire.
 */
export interface ScheduleForm {
	version: string;
	cron: string;
	interval: string;
	timezone: string;
	overlap: string;
	jitter: string;
	catchupWindow: string;
	startAt: string;
	endAt: string;
	description: string;
	input: string;
	pauseOnFailure: boolean;
}

/** The version choice that pins nothing: it becomes the empty wire value. */
export const CURRENT_VERSION = '__current__';

export const EMPTY_SCHEDULE_FORM: Readonly<ScheduleForm> = {
	version: '',
	cron: '',
	interval: '',
	timezone: '',
	overlap: 'skip',
	jitter: '',
	catchupWindow: '',
	startAt: '',
	endAt: '',
	description: '',
	input: '',
	pauseOnFailure: false
};

function durationText(duration: Duration | undefined): string {
	return duration && (duration.seconds > 0n || duration.nanos > 0) ? durationLabel(duration) : '';
}

/** scheduleFormOf is what the edit dialog opens with: every field as stored. */
export function scheduleFormOf(schedule: Schedule): ScheduleForm {
	const spec = schedule.spec;
	const data = schedule.input?.data;
	return {
		version: schedule.orkflowVersion,
		cron: (spec?.cron ?? []).join('\n'),
		interval: durationText(spec?.interval),
		timezone: spec?.timezone ?? '',
		overlap: overlapLabel(spec?.overlap ?? OverlapPolicy.UNSPECIFIED),
		jitter: durationText(spec?.jitter),
		catchupWindow: durationText(spec?.catchupWindow),
		startAt: spec?.startAt ? isoUTC(spec.startAt) : '',
		endAt: spec?.endAt ? isoUTC(spec.endAt) : '',
		description: schedule.description,
		input: data && Object.keys(data).length > 0 ? JSON.stringify(data, null, 2) : '',
		pauseOnFailure: spec?.pauseOnFailure ?? false
	};
}

function cronLines(text: string): string[] {
	return text
		.split('\n')
		.map((line) => line.trim())
		.filter((line) => line !== '');
}

function parseTime(field: string, text: string): { value: Timestamp | undefined } | { error: string } {
	const trimmed = text.trim();
	if (!trimmed) return { value: undefined };
	const date = new Date(trimmed);
	if (Number.isNaN(date.getTime())) return { error: `${field}: "${trimmed}" is not an RFC 3339 time.` };
	return { value: timestampFromDate(date) };
}

/** The mutable fields of a create or update request. */
export interface ScheduleWrite {
	orkflowVersion: string;
	input: Payload | undefined;
	description: string;
	spec: ScheduleSpec;
}

/**
 * scheduleWrite reads a form into a request's fields, or refuses naming the
 * field. An update replaces every field, so a field the operator left alone
 * sends the schedule's own value rather than its text: the form spells a
 * duration or an instant more coarsely than the store can hold one, and saving
 * must not round what nobody touched.
 */
export function scheduleWrite(form: ScheduleForm, original?: Schedule): { value: ScheduleWrite } | { error: string } {
	const before = original ? scheduleFormOf(original) : undefined;
	const stored = original?.spec;
	const untouched = (field: 'interval' | 'jitter' | 'catchupWindow' | 'startAt' | 'endAt' | 'input') =>
		before !== undefined && form[field].trim() === before[field].trim();
	const duration = (field: 'interval' | 'jitter' | 'catchupWindow', label: string) =>
		untouched(field) ? { value: stored?.[field] } : parseDuration(label, form[field]);
	const time = (field: 'startAt' | 'endAt', label: string) =>
		untouched(field) ? { value: stored?.[field] } : parseTime(label, form[field]);

	const interval = duration('interval', 'Interval');
	if ('error' in interval) return interval;
	const jitter = duration('jitter', 'Jitter');
	if ('error' in jitter) return jitter;
	const catchupWindow = duration('catchupWindow', 'Catch-up window');
	if ('error' in catchupWindow) return catchupWindow;
	const startAt = time('startAt', 'Start at');
	if ('error' in startAt) return startAt;
	const endAt = time('endAt', 'End at');
	if ('error' in endAt) return endAt;

	let input: Payload | undefined;
	if (untouched('input')) {
		input = original?.input;
	} else if (form.input.trim()) {
		const parsed = parseInputObject(form.input);
		if ('error' in parsed) return parsed;
		input = create(PayloadSchema, { data: parsed.value });
	}

	return {
		value: {
			orkflowVersion: form.version,
			input,
			description: form.description,
			spec: create(ScheduleSpecSchema, {
				cron: cronLines(form.cron),
				interval: interval.value,
				timezone: form.timezone.trim(),
				jitter: jitter.value,
				catchupWindow: catchupWindow.value,
				startAt: startAt.value,
				endAt: endAt.value,
				overlap: before?.overlap === form.overlap && stored ? stored.overlap : overlapPolicyFromSlug(form.overlap),
				pauseOnFailure: form.pauseOnFailure
			})
		}
	};
}

/** The state a row shows: active, paused, or unavailable. */
export type ScheduleState = 'active' | 'paused' | 'unavailable';

export function scheduleState(schedule: Schedule): ScheduleState {
	if (schedule.unavailableReason) return 'unavailable';
	return schedule.paused ? 'paused' : 'active';
}

/** What one recent fire's RUN cell reads: a run id, refused, preparing, or unavailable. */
export type FireOutcome =
	| { kind: 'run'; runId: string }
	| { kind: 'refused'; refusal: string }
	| { kind: 'preparing' }
	| { kind: 'unavailable' };

export function fireOutcome(fire: ScheduleFire): FireOutcome {
	if (!fire.detailsAvailable) return { kind: 'unavailable' };
	if (fire.runId) return { kind: 'run', runId: fire.runId };
	if (fire.refusal) return { kind: 'refused', refusal: fire.refusal };
	return { kind: 'preparing' };
}

/** The newest fire, which the row's LAST RUN reads. */
export function lastFire(schedule: Schedule): ScheduleFire | undefined {
	return schedule.recentFires.at(-1);
}

/** The counters line, as `orkctl schedule get` prints it. */
export function firesLabel(schedule: Schedule): string {
	return `${schedule.firesTaken} taken, ${schedule.firesSkippedOverlap} skipped (overlap), ${schedule.firesMissedCatchup} missed (catch-up window)`;
}

// A value is written bare when it is plainly one shell word, and
// single-quoted otherwise — so a copied command reads the way a person types
// it and still cannot be misread by a shell whatever the value holds.
const SHELL_SAFE = /^[A-Za-z0-9_./:@=+,-]+$/;

export function shellArg(value: string): string {
	if (value !== '' && SHELL_SAFE.test(value)) return value;
	return `'${value.replaceAll("'", "'\"'\"'")}'`;
}

function nsFlag(namespace: string): string {
	return `--namespace ${shellArg(namespace || 'default')}`;
}

/**
 * scheduleCreateCli is the full `orkctl schedule create …` that would recreate
 * this schedule: every field it carries, and --paused when it is paused.
 */
export function scheduleCreateCli(schedule: Schedule): string {
	const spec = schedule.spec;
	const args = ['orkctl schedule create', shellArg(schedule.name), '--orkflow', shellArg(schedule.orkflowName)];
	if (schedule.orkflowVersion) args.push('--version', shellArg(schedule.orkflowVersion));
	for (const expr of spec?.cron ?? []) args.push('--cron', shellArg(expr));
	if (spec?.interval) args.push('--interval', durationLabel(spec.interval));
	if (spec?.timezone) args.push('--timezone', shellArg(spec.timezone));
	args.push('--overlap', overlapLabel(spec?.overlap ?? OverlapPolicy.UNSPECIFIED));
	if (schedule.paused) args.push('--paused');
	if (spec?.jitter && (spec.jitter.seconds > 0n || spec.jitter.nanos > 0)) args.push('--jitter', durationLabel(spec.jitter));
	if (spec?.catchupWindow) args.push('--catchup-window', durationLabel(spec.catchupWindow));
	if (spec?.pauseOnFailure) args.push('--pause-on-failure');
	if (spec?.startAt) args.push('--start-at', isoUTC(spec.startAt));
	if (spec?.endAt) args.push('--end-at', isoUTC(spec.endAt));
	if (schedule.description) args.push('--description', shellArg(schedule.description));
	if (schedule.input?.data && Object.keys(schedule.input.data).length > 0) {
		args.push('--input-json', shellArg(JSON.stringify(schedule.input.data)));
	}
	args.push(nsFlag(schedule.namespace));
	return args.join(' ');
}

/**
 * scheduleUpdateCli is the `orkctl schedule update` that makes this edit: the
 * CLI reads the schedule and replaces only the flags it is given, so it names
 * only what changed. Empty when no command can: --cron adds expressions, and
 * nothing on the CLI empties the list.
 */
export function scheduleUpdateCli(schedule: Schedule, form: ScheduleForm): string {
	const before = scheduleFormOf(schedule);
	const lines = cronLines(form.cron);
	if (lines.length === 0 && before.cron !== '') return '';
	const args = ['orkctl schedule update', shellArg(schedule.name)];
	if (form.version !== before.version) {
		if (form.version) args.push('--version', shellArg(form.version));
		else args.push('--follow-current');
	}
	if (lines.join('\n') !== before.cron) for (const expr of lines) args.push('--cron', shellArg(expr));
	const text = (flag: string, now: string, was: string) => {
		if (now.trim() !== was.trim()) args.push(flag, shellArg(now.trim()));
	};
	text('--interval', form.interval, before.interval);
	text('--timezone', form.timezone, before.timezone);
	if (form.overlap !== before.overlap) args.push('--overlap', shellArg(form.overlap));
	text('--jitter', form.jitter, before.jitter);
	text('--catchup-window', form.catchupWindow, before.catchupWindow);
	text('--start-at', form.startAt, before.startAt);
	text('--end-at', form.endAt, before.endAt);
	if (form.pauseOnFailure !== before.pauseOnFailure) {
		args.push(form.pauseOnFailure ? '--pause-on-failure' : '--pause-on-failure=false');
	}
	if (form.description !== before.description) args.push('--description', shellArg(form.description));
	if (form.input.trim() !== before.input.trim()) {
		if (!form.input.trim()) {
			args.push('--clear-input');
		} else {
			const parsed = parseInputObject(form.input);
			args.push('--input-json', shellArg('error' in parsed ? form.input.trim() : JSON.stringify(parsed.value)));
		}
	}
	args.push(nsFlag(schedule.namespace));
	return args.join(' ');
}

export function schedulePauseCli(name: string, namespace: string, reason: string): string {
	const args = ['orkctl schedule pause', shellArg(name)];
	if (reason) args.push('--reason', shellArg(reason));
	args.push(nsFlag(namespace));
	return args.join(' ');
}

export function scheduleResumeCli(name: string, namespace: string): string {
	return ['orkctl schedule resume', shellArg(name), nsFlag(namespace)].join(' ');
}

/**
 * scheduleFireCli is Fire now's command. It carries no --overlap: the button
 * sends no override, so the schedule's own policy applies, and so does this.
 */
export function scheduleFireCli(name: string, namespace: string): string {
	return ['orkctl schedule fire', shellArg(name), nsFlag(namespace)].join(' ');
}

export function scheduleDeleteCli(name: string, namespace: string): string {
	return ['orkctl schedule delete', shellArg(name), nsFlag(namespace)].join(' ');
}
