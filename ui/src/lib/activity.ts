import { FailureCategory } from './gen/orkstack/v1/common_pb';
import type { Error as OrkError } from './gen/orkstack/v1/common_pb';
import type { ImageRef } from './gen/orkstack/v1/activity_pb';
import type { Task } from './gen/orkstack/v1/orkflow_pb';
import type {
	ActivityInfo,
	ActivityInterface,
	FieldSchema
} from './gen/orkstack/v1/service_pb';
import { toJson } from '@bufbuild/protobuf';
import { ValueSchema } from '@bufbuild/protobuf/wkt';
import { shellQuote } from './runDetail';

// An activity is an image. This module holds the two things the
// UI needs to say about that, in one place because both are answered from a
// contract rather than from a component's taste: WHICH activity a task names,
// and WHAT KIND of failure a terminal error was.

/**
 * activityLabel is what a task names, in the author's own words.
 *
 * `uses:` is the only image-selecting field in the DSL (§2.3), so it is what
 * the author wrote and what they will search for. `image.reference` is the
 * same string after resolution and is the fallback for a task read back from
 * a definition that carries only the resolved ref. A task with neither is not
 * a hole to paper over with an empty cell — an event stream can name a task
 * before the definition for it has loaded — so it says so.
 */
export function activityLabel(task: Task | undefined): string {
	if (!task) return '';
	return task.uses || task.image?.reference || '';
}

/**
 * shortDigest abbreviates "sha256:<64 hex>" for a table cell, keeping the
 * algorithm and both ends so two digests are still told apart by eye.
 * The full digest always remains available as a title/copyable full string —
 * this is a rendering convenience, never the only place the value appears.
 */
export function shortDigest(digest: string | undefined): string {
	if (!digest) return '';
	const [algo, hex] = digest.includes(':') ? digest.split(':', 2) : ['', digest];
	if (hex.length <= 20) return digest;
	return `${algo ? algo + ':' : ''}${hex.slice(0, 12)}…${hex.slice(-6)}`;
}

/**
 * pinnedOrReference is the string that was actually pulled when there is one.
 * `ImageRef.pinned` is "the only string that may be pulled or run" (§2.2), so
 * when it is present it is the honest answer to "what ran"; a definition that
 * was never resolved has only what the author typed.
 */
export function pinnedOrReference(image: ImageRef | undefined): string {
	if (!image) return '';
	return image.pinned || image.reference || '';
}

// The five AGENTS.md §4 categories, in human terms (§8.1). The label is what
// a reader sees; the blurb is the one sentence that says who produced it and
// therefore who can fix it. Both come from §8.1's own table rather than being
// invented here, so the UI names a failure the way the rest of the system does.
const CATEGORY_LABEL: Record<FailureCategory, string> = {
	[FailureCategory.UNSPECIFIED]: 'Uncategorised',
	[FailureCategory.ACTIVITY_FAILED]: 'Activity failed',
	[FailureCategory.INFRASTRUCTURE_ERROR]: 'Infrastructure error',
	[FailureCategory.CONTRACT_VIOLATION]: 'Contract violation',
	[FailureCategory.TIMED_OUT]: 'Timed out',
	[FailureCategory.CANCELLED]: 'Cancelled'
};

const CATEGORY_BLURB: Record<FailureCategory, string> = {
	[FailureCategory.UNSPECIFIED]:
		'This failure carries no category. It was recorded before the category existed, or by something that does not set one.',
	[FailureCategory.ACTIVITY_FAILED]:
		'The container exited non-zero for its own reasons. The workload failed, not the platform.',
	[FailureCategory.INFRASTRUCTURE_ERROR]:
		'Storage, the registry, the container runtime or the workspace failed. The workload never got a fair run.',
	[FailureCategory.CONTRACT_VIOLATION]:
		'The image or the definition broke the activity contract. Retrying cannot fix it; the image or the Orkflow has to change.',
	[FailureCategory.TIMED_OUT]:
		'The attempt exceeded its timeout and was stopped.',
	[FailureCategory.CANCELLED]: 'The attempt was cancelled and is never retried automatically.'
};

// Colour carries the same distinction the categories do, and nothing more:
// rose for "the workload failed", amber for "something took it away", and
// violet for "this one will never succeed as written" — the category a reader
// must not mistake for a transient blip.
const CATEGORY_CLASSES: Record<FailureCategory, string> = {
	[FailureCategory.UNSPECIFIED]: 'border-border bg-surface-sunken text-fg-secondary',
	[FailureCategory.ACTIVITY_FAILED]: 'border-danger-200 bg-danger-50 text-danger-700',
	[FailureCategory.INFRASTRUCTURE_ERROR]: 'border-orange-200 bg-orange-50 text-orange-700',
	[FailureCategory.CONTRACT_VIOLATION]: 'border-violet-200 bg-violet-50 text-violet-700',
	[FailureCategory.TIMED_OUT]: 'border-warning-200 bg-warning-50 text-warning-700',
	[FailureCategory.CANCELLED]: 'border-border bg-surface-sunken text-fg-muted'
};

export function failureCategoryLabel(category: FailureCategory | undefined): string {
	return CATEGORY_LABEL[category ?? FailureCategory.UNSPECIFIED];
}

export function failureCategoryBlurb(category: FailureCategory | undefined): string {
	return CATEGORY_BLURB[category ?? FailureCategory.UNSPECIFIED];
}

export function failureCategoryClasses(category: FailureCategory | undefined): string {
	return CATEGORY_CLASSES[category ?? FailureCategory.UNSPECIFIED];
}

/**
 * categoryTestid is the stable enum NAME for a test to select on, rather than
 * the human label, which is prose and may be reworded.
 */
export function categoryTestid(category: FailureCategory | undefined): string {
	return FailureCategory[category ?? FailureCategory.UNSPECIFIED];
}

/**
 * hasError is true for an Error that says anything at all. A zero-valued
 * Error message travels on a successful TaskRun, and rendering a red panel
 * for it would report a failure that did not happen.
 */
export function hasError(err: OrkError | undefined): boolean {
	if (!err) return false;
	return Boolean(err.code || err.message || err.category);
}

// ---------------------------------------------------------------------------
// Activity interface manifests: what an image declares about its `with:`
// block, read by the engine from the org.orkstack.activity.interface label.
// Every string below is the CLI's own (cmd/orkctl/activity.go), so both
// clients say the same words for the same entry (docs/UI_CLI_PARITY.md).
// ---------------------------------------------------------------------------

export const INTERFACE_LABEL = 'org.orkstack.activity.interface';

function countNoun(n: number, noun: string): string {
	return `${n} ${noun}${n === 1 ? '' : 's'}`;
}

/**
 * interfaceSummary is the catalog's Interface column: "-" for an entry that
 * did not resolve, "none" for an image that declares nothing, "malformed" for
 * a label the engine could not read, otherwise "N keys" / "N ops".
 */
export function interfaceSummary(info: ActivityInfo): string {
	if (!info.indexDigest) return '-';
	if (info.interfaceError) return 'malformed';
	const iface = info.interface;
	if (!iface) return 'none';
	const parts: string[] = [];
	const keys = Object.keys(iface.fields).length;
	const ops = Object.keys(iface.ops).length;
	if (keys > 0) parts.push(countNoun(keys, 'key'));
	if (ops > 0) parts.push(countNoun(ops, 'op'));
	return parts.length > 0 ? parts.join(', ') : '0 keys';
}

/** interfaceHeadline is `orkctl activity inspect`'s Interface line. */
export function interfaceHeadline(info: ActivityInfo): string {
	if (!info.indexDigest) return '-';
	if (info.interfaceError) return `malformed — ${info.interfaceError}`;
	const iface = info.interface;
	if (!iface) {
		return `none — the image declares no ${INTERFACE_LABEL} label; with: is checked by the image at run time`;
	}
	const schema = `schema ${iface.schemaVersion}`;
	const ops = Object.keys(iface.ops).length;
	const keys = Object.keys(iface.fields).length;
	if (ops === 0) return `${schema} · ${countNoun(keys, 'key')}`;
	const selection = iface.defaultOp ? `default ${iface.defaultOp}` : 'op required';
	let headline = `${schema} · selected by op: (${countNoun(ops, 'op')}, ${selection})`;
	if (keys > 0) headline += ` · ${countNoun(keys, 'key')} shared by every op`;
	return headline;
}

export interface InterfaceRow {
	/** The op, or "*" for a key every op reads. */
	op: string;
	/** The with: key, or "-" for an op that reads nothing beyond the shared keys. */
	key: string;
	type: string;
	required: string;
	default: string;
}

const sorted = (o: object) => Object.keys(o).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

/** defaultText renders a declared default as compact JSON, or "-". */
export function defaultText(field: FieldSchema): string {
	if (!field.default) return '-';
	return JSON.stringify(toJson(ValueSchema, field.default));
}

/**
 * interfaceRows is the interface table, in the CLI's order: shared keys
 * first (op "*"), then each op by name and each op's keys by name.
 */
export function interfaceRows(iface: ActivityInterface): InterfaceRow[] {
	const rows: InterfaceRow[] = [];
	const row = (op: string, key: string, field: FieldSchema): InterfaceRow => ({
		op,
		key,
		type: field.type,
		required: field.required ? 'yes' : 'no',
		default: defaultText(field)
	});
	for (const key of sorted(iface.fields)) rows.push(row('*', key, iface.fields[key]));
	for (const op of sorted(iface.ops)) {
		const fields = iface.ops[op].fields;
		const keys = sorted(fields);
		if (keys.length === 0) {
			rows.push({ op, key: '-', type: '-', required: '-', default: '-' });
			continue;
		}
		for (const key of keys) rows.push(row(op, key, fields[key]));
	}
	return rows;
}

/** valueRows is the VALUES table: the keys an image writes to values.json. */
export function valueRows(iface: ActivityInterface): { key: string; type: string }[] {
	return sorted(iface.values).map((key) => ({ key, type: iface.values[key].type }));
}

/**
 * pinnedReference is registry/repository@digest — the string a task's
 * ImageRef.pinned carries — for a resolved entry. It normalises the reference
 * the way internal/dsl.ParseReference does (Docker Hub's docker.io and
 * library/), so the UI prints the pin `orkctl activity inspect` prints.
 */
export function pinnedReference(info: ActivityInfo): string {
	if (!info.indexDigest) return '-';
	let remainder = info.reference.trim();
	const at = remainder.lastIndexOf('@');
	if (at >= 0) remainder = remainder.slice(0, at);
	let registry = '';
	let name = remainder;
	const slash = remainder.indexOf('/');
	if (slash >= 0) {
		const first = remainder.slice(0, slash);
		if (first === 'localhost' || /[.:]/.test(first)) {
			registry = first;
			name = remainder.slice(slash + 1);
		}
	}
	if (!registry) registry = 'docker.io';
	const colon = name.lastIndexOf(':');
	if (colon >= 0) name = name.slice(0, colon);
	if (registry === 'docker.io' && !name.includes('/')) name = `library/${name}`;
	return `${registry}/${name}@${info.indexDigest}`;
}

/**
 * inspectCommand is the CLI equivalent of the Inspect form. A reference made
 * only of characters a shell leaves alone is written bare (`orkctl activity
 * inspect file`); anything else is single-quoted.
 */
export function inspectCommand(reference: string): string {
	const word = /^[A-Za-z0-9_./:@-]+$/.test(reference) ? reference : shellQuote(reference);
	return `orkctl activity inspect ${word}`;
}
