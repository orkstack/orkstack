import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import { Status } from './gen/orkstack/v1/common_pb';

const statuses: Record<string, Status> = {
	pending: Status.PENDING,
	running: Status.RUNNING,
	succeeded: Status.SUCCEEDED,
	failed: Status.FAILED,
	canceled: Status.CANCELED,
	skipped: Status.SKIPPED
};

export function statusFromParam(value: string): Status { return statuses[value] ?? Status.UNSPECIFIED; }
export function statusParam(value: Status): string { return Object.entries(statuses).find(([, status]) => status === value)?.[0] ?? ''; }
export function pageSize(value: string): number { const parsed = Number(value); return [10, 25, 50, 100].includes(parsed) ? parsed : 25; }
export function instant(value: string) {
	if (!value) return undefined;
	const date = new Date(value);
	return Number.isNaN(date.getTime()) ? undefined : timestampFromDate(date);
}
export function runFilterError(status: string, after: string, before: string): string | null {
	if (status && statusFromParam(status) === Status.UNSPECIFIED) return `Unknown run status ${status}.`;
	if (after && !instant(after)) return 'Created-since must be a valid timestamp.';
	if (before && !instant(before)) return 'Created-until must be a valid timestamp.';
	if (after && before && new Date(before).getTime() <= new Date(after).getTime()) return 'Created-until must be after created-since.';
	return null;
}
export function cursorHistory(value: string): string[] {
	try { const parsed: unknown = JSON.parse(value); return Array.isArray(parsed) && parsed.every((entry) => typeof entry === 'string') ? parsed : []; } catch { return []; }
}
export function pushCursor(history: string[], cursor: string): string[] { return cursor ? [...history, cursor] : history; }

export function statsWindowError(window: string, since: string): string | null {
	if (!['15m', '6h', '24h', 'custom'].includes(window)) return `Unknown dashboard window ${window}.`;
	if (window === 'custom' && !since) return 'Custom dashboard windows require a valid since timestamp.';
	if (since && !instant(since)) return 'Dashboard since must be a valid timestamp.';
	return null;
}
