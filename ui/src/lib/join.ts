// Join policies and failure handlers on the run and Orkflow pages, pure and
// unit-tested (docs/DSL.md "Joins and failure handlers"). Every word mirrors
// cmd/orkctl (dsl.JoinWord, run get's RUNNING (failing), the failing note),
// so both clients say the same thing about one run (docs/UI_CLI_PARITY.md).
import { JoinPolicy } from './gen/orkstack/v1/activity_pb';
import type { Error as OrkError } from './gen/orkstack/v1/common_pb';
import { Status } from './gen/orkstack/v1/common_pb';
import { EventType, type Event } from './gen/orkstack/v1/run_pb';
import { hasError } from './activity';

/** The DSL's word for a policy: all_succeeded for UNSPECIFIED. */
export function joinWord(policy: JoinPolicy | undefined): string {
	switch (policy) {
		case JoinPolicy.NONE_FAILED:
			return 'none_failed';
		case JoinPolicy.ANY_FAILED:
			return 'any_failed';
		case JoinPolicy.ALL_DONE:
			return 'all_done';
		default:
			return 'all_succeeded';
	}
}

/** A failure handler: join any_failed or all_done. */
export function isFailureHandler(policy: JoinPolicy | undefined): boolean {
	return policy === JoinPolicy.ANY_FAILED || policy === JoinPolicy.ALL_DONE;
}

/** The sentence a join's tooltip shows, with the needs it decides on. */
export function joinTitle(policy: JoinPolicy | undefined, needs: readonly string[] = []): string {
	let sentence: string;
	switch (policy) {
		case JoinPolicy.NONE_FAILED:
			sentence =
				'Starts when every need is terminal and none failed or was canceled — branches that were skipped are tolerated';
			break;
		case JoinPolicy.ANY_FAILED:
			sentence = 'Starts only if one of its needs fails — a failure handler';
			break;
		case JoinPolicy.ALL_DONE:
			sentence = 'Starts once every need is terminal, whatever it did — a failure handler';
			break;
		default:
			sentence = 'Starts when every need succeeded';
	}
	return needs.length > 0 ? `${sentence} (needs: ${needs.join(', ')})` : sentence;
}

/**
 * isFailingNote is the one TASK_LOG that is about the run rather than a task:
 * the failing note a run with failure handlers emits when it starts failing.
 * It carries no task id and no item, and nothing else does.
 */
export function isFailingNote(event: Pick<Event, 'type' | 'taskId' | 'item'>): boolean {
	return event.type === EventType.TASK_LOG && !event.taskId && event.item === undefined;
}

/**
 * runFailing is whether the run page shows the Failing banner: the LIVE run
 * status is still RUNNING and either the snapshot's Run.error is set or the
 * failing note arrived. A terminal status — FAILED, or the CANCELED a
 * terminated failing run reads — is never failing.
 */
export function runFailing(status: Status | undefined, error: OrkError | undefined, note: string): boolean {
	return status === Status.RUNNING && (hasError(error) || note !== '');
}

/** One outstanding failure handler, for the banner. */
export interface OutstandingHandler {
	id: string;
	word: 'pending' | 'running' | 'awaiting';
}

function statusWord(status: Status | undefined): OutstandingHandler['word'] | undefined {
	switch (status) {
		case Status.RUNNING:
			return 'running';
		case Status.AWAITING:
			return 'awaiting';
		case Status.PENDING:
		case Status.UNSPECIFIED:
		case undefined:
			return 'pending';
		default:
			return undefined;
	}
}

/**
 * outstandingHandlers is, over the page's LIVE task views in their order, the
 * failure handlers that are not terminal, each with its status word. joinOf
 * reads the pinned definition's Task.join, falling back to TaskRun.join.
 */
export function outstandingHandlers(
	tasks: readonly { id: string; status: Status | undefined }[],
	joinOf: (id: string) => JoinPolicy | undefined
): OutstandingHandler[] {
	const out: OutstandingHandler[] = [];
	for (const task of tasks) {
		if (!isFailureHandler(joinOf(task.id))) continue;
		const word = statusWord(task.status);
		if (word) out.push({ id: task.id, word });
	}
	return out;
}

/** The banner's sentence. */
export function failingBannerText(handlers: readonly OutstandingHandler[]): string {
	if (handlers.length === 0) return 'Failing — finishing.';
	const list = handlers.map((h) => `${h.id} (${h.word})`).join(', ');
	return `Failing — the run ends FAILED once its failure handlers finish: ${list}. Cancel stops them; the run still ends FAILED.`;
}
