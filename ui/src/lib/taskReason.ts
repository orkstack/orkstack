// What a task's row says about why it ended as it did: a word or two. The whole
// sentence is the engine's own and is shown as it is on the task's panel. A row
// never prints a long message, because a run page holds thousands of rows and
// an operator's cancel reason or a condition's whole expression has no bound.
import { Status } from './gen/orkstack/v1/common_pb';

/** The longest message a row prints as it is, when no word stands for it. */
const ROW_MESSAGE_MAX = 40;

const WORDS: readonly (readonly [RegExp, string])[] = [
	[/^task skipped: condition /, 'condition false'],
	[/^task skipped: upstream task /, 'need skipped'],
	[/^task skipped: join none_failed: every need was skipped/, 'every need skipped'],
	[/^task skipped: join none_failed: need /, 'a need did not succeed'],
	[/^task skipped: join any_failed: /, 'no need failed'],
	[/^task canceled: the run is failing/, 'run failing'],
	[/^task canceled: the run was canceled/, 'run canceled'],
	[/^task canceled: (the )?run ended/, 'run ended first'],
	[/^await rejected /, 'rejected']
];
const UNFINISHED = /^task (?:failed|canceled): run ended with (\d+) of (\d+) items unfinished/;

/** The row's word for a task's message, or '' when its status pill already says it all. */
export function reasonWord(status: Status | undefined, message: string): string {
	if (!message) return '';
	for (const [pattern, word] of WORDS) if (pattern.test(message)) return word;
	const unfinished = UNFINISHED.exec(message);
	if (unfinished) return `${unfinished[1]} of ${unfinished[2]} items unfinished`;
	// Any other skip or cancel: the pill says so, and the panel says why.
	if (status === Status.SKIPPED || status === Status.CANCELED) return '';
	return message.length <= ROW_MESSAGE_MAX ? message : '';
}
