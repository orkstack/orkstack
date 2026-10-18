/**
 * What a failure says, in the product's words.
 *
 * A task's error, and the run's RUN_FAILED built from it, carry the text the
 * execution service recorded: the Orker's own sentence ("CONTAINER_EXIT_NONZERO:
 * task "charge": the container exited 3") inside the service's wrapper —
 * "activity error (type: ExecuteTask, scheduledEventID: 86, …): " before it and
 * "(type: ActivityFailed, retryable: true)" after. The wrapper names internal
 * mechanisms (AGENTS.md §7) and says nothing a reader can act on; the events
 * keep it verbatim. plainFailure drops it and splits off the reason code the
 * sentence leads with, so a panel can lead with that code.
 */
export interface PlainFailure {
	/** The §8.2 reason code the sentence led with, when it led with one. */
	code: string | undefined;
	/** The sentence, without its wrapper, its decoration or that code. */
	text: string;
}

const WRAPPER = /^(?:activity|local activity|child workflow execution) error \([^()]*\): /;
const DECORATION = / \(type: [A-Za-z0-9_.]+, retryable: (?:true|false)\)/g;
const LEADING_CODE = /^([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+): ([\s\S]*)$/;

export function plainFailure(message: string): PlainFailure {
	let text = message.trim();
	for (let next = text.replace(WRAPPER, ''); next !== text; next = text.replace(WRAPPER, '')) text = next;
	text = text.replace(DECORATION, '');
	const led = LEADING_CODE.exec(text);
	if (!led) return { code: undefined, text };
	return { code: led[1], text: led[2] };
}
