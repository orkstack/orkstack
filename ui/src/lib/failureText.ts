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
 * sentence leads with — or that leads the part after a fan-out's own "item 3
 * of 6 failed: " — so a panel can lead with that code.
 */
export interface PlainFailure {
	/** The §8.2 reason code the sentence (or a part of it after ": ") led with. */
	code: string | undefined;
	/** The sentence, without its wrapper, its decoration or that code. */
	text: string;
}

// The wrapper may lead the text or follow a sentence of the product's own
// ("item 3 of 6 failed: activity error (…): …"); every occurrence goes.
const WRAPPER = /(?:activity|local activity|child workflow execution) error \([^()]*\): /g;
const DECORATION = / \(type: [A-Za-z0-9_.]+, retryable: (?:true|false)\)/g;
// A reason code leads the sentence, or the part of it after a ": ".
const REASON_CODE = /(^|: )([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+): /;

export function plainFailure(message: string): PlainFailure {
	const text = message.trim().replace(WRAPPER, '').replace(DECORATION, '');
	const found = REASON_CODE.exec(text);
	if (!found) return { code: undefined, text };
	const at = found.index;
	return { code: found[2], text: text.slice(0, at) + found[1] + text.slice(at + found[0].length) };
}
