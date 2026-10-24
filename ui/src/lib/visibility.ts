// Polling that stops while nobody is looking.
//
// A dashboard left open in a background tab kept asking the engine for
// statistics every five seconds, all night, for a page no one could see —
// work for the engine and its store with no reader at the other end.
// pollWhileVisible runs `fn` every `ms` only while the document is visible,
// stops on hidden, and runs it ONCE IMMEDIATELY on becoming visible again, so
// the reader who comes back sees a fresh answer rather than one up to `ms`
// old. The run page's live poll keeps the same rule.

export interface PollEnv {
	document: {
		visibilityState: string;
		addEventListener(type: 'visibilitychange', fn: () => void): void;
		removeEventListener(type: 'visibilitychange', fn: () => void): void;
	};
	setInterval(fn: () => void, ms: number): unknown;
	clearInterval(id: unknown): void;
}

function browserEnv(): PollEnv {
	return {
		document,
		setInterval: (fn, ms) => setInterval(fn, ms),
		clearInterval: (id) => clearInterval(id as ReturnType<typeof setInterval>)
	};
}

export interface PollOptions {
	/** Also run `fn` at once when polling first starts (default false). */
	immediate?: boolean;
}

/** Starts polling; the returned function stops it for good. */
export function pollWhileVisible(
	fn: () => void,
	ms: number,
	options: PollOptions = {},
	env: PollEnv = browserEnv()
): () => void {
	let timer: unknown = undefined;
	const start = (runNow: boolean) => {
		if (timer !== undefined) return;
		if (runNow) fn();
		timer = env.setInterval(fn, ms);
	};
	const stop = () => {
		if (timer === undefined) return;
		env.clearInterval(timer);
		timer = undefined;
	};
	const onVisibility = () => (env.document.visibilityState === 'hidden' ? stop() : start(true));
	env.document.addEventListener('visibilitychange', onVisibility);
	if (env.document.visibilityState !== 'hidden') start(options.immediate ?? false);
	return () => {
		stop();
		env.document.removeEventListener('visibilitychange', onVisibility);
	};
}
