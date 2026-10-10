// When a composed task's card reads its child run, and when it stops.
//
// A run page with twenty composed tasks used to open twenty GetRun reads and
// twenty WebSockets the moment it mounted, one per card, whether or not any
// card was anywhere near the screen. A card now follows its child only while
// it is VISIBLE: the read and the socket start when the card first scrolls
// into view, the socket closes when it leaves (as a collapsed log pane's
// does), and coming back reads once more and follows again. The card's
// summary line — which orkflow, which child run — needs neither, so it is
// there from the first paint.
//
// It also carries the event stream's terminal state ($lib/runEvents): a
// child stream that stopped reconnecting reports 'failed' with its reason,
// and retry() follows again.

export interface ChildFollowerDeps {
	/**
	 * Reads the child once. Resolves true when it should then be followed —
	 * the read succeeded and the child is not terminal.
	 */
	read(runId: string, current: () => boolean): Promise<boolean>;
	/** Opens the child's event stream; returns its stop function. */
	watch(
		runId: string,
		current: () => boolean,
		onState: (state: string, detail?: string) => void
	): () => void;
	/** The stream stopped for good (detail) or is following again (''). */
	onFailure?: (detail: string) => void;
}

export interface ChildFollower {
	setRunId(runId: string): void;
	setVisible(visible: boolean): void;
	retry(): void;
	dispose(): void;
}

export function childFollower(deps: ChildFollowerDeps): ChildFollower {
	let runId = '';
	let visible = false;
	let generation = 0;
	let stop: (() => void) | undefined;

	const halt = () => {
		generation++;
		stop?.();
		stop = undefined;
	};

	const start = () => {
		halt();
		if (!runId || !visible) return;
		const mine = generation;
		const current = () => mine === generation;
		const id = runId;
		deps.onFailure?.('');
		void deps.read(id, current).then((follow) => {
			if (!follow || !current()) return;
			stop = deps.watch(id, current, (state, detail) => {
				if (current() && state === 'failed') deps.onFailure?.(detail ?? 'the event stream stopped');
			});
		});
	};

	return {
		setRunId(next) {
			if (next === runId) return;
			runId = next;
			start();
		},
		setVisible(next) {
			if (next === visible) return;
			visible = next;
			if (visible) start();
			else halt();
		},
		retry() {
			start();
		},
		dispose() {
			halt();
		}
	};
}
