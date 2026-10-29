// One authored document's editing session: the engine checks it is asked
// for, and the findings and status the editor publishes. It is the light half
// of the editor — no CodeMirror, no parser, no schema — so a page holds it from
// its first paint, its Validate button works before the editor has loaded,
// and the problems list sits wherever the page puts it.
//
// The engine is asked at most once per pause in typing, one request at a time
// (a newer text aborts an older request), never about a document that does not
// parse, and never twice about the same text in the same namespace. Each check
// resolves every image the document names (internal/api, prepareOrkflow), so a
// check per keystroke would be a registry request per keystroke.
import { ConnectError } from '@connectrpc/connect';
import { ValidationSeverity } from '../gen/orkstack/v1/orkflow_service_pb';
import type { EngineIssue } from './server';
import { editorStatus, type EditorStatus, type EngineState } from './status';
import type { Diagnostic } from './types';

/** A finding as listed under the editor: its place, when known, and for the engine's, the issue as the engine wrote it. */
export interface Finding extends Diagnostic {
	line: number | null;
	col: number | null;
	/** The engine's own field and sentence, verbatim (an engine finding only). */
	field?: string;
	raw?: string;
}

export type CheckFn = (text: string, namespace: string, signal: AbortSignal) => Promise<EngineIssue[]>;

/** How long the engine waits after the last keystroke. */
export const CHECK_DELAY_MS = 1200;
const CACHE = 24;
/** An answer is reused for the same text this long: the engine also reads state (images, policy) that moves. */
const CACHE_MS = 30_000;

function unplaced(issue: EngineIssue): Finding {
	return {
		from: 0,
		to: 0,
		line: null,
		col: null,
		severity: issue.severity === ValidationSeverity.WARNING ? 'warning' : 'error',
		source: 'engine',
		message: issue.message,
		path: issue.field,
		field: issue.field,
		raw: issue.message
	};
}

function message(err: unknown): string {
	if (err instanceof ConnectError) return err.rawMessage || err.message;
	return err instanceof Error ? err.message : String(err);
}

export class YamlSession {
	engine = $state<EngineState>({ kind: 'idle' });
	/** 'ready' once the editor runs; 'failed' if it could not load (the plain field stays). */
	editor = $state<'loading' | 'ready' | 'failed'>('loading');
	engineFindings = $state<Finding[]>([]);
	localFindings = $state<Finding[]>([]);
	/**
	 * The document and namespace on screen, kept by the field as they change
	 * (YamlField.svelte), so an answer about anything else never reads as
	 * current — not for a keystroke, a namespace switch or an import.
	 */
	text = $state('');
	namespace = $state('');
	/** What the field's live region says: a settled check's sentence, or a key's explanation. */
	announcement = $state('');
	/** Moves the editor's selection to a finding; set by the editor. */
	jump: ((from: number, to: number) => void) | null = null;

	// Set by the constructor; declared, with a value, before the status reads it.
	#cleanNote = '';

	readonly status: EditorStatus = $derived(
		editorStatus({
			text: this.text,
			namespace: this.namespace,
			local: this.localFindings,
			engine: this.engine,
			cleanNote: this.#cleanNote
		})
	);

	#check: CheckFn;
	#delay: number;
	#timer: ReturnType<typeof setTimeout> | null = null;
	#inFlight: AbortController | null = null;
	#cache = new Map<string, { state: EngineState; at: number }>();

	/**
	 * `cleanNote` follows "The engine finds no problems in this document." and
	 * says what the check did not cover: validation is not the apply.
	 */
	constructor(check: CheckFn, cleanNote = '', delay = CHECK_DELAY_MS) {
		this.#check = check;
		this.#cleanNote = cleanNote;
		this.#delay = delay;
	}

	/**
	 * The document changed: ask the engine once typing pauses. `parses` is the
	 * editor's answer to whether the YAML parses at all; a document that does
	 * not is not sent, and a stale answer is never shown as current.
	 */
	schedule(text: string, namespace: string, parses: boolean): void {
		if (text.trim() === '' || !parses) {
			this.#clearTimer();
			this.#abort();
			if (this.engine.kind !== 'idle') this.engine = { kind: 'idle' };
			return;
		}
		// Asked already about exactly this. An answer (or a failure: it is not
		// retried behind the person's back; the next edit or Validate asks
		// again) stands; a pending check stands while its timer or request is
		// alive, and is asked again if neither is.
		const e = this.engine;
		const same = e.kind !== 'idle' && e.text === text && e.namespace === namespace;
		if (same && (e.kind === 'answered' || e.kind === 'failed')) {
			this.#clearTimer();
			return;
		}
		if (same && e.kind === 'pending' && (this.#timer !== null || this.#inFlight !== null)) return;
		this.#clearTimer();
		const cached = this.#cache.get(this.#key(text, namespace));
		if (cached && Date.now() - cached.at < CACHE_MS) {
			this.#abort();
			this.engine = cached.state;
			return;
		}
		this.engine = { kind: 'pending', text, namespace };
		this.#timer = setTimeout(() => {
			this.#timer = null;
			void this.#run(text, namespace, false);
		}, this.#delay);
	}

	/**
	 * Ask now (the Validate button): the engine's issues, the RPC's error
	 * thrown, or null when a newer check superseded this one.
	 */
	async validateNow(text: string, namespace: string): Promise<EngineIssue[] | null> {
		this.#clearTimer();
		return this.#run(text, namespace, true);
	}

	dispose(): void {
		this.#clearTimer();
		this.#abort();
		// Nothing is asking any more: a pending state would stand unanswered.
		if (this.engine.kind === 'pending') this.engine = { kind: 'idle' };
	}

	#key(text: string, namespace: string): string {
		return `${namespace}\u0000${text}`;
	}

	#clearTimer(): void {
		if (this.#timer !== null) clearTimeout(this.#timer);
		this.#timer = null;
	}

	#abort(): void {
		this.#inFlight?.abort();
		this.#inFlight = null;
	}

	async #run(text: string, namespace: string, explicit: boolean): Promise<EngineIssue[] | null> {
		this.#abort();
		const controller = new AbortController();
		this.#inFlight = controller;
		this.engine = { kind: 'pending', text, namespace };
		try {
			const issues = await this.#check(text, namespace, controller.signal);
			if (controller.signal.aborted) return null;
			const answered: EngineState = { kind: 'answered', text, namespace, issues };
			this.engine = answered;
			// Without the editor (still loading, or failed to), the issues are
			// listed as the engine wrote them, unplaced; the editor places them.
			if (this.editor !== 'ready') this.engineFindings = issues.map((issue) => unplaced(issue));
			this.#cache.set(this.#key(text, namespace), { state: answered, at: Date.now() });
			if (this.#cache.size > CACHE) this.#cache.delete(this.#cache.keys().next().value!);
			return issues;
		} catch (err) {
			// Superseded by a newer check: its answer, not this one, is the news.
			if (controller.signal.aborted) return null;
			this.engine = { kind: 'failed', text, namespace, message: message(err) };
			if (explicit) throw err;
			return null;
		} finally {
			if (this.#inFlight === controller) this.#inFlight = null;
		}
	}
}
