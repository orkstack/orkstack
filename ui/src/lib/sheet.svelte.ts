// The Svelte side of the title block: a holder per layout, found through
// context (never module state, so no server request sees another's block),
// and a version counter so the footer re-reads when a page registers or
// leaves. See ./sheet.ts for the contract.
import { getContext, onDestroy, setContext } from 'svelte';
import { TitleBlockHolder, type TitleGetter } from './sheet';

const KEY = Symbol('title-block');

export class TitleBlockContext {
	version = $state(0);
	holder = new TitleBlockHolder(() => {
		this.version++;
	});
}

/** Called once by the root layout. */
export function provideTitleBlock(): TitleBlockContext {
	const ctx = new TitleBlockContext();
	setContext(KEY, ctx);
	return ctx;
}

/** Called by a page during its init, with a getter over its own state. */
export function setTitleBlock(get: TitleGetter): void {
	const ctx = getContext<TitleBlockContext | undefined>(KEY);
	if (!ctx) return;
	onDestroy(ctx.holder.register(get));
}
