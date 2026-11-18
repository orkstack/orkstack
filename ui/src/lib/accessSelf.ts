// What an Access screen says before a change reaches the person making it,
// and how a write that lost a race reads: from what the engine served (the
// caller's own sign-in and the bindings that grant it), never a guess.
import { Code, ConnectError } from '@connectrpc/connect';
import type { GetSigninResponse } from '#lib/gen/orkstack/v1/auth_service_pb.ts';

/** isMe reports whether a principal id is the caller's own. */
export function isMe(me: GetSigninResponse | null | undefined, principalId: string): boolean {
	return !!me && me.principalId === principalId;
}

/** grantsMe lists the roles, with their scopes, a binding grants the caller:
 * empty when the binding is not one the caller holds anything by. */
export function grantsMe(me: GetSigninResponse | null | undefined, binding: string): { role: string; scope: string }[] {
	return (me?.bindings ?? []).filter((b) => b.binding === binding).map((b) => ({ role: b.role, scope: b.scope }));
}

/** isConflict reports whether a write was refused because the access
 * document changed since the page read it: reading it again and saving
 * again is the way on. */
export function isConflict(err: unknown): boolean {
	return err instanceof ConnectError && err.code === Code.Aborted;
}

/** conflictWords is what a screen says for such a refusal. */
export const conflictWords = 'Someone changed access while you were editing. Read it again to see their change, then save yours again.';
