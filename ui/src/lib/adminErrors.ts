// Distinguishing an etag conflict from every other FailedPrecondition:
// UpdateNamespace answers FailedPrecondition for
// TWO unrelated conditions —
//
//   - a stale etag (the "two tabs" case): the object changed since this
//     form read it, and the documented recovery is "read it again and
//     re-apply" (internal/api/namespaces.go UpdateNamespace, both the
//     IfMatch-stale and the IfAbsent-race branches);
//   - this engine has no durable namespace registry wired at all
//     (internal/api/namespaces.go's own `registry()`), which "reload and
//     re-apply" cannot fix — reloading asks the very thing that is absent.
//
// A UI that renders every FailedPrecondition as a conflict offers a retry
// button that repeats the same failure forever for the second condition.
// The two are told apart by the server's own words, not merely its code:
// both real-conflict messages say "since it was read" (see the two error
// strings above), and the "no registry" message never does.
import { ConnectError, Code } from '@connectrpc/connect';

export function isStaleEtagConflict(err: unknown): boolean {
	return (
		err instanceof ConnectError &&
		err.code === Code.FailedPrecondition &&
		err.rawMessage.includes('since it was read')
	);
}
