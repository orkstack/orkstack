import { expect, test } from 'bun:test';
import { ConnectError, Code } from '@connectrpc/connect';
import { isStaleEtagConflict } from '../src/lib/adminErrors';

// UpdateNamespace's two FailedPrecondition conditions, verbatim from
// internal/api/namespaces.go, so this test drifts with the server's own
// wording rather than a paraphrase of it.
const STALE_ETAG_MESSAGE =
	'namespace "payments" changed since it was read (etag "abc123" is stale); ' +
	'read it again and re-apply the change';
const RACE_MESSAGE =
	'namespace "payments" was written by somebody else since it was read ' +
	'(it had no stored version then and has one now); read it again and re-apply the change';
const NO_REGISTRY_MESSAGE =
	'this engine has no namespace registry configured, so namespaces cannot ' +
	'be created, changed or removed';

test('a stale etag (the IfMatch case) is recognized as a conflict', () => {
	const err = new ConnectError(STALE_ETAG_MESSAGE, Code.FailedPrecondition);
	expect(isStaleEtagConflict(err)).toBe(true);
});

test('the IfAbsent race — written underneath a read that found nothing stored — is recognized as a conflict', () => {
	const err = new ConnectError(RACE_MESSAGE, Code.FailedPrecondition);
	expect(isStaleEtagConflict(err)).toBe(true);
});

test('a FailedPrecondition for an absent namespace registry is NOT an etag conflict — reloading cannot fix it', () => {
	const err = new ConnectError(NO_REGISTRY_MESSAGE, Code.FailedPrecondition);
	expect(isStaleEtagConflict(err)).toBe(false);
});

test('a FailedPrecondition with no code at all is never a conflict', () => {
	const err = new ConnectError(STALE_ETAG_MESSAGE, Code.Unavailable);
	expect(isStaleEtagConflict(err)).toBe(false);
});

test('a non-ConnectError is never a conflict', () => {
	expect(isStaleEtagConflict(new Error(STALE_ETAG_MESSAGE))).toBe(false);
	expect(isStaleEtagConflict('nope')).toBe(false);
});
