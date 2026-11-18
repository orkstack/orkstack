import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Code, ConnectError } from '@connectrpc/connect';
import { ExplainedBindingSchema } from '../src/lib/gen/orkstack/v1/access_pb';
import { GetSigninResponseSchema } from '../src/lib/gen/orkstack/v1/auth_service_pb';
import { grantsMe, isConflict, isMe } from '../src/lib/accessSelf';

const me = create(GetSigninResponseSchema, {
	principalId: 'user:people:ana@example.test',
	bindings: [
		create(ExplainedBindingSchema, { binding: 'admins', role: 'admin', scope: 'deployment' }),
		create(ExplainedBindingSchema, { binding: 'ops', role: 'operator', scope: 'namespace:ops' })
	]
});

test('isMe holds only the caller’s own principal id, and nothing without a sign-in', () => {
	expect(isMe(me, 'user:people:ana@example.test')).toBe(true);
	expect(isMe(me, 'user:people:bob@example.test')).toBe(false);
	expect(isMe(null, 'user:people:ana@example.test')).toBe(false);
});

test('grantsMe names what a binding grants the caller, and nothing for another binding', () => {
	expect(grantsMe(me, 'admins')).toEqual([{ role: 'admin', scope: 'deployment' }]);
	expect(grantsMe(me, 'viewers')).toEqual([]);
	expect(grantsMe(undefined, 'admins')).toEqual([]);
});

test('isConflict is the document-changed refusal only', () => {
	expect(isConflict(new ConnectError('the access document changed', Code.Aborted))).toBe(true);
	expect(isConflict(new ConnectError('no', Code.PermissionDenied))).toBe(false);
	expect(isConflict(new Error('aborted'))).toBe(false);
});
