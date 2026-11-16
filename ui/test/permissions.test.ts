import { expect, test } from 'bun:test';
import { can, canAnywhere, covers, missing, permissionRefusal } from '../src/lib/permissions';

test('a scope covers what lies inside it, and nothing beside it', () => {
	expect(covers('deployment', { namespace: 'a', orkflow: 'x' })).toBe(true);
	expect(covers('namespace:a', { namespace: 'a', orkflow: 'x' })).toBe(true);
	expect(covers('namespace:a', { namespace: 'b' })).toBe(false);
	expect(covers('namespace:a', {})).toBe(false);
	expect(covers('orkflow:a/x', { namespace: 'a', orkflow: 'x' })).toBe(true);
	expect(covers('orkflow:a/x', { namespace: 'a' })).toBe(false);
	expect(covers('orkflow:a/x', { namespace: 'a', orkflow: 'y' })).toBe(false);
	expect(covers('pool:gpu', { pool: 'gpu' })).toBe(true);
	expect(covers('pool:gpu', { namespace: 'a' })).toBe(false);
	expect(covers('cluster', { namespace: 'a' })).toBe(false);
});

test('can checks the permission at the target; asserted mode checks nothing', () => {
	const g = { enforced: true, held: [{ scope: 'namespace:a', permissions: ['run.submit', 'run.read'] }] };
	expect(can(g, 'run.submit', { namespace: 'a', orkflow: 'x' })).toBe(true);
	expect(can(g, 'run.submit', { namespace: 'b' })).toBe(false);
	expect(can(g, 'access.write')).toBe(false);
	expect(canAnywhere(g, 'run.read')).toBe(true);
	expect(canAnywhere(g, 'access.read')).toBe(false);
	expect(can({ enforced: false, held: [] }, 'access.write')).toBe(true);
});

test('missing names the permission and where it is needed', () => {
	expect(missing('run.cancel', { namespace: 'a' })).toBe('You need run.cancel on the namespace a.');
	expect(missing('access.write')).toBe('You need access.write on this deployment.');
});

test('permissionRefusal reads a missing permission out of the engine’s refusal, and nothing else', () => {
	expect(
		permissionRefusal('[permission_denied] PERMISSION_DENIED: principal "user:people:vera@example.test" does not hold access.read at deployment')
	).toEqual({ permission: 'access.read', scope: 'deployment' });
	expect(permissionRefusal('[permission_denied] PERMISSION_DENIED: principal "sa:x" does not hold run.cancel at namespace:prod')).toEqual({
		permission: 'run.cancel',
		scope: 'namespace:prod'
	});
	expect(permissionRefusal("[permission_denied] SIGNIN_CSRF: the request does not carry the sign-in's Ork-CSRF token")).toBeNull();
	expect(permissionRefusal('[unavailable] the engine did not answer')).toBeNull();
});
