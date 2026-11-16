import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { PermissionSchema, ScopeKind } from '../src/lib/gen/orkstack/v1/access_pb';
import { groupByResource, whereWords } from '../src/lib/accessRoles';

test('groupByResource keeps the catalogue order within and across resources', () => {
	const perms = ['run.read', 'run.submit', 'orkflow.write', 'run.cancel'].map((name) => create(PermissionSchema, { name }));
	const g = groupByResource(perms);
	expect(g.map((x) => x.resource)).toEqual(['run', 'orkflow']);
	expect(g[0].permissions.map((p) => p.name)).toEqual(['run.read', 'run.submit', 'run.cancel']);
});

test('whereWords names every kind of scope a permission can be bound at', () => {
	const p = create(PermissionSchema, { name: 'run.read', scopeKinds: [ScopeKind.DEPLOYMENT, ScopeKind.NAMESPACE] });
	expect(whereWords(p)).toBe('Bound at the deployment, a namespace');
});
