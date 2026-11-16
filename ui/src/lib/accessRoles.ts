// The role screen's picker: the catalogue grouped by resource, the part of a
// permission's name before its first dot, in the catalogue's own order.
import { ScopeKind, type Permission } from '#lib/gen/orkstack/v1/access_pb.ts';

export interface PermissionGroup {
	resource: string;
	permissions: Permission[];
}

export function groupByResource(perms: Permission[]): PermissionGroup[] {
	const out: PermissionGroup[] = [];
	for (const p of perms) {
		const resource = p.name.split('.', 1)[0];
		let g = out.find((x) => x.resource === resource);
		if (!g) {
			g = { resource, permissions: [] };
			out.push(g);
		}
		g.permissions.push(p);
	}
	return out;
}

const kindWords: Record<number, string> = {
	[ScopeKind.DEPLOYMENT]: 'the deployment',
	[ScopeKind.NAMESPACE]: 'a namespace',
	[ScopeKind.ORKFLOW]: 'an Orkflow',
	[ScopeKind.POOL]: 'a pool'
};

/** Where a permission can be bound, in words. */
export function whereWords(p: Permission): string {
	const words = p.scopeKinds.map((k) => kindWords[k]).filter(Boolean);
	return words.length ? `Bound at ${words.join(', ')}` : '';
}
