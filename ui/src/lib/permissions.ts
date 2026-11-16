// What the caller may do, as GetSignin answered it: its permissions by
// scope. The server decides every request; this only lets a page hide or
// disable what would be refused, and say why (UX35, "The server decides").
//
// A scope covers what lies inside it: the deployment covers everything, a
// namespace covers its Orkflows, an Orkflow covers only itself, a pool only
// itself (internal/authz, docs/ACCESS.md).

export interface ScopedPermissions {
	scope: string;
	permissions: string[];
}

/** Where a request acts: what a permission must cover. */
export interface Target {
	namespace?: string;
	orkflow?: string;
	pool?: string;
}

/** covers reports whether a binding's scope covers target. */
export function covers(scope: string, target: Target): boolean {
	if (scope === 'deployment') return true;
	if (scope.startsWith('namespace:')) {
		return target.namespace !== undefined && scope === `namespace:${target.namespace}`;
	}
	if (scope.startsWith('orkflow:')) {
		return target.namespace !== undefined && target.orkflow !== undefined && scope === `orkflow:${target.namespace}/${target.orkflow}`;
	}
	if (scope.startsWith('pool:')) return target.pool !== undefined && scope === `pool:${target.pool}`;
	return false;
}

/** The caller's permissions, and whether this deployment checks them at all. */
export interface Grants {
	/** False in asserted mode: nothing is checked, and nothing is hidden. */
	enforced: boolean;
	held: ScopedPermissions[];
}

/** can reports whether the caller holds permission at target. A deployment
 * that does not authenticate checks nothing, so everything is allowed. */
export function can(g: Grants, permission: string, target: Target = {}): boolean {
	if (!g.enforced) return true;
	return g.held.some((sp) => sp.permissions.includes(permission) && covers(sp.scope, target));
}

/** canAnywhere reports whether the caller holds permission at any scope: a
 * navigation entry shows when it leads somewhere the caller may act. */
export function canAnywhere(g: Grants, permission: string): boolean {
	if (!g.enforced) return true;
	return g.held.some((sp) => sp.permissions.includes(permission));
}

/** The reason beside a control the caller may not use. */
export function missing(permission: string, target: Target = {}): string {
	const where = target.orkflow
		? `the Orkflow ${target.namespace}/${target.orkflow}`
		: target.namespace
			? `the namespace ${target.namespace}`
			: target.pool
				? `the pool ${target.pool}`
				: 'this deployment';
	return `You need ${permission} on ${where}.`;
}

/** The permission a refusal names, when its message names one. */
export function refusedPermission(message: string): string {
	const m = /\b([a-z]+(?:\.[a-z-]+)+)\b/.exec(message);
	return m ? m[1] : '';
}

/** The permission and scope an engine's PERMISSION_DENIED refusal names
 * (authz: `principal "…" does not hold <permission> at <scope>`); null for
 * any other refusal, a CSRF or sign-in one among them. */
export function permissionRefusal(message: string): { permission: string; scope: string } | null {
	const m = /PERMISSION_DENIED: principal ".*" does not hold ([a-z]+(?:\.[a-z-]+)+) at (\S+)$/.exec(message.trim());
	return m ? { permission: m[1], scope: m[2] } : null;
}
