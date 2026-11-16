// Access → People, from what the engine serves: the sign-ins it holds
// (ListSignins), the principals the document disables, and the literal
// principals its bindings name. A principal seen only in the audit trail is
// not here until the engine lists principals itself.
import type { AccessDocument, Signin } from '#lib/gen/orkstack/v1/access_pb.ts';

export interface PersonRow {
	id: string;
	signins: Signin[];
	disabled: boolean;
	/** The bindings that name this principal itself, not a group of it. */
	bindings: { name: string; role: string; scope: string }[];
}

const PRINCIPAL = /^(user|service|serviceaccount|cert):/;

export function people(doc: AccessDocument, signins: Signin[]): PersonRow[] {
	const rows = new Map<string, PersonRow>();
	const row = (id: string) => {
		let r = rows.get(id);
		if (!r) {
			r = { id, signins: [], disabled: false, bindings: [] };
			rows.set(id, r);
		}
		return r;
	};
	for (const s of signins) row(s.principalId).signins.push(s);
	for (const id of doc.disabled) row(id).disabled = true;
	for (const b of [...doc.configuredBindings, ...doc.bindings]) {
		for (const subject of b.subjects) {
			if (!PRINCIPAL.test(subject) || subject.endsWith('*')) continue;
			row(subject).bindings.push({ name: b.name, role: b.role, scope: b.scope });
		}
	}
	return [...rows.values()].sort((a, b) => a.id.localeCompare(b.id));
}
