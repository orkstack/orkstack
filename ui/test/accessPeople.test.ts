import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { AccessDocumentSchema, BindingSchema, SigninSchema } from '../src/lib/gen/orkstack/v1/access_pb';
import { people } from '../src/lib/accessPeople';

test('people joins sign-ins, the disabled list and the principals bindings name', () => {
	const doc = create(AccessDocumentSchema, {
		disabled: ['user:corp:bo'],
		bindings: [create(BindingSchema, { name: 'ops', role: 'operator', scope: 'deployment', subjects: ['user:corp:ana', 'group:corp:sre', 'user:corp:*'] })]
	});
	const rows = people(doc, [create(SigninSchema, { publicId: 'si_1', principalId: 'user:corp:ana' })]);
	expect(rows.map((r) => r.id)).toEqual(['user:corp:ana', 'user:corp:bo']);
	expect(rows[0].signins.length).toBe(1);
	expect(rows[0].bindings[0].name).toBe('ops');
	expect(rows[1].disabled).toBe(true);
});
