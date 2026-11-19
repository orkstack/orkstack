import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { AccessDocumentSchema, BindingSchema, SeenPrincipalSchema, SigninSchema } from '../src/lib/gen/orkstack/v1/access_pb';
import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import { people, peopleCommand, peopleWindow, readSeen, steppedDownSentence } from '../src/lib/accessPeople';

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

test('people puts who signed in most recently first, with the name the sign-in gave, and the rest by id', () => {
	const doc = create(AccessDocumentSchema, {
		disabled: ['user:corp:zed'],
		bindings: [create(BindingSchema, { name: 'ops', role: 'operator', scope: 'deployment', subjects: ['user:corp:ana'] })]
	});
	const seen = [
		create(SeenPrincipalSchema, { id: 'user:corp:bo', display: 'Bo', issuer: 'corp', lastSeen: timestampFromDate(new Date('2026-11-18T10:00:00Z')) }),
		create(SeenPrincipalSchema, { id: 'user:people:vera', display: 'vera@example.test', issuer: 'people', lastSeen: timestampFromDate(new Date('2026-11-18T11:00:00Z')) })
	];
	const rows = people(doc, [], seen);
	expect(rows.map((r) => r.id)).toEqual(['user:people:vera', 'user:corp:bo', 'user:corp:ana', 'user:corp:zed']);
	expect(rows[0].display).toBe('vera@example.test');
	expect(rows[2].lastSeen).toBeUndefined();
	expect(rows[3].disabled).toBe(true);
});

test('the window the URL names, defaulting to thirty days, and its command', () => {
	expect(peopleWindow('7d')).toBe('7d');
	expect(peopleWindow(null)).toBe('30d');
	expect(peopleWindow('toString')).toBe('30d');
	expect(peopleCommand('30d')).toBe('orkctl admin principal list --window 720h');
	expect(peopleCommand('1d')).toBe('orkctl admin principal list --window 24h');
});

test('readSeen steps down from the window asked for while the engine refuses one as too large, and stops at any other failure', async () => {
	const tooLarge = (err: unknown) => err === 'big';
	const words = (err: unknown) => `failed: ${String(err)}`;
	const reader = (refuse: Record<string, unknown>) => async (w: string) => {
		if (w in refuse) throw refuse[w];
		return { seen: [create(SeenPrincipalSchema, { id: `user:corp:${w}` })], truncated: false };
	};

	const at7 = await readSeen('30d', reader({ '30d': 'big' }), tooLarge, words);
	expect([at7.shown, at7.seen[0].id, at7.error]).toEqual(['7d', 'user:corp:7d', null]);
	expect(steppedDownSentence(at7)).toBe('Sign-ins of the last 7 days; 30 days examines more audit records than one request may.');

	const at1 = await readSeen('30d', reader({ '30d': 'big', '7d': 'big' }), tooLarge, words);
	expect(at1.shown).toBe('1d');
	expect(steppedDownSentence(at1)).toBe('Sign-ins of the last 24 hours; 30 days examines more audit records than one request may.');

	const none = await readSeen('30d', reader({ '30d': 'big', '7d': 'big', '1d': 'big' }), tooLarge, words);
	expect([none.shown, none.tooLong, none.error]).toEqual([null, true, 'failed: big']);

	// Another failure is not mended by a shorter window: it stops at once.
	const down = await readSeen('30d', reader({ '30d': 'unavailable' }), tooLarge, words);
	expect([down.shown, down.tooLong, down.error]).toEqual([null, false, 'failed: unavailable']);

	// Asked for a short window, it never reads a longer one.
	const asked = await readSeen('7d', reader({}), tooLarge, words);
	expect([asked.shown, steppedDownSentence(asked)]).toEqual(['7d', '']);
});
