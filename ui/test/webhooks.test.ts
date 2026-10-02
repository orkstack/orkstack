import { expect, test } from 'bun:test';
import { EventType } from '../src/lib/gen/orkstack/v1/run_pb';
import { RUN_EVENTS, SCOPE_SLUG_PATTERN, TASK_EVENTS, webhookScope } from '../src/lib/webhooks';

// The scope cell reads exactly as orkctl's SCOPE column does
// (cmd/orkctl/webhook.go webhookScopeCell), except that the all-empty case
// says what it means in words.
test('an unscoped subscription reads Deployment-wide', () => {
	expect(webhookScope({ namespace: '', orkflowName: '', taskId: '' })).toBe('Deployment-wide');
});

test('a scope is positional, with * for each unset component', () => {
	expect(webhookScope({ namespace: 'analytics', orkflowName: 'nightly-etl', taskId: 'extract' })).toBe(
		'analytics/nightly-etl/extract'
	);
	expect(webhookScope({ namespace: 'analytics', orkflowName: '', taskId: '' })).toBe('analytics/*/*');
	expect(webhookScope({ namespace: '', orkflowName: 'nightly-etl', taskId: '' })).toBe('*/nightly-etl/*');
	expect(webhookScope({ namespace: 'analytics', orkflowName: '', taskId: 'extract' })).toBe('analytics/*/extract');
});

test('the offered events are the ten subscribable ones in lifecycle order, never TASK_LOG', () => {
	expect(RUN_EVENTS).toEqual([
		EventType.RUN_STARTED,
		EventType.RUN_SUCCEEDED,
		EventType.RUN_FAILED,
		EventType.RUN_CANCELED
	]);
	expect(TASK_EVENTS).toEqual([
		EventType.TASK_SCHEDULED,
		EventType.TASK_STARTED,
		EventType.TASK_AWAITING,
		EventType.TASK_RESOLVED,
		EventType.TASK_SUCCEEDED,
		EventType.TASK_FAILED
	]);
	expect([...RUN_EVENTS, ...TASK_EVENTS]).not.toContain(EventType.TASK_LOG);
});

test('the client slug pattern is the server rule, anchored as the pattern attribute anchors it', () => {
	const slug = new RegExp(`^(?:${SCOPE_SLUG_PATTERN})$`);
	for (const ok of ['analytics', 'nightly-etl', 'a1']) expect(slug.test(ok)).toBe(true);
	for (const bad of ['Analytics', '-x', 'a b', '..', '1a']) expect(slug.test(bad)).toBe(false);
});
