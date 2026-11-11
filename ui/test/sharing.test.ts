import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { OrkflowShareSchema } from '../src/lib/gen/orkstack/v1/orkflow_pb';
import { RunSchema, TaskChildSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { childLabel, childOrkflowHref } from '../src/lib/compose';
import { orkflowOwner, runOrkflowLabel, sharedWithLabel } from '../src/lib/sharing';

test('a child run of a shared Orkflow is named with its owner', () => {
	const shared = create(RunSchema, { orkflowName: 'deploy', namespace: 'search', orkflowNamespace: 'platform' });
	expect(runOrkflowLabel(shared)).toBe('platform/deploy');
	expect(orkflowOwner(shared, 'search')).toBe('platform');
	const own = create(RunSchema, { orkflowName: 'deploy', namespace: 'search' });
	expect(runOrkflowLabel(own)).toBe('deploy');
	expect(orkflowOwner(own, 'search')).toBe('search');
});

test('a composed task of another namespace names and links that namespace', () => {
	const child = create(TaskChildSchema, { orkflowName: 'deploy', orkflowVersion: '1.0.0', orkflowNamespace: 'platform' });
	expect(childLabel(child)).toBe('platform/deploy@1.0.0');
	expect(childOrkflowHref('search', child)).toBe('/orkflows/deploy?version=1.0.0&ns=platform');
	const own = create(TaskChildSchema, { orkflowName: 'helper', orkflowVersion: '1' });
	expect(childOrkflowHref('search', own)).toBe('/orkflows/helper?version=1&ns=search');
});

test('shared_with renders as the CLI renders it', () => {
	expect(
		sharedWithLabel([
			create(OrkflowShareSchema, { namespace: '*' }),
			create(OrkflowShareSchema, { namespace: 'payments' }),
			create(OrkflowShareSchema, { orkflow: 'search/release' })
		])
	).toEqual(['every namespace', 'namespace payments', 'Orkflow search/release']);
	expect(sharedWithLabel([])).toEqual([]);
});

test('grouped rows name a shared Orkflow with its owner', async () => {
	const { FailureGroupBy, FailureGroupSchema, FlakeGroupSchema, UsageRowSchema } = await import('../src/lib/gen/orkstack/v1/stats_pb');
	const { fingerprintCell } = await import('../src/lib/stats');
	const { flakeCell } = await import('../src/lib/flakes');
	const { usageCell } = await import('../src/lib/usage');
	const { ownedLabel } = await import('../src/lib/sharing');
	expect(ownedLabel('platform', 'deploy')).toBe('platform/deploy');
	expect(ownedLabel('', 'deploy')).toBe('deploy');
	const owned = { orkflowName: 'deploy', orkflowNamespace: 'platform' };
	expect(fingerprintCell(create(FailureGroupSchema, owned), FailureGroupBy.ORKFLOW).text).toBe('platform/deploy');
	expect(flakeCell(create(FlakeGroupSchema, owned), FailureGroupBy.ORKFLOW)).toBe('platform/deploy');
	expect(usageCell(create(UsageRowSchema, owned), FailureGroupBy.ORKFLOW)).toBe('platform/deploy');
});
