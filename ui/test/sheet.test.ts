import { expect, test } from 'bun:test';
import { TitleBlockHolder, titleCellTestid, type TitleField } from '../src/lib/sheet';

// The title block's holder: a page registers a getter; the block follows the
// page's own state; a cleanup clears only its own registration.

test('the block follows the state the getter reads, with no second call', () => {
	const holder = new TitleBlockHolder();
	const state = { status: 'RUNNING' };
	holder.register(() => [{ label: 'STATUS', value: state.status }]);
	expect(holder.fields()?.[0].value).toBe('RUNNING');
	state.status = 'FAILED';
	expect(holder.fields()?.[0].value).toBe('FAILED');
});

test('a page that registers nothing leaves the block to its defaults (null)', () => {
	expect(new TitleBlockHolder().fields()).toBeNull();
});

test('the old page cleaning up AFTER the new one registered keeps the new block', () => {
	const holder = new TitleBlockHolder();
	const oldCleanup = holder.register(() => [{ label: 'PAGE', value: 'Runs' }]);
	const fresh: TitleField[] = [{ label: 'PAGE', value: 'Orkflows' }];
	holder.register(() => fresh);
	oldCleanup();
	expect(holder.fields()).toEqual(fresh);
});

test('the old page cleaning up BEFORE the new one registers also ends with the new block', () => {
	const holder = new TitleBlockHolder();
	const oldCleanup = holder.register(() => [{ label: 'PAGE', value: 'Runs' }]);
	oldCleanup();
	expect(holder.fields()).toBeNull();
	holder.register(() => [{ label: 'PAGE', value: 'Orkflows' }]);
	expect(holder.fields()?.[0].value).toBe('Orkflows');
});

test('every registration and cleanup notifies, so the footer re-reads', () => {
	let n = 0;
	const holder = new TitleBlockHolder(() => n++);
	const stop = holder.register(() => []);
	stop();
	stop(); // a second cleanup of the same registration is a no-op
	expect(n).toBe(2);
});

test('cell testids are the label, lower-cased', () => {
	expect(titleCellTestid('STATUS')).toBe('title-block-status');
	expect(titleCellTestid('PAGE')).toBe('title-block-page');
});
