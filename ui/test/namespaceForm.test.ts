import { expect, test } from 'bun:test';
import { allowedSecretsLabel, attributeCountLabel, budgetCommand, budgetPeriodHours, holdRequest, maxRunningLabel, maxRunningRequest, runRetentionLabel, taskLogsSelectValue } from '../src/lib/namespaceForm';
import { createdNamespaceHref, namespacePageHref, namespaceReturnHref } from '../src/lib/namespaceList';
import { TaskLogExport } from '../src/lib/gen/orkstack/v1/telemetry_pb';

test('a stored field reads as what it means, never a dash that could read as its opposite', () => {
	expect(maxRunningLabel(0)).toBe('unlimited');
	expect(maxRunningLabel(3)).toBe('3');
	expect(allowedSecretsLabel([])).toBe('unrestricted');
	expect(allowedSecretsLabel(['env:A', 'env:B'])).toBe('env:A, env:B');
	expect(runRetentionLabel(undefined)).toContain('inherited');
	expect(attributeCountLabel(undefined)).toBe('—');
});

test("what a form's text sends", () => {
	expect(holdRequest('  ')).toBeUndefined();
	expect(holdRequest('INC-1')).toEqual({ reason: 'INC-1' });
	expect(maxRunningRequest('')).toBe(0);
	expect(maxRunningRequest(' 4 ')).toBe(4);
	expect(taskLogsSelectValue(TaskLogExport.ENABLED)).toBe('enabled');
	expect(taskLogsSelectValue(undefined)).toBe('inherit');
	expect(budgetPeriodHours(undefined)).toBe('');
	expect(budgetCommand('ops', '', '', '', false)).toBe('orkctl namespace update ops --clear-budget');
	expect(budgetCommand('ops', '10', '', '24', true)).toBe('orkctl namespace update ops --budget-cpu-seconds 10 --budget-gib-seconds 0 --budget-period 24h --budget-enforce=true');
});

test("a namespace's page returns to the page of the list it came from, and nowhere an address could point it", () => {
	const href = namespacePageHref('ops', '?ns=default&cursor=abc&page=2', 'default');
	const url = new URL(`https://x.invalid${href}`);
	expect(url.pathname).toBe('/admin/namespaces/ops');
	expect(namespaceReturnHref(url, 'ops', 'default')).toBe('/admin/namespaces?ns=default&cursor=abc&page=2');
	const direct = new URL('https://x.invalid/admin/namespaces/ops?ns=default');
	expect(namespaceReturnHref(direct, 'ops', 'default')).toBe(createdNamespaceHref({ ns: 'default' }, 'ops'));
	const away = new URL('https://x.invalid/admin/namespaces/ops?from=//evil.invalid/');
	expect(namespaceReturnHref(away, 'ops', 'default')).toBe(createdNamespaceHref({ ns: 'default' }, 'ops'));
});
