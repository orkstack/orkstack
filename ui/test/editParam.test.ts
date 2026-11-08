import { expect, test } from 'bun:test';
import { closedHref, editHref } from '../src/lib/editParam';

test("a row's edit keeps every other parameter of the address, and closing removes only its own", () => {
	const at = new URL('https://x.invalid/orkflows/deploy?ns=default&tab=triggers');
	const open = editHref(at, 'trg-1');
	expect(open).toBe('/orkflows/deploy?ns=default&tab=triggers&edit=trg-1');
	expect(closedHref(new URL(`https://x.invalid${open}`))).toBe('/orkflows/deploy?ns=default&tab=triggers');
	expect(closedHref(new URL('https://x.invalid/admin/webhooks?edit=wh-1'))).toBe('/admin/webhooks');
	// A second edit replaces the first.
	expect(editHref(new URL(`https://x.invalid${open}`), 'trg-2')).toBe('/orkflows/deploy?ns=default&tab=triggers&edit=trg-2');
});
