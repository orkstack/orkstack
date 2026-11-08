import { expect, test } from 'bun:test';
import { shortVersion, versionTip } from '../src/lib/version';

const HEX = 'b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c';
const UUID7 = '01m6s4zhnqfgp80mhmkpx7kqs9';

test('a derived version shows its short form; a pinned version shows as written', () => {
	const cases: [string, string][] = [
		[HEX, 'b5bb9d80'],
		[UUID7, '…' + UUID7.slice(-7)],
		['1.0.0', '1.0.0'],
		['v2-rc.1', 'v2-rc.1'],
		['', ''],
		// One character off either shape is a pinned version, written as it was.
		[HEX.slice(1), HEX.slice(1)],
		[HEX + '0', HEX + '0'],
		[HEX.toUpperCase(), HEX.toUpperCase()],
		[UUID7.slice(1), UUID7.slice(1)],
		[UUID7 + '0', UUID7 + '0'],
		// The TypeID alphabet has no i, l, o or u, and its first character carries three bits.
		['i1m6s4zhnqfgp80mhmkpx7kqs9', 'i1m6s4zhnqfgp80mhmkpx7kqs9'],
		['01m6s4zhnqfgp80mhmkpx7kqsu', '01m6s4zhnqfgp80mhmkpx7kqsu'],
		['81m6s4zhnqfgp80mhmkpx7kqs9', '81m6s4zhnqfgp80mhmkpx7kqs9']
	];
	for (const [full, short] of cases) expect(shortVersion(full)).toBe(short);
});

test('two versions created together stay apart: a UUIDv7 keeps its random end', () => {
	const a = '01m6s4zhnqfgp80mhmkpx7kqs9';
	const b = '01m6s4zhnqfgp80mhmkp3v2rda';
	expect(shortVersion(a)).toBe('…' + a.slice(-7));
	expect(shortVersion(a)).not.toBe(shortVersion(b));
});

test('the tooltip carries the full value only when the form is short', () => {
	expect(versionTip(HEX)).toBe(HEX);
	expect(versionTip(UUID7)).toBe(UUID7);
	expect(versionTip('1.0.0')).toBeUndefined();
	expect(versionTip('')).toBeUndefined();
});
