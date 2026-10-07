import { expect, test } from 'bun:test';
import { create, fromJson } from '@bufbuild/protobuf';
import {
	ActivityInfoSchema,
	ActivityInterfaceSchema,
	type ActivityInfo,
	type ActivityInterface
} from '../src/lib/gen/orkstack/v1/service_pb';
import {
	inspectCommand,
	interfaceHeadline,
	interfaceRows,
	interfaceSummary,
	pinnedReference,
	valueRows
} from '../src/lib/activity';
import { readFileSync } from 'node:fs';

// The labels the first-party images ship, read from their Dockerfiles so the
// UI is tested against the interface a real image declares.
function label(image: string) {
	const dockerfile = readFileSync(`${import.meta.dir}/../../images/${image}/Dockerfile`, 'utf8');
	const match = dockerfile.match(/^LABEL org\.orkstack\.activity\.interface='([^']*)'$/m);
	if (!match) throw new Error(`${image} has no interface label`);
	return fromJson(ActivityInterfaceSchema, JSON.parse(match[1]));
}

const digest = 'sha256:' + '2b'.repeat(32);
const info = (over: {
	reference?: string;
	indexDigest?: string;
	interface?: ActivityInterface;
	interfaceError?: string;
}): ActivityInfo =>
	create(ActivityInfoSchema, { name: 'file', reference: 'localhost:5001/orkstack/activity-file:dev', indexDigest: digest, ...over });

test('interfaceSummary says what the CLI INTERFACE column says', () => {
	expect(interfaceSummary(info({ interface: label('activity-file') }))).toBe('7 ops');
	expect(interfaceSummary(info({ interface: label('activity-http') }))).toBe('5 keys, 6 ops');
	expect(interfaceSummary(info({ interface: label('activity-shell') }))).toBe('3 ops');
	expect(interfaceSummary(info({}))).toBe('none');
	expect(interfaceSummary(info({ interfaceError: 'schemaVersion 2 is not supported (this engine reads 1)' }))).toBe('malformed');
	expect(interfaceSummary(info({ indexDigest: '' }))).toBe('-');
});

test('interfaceHeadline matches orkctl activity inspect', () => {
	expect(interfaceHeadline(info({ interface: label('activity-file') }))).toBe(
		'schema 1 · selected by op: (7 ops, op required)'
	);
	expect(interfaceHeadline(info({ interface: label('activity-http') }))).toBe(
		'schema 1 · selected by op: (6 ops, default get) · 5 keys shared by every op'
	);
	expect(interfaceHeadline(info({}))).toBe(
		'none — the image declares no org.orkstack.activity.interface label; with: is checked by the image at run time'
	);
	expect(interfaceHeadline(info({ interfaceError: 'boom' }))).toBe('malformed — boom');
	const keysOnly = fromJson(ActivityInterfaceSchema, {
		schemaVersion: 1,
		fields: { target: { type: 'string', required: true }, region: { type: 'string', default: 'eu-west-1' } }
	});
	expect(interfaceHeadline(info({ interface: keysOnly }))).toBe('schema 1 · 2 keys');
	expect(interfaceRows(keysOnly)).toEqual([
		{ op: '*', key: 'region', type: 'string', required: 'no', default: '"eu-west-1"' },
		{ op: '*', key: 'target', type: 'string', required: 'yes', default: '-' }
	]);
});

test('interfaceRows lists shared keys first, then ops by name, as the CLI table does', () => {
	const rows = interfaceRows(label('activity-http'));
	expect(rows.slice(0, 5).map((r) => r.key)).toEqual(['body', 'captureBodyAs', 'headers', 'timeoutSeconds', 'url']);
	expect(rows[4]).toEqual({ op: '*', key: 'url', type: 'string', required: 'yes', default: '-' });
	expect(rows[5]).toEqual({ op: 'delete', key: '-', type: '-', required: '-', default: '-' });
	const file = interfaceRows(label('activity-file'));
	expect(file[0]).toEqual({ op: 'checksum', key: 'src', type: 'string', required: 'yes', default: '-' });
	expect(file.find((r) => r.op === 'copy' && r.key === 'src')?.required).toBe('yes');
	expect(valueRows(label('activity-file'))).toEqual([
		{ key: 'path', type: 'string' },
		{ key: 'sha256', type: 'string' },
		{ key: 'sizeBytes', type: 'number' }
	]);
});

test('pinnedReference normalises a reference the way the engine parses one', () => {
	expect(pinnedReference(info({}))).toBe(`localhost:5001/orkstack/activity-file@${digest}`);
	expect(pinnedReference(info({ reference: 'alpine:3.20' }))).toBe(`docker.io/library/alpine@${digest}`);
	expect(pinnedReference(info({ reference: `ghcr.io/acme/migrate@${digest}` }))).toBe(`ghcr.io/acme/migrate@${digest}`);
	expect(pinnedReference(info({ indexDigest: '' }))).toBe('-');
});

test('inspectCommand writes a plain reference bare and quotes anything else', () => {
	expect(inspectCommand('file')).toBe('orkctl activity inspect file');
	expect(inspectCommand('localhost:5001/orkstack/activity-file:dev')).toBe(
		'orkctl activity inspect localhost:5001/orkstack/activity-file:dev'
	);
	expect(inspectCommand('{{ .x }}')).toBe("orkctl activity inspect '{{ .x }}'");
});

test('the transform images: summary, headline and rows match the CLI', () => {
	expect(interfaceSummary(info({ interface: label('activity-json') }))).toBe('2 ops');
	expect(interfaceSummary(info({ interface: label('activity-git') }))).toBe('5 keys, 2 ops');
	expect(interfaceSummary(info({ interface: label('activity-template') }))).toBe('1 op');
	expect(interfaceHeadline(info({ interface: label('activity-json') }))).toBe(
		'schema 1 · selected by op: (2 ops, op required)'
	);
	expect(interfaceHeadline(info({ interface: label('activity-git') }))).toBe(
		'schema 1 · selected by op: (2 ops, op required) · 5 keys shared by every op'
	);
	expect(interfaceHeadline(info({ interface: label('activity-template') }))).toBe(
		'schema 1 · selected by op: (1 op, op required)'
	);
	const git = interfaceRows(label('activity-git'));
	expect(git.slice(0, 5).map((r) => [r.op, r.key])).toEqual([
		['*', 'caFile'],
		['*', 'ref'],
		['*', 'tokenSecret'],
		['*', 'tokenUser'],
		['*', 'url']
	]);
	expect(git[4]).toEqual({ op: '*', key: 'url', type: 'string', required: 'yes', default: '-' });
	expect(git[3]).toEqual({ op: '*', key: 'tokenUser', type: 'string', required: 'no', default: '"x-access-token"' });
});
