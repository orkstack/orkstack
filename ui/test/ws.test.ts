import { expect, test } from 'bun:test';
import { wsBaseUrl } from '../src/lib/ws';

// A socket goes where the page came from: the engine serves both.
test('a socket is opened on the page origin', () => {
	expect(wsBaseUrl({ protocol: 'http:', host: 'localhost:8080' })).toBe('ws://localhost:8080');
	expect(wsBaseUrl({ protocol: 'https:', host: 'ork.example.com' })).toBe('wss://ork.example.com');
	expect(wsBaseUrl({ protocol: 'http:', host: '[::1]:18911' })).toBe('ws://[::1]:18911');
});
