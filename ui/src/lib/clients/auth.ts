import { createClient, type Client } from '@connectrpc/connect';
import { AuthService } from '../gen/orkstack/v1/auth_service_pb';
import { transport } from '../client';

// Sign-in and "who am I": the issuers a person can sign in with, the
// caller's own principal, permissions and CSRF token, refresh and sign-out.
// Mounted only when authentication is not `asserted`.
//
// This module imports AuthService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type AuthClient = Client<typeof AuthService>;

export function authClient(fetchFn: typeof globalThis.fetch): AuthClient {
	return createClient(AuthService, transport(fetchFn));
}
