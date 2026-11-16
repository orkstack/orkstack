import { errorMessage } from '#lib/client.ts';
import { authClient } from '#lib/clients/auth.ts';
import { systemClient } from '#lib/clients/system.ts';
import type { SigninIssuer } from '#lib/gen/orkstack/v1/auth_service_pb.ts';
import type { PageLoad } from './$types';

// The sign-in page reads the deployment's mode and the issuers a person can
// sign in with. Both are answered with no credential (GetSystemInfo,
// AuthService.GetAuthConfig): this is the page a visitor with none lands on.
export const load: PageLoad = async ({ fetch, url }) => {
	const ret = url.searchParams.get('return') ?? '/';
	const error = url.searchParams.get('error');
	let mode = '';
	try {
		mode = (await systemClient(fetch).getSystemInfo({})).authMode;
	} catch {
		// An engine that refuses even this without a credential authenticates:
		// the issuers say which.
		mode = 'enforce';
	}
	if (mode === '' || mode === 'asserted') {
		return { mode: 'asserted', ret, error, issuers: [] as SigninIssuer[], configError: null as string | null };
	}
	try {
		const cfg = await authClient(fetch).getAuthConfig({});
		return { mode: cfg.mode, ret, error, issuers: cfg.issuers, configError: null as string | null };
	} catch (err) {
		return { mode, ret, error, issuers: [] as SigninIssuer[], configError: errorMessage(err) };
	}
};
