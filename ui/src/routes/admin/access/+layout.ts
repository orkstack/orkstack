import { errorMessage } from '#lib/client.ts';
import { accessClient } from '#lib/clients/access.ts';
import { authClient } from '#lib/clients/auth.ts';
import { readAuthMode } from '#lib/authMode.ts';
import type { GetAccessResponse } from '#lib/gen/orkstack/v1/access_service_pb.ts';
import type { GetSigninResponse } from '#lib/gen/orkstack/v1/auth_service_pb.ts';
import type { Grants } from '#lib/permissions.ts';
import type { LayoutLoad } from './$types';

// Every Access screen reads the access document with its status, and what
// the caller may do. In asserted mode there is no document to read: the
// screens show one panel saying authentication is off (UX35).
export const load: LayoutLoad = async ({ fetch }) => {
	const mode: string = await readAuthMode(fetch);
	if (mode === 'asserted') {
		return { mode, access: null as GetAccessResponse | null, accessError: null as string | null, me: null as GetSigninResponse | null, grants: { enforced: false, held: [] } as Grants };
	}
	const [access, me] = await Promise.allSettled([accessClient(fetch).getAccess({}), authClient(fetch).getSignin({})]);
	const signin = me.status === 'fulfilled' ? me.value : null;
	return {
		mode,
		access: access.status === 'fulfilled' ? access.value : null,
		accessError: access.status === 'rejected' ? errorMessage(access.reason) : null,
		me: signin,
		grants: { enforced: mode === 'enforce', held: signin?.permissions ?? [] } as Grants
	};
};
