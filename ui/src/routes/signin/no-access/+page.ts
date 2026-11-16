import { errorMessage } from '#lib/client.ts';
import { authClient } from '#lib/clients/auth.ts';
import { setCsrfToken } from '#lib/signin.ts';
import type { GetSigninResponse } from '#lib/gen/orkstack/v1/auth_service_pb.ts';
import type { PageLoad } from './$types';

// Signed in and bound to nothing: who OrkStack sees, from GetSignin, which
// needs no permission.
export const load: PageLoad = async ({ fetch }) => {
	try {
		const me = await authClient(fetch).getSignin({});
		// The shell sets no token on this bare page; its Sign out needs one.
		setCsrfToken(me.csrfToken);
		return { me, meError: null as string | null };
	} catch (err) {
		return { me: null as GetSigninResponse | null, meError: errorMessage(err) };
	}
};
