// Why a sign-in did not complete, in words: the engine sends the browser to
// /signin?error=<reason> and keeps the detail in its own log
// (internal/authn/signin.go). An unknown reason is still said, plainly.

export interface SigninProblem {
	title: string;
	detail: string;
}

const START_AGAIN = 'Start the sign-in again.';

const reasons: Record<string, SigninProblem> = {
	no_sign_in_in_progress: { title: 'That sign-in had expired.', detail: `A sign-in must finish within ten minutes, in the browser that started it. ${START_AGAIN}` },
	sign_in_expired: { title: 'That sign-in had expired.', detail: `A sign-in must finish within ten minutes, in the browser that started it. ${START_AGAIN}` },
	state_mismatch: { title: 'That sign-in did not match this browser.', detail: `The identity provider's answer was for another sign-in. ${START_AGAIN}` },
	issuer_mismatch: { title: 'That sign-in came back from another identity provider.', detail: START_AGAIN },
	denied: { title: 'The identity provider did not sign you in.', detail: 'It refused, or the sign-in was cancelled there.' },
	disabled: { title: 'Your account is disabled in OrkStack.', detail: 'An administrator of this deployment can enable it again.' },
	principal_refused: {
		title: 'The identity provider’s answer was not one this deployment accepts.',
		detail: 'It may not have said your email is verified, or it left out a claim this deployment requires. An administrator can check the issuer’s settings.'
	},
	issuer_unavailable: { title: 'The identity provider could not be reached.', detail: 'Try again in a moment.' }
};

const fallback: SigninProblem = {
	title: 'The sign-in could not be completed.',
	detail: 'Try again. If it keeps failing, the deployment’s sign-in settings need a look; the engine’s log says why.'
};

/** problemOf says why a sign-in failed; null when no reason was given. */
export function problemOf(reason: string | null): SigninProblem | null {
	if (!reason) return null;
	return reasons[reason] ?? fallback;
}

/** The host an issuer's URL names, to tell two issuers apart. */
export function issuerHost(url: string): string {
	try {
		return new URL(url).host;
	} catch {
		return url;
	}
}
