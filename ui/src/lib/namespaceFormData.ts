// What the namespace forms offer: the deployment's telemetry sinks and signing
// keys. The list's create form and a namespace's own page read the same.
import { errorMessage } from './client';
import { systemClient } from './clients/system';

// loadTelemetrySinks reads the deployment's declared telemetry sinks
// (SystemService.ListTelemetrySinks) so the create/edit forms can offer a
// select of real names rather than a free-text field the registry would
// likely refuse. A failure here does not fail the page — it only means the
// forms fall back to a plain text input for the sink (TelemetryFields); a namespace admin can still
// read and write everything else.
export async function loadTelemetrySinks(fetchFn: typeof fetch) {
	try {
		const res = await systemClient(fetchFn).listTelemetrySinks({});
		return {
			telemetrySinks: res.names,
			telemetrySinksError: null as string | null
		};
	} catch (err) {
		return {
			telemetrySinks: [] as string[],
			telemetrySinksError: errorMessage(err)
		};
	}
}

// loadImagePolicy reads the deployment's image policy (SystemService.
// GetImagePolicy) for its signing key NAMES only, so the create/edit forms can
// offer a select of `cosign:<name>` signers the engine will accept. A failure
// here does not fail the page — the forms fall back to a plain text input for
// the signer, on the same terms as the telemetry sink select.
export async function loadImagePolicy(fetchFn: typeof fetch) {
	try {
		const res = await systemClient(fetchFn).getImagePolicy({});
		return {
			signingKeys: Object.keys(res.policy?.signingKeys ?? {}).sort(),
			signingKeysError: null as string | null
		};
	} catch (err) {
		return {
			signingKeys: [] as string[],
			signingKeysError: errorMessage(err)
		};
	}
}
