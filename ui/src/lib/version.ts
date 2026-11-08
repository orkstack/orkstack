// The form a version takes on screen (ui/DESIGN.md, the Plate Rule: the same on
// every page). A document that pins no version: is given a long derived one,
// and the backend accepts no short form, so the full value stays in the
// tooltip, every URL and every command line.
//
// A derived version is recognised by its shape alone, and there are two:
//
//   - 64 hex characters, as an earlier release derived them: the first 8;
//   - a UUIDv7 in lowercase base32 (26 characters of the TypeID alphabet, the
//     first carrying three bits, internal/typeid). Its leading characters are
//     a timestamp, so two versions applied together begin alike and a git-style
//     prefix would draw them the same: the last 7, which are random, after a `…`.
//
// A pinned version of the same shape only displays shortened, its full value
// one hover away. Anything else is shown as written.
const HEX_VERSION = /^[0-9a-f]{64}$/;
const UUID7_VERSION = /^[0-7][0-9a-hjkmnp-tv-z]{25}$/;

/** The version as a screen shows it. */
export function shortVersion(version: string): string {
	if (HEX_VERSION.test(version)) return version.slice(0, 8);
	if (UUID7_VERSION.test(version)) return `…${version.slice(-7)}`;
	return version;
}

/** The full value for a tooltip, only when the form on screen is shortened. */
export function versionTip(version: string): string | undefined {
	return shortVersion(version) === version ? undefined : version;
}
