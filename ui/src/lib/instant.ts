import type { Timestamp } from '@bufbuild/protobuf/wkt';

/**
 * instantDate is a timestamp as a Date, truncated to its millisecond.
 *
 * protobuf-es's timestampDate rounds the nanos to the nearest millisecond, so
 * 20:13:23.9996 becomes 20:13:24.000 and a caption at second precision names
 * a second the instant never reached. The engine and orkctl truncate; so does
 * every rendering here that names an instant's second.
 */
export function instantDate(ts: Timestamp): Date {
	return new Date(Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000));
}
