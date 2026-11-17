import { LiveUsageUnavailableReason, type LiveUsage } from './gen/orkstack/v1/run_service_pb';
import { MeasurementMethod, UnmeasuredReason, type MeasuredUsage } from './gen/orkstack/v1/run_pb';
import { formatBytes } from './resources';

// What a task's container used, as its cgroup reported it (E94): live on a
// running task's card from its Orker's last heartbeat, measured on an ended
// attempt's record. A figure that is unset or was not measured is the em dash,
// never 0, and a missing reading says why. The words match `orkctl run
// inspect` and `orkctl stats usage` (cmd/orkctl/usage_measured.go).

/** The em dash every unset figure renders as. */
export const UNSET = '—';

export interface LiveUsageFacts {
	cpu: string;
	memory: string;
	/** Why there is no reading, when there is none; '' when there is. */
	note: string;
	/** The reading is older than two sampling intervals and a heartbeat tick. */
	stale: boolean;
}

/** liveUsageFacts are the CPU and Memory facts of a running attempt. */
export function liveUsageFacts(usage: LiveUsage | undefined): LiveUsageFacts {
	if (!usage) return { cpu: UNSET, memory: UNSET, note: 'not reported by this engine', stale: false };
	if (!usage.available) {
		return { cpu: UNSET, memory: UNSET, note: liveUnavailableWords(usage.unavailableReason), stale: false };
	}
	const cpu = usage.cpuCores === undefined ? UNSET : `${usage.cpuCores.toFixed(2)} cores`;
	const limit = usage.memoryLimitBytes > 0n ? ` of ${formatBytes(usage.memoryLimitBytes)}` : '';
	return { cpu, memory: `${formatBytes(usage.memoryBytes)}${limit}`, note: '', stale: usage.stale };
}

/** liveUnavailableWords are a few words for why a running attempt shows no reading. */
export function liveUnavailableWords(reason: LiveUsageUnavailableReason): string {
	switch (reason) {
		case LiveUsageUnavailableReason.NOT_STARTED:
			return 'not started';
		case LiveUsageUnavailableReason.PREPARING:
			return 'its container has not started';
		case LiveUsageUnavailableReason.CGROUP_NOT_VISIBLE:
			return "its Orker cannot see the container's cgroup";
		case LiveUsageUnavailableReason.NO_CGROUP:
			return 'its container has no cgroup to read (rootless on cgroup v1)';
		case LiveUsageUnavailableReason.DISABLED:
			return 'measuring is off on its Orker';
		case LiveUsageUnavailableReason.NO_SAMPLE_YET:
			return 'not read yet';
		case LiveUsageUnavailableReason.ORKER_TOO_OLD:
			return 'its Orker does not report usage';
		case LiveUsageUnavailableReason.UNDECODABLE:
			return "its Orker's report could not be read";
		case LiveUsageUnavailableReason.READ_FAILED:
			return "its container's cgroup could not be read";
		case LiveUsageUnavailableReason.START_FAILED:
			return "its container's start failed";
		case LiveUsageUnavailableReason.CGROUP_UNVERIFIED:
			return "its container's cgroup could not be verified";
		case LiveUsageUnavailableReason.UNSPECIFIED:
			return 'no reading yet';
	}
	return 'for a reason this page does not know';
}

export interface MeasuredFacts {
	/** "1.2 cpu-s · 64.0 MiB peak", or the em dash. */
	used: string;
	/** What the figures are, or why there are none. */
	tip: string;
	/** Stopped at its memory limit. */
	oom: boolean;
}

/**
 * measuredFacts are what an ended attempt's container used. A record an Orker
 * older than v0.4.0 wrote has none: the em dash, saying so.
 */
export function measuredFacts(m: MeasuredUsage | undefined): MeasuredFacts {
	if (!m || m.method === MeasurementMethod.UNSPECIFIED) {
		return { used: UNSET, tip: 'Recorded by an Orker that does not measure usage.', oom: false };
	}
	if (m.method === MeasurementMethod.UNMEASURED) {
		if (m.unmeasuredReason === UnmeasuredReason.NOT_RUN) {
			return { used: 'no container ran', tip: 'Its result came from a commit or a cached result, so no container ran.', oom: false };
		}
		return { used: UNSET, tip: `Not measured: ${unmeasuredWords(m.unmeasuredReason)}.`, oom: m.oomKilled };
	}
	const used = `${m.cpuSeconds.toFixed(1)} cpu-s · ${formatBytes(m.memoryPeakBytes)} peak`;
	const exact = m.memoryPeakExact ? "the kernel's own peak" : "the most a reading saw, not the kernel's own peak";
	const limit = m.memoryLimitBytes > 0n ? ` of a ${formatBytes(m.memoryLimitBytes)} limit` : '';
	return {
		used,
		tip: `CPU time at its last reading, and ${exact}${limit}, from its container's cgroup.`,
		oom: m.oomKilled
	};
}

/** unmeasuredWords are why an ended attempt was not measured. */
export function unmeasuredWords(reason: UnmeasuredReason): string {
	switch (reason) {
		case UnmeasuredReason.CGROUP_NOT_VISIBLE:
			return "its Orker could not see the container's cgroup";
		case UnmeasuredReason.NO_CGROUP:
			return 'its container had no cgroup to read (rootless on cgroup v1)';
		case UnmeasuredReason.ENDED_BEFORE_FIRST_SAMPLE:
			return 'its container ended before a reading';
		case UnmeasuredReason.READ_FAILED:
			return "its container's cgroup could not be read";
		case UnmeasuredReason.NOT_RUN:
			return 'no container ran';
		case UnmeasuredReason.DISABLED:
			return 'measuring is off on its Orker';
		case UnmeasuredReason.START_FAILED:
			return "its container's start failed";
		case UnmeasuredReason.CGROUP_UNVERIFIED:
			return "its container's cgroup could not be verified (not on a cgroup filesystem)";
		case UnmeasuredReason.UNSPECIFIED:
			return 'no reason was given';
	}
	return 'for a reason this page does not know';
}
