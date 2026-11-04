// A cpu and memory amount in the run page's words: "1 cpu · 2 GiB".
import type { ResourceQuantity, ResourceRequest } from './gen/orkstack/v1/activity_pb';

function gib(bytes: bigint): string {
	const value = Number(bytes);
	if (value >= 2 ** 30) return `${+(value / 2 ** 30).toFixed(1)} GiB`;
	if (value >= 2 ** 20) return `${+(value / 2 ** 20).toFixed(1)} MiB`;
	if (value >= 2 ** 10) return `${+(value / 2 ** 10).toFixed(1)} KiB`;
	return `${value} B`;
}

/** A granted or reserved amount; a dimension that is not set is left out. */
export function fmtQuantity(q: { cpu: number; memoryBytes: bigint } | undefined): string {
	if (!q) return '';
	return [q.cpu > 0 ? `${+q.cpu.toFixed(2)} cpu` : '', q.memoryBytes > 0n ? gib(q.memoryBytes) : ''].filter(Boolean).join(' · ');
}

/** A task's declared request, spelled as authored but for the separator. */
export function fmtRequest(r: ResourceRequest | undefined): string {
	if (!r) return '';
	return [r.cpu ? `${r.cpu} cpu` : '', r.memory ? r.memory.replace(/([KMGT])i$/, ' $1iB') : ''].filter(Boolean).join(' · ');
}

/** "12 of 12 cpu": what stands reserved against what the pool admits, in cpu when it bounds cpu. */
export function reservedOf(reserved: ResourceQuantity | undefined, capacity: ResourceQuantity | undefined): string {
	if ((capacity?.cpu ?? 0) > 0) return `${+(reserved?.cpu ?? 0).toFixed(2)} of ${+capacity!.cpu.toFixed(2)} cpu`;
	if ((capacity?.memoryBytes ?? 0n) > 0n) return `${gib(reserved?.memoryBytes ?? 0n)} of ${gib(capacity!.memoryBytes)}`;
	return '';
}
