// The namespace forms' pure pieces, shared by the list's create form and a
// namespace's own page: how a stored field reads, and what a form's text sends.
import type { Duration } from '@bufbuild/protobuf/wkt';
import { durationFromMs } from '@bufbuild/protobuf/wkt';
import { shellWord } from './flakes';
import type { Budget, NamespaceHold } from './gen/orkstack/v1/namespace_pb';
import { TaskLogExport } from './gen/orkstack/v1/telemetry_pb';
import { parseAttributeLines } from './namespaceRules';
import { hoursToMs } from './retentionForm';
import { fmtDuration } from './stats';
import { fmtTimestamp } from './status';

// NamespaceTelemetryPolicy is the loosely-typed shape this page builds and
// sends for `namespace.telemetry` — a plain object rather than the
// generated TelemetryPolicy message type (which carries its own
// `$typeName`), the same style the rest of this page's request bodies
// already use for `namespace` itself.
export type NamespaceTelemetryPolicy = {
	sink: string;
	taskLogs: TaskLogExport;
	attributes: Record<string, string>;
};

// runRetentionLabel renders an unset run_retention as what it means: this
// namespace inherits the deployment maximum, never a dash that could be
// mistaken for "no retention" (the same rule allowedPoolsLabel-style
// renderers in this file already follow for allowed_pools).
export function runRetentionLabel(d: Duration | undefined): string {
	return d ? fmtDuration(d) : 'inherited (deployment maximum)';
}

// telemetrySinkLabel, taskLogsLabel and attributeCountLabel render a
// namespace's telemetry policy for the list table, the same way
// runRetentionLabel above renders run_retention: an unset policy, or a
// field a policy leaves at its zero value, reads as what it MEANS
// ("inherited (default sink)", "inherited (Orker default)"), never a dash
// a reader could mistake for "no telemetry at all" — the CLI's
// telemetrySinkLabel/taskLogsLabel (cmd/orkctl/namespace.go) render the
// same three fields the same way.
// holdLabel and maxRunningLabel render namespace admission the way the
// CLI's `namespace get` does (cmd/orkctl/namespace.go): "accepting" and
// "unlimited" spelled out, never a dash that would read as the opposite
// of the truth.
export function holdLabel(hold: NamespaceHold | undefined): string {
	return hold ? `since ${fmtTimestamp(hold.since)} — "${hold.reason}"` : 'accepting';
}

export function maxRunningLabel(maxRunning: number): string {
	return maxRunning > 0 ? String(maxRunning) : 'unlimited';
}

// allowedSecretsLabel renders a namespace's secret-source list the way
// the CLI's `namespace list` column does (cmd/orkctl/namespace.go):
// "unrestricted" spelled out, never a dash, which under a heading naming
// a security bound would read as "none allowed" — the opposite of what an
// empty list means.
export function allowedSecretsLabel(allowedSecrets: string[]): string {
	return allowedSecrets.length > 0 ? allowedSecrets.join(', ') : 'unrestricted';
}

// holdRequest is what a form's hold field sends: blank is a deliberate
// "accepting" (no hold), anything else is a hold with that reason. The
// registry stamps since; a client never sends one.
export function holdRequest(reason: string): { reason: string } | undefined {
	return reason.trim() === '' ? undefined : { reason };
}

// maxRunningRequest turns the cap field's text into the number sent; blank
// is 0, which is unlimited. validateNamespaceForm has already refused
// anything that is not a non-negative whole number.
export function maxRunningRequest(text: string): number {
	return text.trim() === '' ? 0 : Number(text.trim());
}

// budgetRequest turns the four budget fields into the budget sent: both
// amounts blank is no budget at all — on an edit, a deliberate clear.
// validateNamespaceForm has already refused everything else the registry
// would (budgetError).
export function budgetRequest(cpu: string, gib: string, periodHours: string, enforce: boolean) {
	if (cpu.trim() === '' && gib.trim() === '') return undefined;
	return {
		cpuSeconds: BigInt(cpu.trim() === '' ? '0' : cpu.trim()),
		gibSeconds: BigInt(gib.trim() === '' ? '0' : gib.trim()),
		period: durationFromMs(hoursToMs(Number(periodHours.trim()))),
		enforce
	};
}

// budgetCommand is the `orkctl namespace update` an edit's save performs
// for the budget: the four flags, or --clear-budget when both amounts are
// blank.
export function budgetCommand(name: string, cpu: string, gib: string, periodHours: string, enforce: boolean): string {
	if (cpu.trim() === '' && gib.trim() === '') return `orkctl namespace update ${shellWord(name)} --clear-budget`;
	const parts = ['orkctl', 'namespace', 'update', shellWord(name)];
	parts.push('--budget-cpu-seconds', cpu.trim() === '' ? '0' : cpu.trim());
	parts.push('--budget-gib-seconds', gib.trim() === '' ? '0' : gib.trim());
	parts.push('--budget-period', `${periodHours.trim() || 'H'}h`);
	parts.push(`--budget-enforce=${enforce}`);
	return parts.join(' ');
}

export function budgetPeriodHours(b: Budget | undefined): string {
	return b?.period ? String(Number(b.period.seconds) / 3600) : '';
}

export function telemetrySinkLabel(policy: NamespaceTelemetryPolicy | undefined): string {
	return policy?.sink ? policy.sink : 'inherited (default sink)';
}

export function taskLogsLabel(policy: NamespaceTelemetryPolicy | undefined): string {
	switch (policy?.taskLogs) {
		case TaskLogExport.ENABLED:
			return 'enabled';
		case TaskLogExport.DISABLED:
			return 'disabled';
		default:
			return 'inherited (Orker default)';
	}
}

export function attributeCountLabel(policy: NamespaceTelemetryPolicy | undefined): string {
	const count = policy ? Object.keys(policy.attributes).length : 0;
	return count === 0 ? '—' : `${count}`;
}

// taskLogsSelectValue/taskLogsFromSelectValue convert between the form's
// three-way select ("inherit" | "enabled" | "disabled") and the proto
// enum, the same mapping cmd/orkctl/namespace.go's parseTaskLogsFlag
// makes for --task-logs.
export type TaskLogsChoice = 'inherit' | 'enabled' | 'disabled';

export function taskLogsSelectValue(taskLogs: TaskLogExport | undefined): TaskLogsChoice {
	switch (taskLogs) {
		case TaskLogExport.ENABLED:
			return 'enabled';
		case TaskLogExport.DISABLED:
			return 'disabled';
		default:
			return 'inherit';
	}
}

export function taskLogsFromSelectValue(choice: TaskLogsChoice): TaskLogExport {
	switch (choice) {
		case 'enabled':
			return TaskLogExport.ENABLED;
		case 'disabled':
			return TaskLogExport.DISABLED;
		default:
			return TaskLogExport.UNSPECIFIED;
	}
}

// buildTelemetryPolicy assembles the TelemetryPolicy a create/update
// request sends from the form's three fields, or undefined when all three
// are at their "no override" value — the difference between "inherited"
// and "explicitly asked for every default" (the same distinction
// applyTelemetryFlags draws in cmd/orkctl/namespace.go).
export function buildTelemetryPolicy(
	sink: string,
	taskLogs: TaskLogsChoice,
	attributesText: string
): NamespaceTelemetryPolicy | undefined {
	const trimmedSink = sink.trim();
	const { attributes } = parseAttributeLines(attributesText);
	if (trimmedSink === '' && taskLogs === 'inherit' && Object.keys(attributes).length === 0) {
		return undefined;
	}
	return {
		sink: trimmedSink,
		taskLogs: taskLogsFromSelectValue(taskLogs),
		attributes
	};
}
