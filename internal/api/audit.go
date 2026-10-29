package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The control-plane audit trail records one immutable object per call of a
// control-plane WRITE RPC, succeeded or refused, from one Connect interceptor
// over every handler NewMux mounts — so no handler can forget to record
// itself — plus two explicit calls in the legacy webhook HTTP adapter, which
// the interceptor cannot see.
//
// Two rules keep it from becoming something else. It records writes only: a
// read is never an audit event, or the volume becomes a log pipeline and
// telemetry already is one (AGENTS.md §11). And a failed audit write is logged
// and counted, never fatal to the operation: an audit-store outage must not
// stop an operator holding a namespace during an incident.

// AuditPrincipalHeader is the request header a deployment's gateway asserts
// the caller's identity in. OrkStack records its value and does not verify it.
const AuditPrincipalHeader = "Ork-Principal"

// auditWriteTimeout bounds the synchronous store write after a handler
// returns. It runs on a context detached from the request's cancellation
// (context.WithoutCancel): a caller that hangs up after its write landed must
// not be the reason the write went unrecorded.
const auditWriteTimeout = 5 * time.Second

const (
	// DefaultAuditLookback is the window a listing with no `since` covers,
	// counted back from `until`.
	DefaultAuditLookback = 7 * 24 * time.Hour
	// DefaultAuditPageSize is the page size a listing with page_size 0 gets.
	DefaultAuditPageSize = 50
	// MaxAuditPageSize is the largest page a listing may ask for. A larger
	// request is refused, not clamped.
	MaxAuditPageSize = 200

	// auditOutcomeWritten and auditOutcomeDropped are the two outcomes an
	// AuditCounter is told.
	auditOutcomeWritten = "written"
	auditOutcomeDropped = "dropped"

	// auditCreateAttempts bounds retries of a create-only put whose freshly
	// drawn id collided — 64 random bits within one nanosecond stamp, so in
	// practice never.
	auditCreateAttempts = 3

	// auditNoTarget is the target of a refused write whose object was never
	// resolved (a document that did not compile, a webhook never created).
	auditNoTarget = "-"
)

// AuditCounter is the telemetry seam. Nil is a no-op.
type AuditCounter interface {
	// CountAudit records one attempt to write an audit record: outcome is
	// "written" or "dropped".
	CountAudit(ctx context.Context, action, outcome string)
}

// AuditRecorder writes one record per audited call. It is the only writer of
// metadata/audit/ in the codebase.
type AuditRecorder struct {
	store   artifacts.AuditStore
	counter AuditCounter
	logger  *slog.Logger
	now     func() time.Time
}

// NewAuditRecorder builds a recorder over store. A nil store yields a nil
// recorder, whose Record is a no-op.
func NewAuditRecorder(store artifacts.AuditStore, counter AuditCounter, logger *slog.Logger, now func() time.Time) *AuditRecorder {
	if store == nil {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	return &AuditRecorder{store: store, counter: counter, logger: logger, now: now}
}

// Record writes rec, best-effort. It never returns an error to a handler:
// a store failure is logged (warn: action, target, principal, error) and
// counted (outcome "dropped"), and the caller's own response is unchanged.
//
// The write is synchronous — a test, an operator and a second replica can read
// the record the moment the response arrives — on a context detached from the
// caller's cancellation and bounded by auditWriteTimeout. A record with no id
// or time is stamped here from the recorder's clock.
func (r *AuditRecorder) Record(ctx context.Context, rec *orkstackv1.AuditRecord) {
	if r == nil || rec == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), auditWriteTimeout)
	defer cancel()
	rec.SchemaVersion = artifacts.AuditSchemaVersion
	if rec.GetAt() == nil {
		rec.At = timestamppb.New(r.now().UTC())
	}
	if rec.GetId() == "" {
		rec.Id = artifacts.NewAuditID(rec.GetAt().AsTime())
	}
	var err error
	for attempt := 0; attempt < auditCreateAttempts; attempt++ {
		err = r.store.PutAuditRecord(ctx, rec)
		if !errors.Is(err, artifacts.ErrAuditRecordExists) {
			break
		}
		rec.Id = artifacts.NewAuditID(rec.GetAt().AsTime())
	}
	outcome := auditOutcomeWritten
	if err != nil {
		outcome = auditOutcomeDropped
		r.logger.Warn("audit record dropped",
			"action", rec.GetAction(),
			"target", rec.GetTarget(),
			"principal", rec.GetPrincipal(),
			"error", err)
	}
	if r.counter != nil {
		r.counter.CountAudit(ctx, rec.GetAction(), outcome)
	}
}

// auditEntry is what an extractor derives from one call.
type auditEntry struct {
	Action, Namespace, Target, Detail string
}

// auditExtractor derives the entry from the request and, on success, the
// response. resp is nil when the handler returned an error.
type auditExtractor func(req, resp proto.Message) auditEntry

// auditedProcedures is the complete classification of the audited writes: one
// row per control-plane write RPC. The extractors below are the ONLY readers
// of request fields for the trail, and none of them reads a webhook signing
// secret or a trigger token (RegisterWebhookRequest.secret,
// RegisterWebhookResponse.secret, CreateTriggerResponse.secret).
//
// A refused call records the request's namespace resolved ("" is spelled
// "default"), because the handler never resolved anything; a successful one
// reads the resolved coordinate from the response where it carries one.
var auditedProcedures = map[string]auditExtractor{
	// A purge deletes memos and nothing else: no run, commit, attempt, log
	// or artifact. Its fact is how many memos it deleted and kept.
	orkstackv1connect.CacheServicePurgeCacheProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.PurgeCacheRequest)
		entry := auditEntry{
			Action:    "cache.purge",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
		}
		switch {
		case in.GetOrkflowName() != "":
			entry.Target = "orkflow/" + in.GetOrkflowName()
		case in.GetNamespaceScoped():
			entry.Target = "namespace-scoped"
		case in.GetAll():
			entry.Target = "all"
		default:
			entry.Target = auditNoTarget
		}
		if task := in.GetTaskId(); task != "" {
			entry.Target += "/" + task
		}
		olderThan := "-"
		if d := in.GetOlderThan(); d != nil && d.AsDuration() > 0 {
			olderThan = d.AsDuration().String()
		}
		entry.Detail = "older_than=" + olderThan
		if out, ok := resp.(*orkstackv1.PurgeCacheResponse); ok && out != nil {
			if out.GetNamespace() != "" {
				entry.Namespace = out.GetNamespace()
			}
			entry.Detail += fmt.Sprintf(" deleted=%d kept=%d more=%t", out.GetDeleted(), out.GetKept(), out.GetMore())
		}
		return entry
	},
	orkstackv1connect.OrkflowServiceApplyOrkflowProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.ApplyOrkflowRequest)
		entry := auditEntry{
			Action:    "orkflow.apply",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    auditNoTarget,
		}
		var parts []string
		if out, ok := resp.(*orkstackv1.ApplyOrkflowResponse); ok && out != nil {
			wf := out.GetOrkflow()
			if wf.GetNamespace() != "" {
				entry.Namespace = wf.GetNamespace()
			}
			entry.Target = wf.GetName() + "@" + wf.GetVersion()
			if out.GetCreated() {
				parts = append(parts, "created digest="+wf.GetDefinitionDigest())
			} else {
				parts = append(parts, "no-op: version already stored")
			}
		}
		if base := in.GetBaseVersion(); base != "" {
			parts = append(parts, "base="+base)
		}
		if head := in.GetExpectedHeadVersion(); head != "" {
			parts = append(parts, "expected_head="+head)
		}
		entry.Detail = strings.Join(parts, " ")
		return entry
	},
	orkstackv1connect.OrkflowServiceSetCurrentOrkflowVersionProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.SetCurrentOrkflowVersionRequest)
		entry := auditEntry{
			Action:    "orkflow.promote",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
			Detail:    "requested " + in.GetVersion(),
		}
		if in.GetAcknowledgeBreaking() {
			entry.Detail += " acknowledge_breaking"
		}
		if out, ok := resp.(*orkstackv1.SetCurrentOrkflowVersionResponse); ok && out != nil {
			previous := out.GetPreviousVersion()
			if previous == "" {
				previous = "-"
			}
			entry.Detail = "current " + previous + " -> " + out.GetCurrentVersion()
			// The interface verdict makes a breaking promotion attributable
			// to whoever acknowledged it (docs/UI_CLI_PARITY.md "Interface compatibility").
			if verdict := out.GetInterfaceVerdict(); verdict != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNSPECIFIED {
				entry.Detail += " interface=" + auditVerdictWord(verdict) + " vs " + out.GetComparedVersion()
				if verdict == orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING {
					entry.Detail += " acknowledged"
				}
			}
		}
		return entry
	},
	orkstackv1connect.OrkflowServiceDeleteOrkflowProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.DeleteOrkflowRequest)
		return auditEntry{
			Action:    "orkflow.delete",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
		}
	},
	orkstackv1connect.RunServiceCancelRunProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.CancelRunRequest)
		return auditEntry{
			Action:    "run.cancel",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetRunId(),
			Detail:    "reason=" + in.GetReason(),
		}
	},
	// The values themselves are never recorded — only their digest — and the
	// callback door is outside Connect and not audited, on the trigger door's
	// grounds: its fact is the run's own record (TaskRun.await.resolved_via).
	orkstackv1connect.RunServiceResolveAwaitProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.ResolveAwaitRequest)
		entry := auditEntry{
			Action:    "run.await.resolve",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetRunId() + "/" + in.GetTaskId(),
		}
		if in.GetReject() {
			entry.Detail = fmt.Sprintf("reject reason=%q", in.GetReason())
		} else if out, ok := resp.(*orkstackv1.ResolveAwaitResponse); ok && out != nil {
			entry.Detail = "complete values_sha256=" + out.GetValuesSha256()
		}
		return entry
	},
	orkstackv1connect.WebhookServiceRegisterWebhookProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.RegisterWebhookRequest)
		names := make([]string, 0, len(in.GetEvents()))
		for _, event := range in.GetEvents() {
			names = append(names, event.String())
		}
		entry := auditEntry{
			Action: "webhook.register",
			Target: auditNoTarget,
			Detail: webhookAuditDetail(in.GetUrl(), names, in.GetNamespace(), in.GetOrkflowName(), in.GetTaskId()) +
				auditNameSuffix(in.GetName()),
		}
		if out, ok := resp.(*orkstackv1.RegisterWebhookResponse); ok && out != nil && out.GetWebhook().GetId() != "" {
			entry.Target = out.GetWebhook().GetId()
		}
		return entry
	},
	orkstackv1connect.WebhookServiceUpdateWebhookProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.UpdateWebhookRequest)
		// Deployment-wide, as webhook.register is. The request carries no
		// secret, and none could reach this detail.
		names := make([]string, 0, len(in.GetEvents()))
		for _, event := range in.GetEvents() {
			names = append(names, strings.TrimPrefix(event.String(), "EVENT_TYPE_"))
		}
		events := "all"
		if len(names) > 0 {
			events = "[" + strings.Join(names, " ") + "]"
		}
		shown := in.GetUrl()
		if parsed, err := url.Parse(shown); err == nil {
			shown = parsed.Redacted()
		}
		part := func(value string) string {
			if value == "" {
				return "*"
			}
			return value
		}
		return auditEntry{
			Action: "webhook.update",
			Target: in.GetId(),
			Detail: fmt.Sprintf("name=%s url=%s events=%s scope=%s/%s/%s", in.GetName(), shown, events,
				part(in.GetNamespace()), part(in.GetOrkflowName()), part(in.GetTaskId())),
		}
	},
	orkstackv1connect.WebhookServiceDeleteWebhookProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.DeleteWebhookRequest)
		return auditEntry{Action: "webhook.delete", Target: in.GetId()}
	},
	orkstackv1connect.WebhookServiceRedeliverWebhookProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.RedeliverWebhookRequest)
		entry := auditEntry{Action: "webhook.redeliver", Target: in.GetId()}
		if out, ok := resp.(*orkstackv1.RedeliverWebhookResponse); ok && out != nil && out.GetDelivery() != nil {
			d := out.GetDelivery()
			status := int32(0)
			if attempts := d.GetAttempts(); len(attempts) > 0 {
				status = attempts[len(attempts)-1].GetStatusCode()
			}
			// Coordinates and outcome only: never the signature, never a
			// secret, never the body.
			entry.Detail = fmt.Sprintf("webhook=%s run=%s event=%s -> %s delivered=%t status=%d",
				d.GetWebhookId(), d.GetRunId(), strings.TrimPrefix(d.GetEvent().String(), "EVENT_TYPE_"),
				d.GetId(), d.GetDelivered(), status)
		}
		return entry
	},
	orkstackv1connect.TriggerServiceCreateTriggerProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.CreateTriggerRequest)
		version := in.GetOrkflowVersion()
		if version == "" {
			version = "current"
		}
		entry := auditEntry{
			Action:    "trigger.create",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    auditNoTarget,
			Detail:    in.GetOrkflowName() + "@" + version,
		}
		if description := in.GetDescription(); description != "" {
			entry.Detail += fmt.Sprintf(" description=%q", description)
		}
		entry.Detail += auditNameSuffix(in.GetName())
		if out, ok := resp.(*orkstackv1.CreateTriggerResponse); ok && out != nil && out.GetTrigger().GetId() != "" {
			entry.Target = out.GetTrigger().GetId()
		}
		return entry
	},
	orkstackv1connect.TriggerServiceUpdateTriggerProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.UpdateTriggerRequest)
		version := in.GetOrkflowVersion()
		if version == "" {
			version = "current"
		}
		// The request carries no token; an update never re-issues one.
		return auditEntry{
			Action:    "trigger.update",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetId(),
			Detail:    fmt.Sprintf("name=%s %s description=%q", in.GetName(), version, in.GetDescription()),
		}
	},
	orkstackv1connect.TriggerServiceDeleteTriggerProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.DeleteTriggerRequest)
		return auditEntry{
			Action:    "trigger.delete",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetId(),
		}
	},
	orkstackv1connect.ScheduleServiceCreateScheduleProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.CreateScheduleRequest)
		entry := auditEntry{
			Action:    "schedule.create",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    auditNoTarget,
			// Coordinates and the spec summary only: never the input.
			Detail: fmt.Sprintf("name=%s %s@%s %s paused=%t", in.GetName(), in.GetOrkflowName(),
				scheduleVersionLabel(in.GetOrkflowVersion()), scheduleSpecSummary(in.GetSpec()), in.GetPaused()),
		}
		if out, ok := resp.(*orkstackv1.CreateScheduleResponse); ok && out != nil && out.GetSchedule().GetId() != "" {
			entry.Target = out.GetSchedule().GetId()
			if ns := out.GetSchedule().GetNamespace(); ns != "" {
				entry.Namespace = ns
			}
		}
		return entry
	},
	orkstackv1connect.ScheduleServiceUpdateScheduleProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.UpdateScheduleRequest)
		return auditEntry{
			Action:    "schedule.update",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
			Detail:    scheduleVersionLabel(in.GetOrkflowVersion()) + " " + scheduleSpecSummary(in.GetSpec()),
		}
	},
	orkstackv1connect.ScheduleServicePauseScheduleProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.PauseScheduleRequest)
		return auditEntry{
			Action:    "schedule.pause",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
			Detail:    "reason=" + in.GetReason(),
		}
	},
	orkstackv1connect.ScheduleServiceResumeScheduleProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.ResumeScheduleRequest)
		return auditEntry{
			Action:    "schedule.resume",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
		}
	},
	orkstackv1connect.ScheduleServiceFireScheduleProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.FireScheduleRequest)
		return auditEntry{
			Action:    "schedule.fire",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
			Detail:    "overlap=" + scheduleOverrideLabel(in.GetOverlap()),
		}
	},
	orkstackv1connect.ScheduleServiceBackfillScheduleProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.BackfillScheduleRequest)
		return auditEntry{
			Action:    "schedule.backfill",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
			Detail: fmt.Sprintf("start=%s end=%s overlap=%s", auditTimestamp(in.GetStart()),
				auditTimestamp(in.GetEnd()), scheduleOverrideLabel(in.GetOverlap())),
		}
	},
	orkstackv1connect.ScheduleServiceDeleteScheduleProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.DeleteScheduleRequest)
		return auditEntry{
			Action:    "schedule.delete",
			Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			Target:    in.GetName(),
		}
	},
	orkstackv1connect.NamespaceServiceCreateNamespaceProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.CreateNamespaceRequest)
		name := artifacts.ResolveNamespace(in.GetNamespace().GetName())
		entry := auditEntry{
			Action:    "namespace.create",
			Namespace: name,
			Target:    name,
			Detail:    namespaceSpecSummary(in.GetNamespace()),
		}
		if out, ok := resp.(*orkstackv1.CreateNamespaceResponse); ok && out != nil {
			outcome := "no-op: identical"
			if out.GetCreated() {
				outcome = "created"
			}
			entry.Detail = strings.TrimSpace(outcome + " " + entry.Detail)
		}
		return entry
	},
	orkstackv1connect.NamespaceServiceUpdateNamespaceProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.UpdateNamespaceRequest)
		name := artifacts.ResolveNamespace(in.GetNamespace().GetName())
		return auditEntry{
			Action:    "namespace.update",
			Namespace: name,
			Target:    name,
			Detail:    namespaceSpecSummary(in.GetNamespace()),
		}
	},
	orkstackv1connect.NamespaceServiceDeleteNamespaceProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.DeleteNamespaceRequest)
		name := artifacts.ResolveNamespace(in.GetName())
		return auditEntry{Action: "namespace.delete", Namespace: name, Target: name}
	},
	orkstackv1connect.ActivityServiceCordonOrkerProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.CordonOrkerRequest)
		// Deployment-wide, as webhook.register and retention.update are: an
		// Orker belongs to no namespace.
		entry := auditEntry{Action: "orker.uncordon", Target: in.GetOrkerId()}
		var parts []string
		if in.GetCordoned() {
			entry.Action = "orker.cordon"
			parts = append(parts, "reason="+strconv.Quote(in.GetReason()))
		}
		if out, ok := resp.(*orkstackv1.CordonOrkerResponse); ok && out != nil && out.GetOrker().GetPool() != "" {
			parts = append(parts, "pool="+out.GetOrker().GetPool())
		}
		entry.Detail = strings.Join(parts, " ")
		return entry
	},
	orkstackv1connect.SystemServiceUpdateRetentionPolicyProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.UpdateRetentionPolicyRequest)
		requested := "-"
		if d := in.GetMaxRunRetention(); d != nil {
			requested = d.AsDuration().String()
		}
		return auditEntry{
			Action: "retention.update",
			Target: "deployment",
			Detail: "max_run_retention=" + requested,
		}
	},
	orkstackv1connect.SystemServiceUpdateImagePolicyProcedure: func(req, _ proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.UpdateImagePolicyRequest)
		// Deployment-wide, as retention.update is. Keys are rendered by
		// fingerprint, never as PEM (imagePolicySummary).
		return auditEntry{
			Action: "image_policy.update",
			Target: "deployment",
			Detail: imagePolicySummary(in.GetPolicy()),
		}
	},
	// The release writes (docs/DEPLOY.md §3). Deployment-wide, as
	// retention.update is: a release belongs to no namespace.
	orkstackv1connect.SystemServicePromoteReleaseProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.PromoteReleaseRequest)
		entry := auditEntry{Action: "release.promote", Target: in.GetBuildId(), Detail: "refused"}
		if out, ok := resp.(*orkstackv1.PromoteReleaseResponse); ok && out != nil {
			if out.GetChanged() {
				entry.Detail = fmt.Sprintf("current %s -> %s", orDashAudit(out.GetPreviousBuildId()), in.GetBuildId())
			} else {
				entry.Detail = "unchanged: already current"
			}
		}
		return entry
	},
	orkstackv1connect.SystemServiceRampReleaseProcedure: func(req, resp proto.Message) auditEntry {
		in, _ := req.(*orkstackv1.RampReleaseRequest)
		entry := auditEntry{Action: "release.ramp", Target: in.GetBuildId(),
			Detail: fmt.Sprintf("percent=%d", in.GetPercent())}
		if out, ok := resp.(*orkstackv1.RampReleaseResponse); ok && out != nil {
			entry.Detail += fmt.Sprintf(" was %s@%s", orDashAudit(out.GetPreviousBuildId()),
				strconv.FormatFloat(float64(out.GetPreviousPercent()), 'g', -1, 32))
		}
		return entry
	},
}

// orDashAudit renders an empty release as "-" in an audit detail.
func orDashAudit(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// unauditedWrites are write RPCs deliberately NOT recorded, each with the
// reason a reader will look for. Adding a name here is a contract decision.
var unauditedWrites = map[string]string{
	orkstackv1connect.RunServiceSubmitRunProcedure: "a run's own record is the durable fact; provenance is run-origin's",
}

// auditNameSuffix is the " name=NAME" a create or register detail gains when
// the object was given a name, and nothing otherwise.
func auditNameSuffix(name string) string {
	if name == "" {
		return ""
	}
	return " name=" + name
}

// webhookAuditDetail renders a registration's detail from the fields that are
// not secret: the URL (its userinfo password redacted), the event names and,
// for a scoped subscription, its scope as `orkctl webhook list` renders it
// (namespace/orkflow/task, "*" per unset component). The signing secret is
// never an argument, so it cannot reach a record.
func webhookAuditDetail(rawURL string, events []string, namespace, orkflowName, taskID string) string {
	shown := rawURL
	if parsed, err := url.Parse(rawURL); err == nil {
		shown = parsed.Redacted()
	}
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(event)), "EVENT_TYPE_"))
	}
	rendered := "all"
	if len(names) > 0 {
		rendered = "[" + strings.Join(names, " ") + "]"
	}
	detail := "url=" + shown + " events=" + rendered
	if namespace == "" && orkflowName == "" && taskID == "" {
		return detail
	}
	part := func(value string) string {
		if value == "" {
			return "*"
		}
		return value
	}
	return detail + " scope=" + part(namespace) + "/" + part(orkflowName) + "/" + part(taskID)
}

// OverlapPolicySlug is the user-facing spelling of an overlap policy: the
// value's name after OVERLAP_POLICY_, lowercased, "_" as "-". UNSPECIFIED and
// any undeclared number have none.
func OverlapPolicySlug(p orkstackv1.OverlapPolicy) (string, bool) {
	name, ok := orkstackv1.OverlapPolicy_name[int32(p)]
	if !ok || p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		return "", false
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(name, "OVERLAP_POLICY_")), "_", "-"), true
}

// scheduleVersionLabel spells an unpinned schedule's version as "current".
func scheduleVersionLabel(version string) string {
	if version == "" {
		return "current"
	}
	return version
}

// scheduleOverrideLabel spells a per-call overlap override: "schedule" when
// there is none, so the schedule's own policy applied.
func scheduleOverrideLabel(p orkstackv1.OverlapPolicy) string {
	if slug, ok := OverlapPolicySlug(p); ok {
		return slug
	}
	if p == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		return "schedule"
	}
	return fmt.Sprintf("%d", int32(p))
}

// scheduleSpecSummary is an audit detail's summary of an authored spec. An
// UNSPECIFIED overlap is spelled as what it is recorded as: skip.
func scheduleSpecSummary(spec *orkstackv1.ScheduleSpec) string {
	interval := "-"
	if iv := spec.GetInterval(); iv != nil {
		interval = iv.AsDuration().String()
	}
	timezone := spec.GetTimezone()
	if timezone == "" {
		timezone = "UTC"
	}
	overlap := spec.GetOverlap()
	if overlap == orkstackv1.OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED {
		overlap = orkstackv1.OverlapPolicy_OVERLAP_POLICY_SKIP
	}
	return fmt.Sprintf("cron=[%s] interval=%s timezone=%s overlap=%s",
		strings.Join(spec.GetCron(), " | "), interval, timezone, scheduleOverrideLabel(overlap))
}

// auditTimestamp spells a timestamp in an audit detail, "-" when unset.
func auditTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return "-"
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}

// namespaceSpecSummary renders the requested spec's mutable fields as
// space-separated key=value pairs. It is the one place a later Namespace
// field is added to the audit detail.
func namespaceSpecSummary(ns *orkstackv1.Namespace) string {
	if ns == nil {
		return ""
	}
	parts := []string{
		"default_pool=" + engine.DefaultPoolFor(ns),
		"allowed_pools=[" + strings.Join(ns.GetAllowedPools(), " ") + "]",
	}
	// Secret policy (docs/DEPLOY.md "Secrets on a shared pool"): allowed_secrets= only when
	// the list is non-empty, so an unrestricted namespace's summary is
	// byte-identical to what it was before the field existed; clearing the
	// list is the record with no key. Entries are in stored order, the
	// allowed_pools rendering, and ahead of run_retention= so a truncation
	// cuts a long labels map, never a pattern. Names only: a pattern is a
	// locator the operator wrote, never a value.
	if allowed := ns.GetAllowedSecrets(); len(allowed) > 0 {
		parts = append(parts, "allowed_secrets=["+strings.Join(allowed, " ")+"]")
	}
	parts = append(parts, "run_retention="+retentionLabel(ns.GetRunRetention()))
	// Image policy (docs/DEPLOY.md "Image supply-chain policy"): require_signed_by= only when
	// set, on the rule allowed_secrets= and hold= follow, so an unrestricted
	// namespace's summary is byte-identical to before the field existed.
	if signer := ns.GetRequireSignedBy(); signer != "" {
		parts = append(parts, "require_signed_by="+signer)
	}
	// Admission (docs/DEPLOY.md "Namespace admission"): hold= only when a hold is
	// requested — released is the absence, as an accepting namespace is
	// hold == nil — quoted so a reason can never end its own value early;
	// since is the registry's and the record's own at says when. max_running
	// is always rendered: max_running=0 on an update is the line that shows a
	// cap was lifted.
	if hold := ns.GetHold(); hold != nil {
		parts = append(parts, "hold="+strconv.Quote(hold.GetReason()))
	}
	parts = append(parts, "max_running="+strconv.FormatInt(int64(ns.GetMaxRunning()), 10))
	// The weight only when set, so a namespace that never set one audits
	// byte-identically to before it existed.
	if w := ns.GetFairShareWeight(); w != 0 {
		parts = append(parts, "fair_share_weight="+strconv.FormatFloat(w, 'f', -1, 64))
	}
	// Budget (docs/DEPLOY.md "Namespace budgets"): budget={…} only when set, on the hold= rule,
	// so a namespace with no budget summarizes byte-identically to before the
	// field existed and a cleared budget is the record with no key.
	if budget := ns.GetBudget(); budget != nil {
		parts = append(parts, "budget="+budgetLabel(budget))
	}
	sink := "inherited"
	if telemetry := ns.GetTelemetry(); telemetry != nil {
		sink = telemetry.GetSink()
		if sink == "" {
			sink = "default"
		}
	}
	parts = append(parts, "telemetry.sink="+sink, "labels="+renderLabels(ns.GetLabels()))
	if description := ns.GetDescription(); description != "" {
		parts = append(parts, fmt.Sprintf("description=%q", description))
	}
	return strings.Join(parts, " ")
}

// auditInterceptor records every call of an audited procedure. Streaming
// handlers pass through untouched by construction — every streaming RPC in the
// API is a read, which TestAudit_EveryUnaryRPCIsClassified also asserts.
func (s *Service) auditInterceptor() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return s.auditUnary(ctx, req.Spec().Procedure, req, next)
		}
	})
}

// auditUnary is the interceptor's body with the procedure passed explicitly.
// Whatever next returns is what the caller gets: the interceptor never alters
// a response or an error.
func (s *Service) auditUnary(
	ctx context.Context,
	procedure string,
	req connect.AnyRequest,
	next connect.UnaryFunc,
) (connect.AnyResponse, error) {
	extract, audited := auditedProcedures[procedure]
	if !audited || s.audit == nil {
		return next(ctx, req)
	}
	resp, err := next(ctx, req)
	reqMsg, _ := req.Any().(proto.Message)
	var respMsg proto.Message
	if err == nil && resp != nil {
		respMsg, _ = resp.Any().(proto.Message)
	}
	entry := extract(reqMsg, respMsg)
	at := s.audit.now().UTC()
	rec := &orkstackv1.AuditRecord{
		SchemaVersion: artifacts.AuditSchemaVersion,
		Id:            artifacts.NewAuditID(at),
		At:            timestamppb.New(at),
		Principal:     req.Header().Get(AuditPrincipalHeader),
		SourceAddress: req.Peer().Addr,
		Action:        entry.Action,
		Procedure:     procedure,
		Namespace:     entry.Namespace,
		Target:        entry.Target,
		Detail:        entry.Detail,
		Succeeded:     err == nil,
	}
	if err != nil {
		rec.ErrorCode = connect.CodeOf(err).String()
	}
	s.audit.Record(ctx, rec)
	return resp, err
}

// auditCursor is the decoded page token: the canonical filter set's key, the
// window as the first page resolved it — so every page of one listing covers
// the same instants even though an unbounded end means "now" and now moves —
// and the key to continue before.
type auditCursor struct {
	Filters     string `json:"filters"`
	WindowSince string `json:"window_since"`
	WindowUntil string `json:"window_until"`
	BeforeKey   string `json:"before_key"`
}

// auditCursorVersion is "v2:": a "v1:" token carried the removed typed
// filters and fails the prefix check, as invalid page_token.
const auditCursorVersion = "v2:"

func encodeAuditCursor(c auditCursor) (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return auditCursorVersion + base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodeAuditCursor reads a page token issued under set, returning the window
// it carries and the key to continue before.
func decodeAuditCursor(token string, set listfilter.Set) (since, until time.Time, beforeKey string, err error) {
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page_token"))
	body, ok := strings.CutPrefix(token, auditCursorVersion)
	if !ok {
		return time.Time{}, time.Time{}, "", invalid
	}
	raw, decodeErr := base64.RawURLEncoding.DecodeString(body)
	if decodeErr != nil {
		return time.Time{}, time.Time{}, "", invalid
	}
	var c auditCursor
	if json.Unmarshal(raw, &c) != nil {
		return time.Time{}, time.Time{}, "", invalid
	}
	if _, _, ok := artifacts.ParseAuditKey(c.BeforeKey); !ok {
		return time.Time{}, time.Time{}, "", invalid
	}
	since, sinceErr := time.Parse(time.RFC3339Nano, c.WindowSince)
	until, untilErr := time.Parse(time.RFC3339Nano, c.WindowUntil)
	if sinceErr != nil || untilErr != nil {
		return time.Time{}, time.Time{}, "", invalid
	}
	if err := checkFilterKey(c.Filters, set); err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	return since.UTC(), until.UTC(), c.BeforeKey, nil
}

// ListAuditRecords reads the trail back from the shared store, newest first.
// It is answered from the store on every call and never from process memory,
// so a replica that recorded nothing answers identically.
//
// It is a deployment-wide listing narrowed by filters, decided on each
// record's body (the key holds only the day and the id). The window is the
// `at` interval the filters bound: its end defaults to now on the engine's
// clock and its start to seven days before the end.
func (s *Service) ListAuditRecords(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListAuditRecordsRequest],
) (*connect.Response[orkstackv1.ListAuditRecordsResponse], error) {
	msg := req.Msg
	set, err := compileFilters(msg)
	if err != nil {
		return nil, err
	}
	pageSize := int(msg.GetPageSize())
	switch {
	case pageSize == 0:
		pageSize = DefaultAuditPageSize
	case pageSize < 0 || pageSize > MaxAuditPageSize:
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("page_size %d is outside 1..%d", msg.GetPageSize(), MaxAuditPageSize))
	}
	if s.auditStore == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("audit store is not configured"))
	}

	var since, until time.Time
	var beforeKey string
	if token := msg.GetPageToken(); token != "" {
		if since, until, beforeKey, err = decodeAuditCursor(token, set); err != nil {
			return nil, err
		}
	} else {
		atOrAfter, before := set.Interval("at")
		until = s.now().UTC()
		if !before.IsZero() {
			until = before.UTC()
		}
		since = until.Add(-DefaultAuditLookback)
		if !atOrAfter.IsZero() {
			since = atOrAfter.UTC()
		}
		if !until.After(since) {
			return nil, emptyWindow("at", since, until)
		}
	}

	var keep func(*orkstackv1.AuditRecord) bool
	if !set.Empty() {
		keep = func(rec *orkstackv1.AuditRecord) bool { return set.Match(rec) }
	}
	page, err := s.auditStore.ListAuditRecords(ctx, artifacts.AuditQuery{
		Since:     since,
		Until:     until,
		Keep:      keep,
		BeforeKey: beforeKey,
		Limit:     pageSize,
	})
	if err != nil {
		if errors.Is(err, artifacts.ErrAuditScanBudget) {
			return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
				"scan examined more than %d records before filling a page; narrow the at window or add filters",
				artifacts.AuditScanMaxObjects))
		}
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read audit records: %w", err))
	}
	out := &orkstackv1.ListAuditRecordsResponse{
		Records: page.Records,
		Since:   timestamppb.New(since),
		Until:   timestamppb.New(until),
	}
	if out.Records == nil {
		out.Records = []*orkstackv1.AuditRecord{}
	}
	if page.NextKey != "" {
		token, err := encodeAuditCursor(auditCursor{
			Filters:     set.Key(),
			WindowSince: since.Format(time.RFC3339Nano),
			WindowUntil: until.Format(time.RFC3339Nano),
			BeforeKey:   page.NextKey,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode page_token: %w", err))
		}
		out.NextPageToken = token
	}
	return connect.NewResponse(out), nil
}

// auditVerdictWord renders an interface verdict in the audit detail's
// lower-case words.
func auditVerdictWord(verdict orkstackv1.InterfaceVerdict) string {
	switch verdict {
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_COMPATIBLE:
		return "compatible"
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING:
		return "breaking"
	case orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNKNOWN:
		return "unknown"
	default:
		return "unspecified"
	}
}
