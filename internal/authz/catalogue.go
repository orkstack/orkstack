package authz

import (
	"slices"
	"sort"
	"strings"
)

// Permission is one entry of the closed catalogue: `<resource>.<verb>`
// (docs/ACCESS.md). A custom role lists permissions by exact name, with no
// wildcard, so a permission a later release adds is never granted by
// accident.
type Permission string

// ScopeKind is where a permission can be held and where a binding is made.
type ScopeKind int

// The four scope kinds. A binding at the deployment covers everything; at a
// namespace, everything in it; at an Orkflow, that Orkflow with its runs,
// schedules and triggers; at a pool, that pool.
const (
	ScopeDeployment ScopeKind = iota + 1
	ScopeNamespace
	ScopeOrkflow
	ScopePool
)

func (k ScopeKind) String() string {
	switch k {
	case ScopeDeployment:
		return "deployment"
	case ScopeNamespace:
		return "namespace"
	case ScopeOrkflow:
		return "orkflow"
	case ScopePool:
		return "pool"
	}
	return "unknown"
}

// covers reports whether a binding of kind k may grant a permission held at
// kind narrower: the deployment covers every kind, a namespace covers itself
// and an Orkflow, and an Orkflow and a pool cover only themselves.
func (k ScopeKind) covers(narrower ScopeKind) bool {
	switch k {
	case ScopeDeployment:
		return true
	case ScopeNamespace:
		return narrower == ScopeNamespace || narrower == ScopeOrkflow
	default:
		return k == narrower
	}
}

// The catalogue. Each constant's entry in catalogue says where it can be held
// and what it allows.
const (
	PermOrkflowValidate Permission = "orkflow.validate"
	PermOrkflowWrite    Permission = "orkflow.write"
	PermOrkflowRead     Permission = "orkflow.read"
	PermOrkflowPromote  Permission = "orkflow.promote"
	PermOrkflowDelete   Permission = "orkflow.delete"

	PermRunSubmit         Permission = "run.submit"
	PermRunRead           Permission = "run.read"
	PermRunLogsRead       Permission = "run.logs.read"
	PermRunArtifactsRead  Permission = "run.artifacts.read"
	PermRunEvidenceExport Permission = "run.evidence.export"
	PermRunCancel         Permission = "run.cancel"
	PermRunAwaitResolve   Permission = "run.await.resolve"
	PermRunResume         Permission = "run.resume"
	// PermRunResumeAcceptGap lets a resume go past a sequence gap or an
	// unverified stop (resume design §28.15, §28.17 C): admin only.
	PermRunResumeAcceptGap Permission = "run.resume.accept_gap"
	PermRunPause           Permission = "run.pause"
	PermRunCancelForce     Permission = "run.cancel.force"
	PermRunCancelBulk      Permission = "run.cancel.bulk"
	PermRunHistoryDecode   Permission = "run.history.decode"
	PermConcurrencyReset   Permission = "concurrency.reset"

	PermScheduleRead    Permission = "schedule.read"
	PermScheduleWrite   Permission = "schedule.write"
	PermScheduleOperate Permission = "schedule.operate"
	PermTriggerRead     Permission = "trigger.read"
	PermTriggerWrite    Permission = "trigger.write"

	PermEventSourceRead     Permission = "eventsource.read"
	PermEventSourceWrite    Permission = "eventsource.write"
	PermEventSourceOperate  Permission = "eventsource.operate"
	PermEventSourceLogsRead Permission = "eventsource.logs.read"
	PermEventSourceHost     Permission = "eventsource.host"

	PermWebhookRead      Permission = "webhook.read"
	PermWebhookWrite     Permission = "webhook.write"
	PermWebhookRedeliver Permission = "webhook.redeliver"

	PermStatsRead      Permission = "stats.read"
	PermCachePurge     Permission = "cache.purge"
	PermAutomationPlan Permission = "automation.plan"

	PermNamespaceRead         Permission = "namespace.read"
	PermNamespaceUpdate       Permission = "namespace.update"
	PermNamespacePolicyUpdate Permission = "namespace.policy.update"
	PermNamespaceCreate       Permission = "namespace.create"
	PermNamespaceDelete       Permission = "namespace.delete"

	PermCatalogRead        Permission = "catalog.read"
	PermOrkerRead          Permission = "orker.read"
	PermOrkerCordon        Permission = "orker.cordon"
	PermSystemRead         Permission = "system.read"
	PermRetentionUpdate    Permission = "retention.update"
	PermSystemRestore      Permission = "system.restore"
	PermImagePolicyUpdate  Permission = "image-policy.update"
	PermReleaseOperate     Permission = "release.operate"
	PermAuditRead          Permission = "audit.read"
	PermAccessRead         Permission = "access.read"
	PermAccessWrite        Permission = "access.write"
	PermAccessBind         Permission = "access.bind"
	PermIdentityRead       Permission = "identity.read"
	PermIdentityKeysRetire Permission = "identity.keys.retire"
	PermIdentityTokenMint  Permission = "identity.token.mint"
	PermTaskResultCommit   Permission = "task.result.commit"
	PermPayloadKeyUse      Permission = "payload.key.use"
	PermPayloadKeysManage  Permission = "payload.keys.manage"
)

// entry is one permission's catalogue row.
type entry struct {
	// kinds are the narrowest kinds it can be held at; a binding of a kind
	// that covers one of them may grant it.
	kinds  []ScopeKind
	allows string
}

var (
	kindO = []ScopeKind{ScopeOrkflow}
	kindN = []ScopeKind{ScopeNamespace}
	kindD = []ScopeKind{ScopeDeployment}
	kindP = []ScopeKind{ScopePool}
)

// catalogue is the closed set. TestCatalogue_Golden holds it, with the
// built-in roles, to a golden file, so a change is seen in review.
var catalogue = map[Permission]entry{
	PermOrkflowValidate: {kindN, "validate an Orkflow document without storing it"},
	PermOrkflowWrite:    {kindO, "create an Orkflow or store a new version of it"},
	PermOrkflowRead:     {kindO, "read an Orkflow, its versions, callers and plans"},
	PermOrkflowPromote:  {kindO, "move an Orkflow's current version"},
	PermOrkflowDelete:   {kindO, "delete an Orkflow"},

	PermRunSubmit:          {kindO, "start a run of an Orkflow"},
	PermRunRead:            {kindO, "read a run, its events, attempts, values and input"},
	PermRunLogsRead:        {kindO, "read a task's logs, live or stored"},
	PermRunArtifactsRead:   {kindO, "read a task's artifact bytes"},
	PermRunEvidenceExport:  {kindO, "export a run's evidence bundle"},
	PermRunCancel:          {kindO, "cancel a run"},
	PermRunAwaitResolve:    {kindO, "complete or reject an awaited task"},
	PermRunResume:          {kindO, "resume a run that ended, with run.submit"},
	PermRunResumeAcceptGap: {kindO, "resume a run past a gap in its events or an unverified stop"},
	PermRunPause:           {kindO, "pause and unpause a run"},
	PermRunCancelForce:     {kindO, "force-cancel a run; held at a namespace, with its descendants"},
	PermRunCancelBulk:      {kindN, "cancel many runs of a namespace in one request"},
	PermRunHistoryDecode:   {kindO, "decode a run's history through the codec endpoint"},
	PermConcurrencyReset:   {kindN, "reset who holds a namespace's concurrency limits"},

	PermScheduleRead:    {kindO, "read schedules"},
	PermScheduleWrite:   {kindO, "create, change and delete schedules, with run.submit"},
	PermScheduleOperate: {kindO, "pause, resume, fire and backfill schedules"},
	PermTriggerRead:     {kindO, "read triggers"},
	PermTriggerWrite:    {kindO, "create, change and delete triggers, with run.submit"},

	PermEventSourceRead:     {kindO, "read event sources and evaluate a message against one; a candidate source with orkflow.read"},
	PermEventSourceWrite:    {kindO, "create, change and delete event sources, with run.submit, and read what a source image declares"},
	PermEventSourceOperate:  {kindO, "pause, resume and restart event sources; a resume with run.submit"},
	PermEventSourceLogsRead: {kindO, "read an event source's instance logs"},
	PermEventSourceHost:     {kindP, "host an event source's instances at a pool, for an instance the caller's Orker holds"},

	PermWebhookRead:      {kindN, "read webhook subscriptions and their deliveries"},
	PermWebhookWrite:     {kindN, "register, change and delete webhook subscriptions, with run.read"},
	PermWebhookRedeliver: {kindN, "deliver a webhook's delivery again"},

	PermStatsRead:      {kindN, "read a namespace's stats, usage and cache stats"},
	PermCachePurge:     {kindN, "purge cached task memos; every namespace's when held at the deployment"},
	PermAutomationPlan: {kindN, "plan and export a namespace's automation document"},

	PermNamespaceRead:         {kindN, "read a namespace and its pools"},
	PermNamespaceUpdate:       {kindN, "change a namespace's description, labels, default pool, retention, telemetry and hold"},
	PermNamespacePolicyUpdate: {kindN, "change what a namespace may reach: pools, secrets, signers, limits, budget, weight and its identity policy"},
	PermNamespaceCreate:       {kindD, "create a namespace"},
	PermNamespaceDelete:       {kindD, "delete a namespace"},

	PermCatalogRead:        {kindD, "read the activity catalogue and telemetry sinks; every authenticated principal holds it"},
	PermOrkerRead:          {kindD, "read the Orker inventory"},
	PermOrkerCordon:        {kindD, "cordon and uncordon an Orker"},
	PermSystemRead:         {kindD, "read system information, policies and releases"},
	PermRetentionUpdate:    {kindD, "change the deployment's retention policy"},
	PermSystemRestore:      {kindD, "reconcile the cluster with the bucket after a restore, deleting the executions of runs retired since"},
	PermImagePolicyUpdate:  {kindD, "change the deployment's image policy"},
	PermReleaseOperate:     {kindD, "ramp and promote a release"},
	PermAuditRead:          {kindN, "read the audit trail; of one namespace with a namespace filter"},
	PermAccessRead:         {kindD, "read the access document and explain a decision"},
	PermAccessWrite:        {kindD, "change roles, bindings, service accounts and disabled principals"},
	PermAccessBind:         {kindN, "bind, at a namespace, a role whose every permission the holder holds there"},
	PermIdentityRead:       {kindN, "read the task identity issuer, its keys, providers and payload keys"},
	PermIdentityKeysRetire: {kindD, "retire an identity signing key"},
	PermIdentityTokenMint:  {kindP, "mint a task's identity tokens for an attempt the caller's Orker holds"},
	PermTaskResultCommit:   {kindP, "commit a task's result for an attempt the caller's Orker holds"},
	PermPayloadKeyUse:      {kindP, "unwrap a run's payload key for an attempt the caller's Orker holds"},
	PermPayloadKeysManage:  {kindN, "rotate and re-wrap a namespace's payload keys"},
}

// Known reports whether p is in the catalogue.
func Known(p Permission) bool {
	_, ok := catalogue[p]
	return ok
}

// Permissions lists the catalogue, sorted.
func Permissions() []Permission {
	out := make([]Permission, 0, len(catalogue))
	for name := range catalogue {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

// ScopeKinds is where p can be held, narrowest first; nil for an unknown
// permission.
func ScopeKinds(p Permission) []ScopeKind {
	e, ok := catalogue[p]
	if !ok {
		return nil
	}
	return slices.Clone(e.kinds)
}

// Allows says in one sentence what p lets its holder do.
func Allows(p Permission) string { return catalogue[p].allows }

// grantableAt reports whether a role bound at kind may grant perm.
func grantableAt(perm Permission, kind ScopeKind) bool {
	for _, k := range catalogue[perm].kinds {
		if kind.covers(k) {
			return true
		}
	}
	return false
}

// The built-in roles. Their names are reserved: no custom role takes one.
const (
	RoleViewer         = "viewer"
	RoleOperator       = "operator"
	RoleDeveloper      = "developer"
	RoleNamespaceAdmin = "namespace-admin"
	RoleAuditor        = "auditor"
	RoleAdmin          = "admin"
	RoleOrker          = "orker"
)

// orkerPermissions are the ones an Orker holds and no person's built-in role
// does: admin holds every permission but these.
var orkerPermissions = []Permission{PermIdentityTokenMint, PermTaskResultCommit, PermPayloadKeyUse, PermEventSourceHost}

var builtInRoles = func() map[string]map[Permission]bool {
	set := func(perms ...Permission) map[Permission]bool {
		m := make(map[Permission]bool, len(perms))
		for _, p := range perms {
			m[p] = true
		}
		return m
	}
	with := func(base map[Permission]bool, perms ...Permission) map[Permission]bool {
		m := make(map[Permission]bool, len(base)+len(perms))
		for p := range base {
			m[p] = true
		}
		for _, p := range perms {
			m[p] = true
		}
		return m
	}
	viewer := set(PermOrkflowRead, PermRunRead, PermScheduleRead, PermTriggerRead, PermEventSourceRead, PermWebhookRead,
		PermStatsRead, PermNamespaceRead, PermOrkerRead, PermSystemRead)
	operator := with(viewer, PermRunSubmit, PermRunCancel, PermRunAwaitResolve, PermRunLogsRead,
		PermRunArtifactsRead, PermScheduleOperate, PermWebhookRedeliver, PermRunResume, PermRunPause,
		PermEventSourceOperate, PermEventSourceLogsRead)
	developer := with(operator, PermOrkflowValidate, PermOrkflowWrite, PermOrkflowPromote,
		PermScheduleWrite, PermTriggerWrite, PermEventSourceWrite, PermCachePurge, PermAutomationPlan, PermRunEvidenceExport)
	namespaceAdmin := with(developer, PermOrkflowDelete, PermWebhookWrite, PermNamespaceUpdate,
		PermAuditRead, PermAccessBind, PermRunCancelForce, PermRunCancelBulk, PermConcurrencyReset,
		PermPayloadKeysManage)
	auditor := set(PermRunEvidenceExport)
	admin := set()
	for name := range catalogue {
		if strings.HasSuffix(string(name), ".read") && name != PermRunLogsRead && name != PermRunArtifactsRead && name != PermEventSourceLogsRead {
			auditor[name] = true
		}
		if !slices.Contains(orkerPermissions, name) {
			admin[name] = true
		}
	}
	return map[string]map[Permission]bool{
		RoleViewer:         viewer,
		RoleOperator:       operator,
		RoleDeveloper:      developer,
		RoleNamespaceAdmin: namespaceAdmin,
		RoleAuditor:        auditor,
		RoleAdmin:          admin,
		RoleOrker:          set(orkerPermissions...),
	}
}()

// BuiltInRoles lists the built-in roles' names, sorted.
func BuiltInRoles() []string {
	names := make([]string, 0, len(builtInRoles))
	for name := range builtInRoles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IsBuiltInRole reports whether name is reserved for a built-in role.
func IsBuiltInRole(name string) bool {
	_, ok := builtInRoles[name]
	return ok
}

// BuiltInRolePermissions lists a built-in role's permissions, sorted; nil
// for a name that is not one.
func BuiltInRolePermissions(name string) []Permission {
	perms, ok := builtInRoles[name]
	if !ok {
		return nil
	}
	out := make([]Permission, 0, len(perms))
	for p := range perms {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

// RolesHolding lists the built-in roles that hold p, sorted.
func RolesHolding(p Permission) []string {
	var out []string
	for name, perms := range builtInRoles {
		if perms[p] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
