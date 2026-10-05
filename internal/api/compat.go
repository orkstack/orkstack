package api

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// The interface compatibility gate (interface-compatibility-gate §5): two
// reads — CompareInterfaces and ListOrkflowCallers — and the helpers the
// promote gate, the version listing and the apply/validate warnings share.
// Nothing here writes: every answer is computed from immutable documents the
// store already holds, or from the trigger and schedule reads, per request.

// maxRefusalFindings bounds how many breaking findings a refusal quotes;
// the rest are counted. CompareInterfaces lists them all.
const maxRefusalFindings = 5

// CompareInterfaces classifies how the declared interface of one stored
// version changes in another. Both sides resolve against ONE reading of the
// version catalog, so an empty version on either side means the same pointer.
func (s *Service) CompareInterfaces(
	ctx context.Context,
	req *connect.Request[orkstackv1.CompareInterfacesRequest],
) (*connect.Response[orkstackv1.CompareInterfacesResponse], error) {
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	versions, err := s.orkflows.Versions(ctx, ns.GetName(), name)
	if err != nil {
		return nil, orkflowStoreError(err, "versions of orkflow %q in namespace %q", name, ns.GetName())
	}
	if len(versions.All) == 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf(
			"orkflow %q in namespace %q not found", name, ns.GetName()))
	}
	resolve := func(version string) (string, *orkstackv1.Orkflow, error) {
		if version == "" {
			version = versions.Resolved()
		}
		doc := storedVersion(versions, version)
		if doc == nil {
			return "", nil, connect.NewError(connect.CodeNotFound, fmt.Errorf(
				"orkflow %q version %q in namespace %q not found", name, version, ns.GetName()))
		}
		return version, doc, nil
	}
	from, fromDoc, err := resolve(req.Msg.GetFromVersion())
	if err != nil {
		return nil, err
	}
	to, toDoc, err := resolve(req.Msg.GetToVersion())
	if err != nil {
		return nil, err
	}
	findings, verdict := dsl.CompareInterfaces(fromDoc, toDoc)
	return connect.NewResponse(&orkstackv1.CompareInterfacesResponse{
		Findings:    findings,
		Verdict:     verdict,
		FromVersion: from,
		ToVersion:   to,
	}), nil
}

// verdictAgainst is what promoting candidate would be against current:
// UNSPECIFIED when there is nothing to compare (current is nil, or candidate
// IS current), else the verdict of dsl.CompareInterfaces(current, candidate).
func verdictAgainst(current, candidate *orkstackv1.Orkflow) orkstackv1.InterfaceVerdict {
	if current == nil || candidate == nil || current.GetVersion() == candidate.GetVersion() {
		return orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_UNSPECIFIED
	}
	_, verdict := dsl.CompareInterfaces(current, candidate)
	return verdict
}

// breakingPromotionRefusal is the one refusal a breaking, unacknowledged
// promotion gets. FailedPrecondition: the request is well-formed and the
// deployment's state refuses it right now — the code the lost-race refusal
// beside it already uses.
//
// namespace is accepted for the caller's symmetry with the lost-race refusal;
// the sentence names the orkflow and the commands, which take -n themselves.
func breakingPromotionRefusal(namespace, name, from, to string, neverPromoted bool,
	findings []*orkstackv1.InterfaceFinding) error {
	breaking := dsl.BreakingFindings(findings)
	what := "the version a run that names no version executes now"
	if neverPromoted {
		what = "its newest applied version, which runs because it was never promoted"
	}
	quoted := make([]string, 0, maxRefusalFindings)
	for i, f := range breaking {
		if i == maxRefusalFindings {
			break
		}
		quoted = append(quoted, f.GetField()+": "+f.GetMessage())
	}
	list := strings.Join(quoted, "; ")
	if extra := len(breaking) - len(quoted); extra > 0 {
		list += fmt.Sprintf("; and %d more", extra)
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"promoting %s to %s breaks the declared interface of %s, %s (%s): %s. "+
			"Review them with `orkctl orkflow compat %s %s %s` and see what starts or composes it "+
			"with `orkctl orkflow callers %s`; to promote anyway, acknowledge it: "+
			"`orkctl orkflow promote %s %s --acknowledge-breaking`",
		name, to, from, what,
		dsl.CountNoun(len(breaking), "breaking change", "breaking changes"), list,
		name, from, to, name, name, to))
}

// interfaceChangeIssues compares a PREPARED document with the version its
// orkflow runs now and returns WARNINGs for a BREAKING change — never an
// error: applying is authoring, and the gate is on promotion. It returns
// nothing for a first version, for a document whose version is the one
// that runs, and for a COMPATIBLE or UNKNOWN change (an UNKNOWN on every
// apply of an orkflow with no outputs: would be noise, not information;
// compat and the Versions tab show it). A store that cannot be read is one
// WARNING at "version", not a failure: the comparison is advice.
func (s *Service) interfaceChangeIssues(ctx context.Context, namespace string,
	wf *orkstackv1.Orkflow) []*orkstackv1.ValidationIssue {
	warn := func(field, message string) *orkstackv1.ValidationIssue {
		return &orkstackv1.ValidationIssue{Field: field, Message: message,
			Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING}
	}
	name, version := wf.GetName(), wf.GetVersion()
	versions, err := s.orkflows.Versions(ctx, namespace, name)
	if err != nil {
		return []*orkstackv1.ValidationIssue{warn("version", fmt.Sprintf(
			"could not compare with the version that runs now: %v; compare once the store answers: "+
				"orkctl orkflow compat %s %s", err, name, version))}
	}
	resolved := versions.Resolved()
	if resolved == "" || resolved == version {
		return nil
	}
	resolvedDoc := storedVersion(versions, resolved)
	if resolvedDoc == nil {
		return nil
	}
	findings, verdict := dsl.CompareInterfaces(resolvedDoc, wf)
	if verdict != orkstackv1.InterfaceVerdict_INTERFACE_VERDICT_BREAKING {
		return nil
	}
	breaking := dsl.BreakingFindings(findings)
	issues := make([]*orkstackv1.ValidationIssue, 0, len(breaking)+1)
	for _, f := range breaking {
		issues = append(issues, warn(f.GetField(),
			f.GetMessage()+" — breaking against "+name+"@"+resolved))
	}
	count := dsl.CountNoun(len(breaking), "breaking change", "breaking changes")
	var summary string
	switch {
	case versions.Current != "":
		summary = fmt.Sprintf("%s@%s breaks the declared interface of %s@%s, the current version (%s). "+
			"Applying it changes nothing that runs; promoting it is refused until acknowledged "+
			"(orkctl orkflow promote %s %s --acknowledge-breaking). What starts or composes %s: "+
			"orkctl orkflow callers %s",
			name, version, name, resolved, count, name, version, name, name)
	case !storesVersion(versions, version):
		summary = fmt.Sprintf("%s@%s breaks the declared interface of %s@%s (%s), and %s was never "+
			"promoted, so a run that names no version executes its newest applied version: applying "+
			"%s makes it what runs. To keep %s running, promote it before applying: "+
			"orkctl orkflow promote %s %s",
			name, version, name, resolved, count, name, version, resolved, name, resolved)
	default:
		summary = fmt.Sprintf("%s@%s is already stored and breaks the declared interface of %s@%s, "+
			"which runs when no version is named (%s); promoting it is refused until acknowledged",
			name, version, name, resolved, count)
	}
	return append(issues, warn("version", summary))
}

// ListOrkflowCallers lists what, in this namespace, references one orkflow:
// every composing task of another orkflow's scanned version, every trigger,
// and every schedule. It is a scan made per request — there is no caller
// index — and it writes nothing.
func (s *Service) ListOrkflowCallers(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListOrkflowCallersRequest],
) (*connect.Response[orkstackv1.ListOrkflowCallersResponse], error) {
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}
	// The name need not be stored (a deleted orkflow still has callers), so
	// it is held to the slug rule here, as DeleteOrkflow does.
	if err := dsl.ValidSlug("orkflow name", name); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	namespace := ns.GetName()
	all := req.Msg.GetAllVersions()

	target, err := s.orkflows.Versions(ctx, namespace, name)
	if err != nil {
		return nil, orkflowStoreError(err, "versions of orkflow %q in namespace %q", name, namespace)
	}
	resp := &orkstackv1.ListOrkflowCallersResponse{
		CurrentVersion: target.Resolved(),
		StoredVersions: int32(len(target.All)),
		AllVersions:    all,
	}

	// Composing orkflows.
	listed, err := s.orkflows.List(ctx, namespace)
	if err != nil {
		return nil, orkflowStoreError(err, "list orkflows in namespace %q", namespace)
	}
	sort.SliceStable(listed, func(i, j int) bool { return listed[i].GetName() < listed[j].GetName() })
	for _, o := range listed {
		if o.GetName() == name {
			continue
		}
		type scanned struct {
			wf      *orkstackv1.Orkflow
			current bool
		}
		docs := []scanned{{wf: o, current: true}}
		if all {
			versions, err := s.orkflows.Versions(ctx, namespace, o.GetName())
			if err != nil {
				return nil, orkflowStoreError(err, "versions of orkflow %q in namespace %q",
					o.GetName(), namespace)
			}
			resolved := versions.Resolved()
			docs = docs[:0]
			for _, wf := range versions.All {
				docs = append(docs, scanned{wf: wf, current: wf.GetVersion() == resolved})
			}
		}
		for _, doc := range docs {
			resp.ScannedVersions++
			rows, err := composingRows(doc.wf, name, doc.current)
			if err != nil {
				return nil, err
			}
			resp.Callers = append(resp.Callers, rows...)
		}
	}

	// Triggers, oldest first: the store's own order. List carries no
	// verification material, and none is copied.
	trigs, err := s.triggers.List(ctx, namespace, name)
	if err != nil {
		return nil, triggerConnectError(err)
	}
	for _, t := range trigs {
		resp.Callers = append(resp.Callers, &orkstackv1.OrkflowCaller{
			Kind:          orkstackv1.OrkflowCallerKind_ORKFLOW_CALLER_KIND_TRIGGER,
			Name:          t.Name,
			Id:            t.ID,
			PinnedVersion: t.OrkflowVersion,
			Description:   t.Description,
		})
	}

	// Schedules, by name. A partial answer would read as "no schedule
	// follows current", so a failed list is Unavailable, never empty.
	if s.schedules != nil {
		views, err := s.schedules.List(ctx, namespace)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"cannot list the schedules of namespace %q: %v", namespace, err))
		}
		var rows []*orkstackv1.OrkflowCaller
		for _, view := range views {
			if view.OrkflowName != name {
				continue
			}
			row := &orkstackv1.OrkflowCaller{
				Kind: orkstackv1.OrkflowCallerKind_ORKFLOW_CALLER_KIND_SCHEDULE,
				Name: view.Name,
				Id:   view.ID,
			}
			if view.Record == nil || view.Unavailable != "" {
				// The version it pins is not known for this read: rendered
				// unavailable, never "follows current". Never the input.
				row.UnavailableReason = view.Unavailable
				if row.UnavailableReason == "" {
					row.UnavailableReason = "the cluster did not describe this schedule"
				}
			} else {
				row.PinnedVersion = view.Record.GetOrkflowVersion()
				row.Description = view.Record.GetDescription()
			}
			rows = append(rows, row)
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].GetName() < rows[j].GetName() })
		resp.Callers = append(resp.Callers, rows...)
	}
	return connect.NewResponse(resp), nil
}

// composingRows is one row per task of wf that composes target, in
// declaration order. An OutputReferences error on a stored definition is
// Internal: its templates passed rules 8 and 28 at apply, so failing now is a
// corrupted store, not a caller.
func composingRows(wf *orkstackv1.Orkflow, target string, current bool) ([]*orkstackv1.OrkflowCaller, error) {
	var composing []*orkstackv1.Task
	for _, t := range wf.GetTasks() {
		if t.GetOrkflow().GetName() == target {
			composing = append(composing, t)
		}
	}
	if len(composing) == 0 {
		return nil, nil
	}
	refs, err := dsl.OutputReferences(wf)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf(
			"read the output references of stored orkflow %s@%s: %w",
			wf.GetName(), wf.GetVersion(), err))
	}
	rows := make([]*orkstackv1.OrkflowCaller, 0, len(composing))
	for _, t := range composing {
		read := map[string]bool{}
		for _, ref := range refs {
			if ref.TaskID == t.GetId() {
				read[ref.Key] = true
			}
		}
		outputs := make([]string, 0, len(read))
		for k := range read {
			outputs = append(outputs, k)
		}
		sort.Strings(outputs)
		supplied := make([]string, 0, len(t.GetWith().GetFields()))
		for k := range t.GetWith().GetFields() {
			supplied = append(supplied, k)
		}
		sort.Strings(supplied)
		rows = append(rows, &orkstackv1.OrkflowCaller{
			Kind:                   orkstackv1.OrkflowCallerKind_ORKFLOW_CALLER_KIND_ORKFLOW,
			Name:                   wf.GetName(),
			CallerVersion:          wf.GetVersion(),
			CallerVersionIsCurrent: current,
			TaskId:                 t.GetId(),
			PinnedVersion:          t.GetOrkflow().GetVersion(),
			PinnedDefinitionDigest: t.GetOrkflow().GetDefinitionDigest(),
			OutputsRead:            outputs,
			InputsSupplied:         supplied,
		})
	}
	return rows, nil
}
