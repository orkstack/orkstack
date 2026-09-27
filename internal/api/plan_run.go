package api

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

// PlanRun answers which tasks of one definition would run for one input,
// which would be skipped and why, and in what order — without starting
// anything.
//
// It is a read. It touches the Orkflow store (read) and prepareOrkflow and
// nothing else: no starter, no run id, no run store, no event bus, no
// idempotency claimer, no archive. That is structural, not a flag — there is
// no path from here to a submission — and it is why a service configured
// with no Starter still answers a plan while SubmitRun refuses.
//
// A plan is not an admission check either: pools with no live Orker, a held
// namespace, and limits are SubmitRun's questions. A plan on a held
// namespace still answers, because it starts nothing.
func (s *Service) PlanRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.PlanRunRequest],
) (*connect.Response[orkstackv1.PlanRunResponse], error) {
	msg := req.Msg
	name, yaml := msg.GetOrkflowName(), msg.GetYaml()

	// 1. Exactly one source.
	if (name == "") == (yaml == "") {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("set exactly one of orkflow_name and yaml"))
	}
	if yaml != "" && msg.GetOrkflowVersion() != "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("orkflow_version cannot be set with yaml: a document has the version it pins or derives"))
	}

	// 2. Resolve the definition.
	var (
		wf *orkstackv1.Orkflow
		ns *orkstackv1.Namespace
	)
	if name != "" {
		var err error
		// GetOrkflow's exact sequence: another namespace's Orkflow is
		// NotFound, an empty version is the current pointer.
		ns, err = s.namespace(ctx, msg.GetNamespace())
		if err != nil {
			return nil, err
		}
		wf, err = s.orkflows.Get(ctx, ns.GetName(), name, msg.GetOrkflowVersion())
		if err != nil {
			return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
				name, msg.GetOrkflowVersion(), ns.GetName())
		}
	} else {
		var err error
		// ValidateOrkflow's preparation, with its errors returned as the
		// refusal they are: `orkctl orkflow validate` itemizes issues, a plan
		// of an invalid document is a refused request.
		wf, ns, err = s.prepareOrkflow(ctx, []byte(yaml), msg.GetNamespace())
		if err != nil {
			return nil, withFieldPath(err)
		}
	}

	// 3. Validate assumptions, so the field path is on the request.
	known := make(map[string]struct{}, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		known[t.GetId()] = struct{}{}
	}
	keys := make([]string, 0, len(msg.GetAssumedOutputs()))
	for id := range msg.GetAssumedOutputs() {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		if _, ok := known[id]; !ok {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
				"assumed_outputs[%q]: not a task of %s@%s", id, wf.GetName(), wf.GetVersion()))
		}
	}

	// 4. Simulate. An error cannot occur after step 3 on a stored or
	// prepared document; if it does, it is the engine's, not the caller's.
	resp, err := engine.Simulate(wf, msg.GetInput(), msg.GetAssumedOutputs())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// 5. Stamp the coordinates: the RESOLVED version, never the request's
	// empty string.
	resp.OrkflowName = wf.GetName()
	resp.OrkflowVersion = wf.GetVersion()
	resp.Namespace = ns.GetName()
	resp.DefinitionDigest = wf.GetDefinitionDigest()
	resp.FromYaml = yaml != ""
	return connect.NewResponse(resp), nil
}

// withFieldPath puts a validation refusal's field path in front of its
// sentence ("tasks[0].needs[0]: task \"a\" needs unknown task ...").
//
// ValidateOrkflow carries the path in ValidationIssue.field; a plan returns
// the refusal as a Connect error, whose only text is the message, so the path
// is spelled into it rather than lost. Errors without a path, and every other
// code, are returned unchanged.
func withFieldPath(err error) error {
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		return err
	}
	field := ""
	var validation *dsl.ValidationError
	if errors.As(err, &validation) {
		field = validation.Field
	}
	var requestField *validationFieldError
	if errors.As(err, &requestField) {
		field = requestField.Field
	}
	if field == "" {
		return err
	}
	message := err.Error()
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		message = connectErr.Message()
	}
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %s", field, message))
}
