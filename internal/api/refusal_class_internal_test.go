package api

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"

	"connectrpc.com/connect"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.temporal.io/api/serviceerror"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// s3Status is what an S3 client returns for a response of status.
func s3Status(status int) error {
	return &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}}, Err: errors.New("the store said")}
}

// TestClassifyRefusal: a refusal is judged by its type or its mark and never
// by its Connect code or its words, whatever wraps it; what nothing names is
// uncertain.
func TestClassifyRefusal(t *testing.T) {
	wait := func(cause string) verdict { return verdict{class: refusalWait, cause: cause} }
	pause := func(code string) verdict { return verdict{class: refusalPause, code: code} }
	uncertain := func(code string) verdict { return verdict{class: refusalUncertain, code: code} }
	for name, c := range map[string]struct {
		err  error
		want verdict
	}{
		"held":                 {&engine.AdmissionRefusedError{Reason: engine.AdmissionHeld}, wait(waitAdmission)},
		"a limit's full line":  {busy(errors.New("full")), wait(waitAdmission)},
		"a limit's full key":   {connect.NewError(connect.CodeFailedPrecondition, &limitReached{err: errors.New("full")}), wait(waitAdmission)},
		"a limit not checked":  {&checkUnavailable{err: errors.New("no gate")}, wait(waitOutage)},
		"no live Orker":        {poolPlanError(&engine.PoolEmptyError{}), wait(waitPool)},
		"a source's mapping":   {&eventsources.Error{Reason: eventsources.ReasonMappingInvalid}, pause(eventsources.ReasonMappingInvalid)},
		"a source's Orkflow":   {sourceError(&eventsources.Error{Reason: eventsources.ReasonOrkflowUnsupported}), pause(eventsources.ReasonOrkflowUnsupported)},
		"no pin host":          {poolPlanError(&engine.PoolNoPinHostError{}), wait(waitPool)},
		"cluster not asked":    {poolPlanError(&engine.PoolUnreachableError{}), wait(waitOutage)},
		"no fan-out support":   {poolPlanError(&engine.PoolNoEachSupportError{}), wait(waitRollout)},
		"no cache support":     {poolPlanError(&engine.PoolNoCacheSupportError{}), wait(waitRollout)},
		"no join support":      {poolPlanError(&engine.PoolNoJoinSupportError{}), wait(waitRollout)},
		"no masking support":   {poolPlanError(&engine.PoolNoSecretMaskingError{}), wait(waitRollout)},
		"no ordinals":          {poolPlanError(&engine.PoolNoAttemptOrdinalsError{}), wait(waitRollout)},
		"no identity support":  {poolPlanError(&engine.PoolNoIdentitySupportError{}), wait(waitRollout)},
		"no provider":          {poolPlanError(&engine.PoolNoProviderError{}), wait(waitRollout)},
		"profile not offered":  {poolPlanError(&engine.ProfileUnavailableError{}), wait(waitRollout)},
		"profile differs":      {poolPlanError(&engine.ProfileMismatchError{}), wait(waitRollout)},
		"profile kind":         {poolPlanError(&engine.ProfileKindUnsupportedError{}), wait(waitRollout)},
		"pool not allowed":     {poolPlanError(&engine.PoolNotAllowedError{}), pause("POOL_NOT_ALLOWED")},
		"pin on two pools":     {poolPlanError(&engine.PinPoolMismatchError{}), pause("PIN_POOL_MISMATCH")},
		"secret not allowed":   {poolPlanError(&engine.SecretNotAllowedError{}), pause("SECRET_NOT_ALLOWED")},
		"too large a request":  {poolPlanError(&engine.PoolResourcesUnsatisfiableError{}), uncertain("RESOURCES_UNSATISFIABLE")},
		"no such profile":      {poolPlanError(&engine.DevicesNotAProfileError{}), uncertain("DEVICES_NOT_A_PROFILE")},
		"gpu with no profile":  {poolPlanError(&engine.GPUWithoutGPUProfileError{}), uncertain("GPU_WITHOUT_PROFILE")},
		"a profile nobody has": {poolPlanError(&engine.ProfileUnavailableError{Nowhere: true}), uncertain("PROFILE_UNAVAILABLE")},
		"a plan's other error": {poolPlanError(errors.New("orkflow has no tasks")), uncertain(codeUnclassified)},
		"a canceled plan":      {poolPlanError(context.Canceled), uncertain(codeUnclassified)},
		"not shared":           {fmt.Errorf("child: %w", &authz.NotSharedError{}), pause("ORKFLOW_NOT_SHARED")},
		"no reader wired":      {outage(fmt.Errorf("max_running: %w", errNoAdmissionReader)), uncertain(codeUnclassified)},
		"a pool's name":        {poolPlanError(fmt.Errorf("pool: %w", task.ErrInvalidPool)), pause("ORKFLOW_INVALID")},
		"audience":             {&identity.AudienceNotPermittedError{}, pause("IDENTITY_NOT_PERMITTED")},
		"subject":              {&identity.SubjectTooLongError{Err: errors.New("long")}, pause("IDENTITY_SUBJECT_TOO_LONG")},
		"grant":                {&identity.GrantRefusedError{Reason: "GRANT_NOT_DEFINED"}, pause("GRANT_NOT_DEFINED")},
		"image":                {&task.ImagePolicyRefusal{Code: task.ReasonImageNotPermitted}, pause("IMAGE_NOT_PERMITTED")},
		"foreign secrets":      {&engine.ForeignSecretsUnboundedError{}, pause("FOREIGN_SECRETS_UNBOUNDED")},
		"composed, missing":    {fmt.Errorf("asks: %w", consent.ErrChildMissing), pause("COMPOSED_ORKFLOW_UNUSABLE")},
		"composed, too deep":   {fmt.Errorf("asks: %w", consent.ErrTooDeep), pause("COMPOSED_ORKFLOW_UNUSABLE")},
		"signature unread":     {outage(&dsl.SignatureError{Code: "IMAGE_SIGNATURE_UNAVAILABLE", Err: errors.New("registry")}), wait(waitOutage)},
		"no such Orkflow":      {orkflowStoreError(artifacts.ErrNotFound, "o"), pause("ORKFLOW_NOT_FOUND")},
		"a store that is down": {orkflowStoreError(errors.New("connection refused"), "o"), wait(waitOutage)},
		"a store that refuses": {orkflowStoreError(fmt.Errorf("get: %w", s3Status(http.StatusForbidden)), "o"), uncertain(codeStoreRefused)},
		"a missing bucket":     {orkflowStoreError(fmt.Errorf("get: %w", s3Status(http.StatusNotFound)), "o"), uncertain(codeStoreRefused)},
		"a store's 503":        {orkflowStoreError(fmt.Errorf("get: %w", s3Status(http.StatusServiceUnavailable)), "o"), wait(waitOutage)},
		"a local store":        {outage(fmt.Errorf("open: %w", fs.ErrPermission)), uncertain(codeStoreRefused)},
		"a cluster's refusal":  {outage(serviceerror.NewPermissionDenied("no", "")), uncertain(codeClusterRefused)},
		"a cluster's outage":   {outage(serviceerror.NewUnavailable("later")), wait(waitOutage)},
		"our own deadline":     {outage(context.DeadlineExceeded), wait(waitTimeout)},
		"a bare deadline":      {fmt.Errorf("read: %w", context.DeadlineExceeded), wait(waitTimeout)},
		"marked":               {connect.NewError(connect.CodeInternal, refused(refusalPause, "X", errors.New("x"))), pause("X")},
		"a Connect code alone": {connect.NewError(connect.CodeUnavailable, errors.New("x")), uncertain(codeUnclassified)},
		"nothing known":        {errors.New("x"), uncertain(codeUnclassified)},
	} {
		if got := classifyRefusal(c.err); got != c.want {
			t.Errorf("%s: %+v; want %+v", name, got, c.want)
		}
		// Through every wrapper a refusal passes on its way to a caller.
		child := childRefusal(c.err, &orkstackv1.OrkflowRef{Name: "child", Version: "1"}, "t")
		if got := classifyRefusal(child); got != c.want {
			t.Errorf("%s, of a child: %+v; want %+v", name, got, c.want)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := classifyRefusal(callerGaveUp(ctx, c.err)); got != c.want {
			t.Errorf("%s, to a caller that left: %+v; want %+v", name, got, c.want)
		}
		if got := classifyRefusal(submitPolicyError(c.err)); got != c.want {
			t.Errorf("%s, through the policy's wrapper: %+v; want %+v", name, got, c.want)
		}
	}
	if classifyRefusal(nil) != (verdict{}) {
		t.Error("no error has a verdict")
	}
}

// A child's refusal is marked and keeps no chain: nothing of the child's
// field paths reaches the parent's answer, and its words are unchanged.
func TestChildRefusal_KeepsNoChain(t *testing.T) {
	inner := connect.NewError(connect.CodeInvalidArgument, &task.ImagePolicyRefusal{Code: task.ReasonImageNotPermitted, Task: "t", TaskIndex: 3})
	got := childRefusal(inner, &orkstackv1.OrkflowRef{Name: "child", Version: "1"}, "t")
	if _, ok := errors.AsType[*task.ImagePolicyRefusal](got); ok {
		t.Fatal("the child's refusal, with its task's index, reaches the parent's answer")
	}
	if connect.CodeOf(got) != connect.CodeInvalidArgument || !strings.HasSuffix(got.Error(), `(task "t"): `+inner.Message()) {
		t.Fatalf("%v", got)
	}
}

// unmarked are the refusals on prepare's path that carry no mark, by the
// function that makes them: how many, and why each needs none.
var unmarked = map[string]struct {
	n   int
	why string
}{
	// The error is typed, and passes through as it is.
	"poolPlanError":           {16, "each branch is a type the table knows, and the rest is uncertain"},
	"s.checkIdentityAtSubmit": {1, "identity.AudienceNotPermittedError"},
	"grantRefusal":            {1, "identity.GrantRefusedError"},
	"foreignSecretsRefusal":   {1, "engine.ForeignSecretsUnboundedError"},
	"s.refuseCode":            {1, "engine.AdmissionRefusedError"},
	"s.asksFor":               {1, "consent.ErrChildMissing or ErrTooDeep"},
	"submitPolicyError":       {1, "a policy refusal, with another code"},
	"s.checkImagePolicy": {4, "task.ImagePolicyRefusal twice (one with its field, one that cannot be); " +
		"an engine with no verifier, and a signer selection it cannot use, are its own faults"},
	// Uncertain: the engine's or the operator's fault, which no table names.
	"s.prepareWith":                 {3, "no Orkflow named, no starter wired, a fingerprint that does not encode"},
	"s.namespace":                   {2, "a name the engine's own record never holds; no registry wired"},
	"validNamespaceName":            {2, "as s.namespace"},
	"s.binding":                     {1, "this engine serves no such namespace"},
	"s.checkStoreScopedForIdentity": {1, "an engine wired with no Orker inventory"},
	"s.requireCapability":           {1, "an engine wired to no cluster"},
	"s.definitionFleetCheck":        {1, "an engine wired to no cluster"},
	"s.prepareAdmission":            {2, "an engine with no issuer, and one that does not enforce authentication"},
	"policyReadError":               {1, "a record that cannot be read until it is repaired"},
	"orkflowStoreError":             {1, "a version stored twice, which no read meets"},
	"s.claimCoordsOf":               {1, "an idempotency key, which a message's start has none of"},
	"unknownStart":                  {1, "a kind of start nobody names, which an event source's is not"},
}

// TestPrepare_EveryRefusalIsClassified: every Connect error made on the path
// of prepare is an internal fault, carries a mark or a typed error, or is
// left uncertain by name with a reason. A refusal added to the path without
// one fails here.
func TestPrepare_EveryRefusalIsClassified(t *testing.T) {
	files := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	// Functions by their name, and the Service's methods by "s." and theirs.
	decls := map[string]*ast.FuncDecl{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(files, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Recv == nil {
				decls[fn.Name.Name] = fn
			} else if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok && exprString(star.X) == "Service" {
				decls["s."+fn.Name.Name] = fn
			}
		}
	}
	reached, queue := map[string]bool{}, []string{"s.prepare"}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if reached[name] {
			continue
		}
		reached[name] = true
		ast.Inspect(decls[name].Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if callee := exprString(call.Fun); decls[callee] != nil {
					queue = append(queue, callee)
				}
			}
			return true
		})
	}
	found := map[string][]string{}
	for name := range reached {
		ast.Inspect(decls[name].Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if ok && len(call.Args) == 2 && exprString(call.Fun) == "connect.NewError" &&
				exprString(call.Args[0]) != "connect.CodeInternal" && !classifiedArg(call.Args[1]) {
				found[name] = append(found[name], files.Position(call.Pos()).String())
			}
			return true
		})
	}
	for name, at := range found {
		sort.Strings(at)
		if len(at) != unmarked[name].n {
			t.Errorf("%s refuses a run %d times without a class, at %s; %d are known (%s)", name, len(at), strings.Join(at, ", "), unmarked[name].n, unmarked[name].why)
		}
	}
	for name := range unmarked {
		if found[name] == nil {
			t.Errorf("unmarked names %s, which makes no unmarked refusal", name)
		}
	}
	if len(reached) < 60 || !reached["s.planChildren"] || !reached["checkRunPlanSize"] || !reached["s.lineChecks"] {
		t.Fatalf("the walk reached %d functions: it no longer follows prepare", len(reached))
	}
}

// classifiedArg reports an error argument whose class is given where it is
// made: a mark, or a typed error the classifier knows.
func classifiedArg(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.CallExpr:
		switch exprString(x.Fun) {
		case "refused", "outage", "unlessTyped":
			return true
		}
	case *ast.UnaryExpr:
		lit, ok := x.X.(*ast.CompositeLit)
		if !ok {
			return false
		}
		switch exprString(lit.Type) {
		case "validationFieldError":
			for _, e := range lit.Elts {
				if kv, ok := e.(*ast.KeyValueExpr); ok && exprString(kv.Key) == "Err" && classifiedArg(kv.Value) {
					return true
				}
			}
		case "busyRefusal", "limitReached", "checkUnavailable", "refusalMark":
			return true
		}
	}
	return false
}

func exprString(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprString(x.X) + "." + x.Sel.Name
	}
	return ""
}

// TestClassifyRefusal_KnowsEveryTypeOfTheSubmitPath: a refusal type added to
// the files the submit path takes its typed refusals from is in the
// classifier's table.
func TestClassifyRefusal_KnowsEveryTypeOfTheSubmitPath(t *testing.T) {
	known := map[string]bool{}
	for _, rule := range refusalTypes {
		known[rule.name] = true
	}
	for dir, names := range map[string][]string{
		"../engine":   {"admission.go", "pools.go", "secrets.go", "shared.go", "profile_submit.go", "identity_probe.go"},
		"../identity": {"audiences.go", "subjects.go", "grants.go"},
		"../task":     {"image_policy.go"},
	} {
		for _, name := range names {
			f, err := parser.ParseFile(token.NewFileSet(), dir+"/"+name, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range f.Decls {
				gen, ok := d.(*ast.GenDecl)
				if !ok || gen.Tok != token.TYPE {
					continue
				}
				for _, spec := range gen.Specs {
					typ := spec.(*ast.TypeSpec)
					if _, ok := typ.Type.(*ast.StructType); !ok {
						continue
					}
					n := typ.Name.Name
					if (strings.HasSuffix(n, "Error") || strings.HasSuffix(n, "Refusal")) && !known[f.Name.Name+"."+n] {
						t.Errorf("%s.%s, in %s, is a refusal the classifier does not know", f.Name.Name, n, name)
					}
				}
			}
		}
	}
	if len(refusalTypes) < 20 {
		t.Fatalf("the table has %d rules", len(refusalTypes))
	}
}

// TestClassifyRefusal_AKeyTheMessageBreaks: a concurrency limit's key that a
// message's values make invalid is that message's own, and rejects it alone.
func TestClassifyRefusal_AKeyTheMessageBreaks(t *testing.T) {
	s := newAccessService(t)
	wf := &orkstackv1.Orkflow{Name: "deploy", Concurrency: &orkstackv1.ConcurrencySpec{Key: "{{ inputs.region }}", Limit: 1}}
	err := s.planConcurrency(context.Background(), &orkstackv1.Namespace{Name: "default"}, wf, &engine.RunPlan{History: &engine.HistoryLimit{}},
		&orkstackv1.SubmitRunRequest{}, &orkstackv1.RunOrigin{}, claimCoords{}, nil)
	if got := classifyRefusal(err); got != (verdict{class: refusalReject, code: eventsources.ReasonMessageConcurrencyKeyInvalid}) {
		t.Fatalf("%+v of %v", got, err)
	}
}
