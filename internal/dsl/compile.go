package dsl

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// DefaultTimeoutSeconds is applied when a task omits timeout_seconds.
const DefaultTimeoutSeconds = 300

// slugPattern is the identifier rule for namespaces, orkflow names, task ids
// and pools (docs/DSL.md validation rules 1, 2, 3 and 7). "default" and "main"
// are ordinary slugs, so the built-in namespace and pool need no special case.
var slugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// MaxSlug bounds every identifier the DSL accepts, and it is half of the slug
// rule, not a detail of it.
//
// A name is written here and stored somewhere else: a namespace, an orkflow
// name and a version are path components of an object key
// (pkg/artifacts.maxOrkflowComponent, the same 200), and a pool becomes an
// execution channel name (internal/task.MaxPoolName, also 200). Without the
// bound here the pattern alone let a 201-character namespace compile and then
// fail at the registry with "exceeds 200 bytes" — a name a document can
// reference and nothing can store, which is the same "one rule, two answers"
// defect the pattern itself closed, measured in the one class the corpus had
// no member of. The two bounds are not one constant, and the reason is
// layering rather than the compiler: the artifact store is a public package
// and this one is not, so although the store imports this package for the
// rules both must share (the slug rule, the document identity of
// VersionedSource), it shares no slug constant with it. They are held
// together by measurement instead —
// TestNamespaceName_RegistryAndCompilerAgree and
// TestNamespacePools_AreExactlyThePoolsDispatchCanReach both carry a name at
// the bound and a name one byte over.
const MaxSlug = 200

// checkSlug reports why value cannot be used as an identifier, naming the kind
// of identifier so the message says which field is wrong.
func checkSlug(kind, value string) error {
	if len(value) > MaxSlug {
		return fmt.Errorf("%s of %d bytes exceeds the %d-byte limit (%.32q…)",
			kind, len(value), MaxSlug, value)
	}
	if !slugPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a slug (%s)", kind, value, slugPattern.String())
	}
	return nil
}

// SlugPattern is the slug rule's pattern, for a schema that states it.
func SlugPattern() string { return slugPattern.String() }

// ValidSlug reports why value cannot be used as an identifier of the named
// kind, so a caller outside this package can hold a client-supplied name to
// the SAME rule the compiler applies.
//
// AGENTS.md §7 asks for one rule at every boundary, and this is how a boundary
// that does not compile YAML still gets it. It is checkSlug under an exported
// name rather than a second pattern, because a second pattern is precisely how
// two boundaries start disagreeing about the same name.
func ValidSlug(kind, value string) error { return checkSlug(kind, value) }

// Options tunes compilation.
//
// Compile stays PURE: no network, no clock, no environment (§4.3). A catalog
// is configuration, so it arrives as an argument rather than being read from
// the process — which is also what keeps every hermetic compiler test, and
// TestCompile_AcceptsEveryShippedExample, independent of how this binary
// happens to be configured.
type Options struct {
	// Catalog is the deployment's activity table (§4.2). Nil selects
	// BuiltinActivityCatalog, which is what the shipped examples compile
	// against.
	Catalog Catalog
}

// catalog is Options.Catalog with its documented default applied.
func (o Options) catalog() Catalog {
	if o.Catalog == nil {
		return BuiltinActivityCatalog()
	}
	return o.Catalog
}

// document is the YAML shape of a *.ork.yaml file.
type document struct {
	// APIVersion and Kind are the document header (docs/DSL.md, "The
	// document header"). checkHeader reads them before this decode; they are
	// decoded so KnownFields accepts them, and never compiled: the header is
	// not part of the definition, so neither the compiled message nor its
	// digest sees it.
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`

	// Namespace scopes the orkflow. Absent means the built-in "default"
	// namespace, and it stays empty in the compiled message: "empty means
	// default" is one rule, resolved once, where the name becomes a key.
	Namespace   string            `yaml:"namespace"`
	Name        string            `yaml:"name"`
	Version     string            `yaml:"version"`
	Description string            `yaml:"description"`
	Labels      map[string]string `yaml:"labels"`
	// Inputs is a data position (docs/JSON.md, "Data written in an
	// authored document"): a yaml.Node VALUE, so it holds exactly what was
	// written — tags, anchors and aliases included — for the strict mapping
	// (freeForm) to judge. A pointer would not: yaml.v3 decodes into a
	// *yaml.Node as an ordinary struct.
	Inputs yaml.Node `yaml:"inputs"`

	// Outputs is the Orkflow's declared return contract: output name to a
	// template over .inputs and .tasks.<id>.output, rendered once by the
	// engine when the run succeeds (docs/DSL.md "Outputs"). Absent and `{}`
	// are one value: no declared outputs.
	Outputs map[string]string `yaml:"outputs"`

	// Pins declares every pin a task may name, by name, each with a
	// (possibly empty) options object — `pins: {build: {}}` is legal.
	//
	// The declaration is what makes a pin a REFERENCE rather than a name that
	// creates itself: a task's `pin:` must be a key of this map, so `pin:
	// buidl` is refused instead of silently opening a second, one-task pin
	// (§4.2). It is also where the pin's own option lives, because a lifetime
	// is a property of the group and not of any one member — asking which
	// task's value wins is the shape of question the compiler exists to
	// refuse.
	Pins  map[string]pinDocument `yaml:"pins"`
	Tasks []taskDocument         `yaml:"tasks"`
}

type taskDocument struct {
	ID             string         `yaml:"id"`
	Needs          []string       `yaml:"needs"`
	Condition      string         `yaml:"condition"`
	TimeoutSeconds *int32         `yaml:"timeout_seconds"`
	Retry          *retryDocument `yaml:"retry"`
	// Pool is the set of Orkers this task runs on. Absent means the
	// namespace's default pool, then the built-in pool "main".
	Pool string `yaml:"pool"`

	// Pin is the name of the declared pin (document.Pins) this task joins, or
	// empty. Where Pool says WHICH ORKERS a task may run on, Pin says which
	// ONE member of that set — for this named group of tasks, together, with
	// one read-write /orkstack/shared for the life of the group (§1, §4.2).
	//
	// A bare name, exactly like Pool: the group's own options live on the
	// declaration, not here. And the word is "pin", not "session": the
	// Temporal Worker Session that implements it is internal, precisely as a
	// queue is internal to a pool (AGENTS.md §7.1).
	Pin string `yaml:"pin"`

	// With is a data position, a yaml.Node value like document.Inputs.
	With yaml.Node `yaml:"with"`

	// Uses is the only image-selecting field in the DSL: a catalog activity
	// name ("shell") or a registry reference ("ghcr.io/acme/migrate:v3",
	// "repo@sha256:..."). A task names an image, never a Go type compiled
	// into an Orker (§4).
	//
	// It must NOT be a template. Resolution happens at APPLY, and a `uses:`
	// selected from `{{ .inputs.image }}` cannot be pinned to a digest at
	// apply time — which is the property the whole commit protocol rests on.
	Uses string `yaml:"uses"`

	// Command overrides the image ENTRYPOINT/CMD. It is argv, never a shell
	// string: an array is what makes it impossible for a rendered value to
	// become code (§10.2 rule 8).
	Command []string `yaml:"command"`

	// Env is workload-supplied environment. The platform puts NOTHING here:
	// required platform state travels as files (AGENTS.md §3).
	Env map[string]string `yaml:"env"`

	// Secrets name files the Orker resolves and mounts read-only under
	// /orkstack/input/secrets. The VALUE never appears in this document.
	Secrets []secretDocument `yaml:"secrets"`

	// Resources is what the task asks the Orker for. Orker policy may cap it;
	// it may never raise it (§9.3).
	Resources *resourceDocument `yaml:"resources"`

	// Network is "default" or "none". Named Orker-defined profiles are
	// deliberately absent (AGENTS.md §6).
	Network string `yaml:"network"`

	// CancellationGracePeriodSeconds is the SIGTERM-to-SIGKILL window.
	CancellationGracePeriodSeconds *int32 `yaml:"cancellation_grace_period_seconds"`

	// RunAsOrkerUser is the §3.1 readability escape hatch, per task. It
	// overrides the image's own USER so the Orker owns every file the
	// workload writes, and it is opt-in because that breaks images which
	// need their baked-in identity.
	RunAsOrkerUser bool `yaml:"run_as_orker_user"`

	// Await makes this an awaited task: no image, no container, no pool —
	// the run waits until the task is resolved from outside, or its deadline
	// passes. Mutually exclusive with Uses and with every execution field.
	Await *awaitDocument `yaml:"await"`

	// Each makes this task a fan-out: one node that runs as one item per
	// element of the array `over` names (docs/DSL.md "Fan-out").
	Each *eachDocument `yaml:"each"`

	// Cache opts this task into memoization across runs (docs/DSL.md
	// "Caching"). KnownFields(true) applies inside it.
	Cache *cacheDocument `yaml:"cache"`

	// Join is when this task may start, decided from how its needs ended:
	// all_succeeded (the default), none_failed, any_failed or all_done
	// (docs/DSL.md "Joins and failure handlers"). Every task kind may carry
	// it — container, awaited and composed (rules 15 and 27 extended).
	Join string `yaml:"join"`
}

// secretDocument is one secret's name and where the Orker gets it.
type secretDocument struct {
	Name   string `yaml:"name"`
	Source string `yaml:"source"`
}

// resourceDocument is the Kubernetes spelling of a resource request.
type resourceDocument struct {
	CPU    string `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

// pinDocument is one declared pin's options.
//
// TimeoutSeconds is a pointer for the same reason taskDocument's is: so
// "absent" and "written" are two states here, at the YAML layer. It bounds the
// whole group's life, from the dispatch of its first task to the end of its
// last. Compiled, 0 means DERIVED — the sum of every member's own wall-clock
// budget, which is the sequential worst case (§6.3) — and that is why the
// pointer does not survive compilation: Pin.timeout_seconds is a plain int32
// where 0 and absent are one value, deliberately, because they mean the same
// thing to the engine.
type pinDocument struct {
	TimeoutSeconds *int32 `yaml:"timeout_seconds"`
}

type retryDocument struct {
	MaximumAttempts        int32   `yaml:"maximum_attempts"`
	InitialIntervalSeconds int32   `yaml:"initial_interval_seconds"`
	BackoffCoefficient     float64 `yaml:"backoff_coefficient"`
	MaximumIntervalSeconds int32   `yaml:"maximum_interval_seconds"`
}

// CompileFile reads path and compiles it into an orkstack.v1.Orkflow.
func CompileFile(path string) (*orkstackv1.Orkflow, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("dsl: read %s: %w", path, err)
	}
	wf, err := Compile(source)
	if err != nil {
		return nil, fmt.Errorf("dsl: %s: %w", path, err)
	}
	return wf, nil
}

// DocumentPinsVersion reports whether the document names its own `version:`.
//
// It exists because §4.4(b) moves the DERIVATION of an unpinned version out of
// Compile and into the apply path, after image resolution — and the applier
// therefore has to tell "the author pinned 1.2.0" from "Compile derived this
// from the source". It cannot read that off the compiled message, because both
// arrive as a non-empty Version.
//
// The alternative was for Compile to leave Version empty and let every caller
// derive it, which would mean `orkctl orkflow validate` and every hermetic
// test either duplicating the derivation or reporting a versionless orkflow.
// One small parse here is cheaper than that.
func DocumentPinsVersion(source []byte) (bool, error) {
	var doc struct {
		Version string `yaml:"version"`
	}
	if err := yaml.Unmarshal(source, &doc); err != nil {
		return false, fmt.Errorf("parse yaml: %w", err)
	}
	return doc.Version != "", nil
}

// Compile parses, validates, and compiles a DSL document. The returned
// Orkflow carries the original source bytes in SourceYaml; when the document
// does not pin `version`, Version is the sha256 of its versioned source (those
// bytes less the two header lines written first), so a version always names
// the source it is stored with (sourceHash).
func Compile(source []byte) (*orkstackv1.Orkflow, error) {
	return CompileWithOptions(source, Options{})
}

// CompileWithOptions is Compile with explicit options.
func CompileWithOptions(source []byte, opts Options) (*orkstackv1.Orkflow, error) {
	// The header first: a document of another kind, or none, is refused by
	// what it is, before a strict decode reports its keys as unknown.
	if err := checkHeader(source); err != nil {
		return nil, err
	}
	// Strict: a key the schema does not define is refused, never ignored.
	// yaml.v3 would otherwise drop what it cannot map, and an author would
	// find out about a misspelled or unsupported key only when the run
	// behaved differently.
	var doc document
	decoder := yaml.NewDecoder(bytes.NewReader(source))
	decoder.KnownFields(true)
	if err := decoder.Decode(&doc); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	wf := &orkstackv1.Orkflow{
		Namespace:   doc.Namespace,
		Name:        doc.Name,
		Version:     doc.Version,
		Description: doc.Description,
		Labels:      doc.Labels,
		SourceYaml:  string(source),
	}

	inputs, err := freeForm("inputs", "inputs", &doc.Inputs)
	if err != nil {
		return nil, err
	}
	wf.Inputs = inputs
	if len(doc.Outputs) > 0 {
		wf.Outputs = doc.Outputs
	}

	// Pins compile SORTED BY NAME, and a document that declares none leaves
	// the field absent rather than empty-but-allocated.
	//
	// Sorted because a Go map has no order to preserve: ranging doc.Pins
	// directly would let one document compile to two different messages, and
	// the message is what both the definition digest (canonicalPins) and the
	// store's "is this apply a repeat?" comparison read (proto.Equal in
	// pkg/artifacts.SameStoredOrkflow). Two authors who type the same two pin
	// keys in either order must get the same bytes; so must one author twice.
	// Absent rather than empty for the same reason normalizeMap is careful
	// about presence: an unpinned document's message must be exactly what it
	// was before pins existed.
	if len(doc.Pins) > 0 {
		names := make([]string, 0, len(doc.Pins))
		for name := range doc.Pins {
			names = append(names, name)
		}
		sort.Strings(names)
		wf.Pins = make([]*orkstackv1.Pin, 0, len(names))
		for _, name := range names {
			pin := &orkstackv1.Pin{Name: name}
			if seconds := doc.Pins[name].TimeoutSeconds; seconds != nil {
				pin.TimeoutSeconds = *seconds
			}
			wf.Pins = append(wf.Pins, pin)
		}
	}

	for taskIndex, td := range doc.Tasks {
		// A composed task is built on its own path, first: it refuses, by
		// name, every execution key its author wrote (rule 27 at the YAML
		// layer — `await:` among them) and keeps TimeoutSeconds 0.
		if strings.HasPrefix(td.Uses, OrkflowUsesPrefix) {
			task, err := compileComposedTask(taskIndex, td, doc.Namespace)
			if err != nil {
				return nil, err
			}
			wf.Tasks = append(wf.Tasks, task)
			continue
		}
		// An awaited task is built on its own path: it refuses, by name, every
		// execution key its author wrote (rule 15 at the YAML layer) and keeps
		// TimeoutSeconds 0 rather than the container default.
		if td.Await != nil {
			task, err := compileAwaitTask(taskIndex, td)
			if err != nil {
				return nil, err
			}
			wf.Tasks = append(wf.Tasks, task)
			continue
		}
		each, err := compileEach(taskIndex, td)
		if err != nil {
			return nil, err
		}
		cache, err := compileCache(taskIndex, td)
		if err != nil {
			return nil, err
		}
		join, err := compileJoin(taskIndex, td)
		if err != nil {
			return nil, err
		}
		task := &orkstackv1.Task{
			Join:           join,
			Each:           each,
			Cache:          cache,
			Id:             td.ID,
			Uses:           td.Uses,
			Command:        td.Command,
			Env:            td.Env,
			Network:        networkModeOf(td.Network),
			RunAsOrkerUser: td.RunAsOrkerUser,
			Needs:          td.Needs,
			Condition:      td.Condition,
			TimeoutSeconds: DefaultTimeoutSeconds,
			Pool:           td.Pool,
			Pin:            td.Pin,
		}
		if td.TimeoutSeconds != nil {
			task.TimeoutSeconds = *td.TimeoutSeconds
		}
		if td.Retry != nil {
			task.Retry = &orkstackv1.RetryPolicy{
				MaximumAttempts:        td.Retry.MaximumAttempts,
				InitialIntervalSeconds: td.Retry.InitialIntervalSeconds,
				BackoffCoefficient:     td.Retry.BackoffCoefficient,
				MaximumIntervalSeconds: td.Retry.MaximumIntervalSeconds,
			}
		}
		if td.CancellationGracePeriodSeconds != nil {
			task.CancellationGracePeriodSeconds = *td.CancellationGracePeriodSeconds
		}
		if td.Resources != nil {
			task.Resources = &orkstackv1.ResourceRequest{
				Cpu:    td.Resources.CPU,
				Memory: td.Resources.Memory,
			}
		}
		for _, sd := range td.Secrets {
			task.Secrets = append(task.Secrets, &orkstackv1.SecretRef{
				Name:   sd.Name,
				Source: sd.Source,
			})
		}
		with, err := freeForm("with", fmt.Sprintf("tasks[%d].with", taskIndex), &td.With)
		if err != nil {
			return nil, err
		}
		task.With = with
		wf.Tasks = append(wf.Tasks, task)
	}

	if err := validate(wf, opts); err != nil {
		return nil, err
	}

	if wf.GetVersion() == "" {
		wf.Version = sourceHash(source)
	}
	return wf, nil
}

// networkWords are the DSL's network modes (AGENTS.md §6: only these two).
var networkWords = []string{"default", "none"}

// networkModes maps each of networkWords to its enum.
var networkModes = map[string]orkstackv1.NetworkMode{
	"default": orkstackv1.NetworkMode_NETWORK_MODE_DEFAULT,
	"none":    orkstackv1.NetworkMode_NETWORK_MODE_NONE,
}

// networkModeOf maps the DSL's word to the enum. An unrecognized value is
// carried as UNSPECIFIED and refused by validate, so the error names the task
// and the value rather than silently selecting a default.
func networkModeOf(raw string) orkstackv1.NetworkMode {
	if raw == "" {
		return orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED
	}
	if mode, ok := networkModes[raw]; ok {
		return mode
	}
	// A sentinel the validator recognizes. Returning DEFAULT here would
	// run a typo'd `network: nonw` with full network access.
	return orkstackv1.NetworkMode(-1)
}
