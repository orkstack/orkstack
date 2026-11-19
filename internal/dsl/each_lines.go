package dsl

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eachlines"
)

// inputsProducer is the producer E87 gives a run's input artifacts. A fan-out
// over one is refused in this release; the refusal says so, because the
// `_inputs` branch lifts it once E87 lands.
const inputsProducer = "_inputs"

// batchSliceSentence names the file a batched item reads its lines from, for
// the refusals of `as:` and of an .each key in a batched fan-out.
const batchSliceSentence = "/orkstack/input/each/lines.jsonl (format: jsonl) or " +
	"/orkstack/input/each/lines.txt (format: lines)"

// ParseOverArtifact splits `each.over_artifact` into its producer and its
// artifact name, by the grammar of the artifact "<producer>/<name>" helper:
// split on the first "/", both halves non-empty. The producer is a task id
// (the slug rule; a "_"-prefixed producer is reserved, E87 §15, and "_inputs"
// is refused in this release). The name follows the output-artifact name rule
// — an Orker writes it as one directory entry of /orkstack/output/artifacts —
// so it is valid UTF-8 and holds no "/" and no NUL, and is not "." or "..".
// Whether the producer is a task the fan-out needs is validateEach's question.
// Pure.
func ParseOverArtifact(s string) (producer, name string, err error) {
	refuse := func(why string) error {
		return fmt.Errorf("each.over_artifact %q is not \"<task>/<name>\": %s", s, why)
	}
	if s == "" {
		return "", "", refuse("it is empty")
	}
	if !utf8.ValidString(s) {
		return "", "", refuse("it is not valid UTF-8")
	}
	producer, name, found := strings.Cut(s, "/")
	switch {
	case !found:
		return "", "", refuse("it has no \"/\" between the task and the artifact's name")
	case producer == "":
		return "", "", refuse("it names no task before the \"/\"")
	case name == "":
		return "", "", refuse("it names no artifact after the \"/\"")
	case producer == inputsProducer:
		return "", "", refuse("a fan-out over a run input artifact is not supported in this release")
	case strings.HasPrefix(producer, "_"):
		return "", "", refuse(fmt.Sprintf("%q is reserved; the producer is a task id", producer))
	case !slugPattern.MatchString(producer) || len(producer) > MaxSlug:
		return "", "", refuse(fmt.Sprintf("%q is not a task id (%s, at most %d bytes)",
			producer, slugPattern, MaxSlug))
	case strings.Contains(name, "/"):
		return "", "", refuse(fmt.Sprintf("the artifact name %q holds a \"/\"; an artifact is one "+
			"file of /orkstack/output/artifacts, never a path", name))
	case strings.ContainsRune(name, 0):
		return "", "", refuse("the artifact name holds a NUL byte")
	case name == "." || name == "..":
		return "", "", refuse(fmt.Sprintf("the artifact name %q is not a file name", name))
	}
	return producer, name, nil
}

// MaxEachBatch is the largest `each.batch`: lines per item.
const MaxEachBatch = eachlines.MaxBatch

// eachLineFormatWords are the words `each.format` takes.
var eachLineFormatWords = map[string]orkstackv1.EachLineFormat{
	"jsonl": orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL,
	"lines": orkstackv1.EachLineFormat_EACH_LINE_FORMAT_LINES,
}

// EachLineFormatWord is the word a format is written as, "" for none.
func EachLineFormatWord(f orkstackv1.EachLineFormat) string {
	for word, value := range eachLineFormatWords {
		if value == f {
			return word
		}
	}
	return ""
}

// IsEachLines reports whether a fan-out runs over an artifact's lines.
func IsEachLines(spec *orkstackv1.EachSpec) bool {
	return spec.GetOverArtifact() != "" || spec.GetSource().GetArtifact() != nil
}

// validateEachLines is rule 53, a fan-out over an artifact's lines, on the
// compiled message: exactly one of over and over_artifact; the source is
// compiled from over_artifact; a format; a batch of 0 or 2 … MaxEachBatch, and
// with one, the default item name; the producer a task in the fan-out's own
// needs that writes artifacts (not a fan-out, an awaited, sleeping or
// composed task); and no template of the fan-out reads the source, which is
// never staged.
func validateEachLines(task *orkstackv1.Task, taskIndex int, byID map[string]*orkstackv1.Task) error {
	spec := task.GetEach()
	id := task.GetId()
	at := func(field string) string { return fmt.Sprintf("tasks[%d].%s", taskIndex, field) }
	if spec.GetOver() != "" {
		return validationError(at("each.over"), fmt.Errorf(
			"task %q: each.over and each.over_artifact are both set; a fan-out runs over an array or over "+
				"an artifact's lines, so name exactly one", id))
	}
	producer, name, err := ParseOverArtifact(spec.GetOverArtifact())
	if err != nil {
		return validationError(at("each.over_artifact"), fmt.Errorf("task %q: %w", id, err))
	}
	want := &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: producer, Name: name}}
	if !proto.Equal(spec.GetSource(), want) {
		return validationError(at("each.source"), fmt.Errorf(
			"task %q: each.source does not match each.over_artifact %q — the source is compiled from "+
				"over_artifact and must agree with it", id, spec.GetOverArtifact()))
	}
	if spec.GetFormat() != orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL &&
		spec.GetFormat() != orkstackv1.EachLineFormat_EACH_LINE_FORMAT_LINES {
		return validationError(at("each.format"), fmt.Errorf(
			"task %q: each.format is required with each.over_artifact: jsonl (each line one JSON value) "+
				"or lines (each line a string)", id))
	}
	switch b := spec.GetBatch(); {
	case b == 1:
		return validationError(at("each.batch"), fmt.Errorf(
			"task %q: each.batch 1 is compiled as no batch; leave it unset", id))
	case b < 0 || b > MaxEachBatch:
		return validationError(at("each.batch"), fmt.Errorf(
			"task %q: each.batch %d is out of range (1 … %d)", id, b, MaxEachBatch))
	case b >= 2 && spec.GetAs() != DefaultEachAs:
		return validationError(at("each.as"), fmt.Errorf(
			"task %q: each.as is refused with batch: %d — a batch has no single value; read %s",
			id, b, batchSliceSentence))
	}
	upstream, exists := byID[producer]
	if !exists {
		return validationError(at("each.over_artifact"), fmt.Errorf(
			"task %q: each.over_artifact names unknown task %q", id, producer))
	}
	if !slices.Contains(task.GetNeeds(), producer) {
		return validationError(at("each.over_artifact"), fmt.Errorf(
			"task %q: each.over_artifact names %q, which is not in its needs: — the producer of the lines "+
				"must be a task this fan-out needs directly", id, producer))
	}
	switch {
	case upstream.GetEach() != nil:
		return validationError(at("each.over_artifact"), fmt.Errorf(
			"task %q: each.over_artifact: %q is a fan-out; a fan-out cannot fan out over another fan-out's "+
				"items (no nesting)", id, producer))
	case upstream.GetAwait() != nil:
		return validationError(at("each.over_artifact"), fmt.Errorf(
			"task %q: each.over_artifact: %q is an awaited task and produces no artifacts", id, producer))
	case upstream.GetSleep() != nil:
		return validationError(at("each.over_artifact"), sleepReadRefusal(id, "each.over_artifact", producer))
	case IsComposedTask(upstream):
		return validationError(at("each.over_artifact"), fmt.Errorf(
			"task %q: each.over_artifact: %q is a composed task; a fan-out over a child run's artifact is "+
				"not supported in this release", id, producer))
	}
	reads, err := taskReadsIn("with", task.GetWith().AsMap())
	if err != nil {
		return nil // the template rules report a template that does not parse
	}
	for _, read := range reads {
		if read.Kind == TaskReadArtifact && read.TaskID == producer && read.Name == name {
			return validationError(at(read.Path), fmt.Errorf(
				"task %q: %s reads artifact %q, the fan-out's own source — the source is never staged; "+
					"each item receives its own line (.each.%s) or batch", id, read.Path,
				producer+"/"+name, spec.GetAs()))
		}
	}
	return nil
}

// EachSourceLabel is what a fan-out runs over, in the words every surface
// shows after "each over": its `over` template for an array, "the lines of
// list/files.jsonl" for lines, "batches of 500 lines of list/urls.jsonl" for
// batches.
func EachSourceLabel(spec *orkstackv1.EachSpec) string {
	if !IsEachLines(spec) {
		return spec.GetOver()
	}
	if b := spec.GetBatch(); b >= 2 {
		return fmt.Sprintf("batches of %d lines of %s", b, spec.GetOverArtifact())
	}
	return "the lines of " + spec.GetOverArtifact()
}
