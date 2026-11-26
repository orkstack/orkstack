package eventsources

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// SpecFormat is the one format of a service spec and a judging spec this
// release writes and reads (03 §3.1). A reader that meets another, or a
// field it does not know, does not use the object: it is a newer release's.
const SpecFormat = 1

// ErrSpecUnreadable is a spec this release cannot read: another format, an
// unknown field, or bytes that do not parse. It is waited for, never read
// with the unknown part left out.
var ErrSpecUnreadable = errors.New("the spec is not one this release reads")

// The service's own timing, in seconds (06 §3, 14 §3.2): every bound the
// Orker enforces and every deadline it keeps, so that a container never
// discovers one by failing.
const (
	DefaultStatusSeconds   = 30
	DefaultStartupSeconds  = 60
	DefaultStopSeconds     = 20
	DefaultAliveSeconds    = 10
	DefaultOrphanSeconds   = 60
	DefaultPollWaitSeconds = 20
	DefaultSettleSeconds   = 30
	DefaultParkSeconds     = 300
	oneTrySeconds          = 300
	answerSlackSeconds     = 10
	holdSlackSeconds       = 60
)

// The most a spec's numbers may be: an Orker keeps every deadline it is given, so one
// nobody would write is not read.
const (
	MaxStopSeconds = 30
	maxStepSeconds = 3600
	maxHoldSeconds = 24 * 3600
)

// HoldSeconds is the bound within which a message in hand gets its
// verdict, from its hand-over (06 §6): one try (5 minutes, fixed), the
// park, one more park for a refused receipt, and a minute.
func HoldSeconds(parkSeconds int) int {
	if parkSeconds <= 0 {
		parkSeconds = DefaultParkSeconds
	}
	return oneTrySeconds + 2*parkSeconds + holdSlackSeconds
}

// ParkSeconds is the park of a spec whose hold bound is holdSeconds.
func ParkSeconds(holdSeconds int) int { return max((holdSeconds-oneTrySeconds-holdSlackSeconds)/2, 1) }

// SpecLimits are a service spec's numbers, written into the container's
// service.json limits as they are.
type SpecLimits struct {
	StatusSeconds      int   `json:"statusSeconds"`
	StartupSeconds     int   `json:"startupSeconds"`
	StopSeconds        int   `json:"stopSeconds"`
	AliveSeconds       int   `json:"aliveSeconds"`
	OrphanSeconds      int   `json:"orphanSeconds"`
	MaxBodyBytes       int64 `json:"maxBodyBytes"`
	MaxAttributesBytes int   `json:"maxAttributesBytes"`
	PollWaitSeconds    int   `json:"pollWaitSeconds"`
	AnswerSeconds      int   `json:"answerSeconds"`
	SettleSeconds      int   `json:"settleSeconds"`
	HoldSeconds        int   `json:"holdSeconds"`
}

// SpecSource names the source a spec is of. The deployment-wide key
// (deployment/namespace/name) is the Orker's to form: it knows its
// deployment's name, and a spec is the same in every deployment that holds
// the same source.
type SpecSource struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// SpecSecret is one declared secret: its name and its source reference,
// never a value.
type SpecSecret struct {
	Name   string `json:"name"`
	Source string `json:"source"`
}

// SpecResources is a source's per-instance request.
type SpecResources struct {
	CPU    string `json:"cpu,omitzero"`
	Memory string `json:"memory,omitzero"`
}

// ServiceSpec is what a source's containers are started with, one per
// service revision (03 §3.1): stored once, immutable, named by the source's
// id and its digest.
type ServiceSpec struct {
	Format      int               `json:"format"`
	Source      SpecSource        `json:"eventSource"`
	Image       string            `json:"image"`
	Contracts   []int32           `json:"contracts"`
	Profiles    []int32           `json:"profiles"`
	Pool        string            `json:"pool"`
	Concurrency int32             `json:"concurrency"`
	Delivery    string            `json:"delivery"`
	With        jsontext.Value    `json:"with,omitzero"`
	Env         map[string]string `json:"env,omitzero"`
	Secrets     []SpecSecret      `json:"secrets,omitzero"`
	Resources   SpecResources     `json:"resources"`
	Limits      SpecLimits        `json:"limits"`
}

// DeliveryWord is a delivery class as the contract's files spell it.
func DeliveryWord(d orkstackv1.DeliveryClass) string {
	if d == orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE {
		return "at-most-once"
	}
	return "at-least-once"
}

// BuildServiceSpec is src's service spec, from the source read with its
// defaults, the image as it was resolved, and the park the deployment
// configured (0 means DefaultParkSeconds). It returns the canonical bytes
// and their SHA-256, which names the spec.
func BuildServiceSpec(src *orkstackv1.EventSource, img Image, l Limits, parkSeconds int) (ServiceSpec, []byte, string, error) {
	e := Effective(src, l)
	spec := ServiceSpec{
		Format:      SpecFormat,
		Source:      SpecSource{ID: e.GetId(), Namespace: e.GetNamespace(), Name: e.GetName()},
		Image:       e.GetImage(),
		Contracts:   append([]int32{}, img.Contracts...),
		Profiles:    append([]int32{}, img.Profiles...),
		Pool:        e.GetPool(),
		Concurrency: e.GetConcurrency(),
		Delivery:    DeliveryWord(e.GetDelivery()),
		Env:         e.GetEnv(),
		Resources:   SpecResources{CPU: e.GetResources().GetCpu(), Memory: e.GetResources().GetMemory()},
		Limits: SpecLimits{
			StatusSeconds: DefaultStatusSeconds, StartupSeconds: DefaultStartupSeconds, StopSeconds: DefaultStopSeconds,
			AliveSeconds: DefaultAliveSeconds, OrphanSeconds: DefaultOrphanSeconds,
			MaxBodyBytes: e.GetMaxBytes(), MaxAttributesBytes: MaxAttributesBytes,
			PollWaitSeconds: DefaultPollWaitSeconds, AnswerSeconds: DefaultPollWaitSeconds + answerSlackSeconds,
			SettleSeconds: DefaultSettleSeconds, HoldSeconds: HoldSeconds(parkSeconds),
		},
	}
	for _, sec := range e.GetSecrets() {
		spec.Secrets = append(spec.Secrets, SpecSecret{Name: sec.GetName(), Source: sec.GetSource()})
	}
	if w := e.GetWith(); w != nil {
		raw, err := protojson.Marshal(w)
		if err != nil {
			return ServiceSpec{}, nil, "", fmt.Errorf("encode the source's with: %w", err)
		}
		var compact bytes.Buffer
		if err := stablejson.Compact(&compact, raw); err != nil {
			return ServiceSpec{}, nil, "", fmt.Errorf("encode the source's with: %w", err)
		}
		spec.With = jsontext.Value(compact.Bytes())
	}
	body, digest, err := encodeSpec(spec)
	if err != nil {
		return ServiceSpec{}, nil, "", err
	}
	return spec, body, digest, nil
}

// ReadServiceSpec reads stored service spec bytes strictly: SpecFormat, and
// no field this release does not know.
func ReadServiceSpec(body []byte) (ServiceSpec, error) {
	var spec ServiceSpec
	if err := readSpec(body, &spec); err != nil {
		return ServiceSpec{}, err
	}
	if spec.Format != SpecFormat {
		return ServiceSpec{}, fmt.Errorf("%w: format %d; this release reads %d", ErrSpecUnreadable, spec.Format, SpecFormat)
	}
	if !ValidID(spec.Source.ID) || !ValidName(spec.Source.Name) {
		return ServiceSpec{}, fmt.Errorf("%w: it names no valid source", ErrSpecUnreadable)
	}
	if spec.Image == "" || len(spec.Contracts) == 0 || len(spec.Profiles) == 0 ||
		(spec.Delivery != DeliveryWord(orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE) &&
			spec.Delivery != DeliveryWord(orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE)) {
		return ServiceSpec{}, fmt.Errorf("%w: no image, contracts, profiles or delivery class", ErrSpecUnreadable)
	}
	l := spec.Limits
	if spec.Concurrency < 1 || spec.Concurrency > MaxConcurrency || l.StatusSeconds < 1 || l.StartupSeconds < 1 || l.StopSeconds < 1 ||
		l.AliveSeconds < 1 || l.OrphanSeconds < 1 || l.MaxBodyBytes < 1 || l.MaxAttributesBytes < 1 || l.PollWaitSeconds < 0 ||
		l.AnswerSeconds < 1 || l.SettleSeconds < 1 || l.HoldSeconds < 1 || l.StopSeconds > MaxStopSeconds || l.HoldSeconds > maxHoldSeconds ||
		l.MaxBodyBytes > MaxMessageBytesCeiling || l.MaxAttributesBytes > MaxAttributesBytes ||
		max(l.StatusSeconds, l.StartupSeconds, l.AliveSeconds, l.OrphanSeconds, l.PollWaitSeconds, l.AnswerSeconds, l.SettleSeconds) > maxStepSeconds {
		return ServiceSpec{}, fmt.Errorf("%w: a lane count or a limit out of range", ErrSpecUnreadable)
	}
	return spec, nil
}

// JudgingSpec is how a source's messages are judged at one judging
// revision (03 §3.1): the source's identity and its judging fields, and
// nothing a container is started with. The Orker judges by it without a
// restart.
type JudgingSpec struct {
	Format   int            `json:"format"`
	Revision int64          `json:"revision"`
	Source   jsontext.Value `json:"source"`
}

// judgingKeeps are the fields a judging spec carries beside the judging
// class: the source's identity.
var judgingKeeps = map[string]bool{"id": true, "namespace": true, "name": true, "orkflow_name": true}

// BuildJudgingSpec is src's judging spec, read with its defaults: every
// field of the judging class (fieldClass) and the source's identity, so a
// field added to the judging class is carried without a change here.
func BuildJudgingSpec(src *orkstackv1.EventSource, l Limits) (JudgingSpec, []byte, string, error) {
	e := Effective(src, l)
	view := &orkstackv1.EventSource{}
	from, to := e.ProtoReflect(), view.ProtoReflect()
	from.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())
		if fieldClass[name] == classJudging || judgingKeeps[name] {
			to.Set(fd, v)
		}
		return true
	})
	raw, err := protojson.Marshal(view)
	if err != nil {
		return JudgingSpec{}, nil, "", fmt.Errorf("encode the judging spec: %w", err)
	}
	var compact bytes.Buffer
	if err := stablejson.Compact(&compact, raw); err != nil {
		return JudgingSpec{}, nil, "", fmt.Errorf("encode the judging spec: %w", err)
	}
	spec := JudgingSpec{Format: SpecFormat, Revision: e.GetJudgingRevision(), Source: jsontext.Value(compact.Bytes())}
	body, digest, err := encodeSpec(spec)
	if err != nil {
		return JudgingSpec{}, nil, "", err
	}
	return spec, body, digest, nil
}

// ReadJudgingSpec reads judging spec bytes strictly, and returns the source
// as it judges: only judging fields and the identity, each checked by the
// rules a stored source is held to.
func ReadJudgingSpec(body []byte) (JudgingSpec, *orkstackv1.EventSource, error) {
	var spec JudgingSpec
	if err := readSpec(body, &spec); err != nil {
		return JudgingSpec{}, nil, err
	}
	if spec.Format != SpecFormat {
		return JudgingSpec{}, nil, fmt.Errorf("%w: format %d; this release reads %d", ErrSpecUnreadable, spec.Format, SpecFormat)
	}
	if spec.Revision < 1 {
		return JudgingSpec{}, nil, fmt.Errorf("%w: revision %d", ErrSpecUnreadable, spec.Revision)
	}
	src := &orkstackv1.EventSource{}
	if err := (protojson.UnmarshalOptions{}).Unmarshal(spec.Source, src); err != nil {
		return JudgingSpec{}, nil, fmt.Errorf("%w: its source does not parse", ErrSpecUnreadable)
	}
	var stray string
	src.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if name := string(fd.Name()); fieldClass[name] != classJudging && !judgingKeeps[name] {
			stray = name
			return false
		}
		return true
	})
	if stray != "" {
		return JudgingSpec{}, nil, fmt.Errorf("%w: it carries %s, which is not a judging field", ErrSpecUnreadable, stray)
	}
	if !ValidID(src.GetId()) || !ValidName(src.GetName()) {
		return JudgingSpec{}, nil, fmt.Errorf("%w: it names no valid source", ErrSpecUnreadable)
	}
	out := proto.Clone(src).(*orkstackv1.EventSource)
	// judging_revision is an output field, so the stored source does not
	// carry it: the spec's own revision is the source's.
	out.JudgingRevision = spec.Revision
	return spec, out, nil
}

// encodeSpec is a spec's canonical bytes and their SHA-256.
func encodeSpec(v any) ([]byte, string, error) {
	body, err := stablejson.Marshal(v)
	if err != nil {
		return nil, "", fmt.Errorf("encode the spec: %w", err)
	}
	sum := sha256.Sum256(body)
	return body, hex.EncodeToString(sum[:]), nil
}

// readSpec decodes spec bytes, refusing an unknown member and trailing
// bytes.
func readSpec(body []byte, v any) error {
	if err := json.Unmarshal(body, v, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("%w: %v", ErrSpecUnreadable, err)
	}
	return nil
}
