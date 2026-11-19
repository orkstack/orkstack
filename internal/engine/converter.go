package engine

import (
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// orkerAnswers are the messages an Orker returns to the engine: an attempt's
// result, its failure's detail, and what DescribeOrker (and CordonOrker)
// answers. Only these are decoded leniently.
var orkerAnswers = map[protoreflect.FullName]bool{
	(&orkstackv1.ExecuteTaskResult{}).ProtoReflect().Descriptor().FullName():     true,
	(&orkstackv1.ActivityFailureDetail{}).ProtoReflect().Descriptor().FullName(): true,
	(&orkstackv1.DescribeOrkerResult{}).ProtoReflect().Descriptor().FullName():   true,
}

// answerProtoJSON is the ProtoJSON payload converter with a split decode: an
// Orker's answer drops a field (or an enum value's name) the engine's build
// does not know, and every other message — a run's definition, its carry, a
// signal, a schedule's arguments, which a newer engine may have written — is
// refused as the SDK default refuses it, so an older engine never runs a run
// with one of its settings silently gone. Encoding is the default's, byte for
// byte.
type answerProtoJSON struct {
	*converter.ProtoJSONPayloadConverter                                      // strict: encodes, and decodes all but answers
	lenient                              *converter.ProtoJSONPayloadConverter // decodes an Orker's answer
}

// FromPayload decodes payload into valuePtr, leniently only for an Orker's
// answer.
func (c answerProtoJSON) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	if name, ok := messageName(valuePtr); ok && orkerAnswers[name] {
		return c.lenient.FromPayload(payload, valuePtr)
	}
	return c.ProtoJSONPayloadConverter.FromPayload(payload, valuePtr)
}

// messageName is the full name of the message valuePtr points at: a pointer
// to a message pointer (set or nil) or a pointer to a message value.
func messageName(valuePtr any) (protoreflect.FullName, bool) {
	v := reflect.ValueOf(valuePtr)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return "", false
	}
	if elem := v.Elem(); elem.Kind() == reflect.Pointer {
		if !elem.IsNil() {
			if m, ok := elem.Interface().(proto.Message); ok {
				return m.ProtoReflect().Descriptor().FullName(), true
			}
			return "", false
		}
		// A nil pointer of a generated message type: a new one names it. A
		// type whose zero value has no descriptor (dynamicpb) is no answer.
		if m, ok := reflect.New(elem.Type().Elem()).Interface().(proto.Message); ok {
			return zeroName(m)
		}
		return "", false
	}
	if m, ok := valuePtr.(proto.Message); ok {
		return m.ProtoReflect().Descriptor().FullName(), true
	}
	return "", false
}

// zeroName is a new message's full name, false for a type whose zero value
// has no descriptor and panics when asked (a dynamicpb.Message).
func zeroName(m proto.Message) (name protoreflect.FullName, ok bool) {
	defer func() {
		if recover() != nil {
			name, ok = "", false
		}
	}()
	return m.ProtoReflect().Descriptor().FullName(), true
}

// dataConverter is the SDK's default converter in the SDK's own order, with
// its ProtoJSON converter replaced by answerProtoJSON. The SDK makes it
// transfer-aware itself wherever it encodes or decodes with it (the client,
// the worker and workflow code, since go.temporal.io/sdk v1.51.0, which made
// that wrapper internal); the engine decodes no transfer-typed value of its
// own.
var dataConverter = payloadcrypt.NewConverter(converter.NewCompositeDataConverter(
	converter.NewNilPayloadConverter(),
	converter.NewByteSlicePayloadConverter(),
	answerProtoJSON{
		ProtoJSONPayloadConverter: converter.NewProtoJSONPayloadConverter(),
		lenient: converter.NewProtoJSONPayloadConverterWithOptions(converter.ProtoJSONPayloadConverterOptions{
			AllowUnknownFields: true,
		}),
	},
	converter.NewProtoPayloadConverter(),
	converter.NewJSONPayloadConverter(),
), nil)

// failureConverter writes a failure's details with dataConverter. With no
// payload key configured it writes every failure as the SDK default does.
var failureConverter = payloadcrypt.NewFailureConverter(dataConverter, false)

// DataConverter is the converter cmd/ork dials with, and the run reader and
// the scheduler decode with. An Orker that leads its engines in a rollout
// answers with fields the engine has never heard of — DescribeOrker's
// capabilities, a failure's detail — and the SDK default would refuse such an
// answer whole, losing the Orker from the inventory or the failure's reason.
// For those answers alone a field the engine does not know is dropped, and an
// enum value's name it does not know reads as the enum's zero: either is
// something the engine could not have decided on, and the strict decode
// would have lost the whole answer instead. Everything else stays strict
// (answerProtoJSON). Every payload is encoded as the SDK default encodes it,
// so every recorded history replays as before
// (TestDataConverter_RecordedHistoriesReplayAsUnderTheSDKDefault).
//
// The Orker keeps the SDK default and stays strict on what it is sent: a
// field of ExecuteTaskInput it cannot read may be a security setting
// (devices, identity, an unmasked secret), and submit's capability
// checks keep such a field away from an Orker that predates it.
//
// It is payload encryption's converter (payloadcrypt.Converter) around that
// composite, holding no key: it passes every payload that is not sealed
// through untouched, and is the guard of payload-encryption.md §4.1 rule 3 —
// workflow code of a run whose memo carries a payload key panics, failing
// the workflow task, rather than deciding anything from it
// (TestDataConverter_IsByteIdenticalWithEncryptionOff).
func DataConverter() converter.DataConverter { return dataConverter }

// FailureConverter is the failure converter cmd/ork dials with: payload
// encryption's (payloadcrypt.FailureConverter) over DataConverter, sealing
// nothing while no key is configured.
func FailureConverter() converter.FailureConverter { return failureConverter }
