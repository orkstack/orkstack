package payloadcrypt

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// scheduledByIDKey is the cluster's own stamp on every workflow a schedule
// starts. A fire's key is bound to it, never to an id a memo names.
var scheduledByIDKey = temporal.NewSearchAttributeKeyKeyword("TemporalScheduledById")

// KeyState is what a run's memo says of its key, as RunKeyOf reads it.
type KeyState struct {
	scope *Scope
	err   error
}

// Sealed reports whether the run has a key this process opened.
func (s KeyState) Sealed() bool { return s.scope.Sealed() }

// Scope is the run's key and own context: Unsealed() for a run that is not
// sealed, nil where Err is set (a miss, or a keyless run).
func (s KeyState) Scope() *Scope {
	if s.err == nil && s.scope == nil {
		return unsealedScope
	}
	return s.scope
}

// Err is nil for a sealed run and for one that is not; ErrEngineKeyMissing
// for a miss; ErrKeyless for a memo key field that yields no key.
func (s KeyState) Err() error { return s.err }

// RunKeyOf reads a run's key from its memo with keys, by arithmetic and no
// call. workflowID is the execution's own id, the context its payloads are
// sealed for; scheduleStamp is the cluster's TemporalScheduledById, read
// only for a fire's memo.
func RunKeyOf(memo *commonpb.Memo, keys *EngineKeys, workflowID, scheduleStamp string) KeyState {
	p, ok := memo.GetFields()[MemoField]
	if !ok {
		return KeyState{}
	}
	if string(p.GetMetadata()[MetaEncoding]) != "json/plain" {
		return KeyState{err: keyless("a key field that is not JSON")}
	}
	m, err := ParseKeyMemo(p.GetData())
	if err != nil {
		return KeyState{err: err}
	}
	k, err := m.Open(keys, scheduleStamp)
	if err != nil {
		return KeyState{err: err}
	}
	if m.Kind == KindSchedule {
		return KeyState{scope: NewScope(k, ScheduleContext(scheduleStamp))}
	}
	if workflowID == "" || len(workflowID) > MaxContextID {
		return KeyState{err: keyless("a workflow id of %d bytes", len(workflowID))}
	}
	return KeyState{scope: NewScope(k, RunContext(workflowID))}
}

// Converter is OrkStack's data converter: first, it writes a Seal'd value
// sealed and a PlainBinary value as binary proto; everything else is the
// converter it wraps, byte for byte. It is installed in every process of
// this release, sealing or not, as the guard of payload-encryption.md §4.1
// rule 3.
//
// Its root face (the client, an activity worker) opens nothing: no key is at
// hand there in this release. Its workflow face (WithWorkflowContext) holds
// the run's key from the run's own memo and keys, and opens a sealed payload
// for the run's own context. Where the memo names an engines' key this
// process does not hold it panics, at the first payload it reads, so the
// workflow task fails and an engine that holds the file takes it; it decides
// nothing.
type Converter struct {
	inner converter.DataConverter
	keys  *EngineKeys
	run   *KeyState // the workflow face's run; nil on the root face
}

var _ converter.DataConverter = (*Converter)(nil)
var _ converter.ContextAware = (*Converter)(nil)

// NewConverter wraps inner. keys is nil where payload encryption is off: the
// converter then seals nothing it is not handed a key for, and still guards.
func NewConverter(inner converter.DataConverter, keys *EngineKeys) *Converter {
	return &Converter{inner: inner, keys: keys}
}

// ToPayload writes v.
func (c *Converter) ToPayload(v any) (*commonpb.Payload, error) {
	c.guard()
	switch x := v.(type) {
	case sealedValue:
		switch x.v.(type) {
		case sealedValue, plainBinary:
			return nil, fmt.Errorf("payloadcrypt: a %T inside a sealed value", x.v)
		}
		p, err := c.inner.ToPayload(x.v)
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, fmt.Errorf("payloadcrypt: %T converts to no payload", x.v)
		}
		key := x.s.key
		out, err := NewCodec(func(Context) (*Key, error) { return key, nil }).Encode([]*commonpb.Payload{Mark(p, x.s.ctx)})
		if err != nil {
			return nil, err
		}
		return out[0], nil
	case plainBinary:
		if x.m == nil {
			return nil, errors.New("payloadcrypt: PlainBinary of no message")
		}
		return converter.NewProtoPayloadConverter().ToPayload(x.m)
	}
	return c.inner.ToPayload(v)
}

// ToPayloads writes each value.
func (c *Converter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	c.guard()
	if len(values) == 0 {
		return nil, nil
	}
	out := &commonpb.Payloads{Payloads: make([]*commonpb.Payload, len(values))}
	for i, v := range values {
		p, err := c.ToPayload(v)
		if err != nil {
			return nil, fmt.Errorf("values[%d]: %w", i, err)
		}
		out.Payloads[i] = p
	}
	return out, nil
}

// guard panics where the run's memo names an engines' key this process
// lacks: the one condition that panics. It is asked at every read and every
// write of the workflow face, so such an engine neither reads nor writes
// anything of the run.
func (c *Converter) guard() {
	if c.run != nil && c.run.err != nil && isMiss(c.run.err) {
		panic(c.run.err)
	}
}

// FromPayload reads p into valuePtr. A raw value is filled as it is and
// opens nothing.
func (c *Converter) FromPayload(p *commonpb.Payload, valuePtr any) error {
	if _, raw := valuePtr.(*converter.RawValue); raw {
		return c.inner.FromPayload(p, valuePtr)
	}
	c.guard()
	if !IsSealed(p) {
		return c.inner.FromPayload(p, valuePtr)
	}
	if c.run == nil {
		return unreadable("a sealed payload outside a run's workflow code")
	}
	if !c.run.Sealed() {
		if c.run.err != nil {
			return unreadable("a sealed payload in a run whose key does not open: %v", c.run.err)
		}
		return unreadable("a sealed payload in a run that carries no key")
	}
	opened, err := c.run.scope.Open(p)
	if err != nil {
		return err
	}
	return c.inner.FromPayload(opened, valuePtr)
}

// FromPayloads reads each payload into its pointer.
func (c *Converter) FromPayloads(ps *commonpb.Payloads, valuePtrs ...any) error {
	c.guard()
	for i, p := range ps.GetPayloads() {
		if i >= len(valuePtrs) {
			break
		}
		if err := c.FromPayload(p, valuePtrs[i]); err != nil {
			return fmt.Errorf("payload item %d: %w", i, err)
		}
	}
	return nil
}

// ToString shows a sealed payload as what it was sealed for, never its
// plaintext.
func (c *Converter) ToString(p *commonpb.Payload) string {
	if IsSealed(p) {
		if c, err := ParseContext(string(p.GetMetadata()[MetaContext])); err == nil {
			return "[sealed for " + c.String() + "]"
		}
		return "[sealed]"
	}
	return c.inner.ToString(p)
}

// ToStrings shows each payload.
func (c *Converter) ToStrings(ps *commonpb.Payloads) []string {
	out := make([]string, len(ps.GetPayloads()))
	for i, p := range ps.GetPayloads() {
		out[i] = c.ToString(p)
	}
	return out
}

// WithWorkflowContext is the workflow face: the run's key, read from the
// execution's own memo (and, for a fire, the cluster's stamp), which the SDK
// hands workflow code before it runs. It calls nothing.
func (c *Converter) WithWorkflowContext(ctx workflow.Context) converter.DataConverter {
	info := workflow.GetInfo(ctx)
	stamp := ""
	if _, ok := info.Memo.GetFields()[MemoField]; ok {
		if v, ok := workflow.GetTypedSearchAttributes(ctx).GetKeyword(scheduledByIDKey); ok {
			stamp = v
		}
	}
	st := RunKeyOf(info.Memo, c.keys, info.WorkflowExecution.ID, stamp)
	inner := c.inner
	if aware, ok := inner.(converter.ContextAware); ok {
		inner = aware.WithWorkflowContext(ctx)
	}
	return &Converter{inner: inner, keys: c.keys, run: &st}
}

// WithContext is the root face, for an activity worker and the client.
func (c *Converter) WithContext(ctx context.Context) converter.DataConverter {
	inner := c.inner
	if aware, ok := inner.(converter.ContextAware); ok {
		inner = aware.WithContext(ctx)
	}
	return &Converter{inner: inner, keys: c.keys}
}
