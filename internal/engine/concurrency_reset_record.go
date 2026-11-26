package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ActivityRecordConcurrencyReset writes one audit record of a reset, on the
// engine worker that runs the gate's workflow task.
const ActivityRecordConcurrencyReset = "RecordConcurrencyReset"

// What a reset's record says the gate did (§5.1.11). The request itself, and
// who asked, are the API's own record.
const (
	ResetRecordApplied   = "applied"
	ResetRecordCompleted = "completed"
	ResetRecordFailed    = "failed"
)

// ConcurrencyResetOutcomes are the outcomes a reset is counted under.
func ConcurrencyResetOutcomes() []string {
	return []string{resetOutcomeStarted, ResetRecordApplied, ResetRecordCompleted, ResetRecordFailed}
}

const resetOutcomeStarted = "started"

// ConcurrencyResetRecord is one record of a reset, as the gate that reset
// began hands it over.
type ConcurrencyResetRecord struct {
	Kind      string    `json:"kind"`
	Namespace string    `json:"namespace"`
	GateID    string    `json:"gate_id"`
	Key       string    `json:"key"`
	RequestID string    `json:"request_id"`
	StartedAt time.Time `json:"started_at"`
	Epoch     int64     `json:"epoch"`
	Holders   int       `json:"holders"`
	Queued    int       `json:"queued"`
	Forced    bool      `json:"forced,omitempty"`
}

// ResetObserver counts a reset's outcomes (E88 §9.9.2, ork.concurrency.resets).
type ResetObserver interface {
	ObserveConcurrencyReset(ctx context.Context, namespace, outcome string, forced bool)
}

// WithResetRecords writes resets' records to store, in the schema the
// deployment's mode writes, and counts each written one on o.
func (a *GateActivities) WithResetRecords(store artifacts.AuditStore, verified func() bool, o ResetObserver) *GateActivities {
	a.audit, a.auditVerified, a.resets = store, verified, o
	return a
}

// RecordConcurrencyReset writes one record, once: its id is the reset's and
// its kind's, so a step run again writes nothing new.
func (a *GateActivities) RecordConcurrencyReset(ctx context.Context, r ConcurrencyResetRecord) error {
	if a.audit == nil {
		return errors.New("record a concurrency reset: this engine has no audit store")
	}
	rec := resetAuditRecord(r)
	if a.auditVerified != nil && a.auditVerified() {
		rec.SchemaVersion = artifacts.AuditSchemaVersionVerified
	}
	err := a.audit.PutAuditRecord(ctx, rec)
	if errors.Is(err, artifacts.ErrAuditRecordExists) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("record a concurrency reset: %w", err)
	}
	if a.resets != nil {
		a.resets.ObserveConcurrencyReset(ctx, r.Namespace, r.Kind, r.Forced)
	}
	return nil
}

func resetAuditRecord(r ConcurrencyResetRecord) *orkstackv1.AuditRecord {
	sum := sha256.Sum256([]byte(strings.Join([]string{"concurrency.reset", r.Kind, r.GateID, r.RequestID}, "\x00")))
	detail := fmt.Sprintf("request %s: epoch=%d holders=%d queued=%d", r.RequestID, r.Epoch, r.Holders, r.Queued)
	rec := &orkstackv1.AuditRecord{
		SchemaVersion: artifacts.AuditSchemaVersion,
		Id:            "aud-" + artifacts.RunStamp(r.StartedAt) + "-" + hex.EncodeToString(sum[:])[:16],
		At:            timestamppb.New(r.StartedAt), Action: "concurrency.reset." + r.Kind, Namespace: r.Namespace, Target: r.Key,
		Succeeded: r.Kind != ResetRecordFailed, Detail: detail,
	}
	if r.Kind == ResetRecordFailed {
		rec.ErrorCode = ResetFailedCode
	}
	return rec
}
