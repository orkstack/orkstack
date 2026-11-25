package main

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func goldenConcurrencyLimit() *orkstackv1.ConcurrencyLimit {
	at := timestamppb.New(fixedNow)
	later := timestamppb.New(fixedNow.Add(2 * time.Hour))
	queue := orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE
	return &orkstackv1.ConcurrencyLimit{Namespace: "payments", Key: "db-migrate:prod", Answering: true, Epoch: 1,
		HoldersCount: 1, QueuedCount: 2, Rebuild: orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_NONE,
		Holders: []*orkstackv1.ConcurrencySlotHolder{{RunId: "ork-01k3", GrantedAt: at, Stalled: true, Strategy: queue, Limit: 1}},
		Line: []*orkstackv1.ConcurrencyLineEntry{
			{RunId: "ork-01k4", ArrivedAt: at, TimesOutAt: later, Strategy: queue, Limit: 1},
			{RunId: "ork-01k5", TaskLevel: true, ArrivedAt: at, TimesOutAt: later, Strategy: queue, Limit: 1},
		}}
}

func TestConcurrencyGet_Golden(t *testing.T) {
	var out bytes.Buffer
	must.OK(t, renderConcurrencyLimit(&out, goldenConcurrencyLimit()))
	requireGolden(t, "concurrency-get.golden", out.String())
}

func TestConcurrencyList_Golden(t *testing.T) {
	reading := &orkstackv1.ConcurrencyLimit{Key: "registry-push", Answering: true, HoldersCount: 0, QueuedCount: 1,
		Rebuild: orkstackv1.ConcurrencyRebuild_CONCURRENCY_REBUILD_READING}
	silent := &orkstackv1.ConcurrencyLimit{NotAnsweringReason: "the limit did not answer within 5s"}
	var out bytes.Buffer
	must.OK(t, renderConcurrencyList(&out, []*orkstackv1.ConcurrencyLimit{goldenConcurrencyLimit(), reading, silent}))
	requireGolden(t, "concurrency-list.golden", out.String())
	out.Reset()
	must.OK(t, renderConcurrencyList(&out, nil))
	if out.String() != "No key has runs holding or waiting.\n" {
		t.Fatalf("empty list = %q", out.String())
	}
}
