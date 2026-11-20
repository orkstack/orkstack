package main

import (
	"context"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// newRecorder builds CommitResult's steps 2 and 4 to 8 beside the mint,
// sharing its admission cache, start reader and describe; nil when the
// engine issues no task identity, which the API answers
// IDENTITY_ENGINE_REFUSED.
func newRecorder(minter *identity.Minter, records *artifacts.RecordStore, definitions identity.DefinitionReader, build string) *identity.Recorder {
	if minter == nil || records == nil {
		return nil
	}
	return &identity.Recorder{
		Stores:      recorderStores{admissions: minter.Stores, records: records},
		Starts:      minter.Starts,
		Describe:    minter.Describe,
		Follows:     minter.Follows,
		PinnedImage: identity.PinnedImageFrom(definitions),
		EngineBuild: build,
		Now:         minter.Now,
	}
}

// recorderStores reads admissions through the mint's cache and writes
// records to the record store.
type recorderStores struct {
	admissions identity.MintStores
	records    *artifacts.RecordStore
}

func (s recorderStores) ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error) {
	return s.admissions.ReadAdmission(ctx, namespace, runID, sha256Hex)
}

func (s recorderStores) RecordCommit(ctx context.Context, commitKey string, c *orkstackv1.ActivityCommit) (artifacts.RecordedCommit, error) {
	return s.records.RecordCommit(ctx, commitKey, c)
}
