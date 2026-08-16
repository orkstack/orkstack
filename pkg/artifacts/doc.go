// Package artifacts defines the artifact Store interface and providers, plus
// the S3 store that holds orkflow definitions.
//
// Artifacts are the only way bytes travel between OrkStack tasks: a plugin
// returns files, the Orker stores them here, and downstream tasks receive
// references (never inline bytes). Logical names remain opaque and appear
// unchanged in ArtifactRef.Name.
//
// Layout differs by provider. Local storage is content-addressed under
// reversible path-safe name tokens, so every local object key ends in the
// lowercase sha256 of its bytes. S3 keys instead name the run an artifact came
// from — orkflows/{orkflow-name}/{run-id}/{task-id}/{artifact-name} — built
// from the Provenance the Orker passes in PutOptions.Metadata, so a URI is
// readable and an S3 lifecycle rule can expire one orkflow by prefix. URIs use
// file:// for local objects and s3:// for S3.
//
// The same bucket is also the engine's system of record for orkflows:
// S3OrkflowStore writes one immutable JSON document per version at
// metadata/orkflows/{name}/{version}.json and reads it back on every call, so
// an engine restart reloads the catalog instead of losing it.
//
//	orkflows, err := artifacts.NewS3OrkflowStore(ctx, artifacts.ConfigFromEnv())
//	err = orkflows.Put(ctx, compiled)             // apply
//	catalog, err := orkflows.List(ctx)            // latest version of each
//
// Selecting a backend:
//
//	store, err := artifacts.NewFromEnv()          // ORK_ARTIFACT_BACKEND
//	store, err := artifacts.New(artifacts.Config{ // or explicit config
//		Backend:  artifacts.BackendLocal,
//		LocalDir: "/var/lib/orkstack/artifacts",
//	})
//
// Storing and reading:
//
//	ref, err := store.Put(ctx, "task-1/report.txt", f, artifacts.PutOptions{})
//	rc, err := store.Get(ctx, ref.URI)
//
// Adding a provider: implement Store and register a Factory from an init
// function. Nothing else in the codebase changes; the new name becomes a valid
// ORK_ARTIFACT_BACKEND value.
//
//	func init() {
//		_ = artifacts.Register("gcs", func(cfg artifacts.Config) (artifacts.Store, error) {
//			return newGCSStore(cfg)
//		})
//	}
package artifacts
