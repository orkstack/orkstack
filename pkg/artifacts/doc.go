// Package artifacts defines the artifact Store interface and providers, plus
// the S3 stores that hold orkflow definitions and the namespace registry.
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
// metadata/orkflows/{namespace}/{name}/{version}.json and reads it back on
// every call, so an engine restart reloads the catalog instead of losing it.
// S3NamespaceStore holds the namespaces those keys are scoped by, one
// document each at metadata/namespaces/{name}.json; "default" resolves
// without one, so nothing must be created before the first apply.
//
//	orkflows, err := artifacts.NewS3OrkflowStore(ctx, artifacts.ConfigFromEnv())
//	_, _, err = orkflows.Put(ctx, compiled, artifacts.NoHeadExpectation())
//	_, _, err = orkflows.Put(ctx, edited, artifacts.ExpectHead("2.0.0")) // an edit
//	catalog, err := orkflows.List(ctx, "default")                       // what each name runs
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
