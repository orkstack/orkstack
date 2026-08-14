// Package artifacts defines the artifact Store interface and providers.
//
// Artifacts are the only way bytes travel between OrkStack tasks: a plugin
// returns files, the Orker stores them here, and downstream tasks receive
// references (never inline bytes). Every object is content-addressed by
// sha256, so its canonical key is "<logical name>/<sha256>" and its URI is
// file://<path> for the local provider or s3://<bucket>/<key> for the S3 one.
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
