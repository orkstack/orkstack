{
  "id": "db9cf8bb",
  "title": "M1: Artifact storage pkg (local FS + S3/MinIO, extensible provider interface) + tests",
  "tags": [
    "milestone",
    "artifacts"
  ],
  "status": "open",
  "created_at": "2026-08-14T02:51:20.218Z"
}

Self-contained Go package pkg/artifacts. Provider interface (Put/Get/Stat/List/Presign), local FS impl, S3 impl (aws-sdk-go-v2, MinIO-compatible). Behavior tests against MinIO container. Proof: test logs + artifact roundtrip trace.
