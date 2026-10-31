# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
breaking change is marked **BREAKING**.

A change lands with its entry under `[Unreleased]`. At a release that section
becomes the version's own, and its text is the release's notes on GitHub,
word for word.

## [Unreleased]

### Added

- The image policy verifies cosign 3's default signature format, a Sigstore bundle stored as an OCI referrer of the image, beside the legacy `sha256-<hex>.sig` layout that images signed before keep, v0.1.0's among them. Sign with cosign 2 or 3; cosign 3 keeps the signature out of the public transparency log under a signing config that names no service ([DEPLOY.md, "Image supply-chain policy"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#image-supply-chain-policy)).
- Each release's page ends with every image's digest and the `cosign verify` and `sha256sum` commands that check the images and the checksums.
- Each release attaches `SHA256SUMS.sigstore.json`, cosign's bundle of the checksum signature, beside `SHA256SUMS.sig`, which openssl still checks ([UPGRADING.md, "Release artifacts"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts)).
- `make proto-breaking`, run by CI on every push and pull request, checks the proto contract against v0.1.0 with `buf breaking`.

### Changed

- **BREAKING** The release signs its images and `SHA256SUMS` in cosign 3's bundle format, with none of the flags cosign 3.1 deprecates, and still records nothing in a transparency log. An engine or Orker of v0.1.0 reads only the legacy layout, and v0.1.0's catalog follows the `0.1` tag to this release's activity images, so a v0.1.0 deployment whose namespace requires `cosign:orkstack` refuses them with `IMAGE_NOT_SIGNED`. Upgrade every engine and Orker, or until then name v0.1.0's activity images by digest in `ORK_ACTIVITY_CATALOG`. During the upgrade, a v0.1.0 Orker still polling refuses a run's bundle-signed images too, as a contract violation that is never retried ([UPGRADING.md, "Release artifacts"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts)).
- A release is refused before anything is built when its page, footer included, is over GitHub's limit of 125,000 characters for a release body, instead of failing at its last step after the tag and the images are out.

## [0.1.0] - 2026-10-30

First release.
