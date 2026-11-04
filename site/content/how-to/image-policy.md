# Image policy

Choose which images this deployment may run, and which cosign keys a namespace must require. The deployment sets a floor; a namespace can only tighten it.

## Example

```bash
orkctl admin image-policy set --require-digest \
  --allowed-registry ghcr.io/acme/ \
  --allowed-registry ghcr.io/orkstack/ \
  --signing-key release=cosign.pub

orkctl admin image-policy get
orkctl namespace update prod-payments --require-signed-by cosign:release
```

The second `--allowed-registry` keeps OrkStack's own activities allowed. The default catalog runs `shell`, `http` and the other first-party activities from `ghcr.io/orkstack/`. Leave that prefix out and the engine refuses the list, because it would exclude them. Each `--allowed-registry` replaces the whole list, so name every registry your images come from.

Until an operator writes a policy, every deployment permits every image. An empty policy is the default.

## Set the deployment floor

`orkctl admin image-policy set` reads the policy, applies the flags you pass, and writes the whole document back. A flag you leave out keeps its value.

| Flag | Effect |
|---|---|
| `--require-digest` | Refuses a `uses:` written as a mutable tag. Catalog names such as `uses: shell` are exempt. |
| `--allowed-registry PREFIX` | Limits images to these `registry/repository` prefixes. Repeat it. It replaces the whole list. Prefixes match on `/` boundaries, so `ghcr.io/acme/` does not match `ghcr.io/acmeevil/`. |
| `--clear-allowed-registries` | Removes the registry limit. |
| `--signing-key NAME=PATH` | Adds or replaces a cosign public key. The deployment holds only public keys. |
| `--remove-signing-key NAME` | Removes a key. Refused while a namespace still names it. |

A list that would exclude an activity in the engine's catalog is refused, and the refusal names the activities.

## Require a signature in one namespace

```bash
orkctl namespace update prod-payments --require-signed-by cosign:release
orkctl namespace update prod-payments --clear-require-signed-by
```

A namespace requires one key. It can tighten the deployment's policy and cannot loosen it. To mix your own images with the first-party ones, sign all of them with the same key.

## When it is checked

- **Apply and validate:** an image that breaks the policy is reported on the task's `uses:`.
- **Submit:** the policy is checked before a run exists, so a definition stored before the policy tightened is refused at its next submit.
- **Dispatch:** the Orker checks each attempt against the policy the run pinned at submit, before it builds the workspace or pulls the image.

A run keeps the policy and the key it was submitted under. Tightening the policy later does not stop a run already accepted. Stop it with `orkctl run cancel`.

A registry that cannot be asked is never treated as a pass. Apply and submit report it as `unavailable`. An Orker retries it under the task's policy.

## Refusals

| Reason | Meaning | Retried |
|---|---|---|
| `IMAGE_NOT_PERMITTED` | The image is outside the floor | No |
| `IMAGE_NOT_SIGNED` | No signature by the required key | No |
| `IMAGE_SIGNATURE_INVALID` | Signed by another key, or over another digest | No |
| `IMAGE_SIGNATURE_UNAVAILABLE` | The Orker's registry credentials could not read the signature | Yes, under the task's policy |

Signatures are checked with the image's digest. Verification uses key-based cosign signatures only, with no keyless signing and no transparency log.

## Sign an image

```bash
cosign generate-key-pair                                   # writes cosign.key and cosign.pub
cosign signing-config create --out signing-config.json     # names no transparency service
cosign sign --key cosign.key --signing-config signing-config.json ghcr.io/acme/migrate@sha256:<digest>
```

`cosign signing-config create` is a cosign 3 command. It writes a signing config that names no transparency service. With cosign 2, leave the signature out of the transparency log with `--tlog-upload=false` instead.

Keep `cosign.key` out of the repository. OrkStack never holds it.

## Check OrkStack's own images

Each release signs its images by digest with one key. Take its public key, `deploy/cosign.pub`, from the repository at the release's tag. The release also attaches the same file as `cosign.pub`. The commands below read it from the current folder as `cosign.pub`:

```bash
orkctl admin image-policy set --signing-key orkstack=cosign.pub
orkctl namespace update ops --require-signed-by cosign:orkstack

cosign verify --key cosign.pub --insecure-ignore-tlog=true ghcr.io/orkstack/ork@sha256:<digest>
```

The images of v0.1.0 use cosign's legacy signature layout. Later releases use cosign 3's bundle layout, which an engine or Orker of v0.1.0 cannot read. Read [Upgrading from 0.1.0](/docs/deploy/upgrades/#upgrading-from-010) before you require this key on a v0.1.0 deployment.

Rotate a key without a gap: add the new key under a new name, re-sign, move each namespace's `--require-signed-by`, then remove the old name.

## See also

- [DEPLOY.md, "Image supply-chain policy"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#image-supply-chain-policy): the floor, the namespace rule, the three checks, and the signing layouts
- [DEPLOY.md, "Secrets on a shared pool"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#secrets-on-a-shared-pool): the same floor-and-tightening pattern for secret sources
- [SECURITY_MODEL.md, "Image policy"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#image-policy): what the policy bounds
- [UI_CLI_PARITY.md, "Image policy"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#image-policy): the same policy in the web UI
- [CLI.md, "orkctl admin image-policy set"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-admin-image-policy-set): every flag
- [REASONS.md](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md): the reasons above, in full
