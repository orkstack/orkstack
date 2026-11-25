# Publish the task identity issuer

Make OrkStack an OpenID Connect issuer that clouds and secret stores can trust. This sets up the keys and the documents relying parties read. Tasks are not issued tokens yet.

## Example

```yaml
identity:
  issuer: https://ork-id.acme.dev
  signing:
    key_files:
      - /etc/orkstack/identity-key.pem
```

Create the key once, and give every engine the same file:

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out identity-key.pem
```

The issuer needs shared object storage. On the local backend the engine refuses to start.

## Put the documents where relying parties can reach them

```bash
orkctl admin identity issuer export --out ./issuer
```

Upload the folder to the issuer URL, for example a bucket behind a CDN. Do it again after every key change. Engines also serve the same documents at `/.well-known/openid-configuration` and `/v1/identity/jwks` for relying parties inside your network.

## Check the keys

```bash
orkctl admin identity keys list
```

A key signs once the issuer URL serves it. A new key added beside an old one first waits out `identity.signing.activation_delay`, a day by default. The list says why a key does not sign yet.

## Rotate a key

1. Put the new key first in `identity.signing.key_files` on every engine, and keep the old one second. Restart the engines.
2. Export and upload.
3. After the activation delay, run `orkctl admin identity keys retire KID` for the old key, then export and upload again.

## Use SPIFFE IDs

```yaml
identity:
  subject_format: spiffe
  spiffe:
    trust_domain: ork.acme.dev
```

Each token's subject is then a SPIFFE ID, and the issuer also serves a SPIFFE bundle. Use a trust domain of OrkStack's own, never one a SPIRE server already serves.

## See also

- [TASK_IDENTITY.md, "The issuer"](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#the-issuer): the documents and the two ways to host them
- [TASK_IDENTITY.md, "When a key signs"](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#when-a-key-signs): the activation rule
- [TASK_IDENTITY.md, "Rotation, with no outage"](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#rotation-with-no-outage): every step of a rotation
- [TASK_IDENTITY.md, "SPIFFE"](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#spiffe): the SPIFFE ID's form and changing format
- [PROVIDERS.md, "Declaring providers"](https://github.com/orkstack/orkstack/blob/main/docs/PROVIDERS.md#declaring-providers): the `providers` setting, where credentials and secrets will come from
- [TASK_IDENTITY.md, "Administering the issuer"](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#administering-the-issuer): every `orkctl admin identity` command
