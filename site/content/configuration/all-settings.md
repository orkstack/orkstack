# All settings

Every setting of `ork` and `orker`, on one page. Each one is an environment variable, and most are a key of one shared YAML file too.

## How to set one

Give either binary a file with `--config PATH` or `ORK_CONFIG_FILE`:

```yaml title="orkstack.yaml"
temporal:
  address: temporal.internal:7233
storage:
  backend: s3
  s3:
    endpoint: https://s3.example.com
    secret_key:
      file: /run/secrets/s3-secret-key
```

Then check it. `--check` starts nothing: it prints the effective configuration, secrets redacted and every value labelled with where it came from, and exits non-zero naming the first error.

```bash
ork --config orkstack.yaml --check
```

- **Order.** Built-in defaults, then the file, then the environment. Each layer overrides only what the one before it set.
- **Strict.** An unknown key, or a number, boolean or duration that is not one, stops the process at boot and names the key.
- **Secrets.** A secret leaf can be a mapping with a `file:` key instead of a literal. A relative path resolves against the file's own directory.
- **Different values per binary.** Any scalar can be a mapping with `ork:` and `orker:` keys.

## Read an entry

**Set on** tags say which binary reads a setting. **Accepts** is the value's shape and bounds. **Checked** says when a bad value is refused, and appears only where the reference states it. **More detail** folds the rest of what the reference says. The longer rules live in [Setting details](/docs/configuration/details/).

<!-- site-generated: settings-all -->

## Every key and its default

The same defaults as one file, as `ork --defaults` and `orker --defaults` print it.

<!-- site-generated: defaults-yaml -->

## See also

- [docs/DEPLOY.md, “Configuration”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): the table these entries are generated from, which stays the full reference.
- [`deploy/orkstack.defaults.yaml`](https://github.com/orkstack/orkstack/blob/main/deploy/orkstack.defaults.yaml): the same defaults as one file. `ork --defaults` and `orker --defaults` print it.
