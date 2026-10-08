# activity-template

Renders one template file into one **text artifact** — a config file, a
manifest — against a merged root context plus any number of named JSON or
YAML datasources. One operation, `render`.

The renderer is [gomplate](https://github.com/hairyhenderson/gomplate) 4.3.3,
installed as the project's **published release binary**, verified against the
release's SHA-256 when the image is built. The first line of every task's
stdout is `activity-template: gomplate version 4.3.3`.

```yaml
- id: config
  uses: template
  needs: [seed, data]
  network: none
  with:
    op: render
    src: '{{ artifact "seed/service.conf.tmpl" }}'
    data: '{{ artifact "data/values.json" }}'
    values:
      environment: staging
    as: service.conf
```

## Why gomplate, and why the render happens in a container

gomplate is a standalone templating CLI, installed exactly the way
`activity-git` installs git and `activity-json` installs jq: a real external
tool doing the work, behind a POSIX `entry.sh` that is the whole of this
image's OrkStack knowledge. gomplate knows nothing about `/orkstack`,
`values.json` or artifacts, and would run identically from a shell prompt.

The Orker renders a task's `with:` block before the container starts, and that
pass is the wrong tool for a file: a `with:` value travels in the run's
history (256 KiB at most), a template written there would live inside the
document beside the DSL's own `{{ }}` with three helpers, and it would produce
no file. Here the template is a **file** an upstream task produced, and the
render is an **artifact** the next task reads.

## Configuration

| Key | Type | Meaning |
|---|---|---|
| `op` | string | **Required.** `render`, the only op — kept so every first-party image has one shape and a second op needs no document change. |
| `src` | string | **Required.** The template file. |
| `data` | string | A JSON file holding an **object**: the root context's base, and the datasource `data`. |
| `values` | object | An inline object, deep-merged over `data` for the root context, and the datasource `values`. |
| `datasources` | object | Name → path. Names `^[A-Za-z0-9_-]{1,64}$`; `data` and `values` are reserved. `.yaml`/`.yml` parse as YAML, every other path as JSON. |
| `leftDelim`, `rightDelim` | string | Both or neither; each 1–16 bytes, no whitespace, different from each other. For templates of files that contain `{{` themselves (Helm charts, CI workflows). |
| `as` | string | The output artifact's name. Default `basename(src)` minus a `.tmpl`, `.tpl` or `.gotmpl` suffix. |

Every path — `src`, `data` and every `datasources` value — resolves inside the
workspace by activity-file's rules: a relative path resolves under
`/orkstack/input/`, an absolute one must already be under `/orkstack/`, a `..`
component is **refused, not normalized**, and the path must name a regular
file. So no datasource can name `/etc/passwd`, a directory, or a path argued
out of the workspace.

Every key is read by **presence, then type**: a key that is present with the
wrong JSON type — `null` and `false` included — fails `activity-template:
with.<key> must be <a string | an object>`, before any work.

**Exact file names.** Every datasource is handed to gomplate as a `file://`
URL whose path is percent-encoded segment by segment, so every path names
exactly the file it says: `a b.json`, `a%41.json` and `x?y.json` are each read
as themselves (a raw `a%41.json` would otherwise be decoded to `aA.json`).

**Types.** Each datasource carries an explicit type hint — YAML for `.yaml`
and `.yml`, JSON for everything else, extensionless files included. gomplate's
own guess for an unknown suffix is "plain string", and `.field` on a string
fails with a confusing type error. JSON datasources are parsed before the
render starts, so a malformed one fails naming the datasource.

## Two ways to reach your data

**Merged, as dot.** `values` deep-merged over `data` (jq's `*`: an inline key
overrides only the leaf it names) becomes the root context:

```gotemplate
listen {{ .service.port }};
```

**Named, as a datasource.** The same two sources are also bound under their own
names, unmerged, beside everything in `datasources`:

```gotemplate
{{ (ds "values").service.port }}          {{/* what the document pinned */}}
{{ (ds "data").service.port }}            {{/* what the run computed   */}}
{{ (ds "regions").eu_west_1.domain }}     {{/* from with.datasources   */}}
```

| | |
|---|---|
| root context (dot) | `values` deep-merged over `data`; `{}` if neither is given |
| `(ds "data")` | `data` alone, exactly as the file has it |
| `(ds "values")` | `values` alone, exactly as the document wrote it |
| `(ds "<name>")` | that entry of `datasources`, untouched — a top-level array is fine to `range` over |

## Missing keys are an error — really

gomplate runs with `--missing-key error`, its own default, written out because
it is a contract this image promises: a template naming a key the data does
not have fails the task, naming the key and the line. One consequence to know
before it surprises you — **`default` rescues an empty value, not an absent
key**, because the key error fires before `default` is called:

```gotemplate
{{ .protocol | default "http" }}            {{/* ERROR if protocol is absent   */}}
{{ .protocol | default "http" }}            {{/* "http" if it is present as "" */}}
{{ index . "protocol" | default "http" }}   {{/* "http" either way             */}}
{{ if has . "protocol" }}…{{ end }}         {{/* explicit guard                */}}
```

(A configuration file of your own can override this rule — see
*Environment*.)

The render is written to `/orkstack/tmp` and moved into the artifacts only
after gomplate exited 0 **and** left a file: gomplate streams its output, so a
template that fails on line 3 has already written lines 1–2, and that
truncated file is never published. The artifact is always mode `0644`
(gomplate would otherwise copy the template's mode, and a `0400` artifact is
unreadable to the Orker).

## Functions

gomplate v4 ships its own namespaced library and **no Sprig**: Helm-flavoured
bare names like `upper`, `b64enc` or `trunc` are not defined. The equivalents
live in gomplate's namespaces — `strings`, `coll`, `conv`, `math`, `data`,
`base64`, `crypto`, `regexp`, `time`, `semver`, `uuid`, `random`, `path`,
`filepath`, `net`, `test`, `tmpl` — and a curated set is available
unqualified: `default`, `quote`, `squote`, `indent`, `toUpper`, `toLower`,
`trim`, `contains`, `split`, `join`, `has`, `keys`, `values`, `dict`, `slice`,
`merge`, `ternary`, `required`, `jq`, `toJSON`, `toYAML`, `getenv`, … Go's own
`text/template` builtins (`range`, `if`, `with`, `index`, `printf`, `len`,
`eq`, `and`, `define`, variables and `{{-`/`-}}` trims) are all present.
`coll.Dict`, `coll.Has` and `coll.Slice` are the current names; the old
`conv.Dict`, `conv.Has` and `conv.Slice` are not registered.

`jq` is the same filter language `activity-json` runs, usable inline:
`{{ (ds "services") | jq "[.[].tier] | unique | join(\",\")" }}`.

## Limits

- **Numbers from datasource files render as written** (`1000000`,
  `12345678901234567890`). A templated `values` leaf is a **string**, and a
  number templated into one arrives in Go's spelling — `1000000` as `1e+06` —
  so numbers a template prints are read from a datasource file
  (`datasources: {up: upstream/<id>/values.json}`), never from a templated
  `values` leaf. Inside a fan-out item, `datasources: {task: task.json}` gives
  `(ds "task").each.item` as exact JSON.
- **YAML coerces like YAML** (`b: 0777` reads as `511`). Data a *system*
  produced should be JSON; YAML is for files a person authored.
- **Never render a secret into a file**: a rendered artifact lives in object
  storage, and this image refuses a task that mounts a secret at all.

## Caching

Writing `cache:` is **your assertion** that reusing an earlier result for the
same memo key is as good as rendering again; OrkStack cannot verify it
(`docs/ACTIVITY_IMAGES.md`, "Network and caching"). This section tells you
which functions make that assertion false.

A task that mounts a secret is never cached; this image refuses such a task
at all.

This inventory is for gomplate 4.3.3, the version the Dockerfile pins. When
`GOMPLATE_VERSION` moves it must be re-derived from gomplate's function
reference at the release's tag, cross-checked against the functions the
binary registers, with every function that could read ambient state run on
the release binary.

**Unsafe to cache when** the template calls one of these:

| Kind | Functions | Why a cached result would be wrong |
|---|---|---|
| the clock | `time.Now`, `time.Since`, `time.Until`; jq's `now` inside `coll.JQ` (alias `jq`) | each reads the wall clock; a cached "age" or "days until expiry" stays stale until the memo expires |
| randomness | `random.ASCII`, `random.Alpha`, `random.AlphaNum`, `random.String`, `random.Item`, `random.Number`, `random.Float`; `uuid.V4`; `uuid.V1` (the clock and a node id); `crypto.Bcrypt` (a random salt); and, only when your `env:` sets `GOMPLATE_EXPERIMENTAL`, the key generators and encryptors listed below | different on every call |
| the environment | `getenv`, `env.Getenv`, `env.ExpandEnv` reading a variable the container runtime sets per container — `HOSTNAME`, each container's own id; and their `<NAME>_FILE` indirection (an unset `X` is read from the file `X_FILE` names) when that file is not a declared input | `HOSTNAME` is in no memo key, and neither is a file outside the workspace |
| the network | `net.LookupIP`, `net.LookupIPs`, `net.LookupCNAME`, `net.LookupSRV`, `net.LookupSRVs`, `net.LookupTXT`; every `aws` function — `aws.EC2Meta`, `aws.EC2Dynamic`, `aws.EC2Region`, `aws.EC2Tag`, `aws.EC2Tags`, `aws.KMSEncrypt`, `aws.KMSDecrypt`, `aws.Account`, `aws.ARN`, `aws.UserID`, and the aliases `ec2meta`, `ec2dynamic`, `ec2region`, `ec2tag`, `ec2tags`; `gcp.Meta`; `datasourceReachable` of a remote datasource | it reaches other hosts |
| host interfaces | `sockaddr.GetAllInterfaces`, `sockaddr.GetDefaultInterfaces`, `sockaddr.GetPrivateInterfaces`, `sockaddr.GetPublicInterfaces`, `sockaddr.GetPrivateIP`, `sockaddr.GetPrivateIPs`, `sockaddr.GetPublicIP`, `sockaddr.GetPublicIPs`, `sockaddr.GetInterfaceIP`, `sockaddr.GetInterfaceIPs` (every other `sockaddr` function transforms what these return) | it reflects the container's network, which no key covers |
| undeclared datasources | `defineDatasource`; `datasource` / `ds` / `include` given a **URL** instead of a declared name (`ds "file:///etc/hostname"` reads the file; a bare path is refused) | it reads a file or URL that `with:` does not list, so the memo key does not cover it |
| files beyond the declared inputs | `file.Read`, `file.Exists`, `file.IsDir`, `file.Stat`, `file.ReadDir`, `file.Walk` | same |
| file writes | `file.Write` (it creates missing directories) | a file written under `/orkstack/output/artifacts/` becomes an extra committed artifact — don't |

**Covered by the memo key although they read the environment:**

- `getenv`, `env.Getenv` and `env.ExpandEnv` of a variable your task's `env:`
  sets, or of an image default (`HOME`, `TZ`): your `env:` is part of the key
  and the image digest fixes the defaults;
- `time.ZoneName`, `time.ZoneOffset`, `time.ParseLocal`, `time.ParseInLocation`
  and the `.Local` of any time value follow `TZ` — `UTC` by default, or the
  zone your `env:` names, read from the image's zone data, which the image
  digest fixes;
- jq's `$ENV` and `env` inside `coll.JQ` — always empty, whatever the
  container's environment; its `input`, `inputs`, `input_filename`,
  `input_line_number`, `debug` and `stderr` are unavailable;
- `tmpl.Path` and `tmpl.PathDir` — the template's own path, which `src` fixes;
- `data.CUE` — it evaluates CUE without running any `tool/*` task.

**Experimental functions** — `crypto.DecryptAES`, `crypto.DecryptAESBytes`,
`crypto.EncryptAES`, `crypto.ECDSAGenerateKey`, `crypto.ECDSADerivePublicKey`,
`crypto.Ed25519GenerateKey`, `crypto.Ed25519GenerateKeyFromSeed`,
`crypto.Ed25519DerivePublicKey`, `crypto.RSADecrypt`, `crypto.RSADecryptBytes`,
`crypto.RSAEncrypt`, `crypto.RSAGenerateKey`, `crypto.RSADerivePublicKey`,
`net.CIDRHost`, `net.CIDRNetmask`, `net.CIDRSubnets`, `net.CIDRSubnetSizes` —
fail "experimental mode not enabled" by default; the image never enables
them. gomplate's own `GOMPLATE_EXPERIMENTAL`, set in your `env:`, does, and
the image honours it. Under it, `crypto.RSAGenerateKey`,
`crypto.ECDSAGenerateKey`, `crypto.Ed25519GenerateKey`, `crypto.EncryptAES` (a
random IV) and `crypto.RSAEncrypt` (random padding) are unsafe to cache; the
decryptors, `crypto.Ed25519GenerateKeyFromSeed`, the `…DerivePublicKey`
functions and `net.CIDR*` are functions of their arguments.

Everything else is a function of its arguments, the declared datasources and
the image: `base64`; `coll` (but for jq's `now`); `conv`; `crypto.PBKDF2`,
`crypto.WPAPSK` and the hashes; the `data` parse and format functions;
`filepath` and `path` (lexical — they never touch the filesystem); `math`;
`net.ParseAddr`, `net.ParseIP`, `net.ParsePrefix`, `net.ParseIPPrefix`,
`net.ParseRange`, `net.ParseIPRange`; `regexp`; `semver`; `strings`; `test`;
`time.Parse`, `time.ParseDuration`, `time.Unix` and the duration and layout
constants; `tmpl.Exec`, `tmpl.Inline`/`tpl`; `uuid.Nil`, `uuid.IsValid`,
`uuid.Parse`; `datasource`/`ds`/`include`, `datasourceExists` and
`listDatasources` of a declared name; and Go's `text/template` builtins.

**Recommended practice**

- `network: none` on every `template` task, so an accidental network
  dependency fails instead of being memoized;
- the values a cached render depends on passed through `values`, `data` and
  declared `datasources` (a `getenv` of a variable your `env:` sets is covered
  by the key as well; one of `HOSTNAME` is not);
- no clock or randomness in a cached render — compute a timestamp in an
  uncached task and pass it as a datasource file;
- cache the tasks **after** an uncached step rather than the step itself: a
  cached task's key covers the content digest of every staged input, so it
  hits exactly when every ancestor reproduced its outputs byte for byte.

## Environment

The environment is yours, and this image honours it like any Unix tool: the
entry script never clears, filters or re-executes it, and gomplate sees the
image defaults below, overridden by your task's `env:`, plus what the container
runtime adds (under Docker, `HOSTNAME`).

| Image default | What it does |
|---|---|
| `HOME=/orkstack/tmp` | a writable, empty, per-attempt home instead of the runtime's read-only `/` |
| `TZ=UTC` | local time is UTC unless your task says otherwise; the image ships zone data, so `env: {TZ: Europe/Berlin}` really means Berlin |

Every variable is readable through `getenv`, `env.Getenv` and
`env.ExpandEnv` — honoured, with the caching consequence above (a variable
your `env:` sets is in the memo key; `HOSTNAME` is not). `AWS_*` and the cloud
SDKs' variables reach the `aws` and `gcp` functions. The image refuses no
variable.

gomplate's own variables are gomplate's to honour, and `with:` beats the
variable that configures the same thing, as a flag beats an environment
variable:

- `GOMPLATE_LEFT_DELIM`/`GOMPLATE_RIGHT_DELIM` switch the delimiters;
  `with.leftDelim`/`rightDelim` are passed as flags and beat them;
- `GOMPLATE_EXPERIMENTAL` enables the experimental functions (*Caching*);
- `GOMPLATE_TYPE_PARAM` renames the URL parameter gomplate reads a type hint
  from. It is the one variable the image reads itself: its type hints follow
  the renamed parameter, so renaming it never changes how a datasource is
  typed;
- `GOMPLATE_LOG_FORMAT` and `GOMPLATE_PLUGIN_TIMEOUT` are gomplate's;
- `GOMPLATE_CONFIG` names a **configuration file**, which gomplate merges under
  the flags this image passes, by gomplate's own rules (4.3.3):
  - the image's flags **win** for the input (the template), the output (the
    artifact — a file's `outputFiles`, `outputDir` or `outputMap` is never
    written), the mode (`0644` over a file's `chmod`), the delimiters when
    `with:` sets them, and a datasource or context alias both name;
  - the file's `leftDelim`/`rightDelim` beat the delimiter variables when
    `with:` sets none; its other datasources, contexts, `templates` and
    `plugins` are added; its `experimental: true` enables the experimental
    functions;
  - gomplate 4.3.3 does **not** let a flag override the file's `missingKey`
    (under `missingKey: zero` a missing key renders `<no value>` despite
    `--missing-key error`), its `postExec` (it runs after the render) or its
    `execPipe` (the output goes to the `postExec` command and no file is
    written — the image then fails the task: `gomplate exited 0 but wrote no
    output file`);
  - a file that cannot be opened or parsed fails the render with gomplate's own
    message (`config file requested, but couldn't be opened`).

  What the file declares is yours: its plugins and `postExec` run inside the
  same container, with the same limits, files and network as the template.
  Without `GOMPLATE_CONFIG` gomplate looks for `.gomplate.yaml` in its working
  directory, the container's read-only `/orkstack`, where no task can place
  one. For caching, `GOMPLATE_CONFIG` itself is in the memo key, and on a task
  that can be cached the file it names is a staged input or a file of the image
  (`/orkstack/tmp` and `/tmp` start empty), so both are covered; what the file
  declares falls under the rows above like anything the template calls.

## Errors

A missing or unknown `op` exits `2`. Everything else exits `1` with the reason
on stderr: a task that mounts a secret; a present key of the wrong type
(`null` and `false` included); `src`, `data` or a datasource path empty,
outside `/orkstack`, containing `..` or not a regular file (naming the path);
`data` not one JSON object; a datasource value not a string, a name malformed
or reserved, a JSON datasource unparseable (naming the datasource); delimiters
half-set, empty, with whitespace, longer than 16 bytes or equal; an unsafe
`as`; a template that does not parse; a missing key (naming the key and the
line); a configuration file named by `GOMPLATE_CONFIG` that cannot be opened or
parsed; gomplate exiting 0 without writing the output file; any other gomplate
failure.

## Interface

The image declares its `with:` contract in the
`org.orkstack.activity.interface` label (`docs/ACTIVITY_IMAGES.md`,
"Declaring the interface"), so `orkctl orkflow validate` and apply check a
task's `with:` before anything runs: a missing `src` or an unknown `op` is an
error, an undeclared key a warning. One op, no default op (an unknown or
missing `op` exits `2`), and the three `values.json` keys of Output. The
delimiters declare no default: a documentary `"{{"` would invite you to write
it, and a literal `{{` in `with:` is a template the Orker would try to render.
The label, as the Dockerfile carries it:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"render":{"fields":{"src":{"type":"string","required":true},"data":{"type":"string"},"values":{"type":"object"},"datasources":{"type":"object"},"leftDelim":{"type":"string"},"rightDelim":{"type":"string"},"as":{"type":"string"}}}},"values":{"path":{"type":"string"},"sizeBytes":{"type":"number"},"sha256":{"type":"string"}}}'
```

## Output

`/orkstack/output/values.json` is activity-file's three keys:

```json
{
  "path": "/orkstack/output/artifacts/service.conf",
  "sizeBytes": 412,
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}
```

## Cancellation

gomplate runs in the background under a TERM trap (the OCI Activity
Contract's §10.1 rule 2a), so a cancellation reaches it within the grace
period rather than being escalated to SIGKILL, and a cancelled render
publishes no artifact.

## What this image does not do

- **no flags for more than one file.** No directory rendering (`--input-dir`),
  no partial templates (`-t`): one template in, one artifact out.
- **no plugins, exec-pipe or configuration file of its own.** The image passes
  none; a template that needs to run a command is asking for a `shell` task.
  (A configuration file your `env:` names is yours — *Environment*.)
- **no experimental functions** unless your `env:` enables them.
- **none of the DSL's own helpers.** `artifact`, `env` and `sh` belong to the
  Orker's render of `with:`; inside this container there is no artifact table.
- **no secrets.** A task that mounts one is refused.

## Building

```bash
make -C images image-template
```

The build downloads the gomplate release asset
(`gomplate_linux-<arch>` of the `v4.3.3` release) from `github.com` and checks
it against the SHA-256 pinned in the Dockerfile, so it needs that host (or a
mirror) at build time, and it needs BuildKit (Docker 23+, or
`DOCKER_BUILDKIT=1`), which supplies the target architecture; amd64 and arm64
are pinned, and any other architecture fails the build explicitly. An
air-gapped builder mirrors the asset and builds with

```bash
make -C images images DOCKER_BUILD_FLAGS='--build-arg GOMPLATE_BASE_URL=https://mirror.example/gomplate'
```

— the checksum verifies the bytes whatever served them, so a mirror can neither
substitute nor corrupt the binary silently. The binary is ~87 MB, so the image
is correspondingly larger, and its first pull shows up in a task's timing
breakdown.
