# activity-git

Two read-only git operations over HTTPS: `clone` materializes one commit — as a
source tarball, or as a working tree in a pin's `/orkstack/shared` — and
`resolve` turns a branch, a tag or `HEAD` into the commit it currently names.
Token authentication only, and nothing that writes to a remote.

```yaml
- id: resolve
  uses: git
  secrets:
    - name: git-token
      source: env:GIT_TOKEN
  with:
    op: resolve
    url: https://github.com/acme/app.git
    ref: main
    tokenSecret: git-token
- id: checkout
  uses: git
  needs: [resolve]
  secrets:
    - name: git-token
      source: env:GIT_TOKEN
  retry: {maximum_attempts: 3, initial_interval_seconds: 2}
  with:
    op: clone
    url: https://github.com/acme/app.git
    ref: '{{ .tasks.resolve.output.commit }}'
    tokenSecret: git-token
```

The computing tool is git 2.45 (Alpine's `git~2.45`, patch releases only); the
first line of every task's stdout names the exact version, e.g.
`activity-git: git version 2.45.4`.

## Why this is a first-party image and not `shell op: exec`

A checkout is three lines of shell. The credential is the hard part, and it is
the reason this image exists. Every obvious way to hand a token to git leaks
it: in the URL it lands in `values.json`, logs and git's own error text; on
the command line it is visible in `/proc`; in the environment it is one `env`
away.

Here the token is a **file** the Orker mounts read-only at
`/orkstack/input/secrets/<name>`, and it reaches git only through an askpass
helper baked into the image: git runs the helper when a remote asks for a
credential, and the helper reads the file. The helper is told the token's
*path*, never its value. So, for the token of `tokenSecret`:

- it is never an argument, never part of a URL, never an environment value of
  any process, and never printed by the image;
- git's own traces never print it — they redact the `Authorization` header, and
  the one setting that would stop them is refused (see *Environment*);
- it is offered **only to `url`'s own origin**. git follows a redirect of its
  first request and then asks for a credential for the *new* origin; the
  helper answers only a prompt that names `url`'s scheme, host and port
  exactly, so a redirect to another host or port gets no answer and the task
  fails with git's `could not read Username for '<other origin>': terminal
  prompts disabled`, after the helper's own line `a credential was asked for
  another origin than with.url's; none is given`. A redirect to a server that
  asks for no credential is followed anonymously;
- without `tokenSecret` (and no credential mechanism of your own), a
  repository that asks for credentials fails immediately with `terminal
  prompts disabled`, not at the activity timeout.

## Operations

| `op` | Reads | Writes | `values.json` |
|---|---|---|---|
| `clone` | `url`, `ref`, `depth` | a tarball (`as`) **or** a working tree (`dest`) | `commit`, `ref`, `path`, and for a tarball `sizeBytes`, `sha256` |
| `resolve` | `url`, `ref` | nothing | `commit`, `ref` |

| Key | Op | Type | Meaning |
|---|---|---|---|
| `op` | — | string | **Required.** `clone` or `resolve`. |
| `url` | both | string | **Required.** An `https://` URL — printable ASCII, no credentials, no query string or fragment, a `host[:port]` authority (*Limits*). |
| `ref` | both | string | A branch, a tag, a full ref name (`refs/…`), a full 40-hex commit id, or absent (`HEAD`). |
| `tokenSecret` | both | string | A `secrets:` entry name (`^[A-Za-z0-9._-]{1,128}$`) whose file holds the token. |
| `tokenUser` | both | string | The username sent with the token, `^[A-Za-z0-9._~-]{1,128}$`. Default `x-access-token` (GitHub); GitLab wants `oauth2`, Bitbucket `x-token-auth`. |
| `caFile` | both | string | A PEM bundle (a workspace path, e.g. `secrets/<name>` or an upstream artifact) that **replaces** git's trust store for this task — include public roots if you need both. |
| `depth` | `clone` | number | A non-negative integer, default `1`; `0` is full history. |
| `as` | `clone` | string | The tarball artifact's name, default `src.tar.gz`. |
| `dest` | `clone` | string | `/orkstack/shared/<path>` of a **pinned** task, instead of a tarball. Exclusive with `as`. |

Every key is read by **presence, then type**: a key that is present with the
wrong JSON type — `null` and `false` included — fails `activity-git:
with.<key> must be <a string | a number>`, before any remote is contacted.

### How `ref` is resolved — one algorithm, both ops

- absent or `HEAD` — the remote's `HEAD`; `ref` reports the branch `HEAD`
  points at (`refs/heads/main`) when the remote advertises it.
- a full 40-hex commit id — taken as itself **without contacting the remote**.
  `resolve` does not verify a commit id; `clone` does, by fetching it.
- `refs/…` — exactly that ref.
- any other name `X` — exactly `refs/heads/X` or `refs/tags/X`. A plain
  `git ls-remote <url> main` matches by tail and would also return
  `refs/heads/feature/main`; this image never picks from such a list. No match
  fails `no branch or tag named 'X'`; both a branch and a tag named `X` fail
  `'X' names both refs/heads/X and refs/tags/X …; write the full ref name`.
- an annotated tag resolves to the **commit** it points at, never the tag
  object.

An abbreviated id is not a commit id: `ref: abc1234` is resolved as a name and
fails `no branch or tag named 'abc1234'`.

## `as` — a source tarball

The default. `clone` fetches the commit into a fresh repository and writes
`git archive --format=tar.gz` of it to `/orkstack/output/artifacts/<as>`. The
archive contains no `.git`, honours the repository's own `export-ignore` and
`export-subst` attributes like any release tarball, and its members have no
`./` prefix (`README`, `templates/app.conf.tmpl`), so
`file untar member: README` works. Members carry the commit's time, not the
time of the checkout.

**Reproducible per commit and platform image.** One commit, archived by one
platform image under the image's default environment, gives one tarball digest
on every run: git compresses in-process with zlib (no `gzip` program is run),
the gzip header's time field is zero, and the fresh repository has no
configuration of its own to read. The claim is scoped exactly as far as it was
measured:

- the compressed bytes depend on the archiving git and zlib, so the claim is
  per **platform image** — one `linux/amd64` or one `linux/arm64` image — not
  per catalog reference. A pool that mixes architectures produces one digest
  per architecture for one commit, so the cached tasks after the clone keep one
  memo per architecture's bytes and miss when a clone lands on the other kind
  of Orker; run the clone's pool on one architecture if you want every hit;
- an image upgrade may re-spell the compression of an unchanged commit. The tar
  inside is the commit's tree either way, but `orkctl run diff` across an
  upgrade shows a changed tarball digest and the cached tasks after the clone
  miss once;
- your task's `env:` can change the bytes, because it is honoured: git
  configuration set through `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_<n>`/
  `GIT_CONFIG_VALUE_<n>` or a `GIT_CONFIG_GLOBAL` you name — `tar.umask`,
  `tar.tar.gz.command` (an external compressor), `core.autocrlf` with the
  repository's text attributes — reaches `git archive`. That is never a wrong
  cache hit: your `env:` is part of the clone's own memo key, and every
  downstream key covers the tarball's content digest, so different bytes miss.

`commit` in `values.json` is the commit actually fetched — the one reported,
even if the ref moved after it was resolved.

## `dest` — a working tree for a pin

`dest` checks the commit out, `.git` and all, under `/orkstack/shared` of a
**pinned** task (`docs/DSL.md`, "Pins"), and commits no artifact. It is the
checkout step of a pin whose later members build what this one checked out.

- `dest` must be `/orkstack/shared/<path>` with a non-empty `<path>`; a `..`
  component is refused, never normalized. A task that is not pinned is refused
  (`with.dest needs a pinned task (pin:)`).
- `dest` must not exist, or be an empty directory. A non-empty `dest` is
  refused.
- Every existing directory between `/orkstack/shared` and `dest` must be a
  directory and not a symbolic link (an earlier member may have created
  either); missing ones are created, so `dest: /orkstack/shared/build/src`
  works in a fresh pin.
- The checkout is **staged** beside `dest` in
  `<parent>/.activity-git-<name>` — a reserved name; a previous attempt's
  leftovers there are removed — and renamed into place only when complete. So
  `dest` exists only after a complete checkout, and a pinned retry that sees
  what a failed attempt left in `/orkstack/shared` still succeeds.

A pinned task is never cached. Give the pin `run_as_orker_user: true` (the
DSL's own recommendation), which also keeps git's ownership check quiet for any
later member that runs git. Later members that need history (`git describe`)
want `depth: 0`.

## Output

`clone` with `as`:

```json
{
  "commit": "7fd1a60b01f91b314f59955a4e4d4e80d8edf11d",
  "ref": "refs/heads/main",
  "path": "/orkstack/output/artifacts/src.tar.gz",
  "sizeBytes": 4096,
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
}
```

`clone` with `dest` reports `commit`, `ref` and `path` only.

`resolve`:

```json
{"commit": "7fd1a60b01f91b314f59955a4e4d4e80d8edf11d", "ref": "refs/heads/main"}
```

`ref` is the remote ref that matched — the symbolic target for `HEAD` — or the
commit id itself when `ref` was one.

## Caching

Nothing in OrkStack expects a task to produce the same output from the same
input: tasks run at least once and may read the clock, the network and their
environment. Exactly one opt-in feature depends on it — `cache:` — and writing
`cache:` is **your assertion** that reusing an earlier result for the same
memo key is as good as running the task again. OrkStack cannot verify it. This
section tells you when that assertion is false for this image.

A task that mounts a secret is never cached. A cache hit reads no secret, so a
cached private clone would keep handing out the tree after its token was
revoked or its repository's access removed, until the memo expired (up to
`ORK_CACHE_MAX_TTL`) or someone purged it — and the key covers a secret's name
and source, never its value, so rotating the token changes nothing. That
includes `tokenSecret` and a `caFile: secrets/…`.

**Unsafe to cache when**

- `ref` names a **moving ref** — a branch, a tag or `HEAD`: a hit would keep
  returning the old tree until the memo expires;
- the task mounts a **secret** (above);
- `resolve`, always: its job is to observe a moving ref over the **network**;
- `dest`, always: a pinned task is never cached;
- the **environment**: a variable the container runtime sets per container,
  such as `HOSTNAME`, is in no memo key — git itself does not read it, but a
  git configuration of your own that does would be unsafe; a variable your
  task's `env:` sets is in the key;
- the **clock** and **randomness** do not enter a tarball: members carry the
  commit's time and the gzip header's time field is zero.

**Recommended practice** — the private-clone pattern:

1. `resolve` the branch first (uncached), then `clone` by **commit id**:
   `ref: '{{ .tasks.resolve.output.commit }}'`;
2. a private clone stays **uncached** — it re-fetches and re-checks the token on
   every run, so revoking the token takes effect on the very next run;
3. **cache the tasks after it** instead: a cached task's key covers the content
   digest of every staged input, so the `untar`, `render` and `query` tasks
   after a clone of an unchanged commit hit on every run, because two uncached
   clones of one commit produce one tarball digest;
4. a **public** clone by commit id that mounts no secret may itself be cached —
   a `caFile` read from an upstream artifact is an input, not a secret — and a
   hit then fetches nothing;
5. give `git` tasks an explicit `retry:` for network faults: a failed fetch is
   an ordinary failure, and a network fault and an expired token look the same
   from inside the container.

## Environment

The environment is yours, and this image honours it like any git: the entry
script never clears, filters or re-executes it, so git sees the image defaults
below, overridden by your task's `env:`, plus what the container runtime adds
(under Docker, `HOSTNAME`).

| Image default | What it does | If your task overrides it |
|---|---|---|
| `HOME=/orkstack/tmp` | an empty, writable, per-attempt home: no `~/.gitconfig`, no credential store | git reads the configuration of the home you name |
| `GIT_CONFIG_NOSYSTEM=1` | git reads no `/etc/gitconfig` | the image ships none, so nothing changes |
| `GIT_CONFIG_GLOBAL=/dev/null` | git reads no global configuration | git reads the file you name |
| `GIT_TERMINAL_PROMPT=0` | a credential git cannot get fails at once ("terminal prompts disabled") instead of waiting for a terminal | there is no terminal in the container either way |
| `GIT_ALLOW_PROTOCOL=https` | git refuses every other transport, including on a redirect and for a submodule | `url` itself must still be `https://`; a widened list only lets a redirect leave HTTPS, and the askpass helper answers no `http://` prompt |

**What the image sets from `with:`** — and these beat the same variables in
your task's environment, because a `with:` key is the more specific statement:
with `tokenSecret`, `GIT_ASKPASS` (the baked-in helper) and the helper's own
`ACTIVITY_GIT_TOKEN_FILE` (a path), `ACTIVITY_GIT_USER` and
`ACTIVITY_GIT_AUTHORITY`; with `caFile`, `GIT_SSL_CAINFO`. Without
`tokenSecret` the image sets none of the four, and a credential mechanism you
configure yourself (`GIT_ASKPASS`, or a `credential.helper` through
`GIT_CONFIG_*`) is git's to use. A credential helper you configure is asked
before `GIT_ASKPASS` and, when the helper's token succeeds, receives it on
git's `approve` call — that is git's credential protocol, which you chose by
configuring one.

**TLS is verified unless you set `GIT_SSL_NO_VERIFY`** (or
`http.sslVerify=false` through `GIT_CONFIG_*`). Verification is git's default
and the image changes nothing about it; there is no `with:` key that turns it
off, and there will not be one. Your task's `env:` can, as with any git — and
on a task with `tokenSecret` the token is then sent to whichever server
answers. For a private CA, use `caFile` instead. `caFile` does not re-enable
verification your environment turned off.

**The one refusal.** On a task with `tokenSecret`, a set `GIT_TRACE_REDACT`
must be a value **git itself reads as true**, because a false value is the one
switch that makes git's traces print the token. The image asks git — with
git's own boolean parser — rather than parsing the value itself: any non-zero
integer (`1`, `2`, `-1`, `1k`) and `true`/`yes`/`on` are accepted; `0`, `00`,
`false`, `no`, `off` and the empty value are refused, and so is a value git
cannot read at all (`maybe`, which would stop git from running). The refusal
happens before any remote is contacted and names the variable and its value.
On a task without `tokenSecret` there is no token to print, and the image
neither reads nor refuses the variable.

**Worth knowing:**

- curl's proxy variables — `HTTPS_PROXY`/`https_proxy`, `ALL_PROXY`/
  `all_proxy`, `NO_PROXY`/`no_proxy` — reach git unchanged;
- `GIT_TRACE`, `GIT_TRACE_PACKET`, `GIT_CURL_VERBOSE` and `GIT_TRACE_CURL`
  trace a fetch into the task's stderr, with credentials redacted
  (`Authorization: Basic <redacted>`);
- `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_<n>`/`GIT_CONFIG_VALUE_<n>` set any git
  configuration — `http.proxy`, `http.extraHeader`, `http.lowSpeedLimit`, … —
  including the keys that change a tarball's bytes (`tar.umask`,
  `tar.tar.gz.command`, `core.autocrlf`);
- a variable your task's `env:` sets is in the memo key; `HOSTNAME` is not.

## Limits

- **The URL rule**, checked before any git command, and no refusal ever echoes
  the URL (a refused URL is exactly the one that may carry a credential):
  1. a string of 1–2048 bytes, every byte printable ASCII — no whitespace, no
     control character, no non-ASCII byte (write an internationalized host or
     path punycoded or percent-encoded);
  2. it starts with exactly `https://` — no other transport is offered;
  3. no query string (`?`) and no fragment (`#`) — a query is where hosting
     services take tokens, and git cannot use one;
  4. an authority of `host[:port]` — a host name, an IPv4 address or a
     bracketed IPv6 address, with an optional numeric port. `user@host` and
     `user:pass@host` are refused (`must not carry credentials — give the
     token with tokenSecret`); an `@` in the path (`https://h/a@b.git`) is
     allowed.
- **SHA-256 repositories are not supported.** A 64-hex `ref` is refused
  explicitly: a fresh repository is SHA-1, so such an id could never be
  fetched into it. Every major hosting service's default is SHA-1.
- **Fetching a commit by id** needs the remote to serve one (GitHub and GitLab
  do). When a fetch by id fails, whatever the cause, the image adds one line
  after git's: name the branch or tag in `ref:` instead if this remote does not
  serve a commit by id.
- **The tarball digest** is per platform image and default environment; a
  mixed-architecture pool misses (*`as` — a source tarball*).
- **A redirect to another origin gets no credential** (*Why this is a
  first-party image*).

## Errors

A missing or unknown `op` exits `2`. Everything else exits `1` with the reason
on stderr: a present key of the wrong type (`null` and `false` included); each
URL rule (naming the rule, never the URL); `tokenSecret` malformed or not
mounted (naming the expected path); `tokenUser` outside its pattern; with
`tokenSecret`, a `GIT_TRACE_REDACT` git does not read as true; `caFile`
unreadable; `depth` not a non-negative integer; `as` with `dest`; each `dest`
rule, including a parent that is a symbolic link or not a directory; a 64-hex
`ref`; no matching ref, or an ambiguous one; a credential prompt for another
origin; a fetch, checkout or archive failure (with git's own message above the
image's line).

A failed fetch is an ordinary activity failure, and only your `retry:` decides
whether to try again.

## Interface

The image declares its `with:` contract in the
`org.orkstack.activity.interface` label (`docs/ACTIVITY_IMAGES.md`,
"Declaring the interface"), so `orkctl orkflow validate` and apply check a
task's `with:` before anything runs: a missing `url` or an unknown `op` is an
error, an undeclared key a warning. Five keys shared by both ops, two ops, no
default op (an unknown or missing `op` exits `2`). The label, as the
Dockerfile carries it:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"fields":{"url":{"type":"string","required":true},"ref":{"type":"string"},"tokenSecret":{"type":"string"},"tokenUser":{"type":"string","default":"x-access-token"},"caFile":{"type":"string"}},"ops":{"clone":{"fields":{"depth":{"type":"number","default":1},"as":{"type":"string","default":"src.tar.gz"},"dest":{"type":"string"}}},"resolve":{}},"values":{"commit":{"type":"string"},"ref":{"type":"string"},"path":{"type":"string"},"sizeBytes":{"type":"number"},"sha256":{"type":"string"}}}'
```

## Cancellation

`ls-remote`, the fetch, the checkout and the archive each run in the
background under a TERM trap (§10.2 rule 2a), so cancelling a run interrupts a
slow fetch within the grace period rather than being escalated to SIGKILL.

## What this image does not do

- **nothing that writes to a remote.** No push, commit, tag, merge or rebase.
- **no forge API.** No pull requests, issues, releases or checks — that is a
  SaaS connector; `uses: http` already makes one REST call.
- **no SSH transport.** HTTPS and a token only: SSH would need a private key
  inside the container and a host-key decision nobody is there to make.
- **no submodule recursion, LFS, sparse or partial clone.** A submodule is a
  second remote with a second credential question.
- **no `sslVerify`/`insecure` key.** Verification is off only when your own
  environment says so.

## Building

```bash
make -C images image-git
```
