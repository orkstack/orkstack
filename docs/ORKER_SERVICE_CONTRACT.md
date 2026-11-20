# Orker Service Contract

A **service** is a container an Orker keeps running for as long as its owner
wants it, and talks to through files. An event source's instance is one: its
container reads a broker and hands the Orker one message at a time
([EVENT_SOURCES.md](EVENT_SOURCES.md)). The contract is to a service what the
[OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md) is to a task: an image needs
no OrkStack SDK, callback or socket; it reads files, writes files, logs to
stdout and stderr, and exits.

This release hosts contract version `1` and the `event-source` profile at
version `1`, which [EVENT_SOURCE_IMAGES.md](EVENT_SOURCE_IMAGES.md) describes.
No release yet runs a service: the Orker side is in place, and the engine
side that starts instances comes with a later release.

## A service image

| Label | Value | Required |
|---|---|---|
| `org.orkstack.service.contract` | the contract versions the image implements, comma-separated: `1` | yes |
| `org.orkstack.service.profile` | the profile and its versions: `event-source/1` | yes |
| `org.orkstack.service.interface` | the packed `kind: ServiceInterface` document | no |

An image without the first two labels, or whose versions no Orker of this
release hosts, is refused when the object naming it is written
(`SERVICE_CONTRACT_UNSUPPORTED`, `SERVICE_PROFILE_UNSUPPORTED`).

The interface is authored beside the Dockerfile, by convention
`service.ork.yaml`, and packed with `orkctl activity interface pack`
([ACTIVITY_IMAGES.md](ACTIVITY_IMAGES.md)):

```yaml
apiVersion: orkstack/v1
kind: ServiceInterface
profile: event-source
with:
  stream:
    type: string
    required: true
event_source:
  delivery: at-least-once
  max_concurrency: 4
```

`with:` uses the activity interface's grammar: each key with its own `type`,
`required` and `default`. `event_source:` declares the image's delivery class,
how many messages one container handles at once (1 to 64), and, for an
at-least-once image, that its broker dead-letters after a maximum and that it
keeps a message leased until its verdict.

**A service image runs as an arbitrary non-root uid.** The Orker starts every
service container as its own unprivileged user, whatever the image declares,
so that it can read the container's files without holding any privilege. An
image must work under any uid: no file it needs may be readable only by root,
and it must not need to bind a port below 1024 or change its own user. An
image that declares the root user and ends before its first status fails
`SERVICE_RUNS_AS_ROOT` ([REASONS.md](REASONS.md#service-attempts)).

## The tree

```text
/orkstack/
├── input/                    read-only, sealed before the container starts
│   ├── service.json          the service's context
│   ├── config.json           its `with:`, as JSON by the strict mapping
│   └── secrets/<name>        its own secrets, memory-backed
└── service/
    ├── ask/                  READ-ONLY to the container: everything the Orker writes
    │   ├── alive.json        the Orker's beat
    │   ├── check.json        "check now"
    │   ├── stop.json         "I am about to stop you"
    │   └── lanes/<k>/…       the Orker's steps
    └── answer/               read-write: everything the container writes,
        │                     a bounded memory filesystem of its own
        ├── status.json       readiness and health
        ├── tmp/              the container's staging, for rename
        └── lanes/<k>/…       the container's steps
```

- **Two mounts, one per direction.** The container cannot write `ask/`; the
  Orker never writes `answer/`.
- **Fixed names, replaced by rename.** Every file has a name the profile
  gives. A file is written under a temporary name (the container's in
  `answer/tmp`) and renamed into place. Neither side deletes.
- **`answer/` is bounded.** It is a memory filesystem with a size and an inode
  count, charged to the container's memory: a container that fills it gets
  "no space", and nothing it writes there can reach a disk.
- **Configuration is `config.json`** and nothing else. The platform sets no
  environment variable a service needs.

### `input/service.json`

```json
{
  "schemaVersion": 1,
  "contract": 1,
  "profile": { "name": "event-source", "version": 1 },
  "deployment": "prod-eu",
  "namespace": "payments",
  "instance": { "index": 0, "of": 2, "key": "prod-eu/payments/order-created/0", "run": "12-1" },
  "lanes": 4,
  "eventSource": { "id": "evs-4a1c9e0b7d2f6a35", "name": "order-created", "key": "prod-eu/payments/order-created", "delivery": "at-least-once" },
  "limits": {
    "statusSeconds": 30, "startupSeconds": 60, "stopSeconds": 20,
    "aliveSeconds": 10, "orphanSeconds": 60,
    "maxBodyBytes": 1048576, "maxAttributesBytes": 65536,
    "pollWaitSeconds": 20, "answerSeconds": 30, "settleSeconds": 30, "holdSeconds": 960
  },
  "paths": { "config": "/orkstack/input/config.json", "secrets": "/orkstack/input/secrets",
             "ask": "/orkstack/service/ask", "answer": "/orkstack/service/answer" }
}
```

`contract` and `profile` are the highest versions the image and its Orker
share. `instance.key` is the same on every start of this instance, and is
**not** unique among running containers: while an instance is replaced, two
carry it. `instance.run` is different for every start of any container.
`limits` holds every bound the Orker enforces and every deadline it keeps.

## The conversation

A **lane** is one strictly sequential conversation; a service has `lanes` of
them. On a lane one **exchange** happens at a time: a short fixed sequence of
**steps**, each one JSON file, and the **Orker always writes the first**.

Every step obeys these rules:

1. **Sequence number.** The Orker's first step of an exchange carries `seq`,
   which rises by one per exchange on that lane and starts at 1 on every
   start of the container. Every later step of the exchange carries the same
   `seq`. A file of an earlier `seq` is a leftover and is ignored.
2. **Written, then renamed.** No reader sees half a file.
3. **Schema version.** Every file carries `"schemaVersion": 1`. A reader
   refuses a higher version and ignores a field it does not know.
4. **Flat regular files.** Under `answer/` only `tmp/`, the lane directories
   and regular files at the profile's names exist. A link, a device, a
   socket, a FIFO, a directory, a file with more than one name or a file over
   its bound at a name the Orker reads is a contract violation.
5. **One closing step** ends an exchange; any later file of that `seq` is
   ignored by both sides.

**How the Orker reads.** It holds one handle on the `answer` mount, opened
once from the verified container process, and opens every file relative to
it with no symbolic link followed anywhere, no other filesystem crossed and
without blocking (`openat2` with `RESOLVE_BENEATH`, `RESOLVE_NO_SYMLINKS`,
`RESOLVE_NO_XDEV`, `RESOLVE_NO_MAGICLINKS`). It checks the open file is
regular, has one name and is within its bound, and copies its bytes once:
what it parses, hashes and uploads is that copy. It never lists or removes
anything under `answer/`.

**Deadlines.** Every step has one, and both sides know them all. A late step
is counted when its deadline passes, and its lane then waits for it (the Orker
starts nothing new there). Three exchanges in a row with a late step on one
lane end the attempt, and so does more than a quarter of the lanes, and at
least one, waiting past twice their deadline, or any lane waiting past the
profile's longest hold (`SERVICE_UNRESPONSIVE`). A step out of sequence or a
malformed file ends it `SERVICE_CONTRACT_VIOLATION`, which is not retried.

## Readiness and health

`answer/status.json`, written as soon as the container can say anything, then
at least every `statusSeconds`, and after each `check.json`:

```json
{ "schemaVersion": 1, "checkSeq": 7, "ready": true, "fatal": false,
  "check": { "ok": true, "reason": "", "field": "", "warnings": [] } }
```

- `ready`: the container can answer a first step; until then no exchange
  starts.
- `fatal`: it can never work with this configuration. The attempt ends
  `SERVICE_CONFIG_REJECTED`, not retried.
- `check.ok: false` with a `reason`: the service must not be used now. It
  stays up, and a later status can clear it. General reasons:
  `CONFIG_INVALID` (with `field`, at most 64 printable bytes),
  `NOT_AUTHORIZED`, `UNREACHABLE`; a profile adds its own, and its warnings.
- The status is read whether or not `ready`. No status within
  `startupSeconds` fails the attempt `SERVICE_NOT_READY`; a status not
  rewritten for three `statusSeconds` counts as no verdict.
- An image should not exit because something it depends on is unreachable:
  it reports `UNREACHABLE` and keeps trying.

## Liveness, stopping, and what an exit means

**The beat.** The Orker rewrites `ask/alive.json`
(`{"schemaVersion":1,"beat":412}`) every `aliveSeconds` with a higher number.
A service that has seen no new beat for `orphanSeconds` must end every open
exchange as its profile says for a stop, and exit. This is the image's duty:
nothing of OrkStack may be left on the host to stop it.

**The stop sequence**, within the 30-second cancellation grace:

1. The Orker starts no further exchange, writes `ask/stop.json`, and finishes
   its side of each open exchange.
2. SIGTERM. The container finishes or abandons each open exchange as its
   profile says and exits within `stopSeconds`.
3. No exit in time: SIGKILL, then removal.

A container not gone a minute after its SIGKILL ends the attempt
`SERVICE_STOP_STUCK`. The Orker keeps its reservation and tries to remove it,
stops writing its beat, and takes no further service until it is gone.

| The container | Ends |
|---|---|
| exits after SIGTERM, with any code | stopped; never the image's failure |
| exits `0` when nobody asked | `SERVICE_EXITED`, retried |
| exits non-zero when nobody asked | `SERVICE_FAILED` with the code, retried |
| is killed for memory | `SERVICE_OOM_KILLED`, retried |
| was stopped by its runtime from outside | `CONTAINER_STOPPED_BY_RUNTIME` |

Who retries, and with what back-off, is the service's owner's.

## Testing a service image

`orkctl service test <image>` runs a service image through the Orker's own
host code with no deployment: as this user's non-root uid, with network
`none`, the bounded answer filesystem, no swap and no core dumps. It checks
the labels and the interface, a first status within `startupSeconds`, the
profile's own exchanges, the stop sequence, and that a container whose beat
stops ends itself; it exits non-zero when a check fails. The checks of the
`event-source` profile, and the mandatory Bash fixture that passes them, are
in [EVENT_SOURCE_IMAGES.md](EVENT_SOURCE_IMAGES.md#testing-a-source-image).
