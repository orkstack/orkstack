# Event source images

A **source image** is the container an [event source](EVENT_SOURCES.md) runs
to read its broker. It is a service image of the
[Orker Service Contract](ORKER_SERVICE_CONTRACT.md) whose profile is
`event-source`, and this page is that profile: what a source container and
its Orker exchange, how to build and declare one, and how to test it. What
every service shares — the two mounts, how the Orker reads a file, deadlines,
the status, the beat, stopping and exits — is the contract's and is not
repeated here.

No release yet starts an instance of a source: the Orker side and
`orkctl service test` are in place, and the engine side comes with a later
release.

## The shape

- The container is started once per instance and runs until its Orker stops
  it.
- On each lane the Orker asks for one message (a **poll**). The container
  answers with one message or with "nothing". For a message the Orker later
  writes a **verdict**; the container applies it at its broker and says how
  that went (**settled**).
- The container never hands over a message nobody asked for, and never
  settles one without a verdict, with one exception it is told the time of:
  the hold bound.

The conversation is a poll so that a rate limit holds no lease and spends no
delivery, a source that waits holds nothing, at most one message per lane
exists, and one lane is a plain loop in Bash. The container is long-lived so
that it can keep a lease while a start is slow, and so that a message costs
no container start.

## Labels and the interface

| Label | Value |
|---|---|
| `org.orkstack.service.contract` | `1` |
| `org.orkstack.service.profile` | `event-source/1` |
| `org.orkstack.service.interface` | the packed `kind: ServiceInterface` document (optional) |

The interface is authored beside the Dockerfile, by convention
`service.ork.yaml`, and packed into the label with
`orkctl activity interface pack`:

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
  ordering: false
  dead_letter: true
  extends_leases: true
```

| `event_source:` key | Meaning | Absent means |
|---|---|---|
| `delivery` | `at-least-once`: a message not acknowledged is delivered again. `at-most-once`: it is not, and a message not accepted is lost | `at-least-once` |
| `max_concurrency` | the most lanes one container serves, 1 to 64 | `1` |
| `ordering` | messages can carry a `group` | `false` |
| `dead_letter` | the broker stops delivering a message after a maximum, which the status reports | `true` for at-least-once |
| `extends_leases` | the image keeps a message leased until its verdict | `true` for at-least-once |

An at-least-once image must declare `dead_letter` and `extends_leases`; one
that declares otherwise is refused when a source naming it is written
(`EVENT_SOURCE_IMAGE_UNSUPPORTED`). A source's own `delivery:` must equal its
image's, and its `concurrency` may not exceed `max_concurrency`.

**The image runs as an arbitrary non-root uid**, the Orker's own, with no
network but the one its source host gives it and the bounded answer
filesystem ([ORKER_SERVICE_CONTRACT.md](ORKER_SERVICE_CONTRACT.md#a-service-image)).

## The context

`input/service.json` is the contract's, with the source's block and the
profile's limits:

```json
{
  "schemaVersion": 1,
  "contract": 1,
  "profile": { "name": "event-source", "version": 1 },
  "instance": { "index": 0, "of": 2, "key": "prod-eu/payments/order-created/0", "run": "12-1" },
  "lanes": 4,
  "eventSource": { "id": "evs-4a1c9e0b7d2f6a35", "name": "order-created", "key": "prod-eu/payments/order-created", "delivery": "at-least-once" },
  "limits": {
    "statusSeconds": 30, "startupSeconds": 60, "stopSeconds": 20,
    "aliveSeconds": 10, "orphanSeconds": 60,
    "maxBodyBytes": 1048576, "maxAttributesBytes": 65536,
    "pollWaitSeconds": 20, "answerSeconds": 30, "settleSeconds": 30, "holdSeconds": 960
  }
}
```

`eventSource.key` is the same for every instance of one source and for no
other source of any deployment: use it to name a shared membership at the
broker (a NATS queue group). `instance.key` adds the instance's index.
Neither is unique among running containers; `instance.run` is.

## A lane's exchange

```text
Orker (ask/lanes/k/)                    container (answer/lanes/k/)
  │ poll.json (seq n) ─────────────────▶ │  receive one message, waiting up to waitSeconds
  │ ◀──────────── empty.json (seq n)     │  nothing came, or the receive failed: closes the exchange
  │ ◀──────────── body, then             │  one message, kept leased
  │               message.json (seq n)   │
  │ verdict.json (seq n) ──────────────▶ │  ack | release | reject
  │ ◀──────────── settled.json (seq n)   │  how the broker answered: closes the exchange
```

| Step | Writer | Deadline for the step after it |
|---|---|---|
| `poll.json` | Orker | answered within `answerSeconds` (the poll's `waitSeconds` plus 10 s) |
| `body`, then `message.json`; or `empty.json` | container | for a message: the Orker's verdict within `holdSeconds` less a minute |
| `verdict.json` | Orker | settled within `settleSeconds` |
| `settled.json` | container | — |

Every file has the fixed name above, is written under `answer/tmp` and
renamed into place, and is replaced by the next exchange's; none is ever
deleted.

### `poll.json`

```json
{ "schemaVersion": 1, "seq": 42, "waitSeconds": 20, "leaseSeconds": 60 }
```

`leaseSeconds` is the lease the Orker would like: use it where the broker
lets a consumer choose (an SQS visibility timeout), and ignore it where the
broker's own object decides (a JetStream consumer's acknowledgement wait).

### `message.json` and `body`

```json
{
  "schemaVersion": 1,
  "seq": 42,
  "id": "jetstream:ORDERS:48213:1762853400123456789",
  "bodyBytes": 1832,
  "subject": "orders.eu.created",
  "attributes": { "Ce-Type": ["com.acme.order.created"] },
  "binaryAttributes": {},
  "publishedAt": "2026-11-11T09:30:00Z",
  "deliveryCount": 1,
  "group": "",
  "broker": { "stream": "ORDERS", "streamSequence": "48213" }
}
```

| Field | Rule |
|---|---|
| `id` | **required.** The same on every delivery of the message and different for every other message the source will ever hand over; at most 256 bytes, no control character. Where the broker's id is unique only inside one object, put the object's name in front |
| `bodyBytes` | the size of `body`; the Orker checks its copy against it |
| `subject` | at most 1 KiB of UTF-8; `""` when the broker has none |
| `attributes` | a name to every value it was sent with, in order; names at most 256 bytes; names and values together at most `maxAttributesBytes` |
| `binaryAttributes` | a name to base64 |
| `publishedAt` | the broker's own time, RFC 3339; absent when unknown |
| `deliveryCount` | the broker's count, 1 on the first delivery; 0 when the broker cannot say |
| `group` | the broker's ordering group; `""` when unordered |
| `broker` | at most 16 short string facts, at most 1 KiB in all |
| `oversizeBytes` | instead of `bodyBytes` when the payload is over `maxBodyBytes`: its size, and no `body` is written. The message is rejected `MESSAGE_TOO_LARGE` |

`body` is the exact payload, never decoded or re-encoded, and is renamed into
place **before** `message.json`: `message.json` is the marker that the
message is complete. Receive one message per poll, never a batch the
container then holds.

### `empty.json`

```json
{ "schemaVersion": 1, "seq": 42 }
```

```json
{ "schemaVersion": 1, "seq": 42, "error": { "kind": "transient", "code": "timeout" } }
```

The first is a quiet poll. The second is a receive that failed: `transient`
(the broker did not answer), `refused` (it answered no), or `local` (the
container received a message and could not hand it over for a reason of its
own, and released it). A `local` failure is counted against the image, never
the broker: three in a row on a lane end the attempt `SERVICE_UNRESPONSIVE`.

### `verdict.json`

```json
{ "schemaVersion": 1, "seq": 42, "verdictId": "42-9f3c01ab7d2e4c58a1b0e6f3d9c27a14", "action": "ack", "delaySeconds": 0 }
```

`verdictId` names this verdict: fresh for each one, with 128 random bits, so
no image can make it up without reading it. The `settled.json` that answers
it carries it back, so a settle the Orker did not ask for is recognised
whenever it lands.

| `action` | The container must |
|---|---|
| `ack` | tell the broker the message is done, and wait for its answer |
| `release` | give the message back for another delivery after `delaySeconds` |
| `reject` | give it back, after `delaySeconds`, in the way that lets the broker's own maximum dead-letter it. Never delete it, never terminate it |

For an at-most-once image `release` and `reject` both mean "forget it".

### `settled.json`

```json
{ "schemaVersion": 1, "seq": 42, "verdictId": "42-9f3c01ab7d2e4c58a1b0e6f3d9c27a14", "ok": true }
```

```json
{ "schemaVersion": 1, "seq": 42, "verdictId": "42-9f3c01ab7d2e4c58a1b0e6f3d9c27a14", "ok": false, "error": { "kind": "refused", "code": "AccessDenied" } }
```

A settle that answers a verdict carries that verdict's `verdictId`, exactly
as written. Only the container's own close at the hold bound
(`hold_expired`) carries none, and it is taken only once `holdSeconds` have
passed. A settle of the exchange with another id, with none, or closing it
before its bound ends the attempt `SERVICE_CONTRACT_VIOLATION`: the Orker
never records it as the verdict's outcome. The id proves that the container
read the verdict before it settled, and nothing of what the broker did: that
stays the image's word.

| `error.kind` | Means |
|---|---|
| `lease_lost` | the lease had ended; the broker will deliver the message again. Nothing is counted against the source |
| `transient` | the broker did not answer |
| `refused` | the broker answered no; repeated refusals pause the source |
| `hold_expired` | no verdict came within `holdSeconds`, and the container released the message itself |

`code` is the broker's own short code, at most 64 bytes of printable ASCII,
shown as the image's word.

## The status's `broker` block

`answer/status.json` is the contract's, and this profile adds a `broker`
block and its own reasons:

```json
{
  "schemaVersion": 1, "checkSeq": 7, "ready": true, "fatal": false,
  "check": { "ok": true, "reason": "", "field": "", "warnings": ["DELIVERY_LIMIT_LOW"] },
  "broker": {
    "object": "jetstream:ORDERS/payments-orders",
    "leaseSeconds": 60, "maxDeliveries": 10, "messageLimitBytes": 1048576,
    "retentionSeconds": 604800, "pending": 12, "unacknowledged": 1,
    "redelivered": 0, "waitingPulls": 2,
    "deadLetter": { "name": "", "depth": -1 }, "dropped": 0
  }
}
```

| Field | Rule |
|---|---|
| `check.reason` | the contract's, plus `OBJECT_MISSING`, `OBJECT_INVALID` (with `field`), `NO_DEAD_LETTER`, `MESSAGE_LIMIT` |
| `check.warnings` | `DELIVERY_LIMIT_LOW`, `PULL_LIMIT_LOW`, `MESSAGE_LIMIT_ABOVE_BOUND` |
| `broker.object` | a canonical name for what the source reads, at most 256 bytes |
| `broker.maxDeliveries` | the broker's delivery limit; required when `check.ok` for an at-least-once image. **Only 2 to 100 is accepted**: anything else is treated as `OBJECT_INVALID` |
| `broker.messageLimitBytes` | the largest message the object can deliver; required when `check.ok`. An object with no limit the image can read is `MESSAGE_LIMIT` |
| `broker.retentionSeconds` | how long an unconsumed message is kept; 0 when by size only or unknown |
| every count | `-1` when the broker cannot say. `dropped` is an at-most-once image's count of messages it received and never handed over |

While `check.ok` is false the Orker writes no poll. An image that finds its
configuration can never work (a subject it may never subscribe to, a
credential kind it does not know) says `fatal`, and the source is paused with
that reason instead of being started again and again. Everything in the
`broker` block is the image's word; a message handed over again with the same
`id` and a `deliveryCount` that did not rise is counted by the Orker as
suspect.

## Leases and the hold bound

From hand-over to verdict the container extends the message's lease every
third of it; a lease that was lost shows as `lease_lost` when the verdict is
applied.

The Orker gives a verdict within **one try plus two parks** of the hand-over:
5 minutes for one attempt at accepting, the source's park (5 minutes by
default) for a refusal to clear, and one more park for a required receipt
that object storage refuses — 15 minutes by default. `holdSeconds` is that
plus a minute (960). Past `holdSeconds` without a verdict the container
releases the message itself and writes `settled.json` with `hold_expired`,
which closes the exchange; a verdict written at the same moment is ignored by
both sides, and the receipt says `MESSAGE_RELEASED_HOLD_EXPIRED` unless a run
was already started for the message, which keeps its accepted receipt. A
receipt is written before its verdict, so a close that crosses one leaves the
decided receipt and this one, and this one says what happened. An Orker that
hangs therefore cannot keep a message leased for ever, and a container that
keeps the bound is never called a violator for it.

## Stopping, and a lost beat

1. The Orker writes no further poll and writes `ask/stop.json`.
   - **At-least-once:** for each lane with a message in hand the Orker writes
     the verdict it has, or `release`.
   - **At-most-once:** on the notice the container stops taking messages and
     keeps what it buffered; the Orker polls with a wait of zero until every
     lane answers `empty.json` or 10 seconds pass. What is still buffered is
     lost and counted in the last status's `broker.dropped`.
2. On SIGTERM the container answers a pending poll with `empty.json`,
   releases a message it receives in that instant, applies every verdict
   already written, releases every message still without one at the broker
   and writes no `settled.json` for it (there is no verdict to name; the exit
   ends the exchange), lets settlements in flight finish, and exits `0`.
3. After a SIGKILL, what the container held returns when its leases expire.

Each release spends one delivery count at the broker.

**A lost beat** is a stop nobody will finish: when `ask/alive.json` has not
changed for `orphanSeconds`, the container releases what it holds, with no
`settled.json` either, stops taking messages and exits. It is what keeps a
container whose Orker was killed from holding a share of a queue and dropping
it in silence, and it is the image's duty: nothing of OrkStack is left on the
host to end it.

## Limits

| Limit | Value | Over it |
|---|---|---|
| body | the source's `max_bytes` (`limits.maxBodyBytes`) | `oversizeBytes`, rejected `MESSAGE_TOO_LARGE` |
| attributes | 64 KiB | rejected `MESSAGE_ATTRIBUTES_TOO_LARGE` |
| lanes | the source's `concurrency`, at most the image's `max_concurrency` | refused when the source is written |
| messages | one per lane | a second with the current `seq` is `SERVICE_CONTRACT_VIOLATION` |
| `answer/` | `lanes × (2 × maxBodyBytes + 256 KiB)` plus 1 MiB, at most `orker.event_sources.spool_bytes` | "no space", which a container that keeps the contract never meets |
| rate | the source's `rate`, applied by the Orker before a poll | no poll is written |

The answer filesystem holds **two** bodies per lane because a lane's next
`body` is written beside the last before it replaces it. It is charged to the
container's memory, so a source's `resources.memory` must cover it.

## What an image asserts, and what it cannot do

OrkStack cannot see the broker. These are the image author's assertions, as
`cache:` is an Orkflow author's:

- it acknowledges only on an `ack` verdict;
- `id` is the same on every delivery of a message and never shared;
- `deliveryCount`, `maxDeliveries` and every other broker fact are the
  broker's;
- `reject` leaves the message to the broker's dead-letter policy;
- it ends itself when its Orker's beat stops.

Whatever it asserts, an image cannot start a run (only the engine does),
choose a namespace, Orkflow, version, pool or secret, exceed the body,
attribute or input bounds or the source's rate, make its Orker read or write
outside the two conversation mounts, or fill a disk. What it can reach over
the network is its source host's, and the operator's decision
([SECURITY_MODEL.md](SECURITY_MODEL.md#service-containers)).

## Testing a source image

```bash
orkctl service test registry.example.com/acme/my-source:1 --publish-exec ./publish-one.sh
```

`orkctl service test` runs the image through the Orker's own host code, with
no deployment: as this user's non-root uid, with network `none`, the bounded
answer filesystem, no swap and no core dumps. The image is pinned by digest
first, so push it to a registry, as for `orkctl activity test`. The engine's
side is played by an in-process door that accepts every message.

| Check | How |
|---|---|
| labels and interface | the image's labels are read and the interface parsed |
| a status within the startup bound, ready and healthy | observed |
| a quiet poll is answered `empty.json` within the wait | a poll |
| a message is handed over and acknowledged | with `--publish-exec`: a poll, an `ack`, then a second poll that must not deliver it again |
| a released message is delivered again, counted | with `--publish-exec`: a `release`, then a poll that must return the same `id` with a higher `deliveryCount` |
| stop: the notice, SIGTERM, an exit within `stopSeconds` | a stop |
| a lost beat: the container ends itself | a second container whose beat stops |

`--publish-exec CMD` runs a command that publishes one message to the
image's broker. It is given `ORK_SERVICE_ROOT`, the container's root as the
host sees it, so a broker that lives inside the container (as the Bash
fixture's does) can be written to. Without it the message checks are
reported skipped, never passed. `--keep` keeps the workspace for inspection,
and `--startup` changes how long the image may take to write its first
status (60 s by default). It exits non-zero if any check fails; `--json`
prints the report as JSON.

The status is judged as an Orker judges it: a status whose `check.ok` is
true but whose `broker` block has no accepted `maxDeliveries` (2 to 100) or
no `messageLimitBytes` reads as `OBJECT_INVALID` or `MESSAGE_LIMIT`, and a
status that is still not ready and healthy when the startup bound ends
fails the status check with its reason. A `settled.json` written before the
Orker gave a verdict, or one that does not name the verdict it answers,
fails the message check: an image never acknowledges on its own.

It cannot prove an image's behaviour at a real broker: a message check runs
against whatever `--publish-exec` reaches.

## A source in Bash

The mandatory fixture is
[`test/fixtures/source-bash`](../test/fixtures/source-bash/entry.sh): one
lane, Bash, `jq` and coreutils, no SDK, no watcher and no background job. Its
"broker" is a directory under the container's own `/tmp`: a file in `ready/`
is a message, moving it to `held/` is the lease, `done/` the
acknowledgement, `dead/` the dead letter, and `count/<id>` its delivery
count. It keeps the whole profile — a delivery count that rises, a dead
letter at the limit, a status rewritten on time and after a check also while
it holds a message, a pending poll answered at a stop, the hold bound and the
beat — and passes every check above:

```bash
docker build -t localhost:5000/source-bash:dev test/fixtures/source-bash
docker push localhost:5000/source-bash:dev
orkctl service test localhost:5000/source-bash:dev \
  --publish-exec 'b=$ORK_SERVICE_ROOT/tmp/broker; echo hello > "$b/m1" && mv "$b/m1" "$b/ready/m1"'
```

The publish command is run with `sh -c`, and writes the message beside
`ready/` before renaming it in, so the fixture never takes a half-written
file.

The same source is written in Go
([`test/fixtures/source-go`](../test/fixtures/source-go/main.go), the
standard library only) and in Python
([`test/fixtures/source-python`](../test/fixtures/source-python/source.py)),
and passes the same checks. Beside them,
[`test/fixtures/source-hostile`](../test/fixtures/source-hostile/entry.sh)
is the Bash source with one thing a contract-keeping image never does,
chosen by `--build-arg MODE=`: it hands over a symbolic link, never answers
a poll, ignores SIGTERM or the beat, reports a delivery limit of a billion,
or settles before a verdict or without naming one, and fails the check about it; or it fills its
answer filesystem, or nests directories in it, until "no space", and then
passes every check, which shows the filesystem's bound held.

A real broker's image changes two things in that loop: it extends the lease
while it waits for the verdict, and it calls the broker in place of `mv`.
More lanes are more copies of the loop.
