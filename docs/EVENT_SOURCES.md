# Event sources

An **event source** binds one Orkflow to one **source image**: an OCI image
that reads a broker (NATS, SQS, anything its author supports), so that each
message it receives starts one run of the Orkflow. It is the fourth way runs
start, beside a submit, a [schedule](ORKFLOW_SCHEDULES.md) and a
[trigger](ORKFLOW_TRIGGERS.md).

**In this release no source consumes anything.** A source can be created,
read, changed, paused, deleted and evaluated against a message you supply,
and its document is all there is of it: no instance runs and no message is
read. A later release runs sources on Orkers. No first-party source image
ships yet: a source names an image by its registry reference.

## Before you start

Event sources are off by default. Three things must hold for a source to be
written:

1. `event_sources.enabled: true` on every engine
   ([DEPLOY.md](DEPLOY.md), §1). Off, every write but a delete is refused
   `EVENT_SOURCES_DISABLED` and a source reads `DISABLED`.
2. The deployment runs as a release: `ORK_BUILD_ID` set on `ork` and
   `orker`, and the release promoted (`orkctl release promote`). Without
   it every write but a delete is refused
   `EVENT_SOURCES_REQUIRE_VERSIONING`.
3. Callers are authenticated (`auth.mode: enforce`,
   [AUTHENTICATION.md](AUTHENTICATION.md)). Otherwise every request of the
   event-source API, reads included, is refused
   `EVENT_SOURCES_REQUIRE_AUTHENTICATION`. An operator whose gateway is the
   authentication may set `event_sources.allow_asserted: true`, which lets
   sources be written, read and evaluated by whoever the gateway lets
   through, is logged as a warning at every boot, and never lets an
   instance run.

## The object

A source lives in one namespace, under a name fixed at creation, and binds
an Orkflow of that namespace, also fixed at creation. Its keys are the
`event_sources` entry's ([AUTOMATION.md](AUTOMATION.md#event_sources),
which lists each with what its absence means):

```yaml
apiVersion: orkstack/v1
kind: Automation
namespace: payments
event_sources:
  order-created:
    orkflow: handle-order
    version: "1.4.0"
    uses: ghcr.io/acme/source-orders:2
    with:
      stream: ORDERS
    instances: 2
    format: json
    match:
      - attribute: Ce-Type
        equals: com.acme.order.created
      - pointer: /detail/status
        in:
          - PAID
          - SHIPPED
    input:
      mode: live
    map:
      order_id:
        pointer: /detail/order_id
      region:
        attribute: Ce-Region
```

A source has an id, `evs-` and 16 hexadecimal digits. A source created
again under a name gets a new one.

Each field is one of two kinds. A change to a **service** field (`uses`,
`with`, `env`, `secrets`, `resources`, `pool`, `concurrency`, `delivery`)
raises the source's service revision: it changes what the source's
containers are. A change to a **judging** field (`version`, `format`,
`max_bytes`, `match`, `input`, `map`, `artifacts`, `rate`,
`reject_breaker`) raises its judging revision: it changes only how a message
is judged. `description` and `instances` raise neither.

### The image

`uses:` names the source image by registry reference. It is resolved to a
digest when the source is written, stored as written, and resolved again
only when its text changes: a tag that moves in its registry changes
nothing. The image is held to the deployment's and the namespace's image
policy ([DEPLOY.md](DEPLOY.md#image-supply-chain-policy)).

A source image carries the labels of the
[Orker Service Contract](ORKER_SERVICE_CONTRACT.md), and runs as an arbitrary
non-root uid ([EVENT_SOURCE_IMAGES.md](EVENT_SOURCE_IMAGES.md) says how to
build and test one):
`org.orkstack.service.contract` (the contract versions it implements, `1`),
`org.orkstack.service.profile` (`event-source/1`), and optionally
`org.orkstack.service.interface`, its declared interface, packed from an
authored `kind: ServiceInterface` document. An image without the first two,
or of versions this release does not host, is refused
`EVENT_SOURCE_IMAGE_UNSUPPORTED`. Where the image declares an interface,
`with:` is checked against it: a required key missing, an undeclared key or
a value of another type is refused `EVENT_SOURCE_CONFIG_INVALID`, naming the
key. The source's `delivery:` must be the image's class
(`EVENT_SOURCE_DELIVERY_MISMATCH`), and its `concurrency` at most the
image's maximum. `orkctl event-source images REFERENCE` shows what an image
declares.

The names `nats-jetstream`, `nats` and `sqs` are reserved for first-party
source images and are refused until those images ship.

### Secrets and resources

`secrets:` are declared as a task's (`name`, `source`), held to the
namespace's `allowed_secrets`. A source's own `identities:`, `credentials:`
and secrets through a grant are refused `EVENT_SOURCE_IDENTITY_UNSUPPORTED`
until task identity is available. `env:` is configuration, never a secret.

`resources.memory` must cover a message in hand on every lane:
`2 × concurrency × max_bytes + 32 MiB`. Less is refused, with the arithmetic.

## Mapping

Three keys fill the Orkflow's declared inputs, and nothing else of a message
enters a run's input:

| Key | Fills | From |
|---|---|---|
| `input:` | value inputs, with constants | a YAML mapping, converted by the strict mapping ([JSON.md](JSON.md)) |
| `map:` | value inputs, from the message | one selector per input: `pointer:` (an RFC 6901 JSON Pointer into the body, `format: json` only), `attribute:` (a header or attribute, its first value), or `field:` (`subject`, `id`, `published_at`, `group`, facts the broker supplies) |
| `artifacts:` | artifact inputs | `body` or `metadata`, each a file the run reads as `_inputs/<name>`; the run, run again, receives the same files |

A name appears under one key only; a `map:` or `input:` name is a value
input, an `artifacts:` name an artifact input; every artifact input is
bound; `attribute:` and `field:` fill only `type: string` inputs; a constant
has its input's declared type; and every required input without a default
is covered. A source that breaks one of these is refused
`EVENT_SOURCE_MAPPING_INVALID`, naming the field. For each message, a
selector that selects nothing or `null` leaves out an input that has a
default or is not required and otherwise rejects the message
(`MESSAGE_FIELD_MISSING`); a value of another JSON type rejects it, and so does a number a run's input
would change, one a double does not hold exactly, such as an integer past
2^53: send it as a string (`MESSAGE_FIELD_TYPE`).

A source cannot bind an Orkflow with an await whose callback nobody would
receive, nor, yet, one whose tasks ask for identities or credentials:
`EVENT_SOURCE_ORKFLOW_UNSUPPORTED`.

## Filtering

`match:` is a list of tests, all of which must hold. Each has one selector
(`attribute:`, `pointer:` or `field: subject`) and one test: `equals:` a
scalar, `in:` a list of scalars, or `exists: true|false`. There are no
patterns, ranges, negation or OR.

A value equals a literal when it is the same JSON type and, for a string,
the same code points after JSON's escapes are undone (no normalization, no
case folding); for a number, the same mathematical value, compared as exact
decimals and never through a float, so `1`, `1.0` and `1e0` are equal and
`"1"` never equals `1`. A body that is not strict JSON, on a `format: json`
source, is a rejection (`MESSAGE_NOT_JSON`), never a test that failed.

## Bounds

A document is at most 64 KiB: 16 tests, 32 values in an `in:`, 256 bytes a
literal, 64 `map:` entries, 32 KiB of `with:`, 8 secrets, and constants
within `event_sources.max_input_bytes`. A message's body is at most the
source's `max_bytes` (at most `event_sources.max_message_bytes`), its
attributes 64 KiB in all, its identity 256 bytes with no control character,
and a body read as JSON nests at most 64 levels. Over a bound a message is
rejected, never truncated.

Quotas are checked when a source is written (`EVENT_SOURCE_LIMIT`, with the
arithmetic): `event_sources.max_sources` per deployment,
`max_per_namespace` per namespace, `max_instances` a source,
`namespace_instances` the instances of a namespace's sources, and
`namespace_rate` their `rate × instances`.
The quotas are read without a lock, so writes that race on several
engines can overshoot one by the writes in flight; a lock would be the
distributed lock OrkStack does not keep (AGENTS.md §2.1).

## Evaluate a message

```bash
orkctl event-source evaluate order-created --body-file order.json \
  --attribute Ce-Type=com.acme.order.created --subject orders.eu
orkctl event-source evaluate -f candidate.yaml --body-file order.json
```

An evaluation runs a stored source, or a candidate that need not be stored,
over the message given, in the engine, with the code a source host judges
by. It says whether the message would be accepted, filtered
(`MESSAGE_NOT_MATCHED`, naming the first test that did not hold) or
rejected (a `MESSAGE_*` reason), with each test's answer and the run input
it would start. It starts nothing and writes nothing. A namespace runs at
most two evaluations at once on an engine.

## Commands

| Command | Does |
|---|---|
| `orkctl event-source create NAME --orkflow O --uses IMAGE [--with K=V] [--file F]` | create a source |
| `orkctl event-source list [--filter …]` | a namespace's sources |
| `orkctl event-source get NAME` | the source and its state |
| `orkctl event-source update NAME [flags \| --file F] [--rebind]` | change it, under the version it read |
| `orkctl event-source pause NAME --reason TEXT`, `resume NAME` | pause and resume it |
| `orkctl event-source delete NAME` | delete it, by the id read under the name |
| `orkctl event-source evaluate` | evaluate a message (above) |
| `orkctl event-source images [REFERENCE]` | what a source image declares |

`--file` reads an event source document: it begins with
`apiVersion: orkstack/v1` and `kind: EventSource`, may name a `namespace:`
and a `name:`, and holds an `event_sources` entry's keys
([AUTOMATION.md](AUTOMATION.md)). It is read through the strict mapping:
anchors, aliases, merge keys and a plain scalar YAML readers resolve
differently are refused, and so is a document of another kind, named for
what it is. A document that names a namespace is acted in there, and `-n`
naming another is refused `EVENT_SOURCE_NAMESPACE_MISMATCH`, naming both; a
document with no namespace takes `-n`. A document that names a source must
agree with the command's `NAME`; one with no name takes it. Editors complete
it from [`eventsource.v1.json`](schemas/eventsource.v1.json).

```yaml
apiVersion: orkstack/v1
kind: EventSource
namespace: payments
name: orders
orkflow: handle-order
uses: ghcr.io/acme/source:1
with:
  stream: ORDERS
format: json
match:
  - pointer: /kind
    equals: created
```

`orkctl orkflow create` and `orkctl automation apply` refuse a
`kind: EventSource` document, in a directory too, naming the command that
reads it.

Every request is decided in the namespace the source names, at the Orkflow
it binds ([ACCESS.md](ACCESS.md)): `eventsource.read` for the reads and
for evaluating a stored source, and `orkflow.read` beside it for evaluating
a candidate, whose answer shows its Orkflow's inputs; `eventsource.write`
(with `run.submit` on the Orkflow) for create and update, and alone for
delete and for `orkctl event-source images`, which makes the engine fetch
from a registry with its own credentials, the namespace's image policy
checked first and at most 30 a minute a namespace on an engine; and
`eventsource.operate` for pause and resume (a resume with `run.submit`).
A delete of a source this engine cannot read (`orkctl event-source delete
NAME --id ID`) is decided at its namespace. An update whose source names
another Orkflow than the stored one is refused
`EVENT_SOURCE_NAMESPACE_MISMATCH`, naming both.

A delete names the source by its id, on every attempt. A name that now
holds another source is refused `EVENT_SOURCE_ID_MISMATCH`, naming the id it
holds, and nothing is deleted. `orkctl event-source delete` and `orkctl
automation apply` send a delete whose answer is not known again with the
same id, five attempts in all, and report a delete answered `NOT_FOUND`, or
never answered, as not finished, never as gone. A namespace that holds a
source cannot be deleted.

## Who bound a source

Creating a source makes its writer its **binder**, and so does an update
that changes what it submits (`version`, `input`, `map`, `artifacts`) or
says `--rebind`. A change of `uses:`, `with:` or a secret changes neither the
binder nor the consent, and a resume does neither. `--rebind` keeps the
consent, and `--consent=DIGEST` records one without changing the binder
([TASK_IDENTITY.md](./TASK_IDENTITY.md#consent)). A source's binder, and
whether it was verified or asserted, are shown by `orkctl event-source get`.
