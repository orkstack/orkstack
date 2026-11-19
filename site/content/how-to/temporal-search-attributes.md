# Add Temporal search attributes

OrkStack asks Temporal to index a few run fields, so run lists filter in the cluster instead of reading every run. Each engine adds them at boot. Add them yourself when the cluster does not let the engine.

## Check what the engine found

Look for this line in an engine's log at boot:

```text
INFO connected to temporal … run_namespace_index=true run_origin_index=true run_root_index=true run_paused_index=true concurrency_index=true
```

Every `true` is a group the cluster indexes. A `false` comes with a warning that names the missing attribute and what it costs:

```text
WARN visibility does not index a run's orkflow namespace …
```

The engine still starts. Without the index, run lists and counts read through every run instead and are slower, and one that cannot finish fails rather than returning a short answer.

## Add them by hand

Use your `temporal.namespace` in place of `ork-main`:

```bash
temporal operator search-attribute create --namespace ork-main --name OrkflowNamespace --type Keyword
temporal operator search-attribute create --namespace ork-main --name OrkflowOriginKind --type Keyword
temporal operator search-attribute create --namespace ork-main --name OrkflowOriginId --type Keyword
temporal operator search-attribute create --namespace ork-main --name OrkflowOriginRootRunId --type Keyword
temporal operator search-attribute create --namespace ork-main --name OrkflowRunPaused --type Keyword
temporal operator search-attribute create --namespace ork-main --name OrkflowGatePhase --type Keyword
temporal operator search-attribute create --namespace ork-main --name OrkflowRunQueued --type Keyword
temporal operator search-attribute create --namespace ork-main --name OrkflowConcurrencyHeld --type KeywordList
temporal operator search-attribute create --namespace ork-main --name OrkflowConcurrencyQueued --type KeywordList
```

Then restart the engines, so each one finds them. On Temporal Cloud, which OrkStack does not support yet, the command is different: see DEPLOY.md below. Check with:

```bash
temporal operator search-attribute list --namespace ork-main
```

## Run without admin rights

On a cluster another team runs, ask its admin to create the namespace with a retention of at least four days (`--retention 96h`) and the attributes above. The engine then needs no admin rights. It takes its maximum run retention from that retention, and refuses to start, naming what to set, when the retention is too short. DEPLOY.md lists the exact permissions.

## Mind the limits

Temporal limits custom attributes per namespace. A SQL visibility store (PostgreSQL, MySQL, SQLite) takes 10 Keyword and 3 KeywordList attributes. OrkStack uses seven Keyword and two KeywordList, leaving three and one for your own.

## Never remove one while runs are open

A run that was started while `OrkflowRunPaused` existed stops at its next pause or unpause if the attribute is removed, until it is added again.

## See also

- [DEPLOY.md, "Temporal search attributes"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#temporal-search-attributes): every attribute, what it is for, what happens without it, Temporal Cloud and the limits per store
- [DEPLOY.md, "Running without Temporal admin rights"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-without-temporal-admin-rights): what the cluster admin creates, the permissions the engine needs, and the boot lines that confirm it
- [LIMITATIONS.md, "Engine, cluster and gateway"](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md#engine-cluster-and-gateway): the paused-run index, and Temporal Cloud
