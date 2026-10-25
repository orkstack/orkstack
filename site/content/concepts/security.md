# Security model

Learn what an activity container can reach, what OrkStack refuses to run, and where the trust boundary sits.

```yaml
    secrets:
      - name: demo-token
        source: "env:ADVANCED_DEMO_TOKEN"
```

This excerpt is from [`examples/advanced-demo.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/advanced-demo.ork.yaml). The Orker reads the token from its own environment and gives the container a read-only file. The container never sees the source.

## The Orker is the boundary

The Orker is the only process that talks to the container runtime. It checks each attempt against the namespace's and the deployment's policies when it dispatches the attempt. An activity container gets no Temporal, storage or OrkStack credentials, unless you hand it a secret on purpose.

A container sees only what the Orker mounts. Its inputs are read-only, and its outputs go to `/orkstack/output/`. Its root filesystem is read-only, and it has no Linux capabilities. Pinned tasks also get `/orkstack/shared/`, which only their pin can see.

## What OrkStack refuses

OrkStack refuses to start a container in privileged mode, in a host namespace, or with an extra capability or device. It refuses any bind mount from the host, and the Docker socket. It accepts only two network modes, `default` and `none`. No task field can ask for any of these.

Every container also runs with `no-new-privileges`, the runtime's default seccomp profile, an init process, and a process limit of 512 by default. Its CPU and memory are always limited.

## Resources

A task that requests no resources gets 1 CPU and 1 GiB of memory by default. The Orker caps requests at 8 CPU and 16 GiB by default. A request above a cap fails the task. OrkStack never reduces it silently, so a task never runs with less than it asked for.

## Secrets

A task names each secret and its source in `secrets:`. The source is `env:NAME`, a variable in the Orker's environment, or `file:/path`, a file the Orker reads. The Orker reads the value when it dispatches the attempt, and writes it to a read-only file under `/orkstack/input/secrets/`.

A secret value does not appear in the Orkflow, the logs, the archive or `task.json`. Two lists must both allow a source: the Orker's floor, set by `ORK_ORKER_SECRET_ALLOW`, and the namespace's `--allow-secret` list.

An `env:` value in a task is configuration. It is stored in the Orkflow, so put credentials in `secrets:`, never in `env:`.

## Callers and the principal

OrkStack does not authenticate callers. Your gateway does. The gateway may assert a caller's name in the `Ork-Principal` header, and OrkStack records that name as asserted. It never presents the name as a verified identity.

Trigger tokens and await callback tokens are shown once. OrkStack stores only a salted hash of each.

## What this does not cover

- Who may reach the API, and what they may do. That is your gateway's job. Listening on loopback, as the quickstart does, is not a login: any program on the machine, or a web page using DNS rebinding, can reach it.
- Encryption. The engine serves plain HTTP. Encrypt the connections to Temporal and object storage yourself.
- Network policy. Egress from the `default` network is not filtered.
- What a workload does with a secret it was given.
- How strong the container isolation is. This page makes no claim about it.

## See also

- [docs/SECURITY_MODEL.md, Who holds what](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#who-holds-what): what each component holds, and what it never holds.
- [docs/SECURITY_MODEL.md, What the Orker refuses](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#what-the-orker-refuses): the refusals, and the container profile.
- [docs/SECURITY_MODEL.md, Secrets](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#secrets): sources, lists and refusals.
- [docs/SECURITY_MODEL.md, What this model does not cover](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#what-this-model-does-not-cover): the areas left to the deployment.
