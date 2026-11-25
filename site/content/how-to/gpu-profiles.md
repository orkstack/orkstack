# Give a task GPU units

Let a task run on the GPUs of an Orker's host. The Orker's operator defines a GPU profile once, through the Container Device Interface (CDI); a task asks for a number of units and selects the profile by name.

## Define the profile on the Orker

On the GPU host, write a CDI spec for the GPU as root. This one is for an AMD GPU; the repository has the Intel spec and the steps to find each render node:

```yaml
cdiVersion: 0.7.0
kind: amd.com/gpu
devices:
  - name: gpu0
    containerEdits:
      deviceNodes:
        - path: /dev/dri/renderD128
          hostPath: /dev/dri/renderD128
          type: c
          permissions: rw
        - path: /dev/kfd
          hostPath: /dev/kfd
          type: c
          permissions: rw
      additionalGids:
        - 110
```

Then add the profile to the Orker's configuration, and restart the Orker:

```yaml
orker:
  profiles:
    amd:
      kind: gpu
      cdi:
        kind: amd.com/gpu
        units:
          - gpu0
```

- `units` names the devices of the spec's kind, in order. Each unit is exclusive: a task holds the units it asks for while it runs, and no other task holds them.
- The Orker checks its devices with its own release image, which it pulls by digest and verifies at boot. Set `probe_image` only on a host that cannot reach ghcr.io, or on an Orker you built from source.
- The AMD spec's `/dev/kfd` is shared by every AMD GPU on the host, so one host holds one AMD unit.

## Ask for units in a task

```yaml
apiVersion: orkstack/v1
kind: Orkflow
name: encode
tasks:
  - id: encode
    uses: ghcr.io/acme/encode@sha256:...
    pool: gpu-amd
    devices:
      - amd
    resources:
      gpu: 1
```

- `devices:` names the profile, and `resources.gpu` is how many units the task holds, from 1 to 64.
- A task that names the profile without `resources.gpu` fails `GPU_COUNT_REQUIRED` before it starts, and is not retried.
- A pinned task cannot hold units in this release.

## What submit checks

| Refusal | Meaning | What to do |
|---|---|---|
| `PROFILE_UNAVAILABLE` | An Orker of the pool does not offer the profile, or no Orker answered | Define the profile on every Orker of the pool |
| `RESOURCES_UNSATISFIABLE` | `resources.gpu` is more than the pool's GPU cap, the lower of `orker.resources.max_gpu` and the unit count | Ask for fewer units, or raise `max_gpu` and add units |

## When a unit or the spec fails

- A unit that fails its probe leaves service until it passes three probes. Runs wait while fewer units are usable than the cap, and submit still accepts them until their own `timeout:`. Set `max_gpu` below the unit count, for example `1` on two units, so that one unit's fault does not stop GPU work.
- A spec that changes under a running Orker puts the profile into drift. The Orker takes no GPU work until it restarts, and an attempt that meets the drift runs again on another Orker of the pool without spending one of its attempts.

## Limits

- **Linux hosts only.** The Orker must run on the container daemon's own host, as for a device profile.
- **Memory is not cleared.** OrkStack does not clear a unit's memory between the tasks that hold it.
- **One GPU profile per Orker, one vendor per host at a time.** Every GPU Orker of a host shares one lock.
- **NVIDIA is experimental.** An `nvidia.com/gpu` spec is accepted from toolkit 1.19.1, generated with the flags DEPLOY.md gives; it is proven by emulation and not on NVIDIA hardware.

## See also

- [DEPLOY.md, "Running a GPU Orker"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-gpu-orker): the host lock, the CDI specs, capacity, and what an unavailable or drifted profile looks like
- [DEPLOY.md, "Changing a profile"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#changing-a-profile): rolling a change across a pool
- [DSL.md, "Device profiles"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#device-profiles): the `devices:` key, `resources.gpu` and their rules
- [SECURITY_MODEL.md, "CDI trust"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#cdi-trust): what a spec may carry and which directories are read
- [SECURITY_MODEL.md, "What a unit does not guarantee"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#what-a-unit-does-not-guarantee): what a unit does not isolate
