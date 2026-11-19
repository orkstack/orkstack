# Give a task a USB serial device

Let a task open a device plugged into an Orker's host, such as a USB serial adapter. The Orker's operator defines a device profile once; an Orkflow selects it by name.

## Define the profile on the Orker

Add the profile to every Orker of the pool, in its configuration file, and restart each one:

```yaml
orker:
  profiles:
    probe_image: ghcr.io/orkstack/orker@sha256:<digest of this release's orker image>
    sensor:
      kind: devices
      devices:
        - path: /dev/serial/by-id/usb-FTDI_FT232R-if00-port0
          container_path: /dev/sensor
          permissions: rw
```

- `path` is the device on the host. A `/dev/serial/by-id/` link keeps its name when the adapter moves to another port.
- `container_path` is where the task sees it.
- `permissions` is `r`, `w` or `rw`.
- `probe_image` is this release's orker image, pinned by digest. The Orker uses it to check what a container sees.

The Orker refuses to boot if the device is not a USB serial node (`ttyUSB` or `ttyACM`).

## Select it in a task

```yaml
apiVersion: orkstack/v1
kind: Orkflow
name: read-sensor
tasks:
  - id: read
    uses: shell
    pool: lab
    devices:
      - sensor
    retry:
      maximum_attempts: 3
    with:
      op: script
      interpreter: sh
      script: head -c 64 /dev/sensor | od -c
```

If the device is replaced while the Orker runs, the attempt that meets it fails `PROFILE_DRIFTED` and the task runs again on another Orker of the pool without spending an attempt. If it is unplugged, the attempt fails `PROFILE_NOT_OFFERED` and spends one, so give the task a `retry:`. An adapter plugged back as the same node returns the profile to service within about 15 seconds.

## What submit checks

| Refusal | Meaning | What to do |
|---|---|---|
| `PROFILE_UNAVAILABLE` | An Orker of the pool does not define the profile, or lacks the contract the task's image needs, or no Orker answered | Define it, with that contract, on every Orker of the pool |
| `PROFILE_MISMATCH` | Two Orkers define one contract differently, or their default contracts share no label | Give a changed definition a new contract label, and let it accept the old one |

`allow_empty_pool` does not change either: a device is never assumed to be there.

## Change a profile

Change a profile one Orker at a time, under a new **contract** label, so runs keep running while the pool is mixed. Give the new definition a label, and say which older label it still serves:

```yaml
    sensor:
      kind: devices
      contract: "2"
      accepts:
        - "1"
      devices:
        - path: /dev/serial/by-id/usb-FTDI_FT232R-if00-port0
          container_path: /dev/sensor
          permissions: rw
```

Never edit a contract in place: two Orkers that define one label differently are refused `PROFILE_MISMATCH`. When images must change too, define both contracts on every Orker, and let the new images declare the one they need with `devices: {sensor: "2"}` in their `interface.ork.yaml`.

## Limits

- **Linux hosts only.** The Orker must run on the container daemon's own host, as a service with no mount namespacing. Docker Desktop on macOS or Windows cannot pass a host device.
- **Shared, not exclusive.** Every task that holds the profile opens the same device.
- **Groups.** A task that runs as a user other than root needs the device's group, usually `dialout`.
- **Fixed until restart.** An Orker reads its profiles at boot; to change one, drain, stop, edit and start each Orker in turn, as below.

## See also

- [DEPLOY.md, "Running a device Orker"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-device-orker): the configuration, the device classes, the systemd unit and drift
- [DEPLOY.md, "Changing a profile"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#changing-a-profile): rolling a change, a breaking change, rollback, and when an old contract is unused
- [DSL.md, "Device profiles"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#device-profiles): the `devices:` key and its rules
- [SECURITY_MODEL.md, "Device profiles"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#device-profiles): what a device profile does and does not grant
