# Give a task a USB serial device

Let a task open a device plugged into an Orker's host, such as a USB serial adapter. The Orker's operator defines a device profile once; an Orkflow selects it by name.

## Define the profile on the Orker

Add the profile to every Orker of the pool, in its configuration file, and restart each one:

```yaml
orker:
  profiles:
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
- The Orker checks what a container sees with its own release image. Set `probe_image` only on a host that cannot reach ghcr.io, to your mirrored copy pinned by digest, or on an Orker you built from source.

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
| `PROFILE_UNAVAILABLE` | An Orker of the pool does not define the profile, or no Orker answered | Define it on every Orker of the pool, or start one that does |
| `PROFILE_MISMATCH` | Two Orkers of the pool define it differently | Give every Orker the same definition |

`allow_empty_pool` does not change either: a device is never assumed to be there.

## Limits

- **Linux hosts only.** The Orker must run on the container daemon's own host, as a service with no mount namespacing. Docker Desktop on macOS or Windows cannot pass a host device.
- **Shared, not exclusive.** Every task that holds the profile opens the same device.
- **Groups.** A task that runs as a user other than root needs the device's group, usually `dialout`.
- **Fixed until restart.** Changing a profile means drain, stop, edit and start the Orker.

## See also

- [DEPLOY.md, "Running a device Orker"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-device-orker): the configuration, the device classes, the systemd unit and drift
- [DSL.md, "Device profiles"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#device-profiles): the `devices:` key and its rules
- [SECURITY_MODEL.md, "Device profiles"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#device-profiles): what a device profile does and does not grant
