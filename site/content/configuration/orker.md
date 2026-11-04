# Orker settings

These settings configure `orker`, the process that runs task containers. They cover the pool an Orker joins, its container engine, the resources a task may take, the secrets it may read, its live task logs, and whether it hosts pins.

## Decide these first

| Setting | Why a deployment must look at it |
|---|---|
| [`orker.id`](/docs/configuration/#orker-id) | Must be unique across the whole deployment. Set it explicitly in a container. |
| [`orker.workdir`](/docs/configuration/#orker-workdir) | The daemon bind-mounts each task's workspace from here, so the path must be one the daemon can reach. |
| [`orker.max_concurrent`](/docs/configuration/#orker-max-concurrent) | Tasks this Orker runs at once. Size it to the host and the per-task caps. |
| [`orker.resources.max_cpu`](/docs/configuration/#orker-resources-max-cpu) | The most one task may request. Set the same caps on every Orker of a pool. |
| [`orker.pins.max_concurrent`](/docs/configuration/#orker-pins-max-concurrent) | Above `0` makes this Orker a pin host. Set it the same on every Orker of a pool. |
| [`orker.secret_allow`](/docs/configuration/#orker-secret-allow) | The floor of secret sources this Orker will read. Set it on every Orker of a shared pool. |

## Every orker setting

<!-- site-generated: settings-table-orker -->

## See also

- [DEPLOY.md, "1. Configuration"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): the full table, each key's variable and default
- [DEPLOY.md, "Sizing an Orker's task ceiling"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#sizing-an-orkers-task-ceiling): sizing `orker.max_concurrent`
- [DEPLOY.md, "Declaring an Orker's capacity"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#declaring-an-orkers-capacity): the capacity settings and their metrics
- [DEPLOY.md, "Every Orker of a pool has the same settings"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#every-orker-of-a-pool-has-the-same-settings): the settings a pool shares
- [DEPLOY.md, "Secrets on a shared pool"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#secrets-on-a-shared-pool): the secret floor
- [DEPLOY.md, "Running on Podman"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-on-podman): the container engine settings
- Setting details: [Setting details](/docs/configuration/details/)
- All settings: [All settings](/docs/configuration/)
