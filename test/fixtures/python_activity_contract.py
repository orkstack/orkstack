#!/usr/bin/env python3
"""python_activity_contract.py — a complete OrkStack activity, in Python, with no SDK.

A REFERENCE IMPLEMENTATION and an optional companion to the mandatory Bash
fixture beside it (§10.3). It exists to show the ABI is the same in every
language: read files, write files, exit.

Nothing here imports OrkStack. There is no package to install, no client to
construct, no worker to register and no callback to serve. The imports are the
standard library, and two of them are only for the signal handler.

Build it into any image you like:

    FROM python:3.12-alpine
    COPY python_activity_contract.py /usr/local/bin/activity
    USER 65532:65532
    ENTRYPOINT ["/usr/local/bin/activity"]

Run it under the contract suite:

    orkctl activity test <your-image>

THE WHOLE ABI, in the order it is used:

    /orkstack/input/task.json     who you are, what your limits are (§3.2)
    /orkstack/input/config.json   your rendered `with:` block       (§3.3)
    /orkstack/output/values.json  your structured result            (§3.4)
    /orkstack/output/artifacts/   any regular file you write        (§3.5)
    stdout / stderr               two independent logs              (§3.6)
    exit status                   0 succeeded, anything else failed
"""

import json
import os
import signal
import sys

INPUT_DIR = "/orkstack/input"
OUTPUT_DIR = "/orkstack/output"


def read_json(path, default=None):
    """Read one JSON file. A missing file returns `default` rather than raising.

    A task with no `with:` block has no config.json, which is ordinary and not
    an error. task.json, by contrast, is always present — so its caller passes
    no default and a missing file is the exception it should be.
    """
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        if default is None:
            raise
        return default


def write_values(values, max_bytes):
    """Write values.json, checking the limit BEFORE writing.

    Order matters. A values.json over the limit is a ContractViolation, which
    does not retry; discovering that after a partial write would turn a clear
    refusal into a corrupt result.
    """
    encoded = json.dumps(values).encode("utf-8")
    if max_bytes and len(encoded) > max_bytes:
        raise ValueError(
            f"values.json is {len(encoded)} bytes, over the {max_bytes}-byte "
            "limit; put large data in an artifact instead"
        )
    with open(os.path.join(OUTPUT_DIR, "values.json"), "wb") as handle:
        handle.write(encoded)


def main():
    # Cancellation is an ordinary Unix signal. Handling it is optional — the
    # Orker's SIGTERM -> grace -> SIGKILL sequence stops you correctly either
    # way. What matters is not exiting before values.json if you mean to
    # report a result.
    def on_term(signum, _frame):
        print(f"activity: received signal {signum}, stopping", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_term)

    # Decode only the fields you use. An activity that insisted on the whole
    # schema would break the first time the platform added a field it never
    # wanted.
    task = read_json(os.path.join(INPUT_DIR, "task.json"))
    config = read_json(os.path.join(INPUT_DIR, "config.json"), default={})

    run = task.get("run", {})
    task_meta = task.get("task", {})
    limits = task.get("limits", {})

    # stdout reaches the live stream AND the durable stdout.log. Plain text is
    # enough; there is no logging SDK. flush=True because a buffered line that
    # never flushes is a line an operator never sees.
    print(
        f"activity: run={run.get('id')} task={task_meta.get('id')} "
        f"attempt={task_meta.get('attempt')}",
        flush=True,
    )

    message = config.get("message") or "hello from a Python activity"

    # An artifact is any REGULAR FILE written here, named exactly as the file.
    # Directories, symlinks, devices, sockets and FIFOs are refused at the scan
    # (§3.5) — archive a hierarchy yourself as a .tar.gz.
    artifact = os.path.join(OUTPUT_DIR, "artifacts", "message.txt")
    with open(artifact, "w", encoding="utf-8") as handle:
        handle.write(message + "\n")

    write_values(
        {
            "message": message,
            "idempotencyKey": task.get("idempotencyKey"),
            "bytes": len(message.encode("utf-8")) + 1,
        },
        limits.get("valuesMaxBytes"),
    )

    # Exit 0 means the computation succeeded; the Orker then validates and
    # commits the outputs. There are no magic exit codes — any non-zero status
    # fails the activity, and Temporal's retry policy decides what follows.
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as err:  # noqa: BLE001 - stderr is a log, not a protocol
        print(f"activity: {err}", file=sys.stderr, flush=True)
        sys.exit(1)
