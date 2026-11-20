#!/usr/bin/env python3
"""The event-source fixture in Python (docs/EVENT_SOURCE_IMAGES.md).

The Bash fixture's broker and exchange (test/fixtures/source-bash/entry.sh),
written with the standard library and nothing of OrkStack's, to show the
profile is files in any language.

Its "broker" is a directory under the container's memory-backed /tmp: a file
in ready/ is a message, held/ is the lease, done/ the acknowledgement, dead/
the dead letter, and count/<id> its delivery count. `orkctl service test
--publish-exec` writes a file into ready/ through the container's own root.
"""
import json
import os
import signal
import sys
import time

ASK = "/orkstack/service/ask"
ANS = "/orkstack/service/answer"
LANE = "lanes/0"
BROKER = "/tmp/broker"
MAX_DELIVER = 5

stopping = False


def on_term(_signum, _frame):
    global stopping
    stopping = True


def read_json(path):
    """One of the Orker's files; one not yet there, or caught mid-replace,
    reads as nothing, and the next tick reads it again."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def say(name, value):
    """Write a step, then rename it into place: a reader never sees half."""
    tmp = ANS + "/tmp/f"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, separators=(",", ":"))
    os.rename(tmp, ANS + "/" + name)


class Source:
    def __init__(self, limits):
        self.hold = limits["holdSeconds"]
        self.orphan = limits["orphanSeconds"]
        self.beat = None
        self.beat_at = time.monotonic()
        self.check = 0
        self.status_at = 0.0
        self.answered = 0
        self.orphaned = False

    def status(self):
        self.status_at = time.monotonic()
        say("status.json", {
            "schemaVersion": 1, "checkSeq": self.check, "ready": True, "check": {"ok": True},
            "broker": {"object": "dir:" + BROKER, "maxDeliveries": MAX_DELIVER,
                       "messageLimitBytes": 1 << 20, "leaseSeconds": 60},
        })

    def tick(self):
        """Everything owed whatever the lane is doing: the beat, a check
        asked for, and the status at least every 20 seconds."""
        b = read_json(ASK + "/alive.json").get("beat")
        if b != self.beat:
            self.beat, self.beat_at = b, time.monotonic()
        self.orphaned = time.monotonic() - self.beat_at > self.orphan
        c = read_json(ASK + "/check.json").get("seq", 0)
        if c != self.check or time.monotonic() - self.status_at >= 20:
            self.check = c
            self.status()

    def exchange(self, seq):
        ready = sorted(os.listdir(BROKER + "/ready"))
        if not ready or stopping:
            if not stopping:
                time.sleep(1)
            say(LANE + "/empty.json", {"schemaVersion": 1, "seq": seq})
            return
        mid = ready[0]
        os.rename(BROKER + "/ready/" + mid, BROKER + "/held/" + mid)  # the lease
        try:
            with open(BROKER + "/count/" + mid, encoding="utf-8") as f:
                n = int(f.read().strip()) + 1
        except (OSError, ValueError):
            n = 1
        with open(BROKER + "/count/" + mid, "w", encoding="utf-8") as f:
            f.write(str(n))
        with open(BROKER + "/held/" + mid, "rb") as f:
            body = f.read()
        with open(ANS + "/tmp/b", "wb") as f:
            f.write(body)
        os.rename(ANS + "/tmp/b", ANS + "/" + LANE + "/body")
        say(LANE + "/message.json", {"schemaVersion": 1, "seq": seq, "id": "dir:" + mid,
                                     "bodyBytes": len(body), "deliveryCount": n})
        held_at, action = time.monotonic(), None
        while action is None:
            self.tick()
            v = read_json(ASK + "/" + LANE + "/verdict.json")
            if v.get("seq") == seq and v.get("action"):
                action = v["action"]
            elif stopping or self.orphaned:
                action = "release"
            elif time.monotonic() - held_at >= self.hold:
                action = "hold_expired"
            else:
                time.sleep(0.2)
        settled = {"schemaVersion": 1, "seq": seq, "ok": True}
        if action == "ack":
            os.rename(BROKER + "/held/" + mid, BROKER + "/done/" + mid)
        else:
            to = "/dead/" if n >= MAX_DELIVER else "/ready/"
            os.rename(BROKER + "/held/" + mid, BROKER + to + mid)
            if action == "hold_expired":
                settled["ok"], settled["error"] = False, {"kind": "hold_expired"}
        say(LANE + "/settled.json", settled)


def main():
    signal.signal(signal.SIGTERM, on_term)
    limits = read_json("/orkstack/input/service.json").get("limits")
    if not limits:
        print("source-python: /orkstack/input/service.json has no limits", file=sys.stderr)
        return 1
    for d in (ANS + "/tmp", ANS + "/" + LANE, BROKER + "/ready", BROKER + "/held",
              BROKER + "/done", BROKER + "/dead", BROKER + "/count"):
        os.makedirs(d, exist_ok=True)
    src = Source(limits)
    src.status()
    while True:
        src.tick()
        if src.orphaned:
            return 3
        seq = read_json(ASK + "/" + LANE + "/poll.json").get("seq", 0)
        if seq == 0 or seq == src.answered:
            if stopping:
                return 0
            time.sleep(0.2)
            continue
        src.exchange(seq)
        src.answered = seq
        if src.orphaned:
            return 3


if __name__ == "__main__":
    sys.exit(main())
