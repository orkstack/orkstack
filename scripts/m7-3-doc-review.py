#!/usr/bin/env python3
"""Diff the M7.3 documentation against the ops the plugins actually advertise.

Source of truth: the `op` enum each plugin returns from Describe(), surfaced by
internal/orker's TestBuiltinPlugins_AllExposeAndValidateTheGenericOpSelector as
`advertised ops: <plugin>=<op>,<op>,...`. Everything else — the per-plugin
READMEs, docs/PLUGIN_DESIGN.md, docs/PLUGINS.md, ARCHITECTURE.md and the shipped
examples — is checked against it.

Usage: scripts/m7-3-doc-review.py
Exits non-zero when a document and the code disagree.
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PLUGINS = ("http", "shell", "file")
failures: list[str] = []


def report(ok: bool, message: str) -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {message}")
    if not ok:
        failures.append(message)


def advertised_ops() -> dict[str, list[str]]:
    """Run the builtin-op test and read the enums it logs."""
    proc = subprocess.run(
        ["go", "test", "./internal/orker", "-run",
         "TestBuiltinPlugins_AllExposeAndValidateTheGenericOpSelector",
         "-v", "-count=1"],
        cwd=ROOT, capture_output=True, text=True,
    )
    print("$ go test ./internal/orker -run "
          "TestBuiltinPlugins_AllExposeAndValidateTheGenericOpSelector -v -count=1")
    print(proc.stdout.strip() or proc.stderr.strip())
    print()
    if proc.returncode != 0:
        failures.append("the builtin op-contract test failed")
    ops = {}
    for plugin, values in re.findall(r"advertised ops: (\w+)=([\w,]+)", proc.stdout):
        ops[plugin] = values.split(",")
    return ops


def backticked(text: str) -> list[str]:
    return re.findall(r"`([a-z]+)`", text)


def readme_ops(plugin: str) -> list[str]:
    """Ops named in the first column of the README's `## Ops` table."""
    path = ROOT / "pkg" / "plugins" / plugin / "README.md"
    if not path.is_file():
        failures.append(f"{path.relative_to(ROOT)} is missing")
        return []
    section = path.read_text().split("## Ops", 1)[-1].split("\n## ", 1)[0]
    found = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cell = line.split("|")[1]
        for token in backticked(cell):
            if token != "op" and token not in found:
                found.append(token)
    return found


def design_ops(plugin: str) -> list[str]:
    """Ops listed for a plugin in the docs/PLUGIN_DESIGN.md summary table."""
    text = (ROOT / "docs" / "PLUGIN_DESIGN.md").read_text()
    for line in text.splitlines():
        if line.startswith(f"| `{plugin}`"):
            return [t for t in backticked(line.split("|")[2]) if t != "op"]
    return []


def example_ops() -> dict[str, list[str]]:
    """(plugin, op) pairs used by the shipped examples."""
    used: dict[str, list[str]] = {}
    for path in sorted((ROOT / "examples").glob("*.ork.yaml")):
        plugin = None
        tasks = ops = 0
        for line in path.read_text().splitlines():
            line = line.split("#")[0] if not line.lstrip().startswith("#") else ""
            match = re.match(r"\s*-?\s*plugin:\s*(\S+)", line)
            if match:
                plugin, tasks = match.group(1), tasks + 1
                continue
            match = re.match(r"\s*op:\s*(\S+)", line)
            if match and plugin:
                ops += 1
                used.setdefault(plugin, [])
                if match.group(1) not in used[plugin]:
                    used[plugin].append(match.group(1))
        report(tasks == ops,
               f"{path.relative_to(ROOT)}: {ops} explicit op(s) for {tasks} task(s)")
    return used


def main() -> int:
    live = advertised_ops()
    report(sorted(live) == sorted(PLUGINS),
           f"every builtin advertises an op enum: {live}")

    for plugin in PLUGINS:
        want = live.get(plugin, [])
        readme = readme_ops(plugin)
        report(sorted(readme) == sorted(want),
               f"pkg/plugins/{plugin}/README.md documents {readme}, code advertises {want}")
        design = design_ops(plugin)
        report(sorted(design) == sorted(want),
               f"docs/PLUGIN_DESIGN.md documents {plugin} as {design}, code advertises {want}")

    # Every plugin README must state what an unknown op does.
    for plugin in PLUGINS:
        text = (ROOT / "pkg" / "plugins" / plugin / "README.md").read_text()
        report("unknown `op`" in text and "permanently" in text,
               f"pkg/plugins/{plugin}/README.md states the unknown-op behavior")

    # Cross-document references.
    checks = [
        ("ARCHITECTURE.md", "docs/PLUGIN_DESIGN.md"),
        ("docs/PLUGINS.md", "PLUGIN_DESIGN.md"),
        ("docs/PLUGIN_DESIGN.md", "examples/http-post.ork.yaml"),
        ("docs/PLUGIN_DESIGN.md", "examples/file-read.ork.yaml"),
        ("docs/PLUGIN_DESIGN.md", "examples/shell-eval.ork.yaml"),
    ]
    for path, needle in checks:
        report(needle in (ROOT / path).read_text(), f"{path} references {needle}")

    # ARCHITECTURE.md must name each plugin's ops.
    architecture = (ROOT / "ARCHITECTURE.md").read_text()
    for plugin, ops in live.items():
        report(all(f"`op: " in architecture and op in architecture for op in ops),
               f"ARCHITECTURE.md names the {plugin} ops {ops}")

    print()
    for plugin, ops in sorted(example_ops().items()):
        unknown = [op for op in ops if op not in live.get(plugin, [])]
        report(not unknown,
               f"examples use {plugin} ops {ops}; unknown: {unknown or 'none'}")

    print()
    if failures:
        print(f"DOCUMENTATION REVIEW FAILED — {len(failures)} finding(s)")
        for item in failures:
            print(f"  - {item}")
        return 1
    print("DOCUMENTATION REVIEW PASSED — docs, examples and code agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
