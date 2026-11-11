# Stats and usage

See how a namespace is doing over a time window: how its runs ended, how long they took, what fails most, which tasks need retries, how often cached results were reused, and how much cpu and memory it reserved. Every number is worked out when you ask, from records the deployment already keeps.

## Outcomes and durations

```bash
orkctl -n payments stats                       # the last 24 hours
orkctl -n payments stats -s 6h
orkctl -n payments stats --by day --buckets 7  # runs created each day, the last 7 days
```

Stats counts the runs a window created, whenever they end. It reports the runs still running, how the finished ones ended, the success rate, and the median and 95th-percentile durations. A run that started in the window and is still going has no duration yet.

`--by day` or `--by hour` adds a series. Each bucket counts the runs it created, on the wall clock of `--timezone`. The default zone is the machine's own.

## Failures

```bash
orkctl -n payments stats failures -s 24h
orkctl -n payments stats failures -s 6h -g task,code,image,phase
orkctl -n payments stats failures --category infrastructure_error
```

Failed runs are grouped by what failed and why. The default columns are Orkflow, task, code and category, and `--group-by` picks others. Image and phase come from attempt records, which are removed on the run's retention schedule. A group whose records are gone says `unavailable`, never a blank.

If a window has more failed runs than one read can hold, the command refuses. Narrow the window with `--since`, `--until` or `--orkflow`.

## Flaky tasks

```bash
orkctl -n payments stats flakes -s 168h
```

`stats flakes` lists the tasks that needed more than one attempt, flakiest first. A run that succeeded after a retry is counted here, although `stats` shows it as a success. A run whose records are gone is left out of every rate. It is never counted as clean.

## Cache hit rate

```bash
orkctl -n payments cache stats
```

For each cached task, the command shows its hits, misses and bypassed runs, and the hit rate. The rate is `unavailable` when there are no hits and no misses, because bypassed runs asked not to look.

## Usage and budgets

```bash
orkctl -n analytics stats usage -s 720h -g pool
orkctl stats usage -s 720h -g namespace
```

Usage is in three units: task-seconds, cpu-seconds and GiB-seconds. Each attempt's reservation, the cpu and memory the Orker granted it, is multiplied by how long the Orker held it.

Usage is a reservation, not a measurement. It shows what was reserved, not what the containers used or what they cost.

Usage counts only finished runs. Records removed by retention are reported as missing, and they are not folded into a smaller total.

A usage read over more than 10,000 finished runs is refused, rather than answered from part of the window. Narrow the window with a shorter `--since`. This is the usual reason an enforced budget cannot be measured: its period holds too many finished runs.

A budget bounds a namespace's reserved cpu-seconds or GiB-seconds over a rolling period, from 1 to 744 hours:

```bash
orkctl namespace update analytics --budget-cpu-seconds 5000000 --budget-period 720h   # reported only
orkctl namespace update analytics --budget-enforce                                    # refuse new runs at the limit
```

A budget is soft by default. It appears on every usage read and refuses nothing. With `--budget-enforce`, a submit is refused once usage reaches the bound, and accepted runs are not touched. An enforced budget that cannot be measured also refuses new runs. Before you enforce a budget, check that `orkctl stats usage -s <period>` answers.

Enforcement is best effort. It does not hold a lock, so a few runs can start past the bound.

## See also

- [docs/STATS.md, "1. Two sources, and no third"](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md#1-two-sources-and-no-third): where each number comes from
- [docs/STATS.md, "2. The window"](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md#2-the-window): windows, series and time zones
- [docs/STATS.md, "6. Failure fingerprints"](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md#6-failure-fingerprints): how failures are grouped
- [docs/STATS.md, "7. Flake index"](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md#7-flake-index): what counts as a flake
- [docs/STATS.md, "8. Usage"](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md#8-usage): the units, the gaps and the cost of a read
- [docs/STATS.md, "9. Cache"](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md#9-cache): the cache hit rate
- [docs/DEPLOY.md, "Namespace budgets"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#namespace-budgets-reserved-consumption-soft-or-enforced): soft and enforced budgets, and the margin
- [docs/CLI.md, "orkctl stats"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-stats): every stats flag
