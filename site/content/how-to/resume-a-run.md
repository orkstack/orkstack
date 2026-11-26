# Resume a failed run

> No build resumes runs yet. The command answers "this engine resumes no runs", and the run page offers no Resume. This page describes how it works once it is switched on.

When a run fails, you can resume it instead of starting it again. Its failed tasks, and everything downstream of them, run again. Every other task keeps its result. The run keeps its id, and its events carry on.

## 1. Preview what would run again

```bash
orkctl run resume ork-7f3k2abcde --dry-run
```

The preview lists the tasks that run again, the tasks whose results are kept, and how many of the run's 20 resumes are used. It changes nothing.

## 2. Resume

```bash
orkctl run resume ork-7f3k2abcde --reason "the registry was down"
```

If a task that succeeded before would run again, the command asks you to type `yes`, because its side effects repeat. In a script, pass `--yes`. Add `--wait` to watch the run until it ends.

On the run page, **Resume** in the header does the same.

## Retry from a task

If a task succeeded with a result you don't want to keep, retry from it. It and everything downstream of it run again too:

```bash
orkctl run resume ork-7f3k2abcde --from build
```

On the run page, open the task's menu and choose **Retry from here**.

## When a resume is refused

The message says why, and the exit code says what to do:

- **75**: try again later. For example, containers of the earlier attempt are still stopping.
- **76**: preview again and decide. For example, the plan changed since you looked.
- **1**: a resume can't fix this run. Run it again with `orkctl run again`.

## See also

- [Resuming a run](https://github.com/orkstack/orkstack/blob/main/docs/RESUME.md): what reruns, what is kept, the exit codes and the limits
- [Error reasons](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md): every reason a resume is refused
