# images/activity-shell/Dockerfile
#
# The Dockerfile contract of docs/OCI_ACTIVITY_CONTRACT.md §10.1, applied.
# Every rule it names is visible here:
#
#   rule 1  ENTRYPOINT in exec form (shell form does not forward SIGTERM)
#   rule 3  USER is not root
#   rule 4  no VOLUME — it would shadow the workspace mounts
#   rule 5  the base image is digest-pinned inside the Dockerfile
#
# The annotations live above their instructions, never trailing them: a
# Dockerfile has no inline comment, so `USER 65532:65532   # never root`
# makes the group literally "65532   # never root" and the daemon refuses the
# container with "unable to find group".
#
# alpine 3.24's index digest, the base every first-party image pins and the
# Orker's boot probe runs (orker.BindProbeImage).
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

# jq is the only "SDK": a JSON reader.
RUN apk add --no-cache jq

COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh

# Never root.
USER 65532:65532

# The with: contract, the icon and the descriptions, declared
# (docs/ACTIVITY_IMAGES.md, "Declaring the interface"). Optional: the engine
# checks a document's with: against the interface at validate and apply, and
# clients draw the icon and show the descriptions; the container never reads
# them. The three lines are generated from interface.ork.yaml by
# `go run ./internal/schemas/cmd/genschemas`: edit that file, never these lines.
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"eval":{"fields":{"command":{"type":"array","required":true}}},"exec":{"fields":{"command":{"type":"array","required":true}}},"script":{"fields":{"command":{"type":"array"},"interpreter":{"type":"string"},"script":{"type":"string","required":true}}}},"defaultOp":"exec","values":{"exitCode":{"type":"number"},"scriptPath":{"type":"string"},"value":{"type":"string"}}}'
LABEL org.orkstack.activity.icon='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><rect x="1.75" y="1.75" width="12.5" height="12.5" rx="2.5" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M10 5.6c-.4-.6-1.1-.9-2-.9-1.1 0-1.9.6-1.9 1.4 0 .9.8 1.2 1.9 1.5 1.1.3 1.9.7 1.9 1.6 0 .9-.8 1.5-1.9 1.5-.9 0-1.6-.3-2-.9M8 3.6v1.1M8 11.2v1.2" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/></svg>'
LABEL org.orkstack.activity.docs='{"schemaVersion":1,"description":"Runs a command, a script or an expression in the container and reports how it ended. `command` is argv, never a shell string, so a rendered value cannot become code.","ops":{"eval":{"description":"Runs `command` as argv and reports its stdout as `value`. A non-zero exit is reported in `exitCode`, not as a failure.","fields":{"command":"The argv to run, one element per argument. What it prints on stdout becomes `value`."}},"exec":{"description":"Runs `command` as argv; its exit status is the task\u0027s. The op used when `op` is absent.","fields":{"command":"The argv to run, one element per argument, never handed to a shell."}},"script":{"description":"Writes `script` to a file and runs it; its exit status is the task\u0027s.","fields":{"command":"Arguments passed to the script as its positional parameters.","interpreter":"The program that runs the script, such as `python3`. Without it the script\u0027s own shebang line decides, and `sh` runs a script without one.","script":"The program text, written to `/orkstack/tmp/script` and made executable."}}},"values":{"exitCode":"The exit status of the command or script.","scriptPath":"Where `op: script` wrote the script.","value":"What the command of `op: eval` printed on stdout."}}'

# Exec form, mandatory.
ENTRYPOINT ["/usr/local/bin/entry.sh"]
