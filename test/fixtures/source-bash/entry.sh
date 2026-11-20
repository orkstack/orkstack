#!/usr/bin/env bash
# The mandatory Bash event source (docs/ORKER_SERVICE_CONTRACT.md,
# docs/EVENT_SOURCES.md): one lane, no SDK, no watcher, no background job.
# Its "broker" is a directory under the container's memory-backed /tmp: a
# file in ready/ is a message, held/ is the lease, done/ the
# acknowledgement, dead/ the dead letter, and count/<id> its delivery count.
# `orkctl service test --publish-exec` writes a file into ready/ through the
# container's own root.
set -euo pipefail
ask=/orkstack/service/ask ans=/orkstack/service/answer lane=lanes/0 broker=/tmp/broker
hold=$(jq -r .limits.holdSeconds /orkstack/input/service.json)
orphan=$(jq -r .limits.orphanSeconds /orkstack/input/service.json)
mkdir -p "$ans/tmp" "$ans/$lane" "$broker/ready" "$broker/held" "$broker/done" "$broker/dead" "$broker/count"
stop=0; trap 'stop=1' TERM
max=5 beat='' beat_at=$(date +%s) status_at=0 check=0 answered=0 orphaned=0

say() {                        # say PATH JSON : write, then rename into place
  printf '%s' "$2" > "$ans/tmp/f" && mv "$ans/tmp/f" "$ans/$1"
}
status() {
  say status.json "{\"schemaVersion\":1,\"checkSeq\":$check,\"ready\":true,\"check\":{\"ok\":true},\"broker\":{\"object\":\"dir:$broker\",\"maxDeliveries\":$max,\"messageLimitBytes\":1048576,\"leaseSeconds\":60}}"
  status_at=$(date +%s)
}
tick() {                       # everything owed whatever the lane is doing; sets orphaned on a lost beat
  b=$(jq -r .beat "$ask/alive.json" 2>/dev/null || echo '')
  if [ "$b" != "$beat" ]; then beat=$b; beat_at=$(date +%s); fi
  if [ $(( $(date +%s) - beat_at )) -gt "$orphan" ]; then orphaned=1; else orphaned=0; fi
  c=$(jq -r .seq "$ask/check.json" 2>/dev/null || echo 0)
  if [ "$c" != "$check" ] || [ $(( $(date +%s) - status_at )) -ge 20 ]; then check=$c; status; fi
}
give_back() {                  # the lease ends: back to ready, or to the dead letter at the limit
  if [ "$(cat "$broker/count/$1")" -ge "$max" ]; then mv "$broker/held/$1" "$broker/dead/$1"
  else mv "$broker/held/$1" "$broker/ready/$1"; fi
}

status
while :; do
  tick; if [ "$orphaned" = 1 ]; then exit 3; fi
  seq=$(jq -r .seq "$ask/$lane/poll.json" 2>/dev/null || echo 0)
  if [ "$seq" = 0 ] || [ "$seq" = "$answered" ]; then
    if [ "$stop" = 1 ]; then exit 0; fi
    sleep 0.2; continue
  fi
  msg=$(find "$broker/ready" -type f -print -quit)
  if [ -z "$msg" ] || [ "$stop" = 1 ]; then
    if [ "$stop" = 0 ]; then sleep 1; fi
    say "$lane/empty.json" "{\"schemaVersion\":1,\"seq\":$seq}"; answered=$seq; continue
  fi
  id=$(basename "$msg"); mv "$msg" "$broker/held/$id"                     # the lease
  n=$(( $(cat "$broker/count/$id" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$broker/count/$id"
  cp "$broker/held/$id" "$ans/tmp/b" && mv "$ans/tmp/b" "$ans/$lane/body"
  say "$lane/message.json" "{\"schemaVersion\":1,\"seq\":$seq,\"id\":\"dir:$id\",\"bodyBytes\":$(stat -c %s "$ans/$lane/body"),\"deliveryCount\":$n}"
  held_at=$(date +%s) action=''
  while [ -z "$action" ]; do
    tick                                                                  # status, checks and the beat while holding
    v=$(jq -r "select(.seq==$seq) | .action" "$ask/$lane/verdict.json" 2>/dev/null || true)
    if [ -n "$v" ]; then action=$v
    elif [ "$stop" = 1 ] || [ "$orphaned" = 1 ]; then action=release
    elif [ $(( $(date +%s) - held_at )) -ge "$hold" ]; then action=hold_expired
    else sleep 0.2; fi
  done
  case $action in
    ack) mv "$broker/held/$id" "$broker/done/$id"
         say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":true}" ;;
    hold_expired) give_back "$id"
         say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":false,\"error\":{\"kind\":\"hold_expired\"}}" ;;
    *)   give_back "$id"
         say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":true}" ;;
  esac
  answered=$seq
  if [ "$orphaned" = 1 ]; then exit 3; fi
done
