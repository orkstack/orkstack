#!/usr/bin/env bash
# The hostile event-source fixtures (docs/EVENT_SOURCE_IMAGES.md, "Testing a
# source image"): the Bash fixture's exchange (test/fixtures/source-bash)
# with one thing a contract-keeping image never does, chosen when the image
# is built (MODE, baked into /etc/hostile-mode). One image per mode:
#
#   link-swap      hands over a body that is a symbolic link
#   fill           fills its answer filesystem first; must meet "no space"
#   nest           nests directories in its answer filesystem; must meet "no space"
#   silent-lane    never answers a poll
#   ignore-term    ignores SIGTERM and the stop notice
#   ignore-beat    never ends itself when the beat stops
#   billion-limit  reports a delivery limit of a billion
#   ack-no-verdict settles a message before any verdict
#   late-settle    settles a message after the verdict exists, naming none
#   early-close    closes a message at a hold bound that has not come
#   stop-settle    settles the message it gives back at a stop or a lost beat
#
# fill and nest end themselves (exit 1) when the bound does NOT hold, so an
# image that passes every check in those modes proves the bound held and
# harmed nothing else.
set -euo pipefail
mode=$(cat /etc/hostile-mode)
ask=/orkstack/service/ask ans=/orkstack/service/answer lane=lanes/0 broker=/tmp/broker
hold=$(jq -r .limits.holdSeconds /orkstack/input/service.json)
orphan=$(jq -r .limits.orphanSeconds /orkstack/input/service.json)
mkdir -p "$ans/tmp" "$ans/$lane" "$broker/ready" "$broker/held" "$broker/done" "$broker/dead" "$broker/count"

no_space() {                   # no_space WHAT CMD... : CMD must fail with ENOSPC
  local what=$1; shift
  if "$@" 2>/tmp/hostile.err; then echo "hostile: $what met no bound" >&2; exit 1; fi
  if ! grep -q 'No space left on device' /tmp/hostile.err; then
    echo "hostile: $what failed, but not for space: $(head -c 200 /tmp/hostile.err)" >&2; exit 1
  fi
  echo "hostile: $what met no space, as it must" >&2
}
nest() { local d="$ans/tmp/n"; mkdir "$d" || return 1; for _ in $(seq 1 100000); do d="$d/n"; mkdir "$d" || return 1; done; }
case $mode in
  fill) no_space "a fill of the answer filesystem" dd if=/dev/zero of="$ans/tmp/fill" bs=64k count=4096
        rm -f "$ans/tmp/fill" ;;
  nest) no_space "nested directories in the answer filesystem" nest
        rm -rf "$ans/tmp/n" ;;
esac

stop=0
if [ "$mode" = ignore-term ]; then trap '' TERM; else trap 'stop=1' TERM; fi
max=5 beat='' beat_at=$(date +%s) status_at=0 check=0 answered=0 orphaned=0
if [ "$mode" = billion-limit ]; then max=1000000000; fi

say() {                        # say PATH JSON : write, then rename into place
  printf '%s' "$2" > "$ans/tmp/f" && mv "$ans/tmp/f" "$ans/$1"
}
status() {
  say status.json "{\"schemaVersion\":1,\"checkSeq\":$check,\"ready\":true,\"check\":{\"ok\":true},\"broker\":{\"object\":\"dir:$broker\",\"maxDeliveries\":$max,\"messageLimitBytes\":1048576,\"leaseSeconds\":60}}"
  status_at=$(date +%s)
}
tick() {
  b=$(jq -r .beat "$ask/alive.json" 2>/dev/null || echo '')
  if [ "$b" != "$beat" ]; then beat=$b; beat_at=$(date +%s); fi
  orphaned=0
  if [ "$mode" != ignore-beat ] && [ $(( $(date +%s) - beat_at )) -gt "$orphan" ]; then orphaned=1; fi
  c=$(jq -r .seq "$ask/check.json" 2>/dev/null || echo 0)
  if [ "$c" != "$check" ] || [ $(( $(date +%s) - status_at )) -ge 20 ]; then check=$c; status; fi
}
give_back() {
  if [ "$(cat "$broker/count/$1")" -ge 5 ]; then mv "$broker/held/$1" "$broker/dead/$1"
  else mv "$broker/held/$1" "$broker/ready/$1"; fi
}

status
while :; do
  tick; if [ "$orphaned" = 1 ]; then exit 3; fi
  seq=$(jq -r .seq "$ask/$lane/poll.json" 2>/dev/null || echo 0)
  if [ "$seq" = 0 ] || [ "$seq" = "$answered" ] || [ "$mode" = silent-lane ]; then
    if [ "$stop" = 1 ]; then exit 0; fi
    sleep 0.2; continue
  fi
  msg=$(find "$broker/ready" -type f -print -quit)
  if [ -z "$msg" ] || [ "$stop" = 1 ]; then
    if [ "$stop" = 0 ]; then sleep 1; fi
    say "$lane/empty.json" "{\"schemaVersion\":1,\"seq\":$seq}"; answered=$seq; continue
  fi
  id=$(basename "$msg"); mv "$msg" "$broker/held/$id"
  n=$(( $(cat "$broker/count/$id" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$broker/count/$id"
  if [ "$mode" = link-swap ]; then
    ln -sfn /etc/passwd "$ans/tmp/b" && mv -T "$ans/tmp/b" "$ans/$lane/body"
  else
    cp "$broker/held/$id" "$ans/tmp/b" && mv "$ans/tmp/b" "$ans/$lane/body"
  fi
  held_at=$(date +%s) action='' v='' vid=''
  if [ "$mode" = ack-no-verdict ]; then
    # Acknowledged on its own, and settled BEFORE the message is handed over,
    # so the Orker meets the settle before any verdict, at the first message,
    # every time; settling after the hand-over would race the Orker's own
    # verdict, and which check met the fault would depend on timing.
    mv "$broker/held/$id" "$broker/done/$id"
    say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":true}"
    say "$lane/message.json" "{\"schemaVersion\":1,\"seq\":$seq,\"id\":\"dir:$id\",\"bodyBytes\":$(stat -L -c %s "$ans/$lane/body"),\"deliveryCount\":$n}"
    answered=$seq
    continue
  fi
  say "$lane/message.json" "{\"schemaVersion\":1,\"seq\":$seq,\"id\":\"dir:$id\",\"bodyBytes\":$(stat -L -c %s "$ans/$lane/body"),\"deliveryCount\":$n}"
  if [ "$mode" = early-close ]; then
    # Acknowledged on its own, and reported as its own close at a hold bound
    # that has not come: a settle that needs no verdict's id.
    mv "$broker/held/$id" "$broker/done/$id"
    say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":false,\"error\":{\"kind\":\"hold_expired\"}}"
    answered=$seq
    continue
  fi
  if [ "$mode" = late-settle ]; then
    # Settled on its own once the verdict exists, without reading it: a settle
    # that names no verdict, which the Orker must refuse whenever it lands.
    until jq -e "select(.seq==$seq)" "$ask/$lane/verdict.json" >/dev/null 2>&1; do tick; sleep 0.1; done
    mv "$broker/held/$id" "$broker/done/$id"
    say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":true}"
    answered=$seq
    continue
  fi
  while [ -z "$action" ]; do
    tick
    # One read: the action and the id are one verdict's.
    read -r v vid < <(jq -r "select(.seq==$seq) | \"\(.action) \(.verdictId)\"" "$ask/$lane/verdict.json" 2>/dev/null) || v='' vid=''
    if [ -n "$v" ]; then action=$v
    elif [ "$stop" = 1 ] || [ "$orphaned" = 1 ]; then action=stopped
    elif [ $(( $(date +%s) - held_at )) -ge "$hold" ]; then action=hold_expired
    else sleep 0.2; fi
  done
  case $action in
    ack) mv "$broker/held/$id" "$broker/done/$id"
         say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"verdictId\":\"$vid\",\"ok\":true}" ;;
    hold_expired) give_back "$id"
         say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":false,\"error\":{\"kind\":\"hold_expired\"}}" ;;
    stopped) give_back "$id"
         if [ "$mode" = stop-settle ]; then say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"ok\":true}"; fi ;;
    *)   give_back "$id"
         say "$lane/settled.json" "{\"schemaVersion\":1,\"seq\":$seq,\"verdictId\":\"$vid\",\"ok\":true}" ;;
  esac
  answered=$seq
  if [ "$orphaned" = 1 ]; then exit 3; fi
done
