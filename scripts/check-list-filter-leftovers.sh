#!/usr/bin/env bash
# check-list-filter-leftovers.sh STAGE — prints every spelling that the
# structured list filters (stages 1..STAGE) removed and that is still in the
# tree: removed request fields, flags, URL keys and untyped e2e request
# bodies. Exit 0 and no output: clean. Exit 1: the leftovers above. Exit 2:
# usage. Untracked files are scanned too (--untracked), ignored ones are not.
# It never matches the persisted memo key orkflow_origin_kind
# (internal/engine/reader.go) or the DeliveryOutcome*Segment key constants
# (pkg/artifacts/deliveries.go): every pattern is word-anchored. The one file
# that must spell removed flags, cmd/orkctl/removed_flags_test.go (it proves
# they are refused), is outside the scan.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2
stage=${1:-}
[[ $stage == [123] ]] || { echo "usage: $0 1|2|3" >&2; exit 2; }
# Without PCRE every git grep -P below would fail quietly and read as clean.
git grep -qP '\bAGENTS\b' -- AGENTS.md || { echo "$0: git grep -P is unavailable" >&2; exit 2; }
Q='[\x22\x27]'                                   # a double or single quote
argv() { echo "(\\b$1 $2|$Q$1$Q,\\s*$Q$2$Q)"; }  # "run list" or 'run', 'list'
NOT='(?![\w-])'
ids='name_contains|nameContains|NameContains|namespaceFilterParam'
flags='pool|name-contains'
anchored="$(argv trigger list)[^\n]*--orkflow$NOT|$(argv schedule list)[^\n]*--orkflow$NOT"
built=''
urls="/admin/namespaces\?[^\x27\x22\x60]*\bq="
bodies=( "(TriggerService/ListTriggers|ScheduleService/ListSchedules)$Q" 'orkflowName'
         "ActivityService/ListOrkers$Q|\blistOrkers\(" 'pool|orkerId'
         "SystemService/ListReleases$Q|\blistReleases\(" 'buildId'
         "NamespaceService/ListNamespaces$Q" 'nameContains' )
if (( stage >= 2 )); then
  ids+='|action_prefix|actionPrefix|ActionPrefix|DeliveryOutcome|DELIVERY_OUTCOME_[A-Z]+'
  flags+='|action|principal|delivered|webhook|run'
  anchored+="|$(argv webhook deliveries)[^\n]*--(event|since|until)$NOT|$(argv admin audit)[^\n]*--(since|until)$NOT"
  anchored+="|$Q-n$Q,[^\n]*$(argv admin audit)|$Q-n$Q,[^\n]*$(argv webhook deliveries)|-n \S+ (admin audit|webhook deliveries)|(admin audit|webhook deliveries)[^\n]* -n "
  built+='cmd/orkctl/webhook_deliveries.go cmd/orkctl/admin_audit.go ui/src/lib/deliveries.ts ui/src/lib/audit.ts'
  urls+="|/admin/webhooks\?[^\x27\x22\x60]*\b(webhook|run|event|delivered|since|until)=|/admin/audit\?[^\x27\x22\x60]*\b(scope|action|principal|since|until)="
  bodies+=( "WebhookService/ListDeliveries$Q" 'webhookId|runId|event|outcome|namespace|since|until'
            "AuditService/ListAuditRecords$Q" 'namespace|actionPrefix|principal|since|until' )
fi
if (( stage >= 3 )); then
  ids+='|created_after|created_before|createdAfter|createdBefore|CreatedAfter|CreatedBefore|origin_kind|origin_id'
  flags+='|origin|status'
  anchored+="|$(argv run list)[^\n]*--(orkflow|since|until)$NOT"
  built+=' cmd/orkctl/run.go ui/src/routes/runs'
  urls+="|/runs\?[^\x27\x22\x60]*\b(orkflow|status|since|until|origin|originId)="
  bodies+=( "RunService/ListRuns$Q" 'orkflowName|status|createdAfter|createdBefore|originKind|originId' )
fi
scope=(proto cmd internal pkg ui/src ui/e2e ui/test docs README.md
       ':!ui/src/lib/gen' ':!cmd/orkctl/removed_flags_test.go')
leftovers=$(
git grep --untracked -nP "\\b($ids)\\b" -- "${scope[@]}" | grep -vP '^[^:]+:\d+:\s*reserved\b'
git grep --untracked -nP -- "--($flags)$NOT" -- "${scope[@]}"
git grep --untracked -nP -- "$anchored" -- "${scope[@]}"
[[ -n $built ]] && git grep --untracked -nP -- "$Q--(orkflow|since|until|event)\\b" -- $built
git grep --untracked -nP -- "$urls" -- ui/src ui/e2e ui/test docs
for ((i = 0; i < ${#bodies[@]}; i += 2)); do
  git grep --untracked -n -A3 -P -- "${bodies[i]}" -- ui/e2e | grep -P "\\b(${bodies[i+1]})\\??\\s*:"
done
if (( stage >= 3 )); then
  git grep --untracked -n -A6 -P "listHref\\($Q/runs$Q" -- ui/src | grep -P '\b(orkflow|status|since|until|origin|originId)\s*:'
fi
)
[[ -z $leftovers ]] && exit 0
printf '%s\n' "$leftovers"
exit 1
