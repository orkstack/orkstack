---
source_url:
  - https://docs.temporal.io/visibility
  - https://docs.temporal.io/search-attribute
  - https://docs.temporal.io/list-filter
  - https://docs.temporal.io/temporal-service/temporal-server#retention-period
  - https://docs.temporal.io/self-hosted-guide/visibility
  - https://github.com/temporalio/temporal/blob/83b35dc3bac4ea05296154e0b50055026d200150/common/dynamicconfig/constants.go
  - https://raw.githubusercontent.com/temporalio/docker-builds/main/docker/auto-setup.sh
fetched_at: 2026-08-14
fetch_method: fetch_content + git clone (temporalio/temporal @ 83b35dc3bac4ea05296154e0b50055026d200150, temporalio/cli @ 9112b4e0ac7a2b4321251f5922659363813feb7a)
topic: Temporal visibility/persistence limits — search attributes, retention, history size, list API
---

Cached extract of the Temporal docs pages and dynamic-config defaults used for the
"can Temporal replace a run DB?" decision. See
research/web/temporal-visibility-findings.md for the analysed findings.

Key raw defaults (common/dynamicconfig/constants.go @ 83b35dc):
- limit.blobSize.error = 2*1024*1024 ; limit.blobSize.warn = 512*1024
- limit.memoSize.error = 2*1024*1024 ; limit.memoSize.warn = 2*1024
- limit.historySize.error = 50*1024*1024 ; .warn = 10*1024*1024 ; .suggestContinueAsNew = 4*1024*1024
- limit.historyCount.error = 50*1024 (51200 events) ; .warn = 10*1024 (10240)
- limit.mutableStateSize.error = 8*1024*1024 ; .warn = 1*1024*1024
- frontend.visibilityMaxPageSize = 1000 ; frontend.historyMaxPageSize = 256 (primitives.GetHistoryMaxPageSize)
- frontend.searchAttributesNumberOfKeysLimit = 100
- frontend.searchAttributesSizeOfValueLimit = 2*1024
- frontend.searchAttributesTotalSizeLimit = 40*1024
- system.namespaceMinRetentionGlobal = 24h ; system.namespaceMinRetentionLocal = 1h
- system.visibilityDisableOrderByClause = true (ES) ; worker.ESProcessorFlushInterval = 1s
- schema/sqlite/setup.go: dev-server default namespace Retention = 24h
- docker auto-setup.sh: DEFAULT_NAMESPACE_RETENTION=24h
