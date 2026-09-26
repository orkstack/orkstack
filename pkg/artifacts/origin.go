package artifacts

import (
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// originKindPrefix is the proto enum prefix every RunOriginKind value name
// carries and no user-facing spelling does.
const originKindPrefix = "RUN_ORIGIN_KIND_"

// OriginKindSlug is the user-facing spelling of a RunOriginKind: the proto
// value's name after RUN_ORIGIN_KIND_, lowercased, "_" as "-" — "submit",
// "trigger", "schedule", "parent-run", "run-again". It is the archive index
// segment, the memo value, the search attribute value, the CLI flag value
// and the URL parameter, so one rule spells all five. ok is false for
// UNSPECIFIED and for a number the enum does not declare.
func OriginKindSlug(kind orkstackv1.RunOriginKind) (slug string, ok bool) {
	if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
		return "", false
	}
	name, declared := orkstackv1.RunOriginKind_name[int32(kind)]
	if !declared {
		return "", false
	}
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(name, originKindPrefix)), "_", "-"), true
}

// ParseOriginKindSlug is OriginKindSlug's inverse; ok is false for any
// other string, including "" and "unspecified".
func ParseOriginKindSlug(slug string) (orkstackv1.RunOriginKind, bool) {
	if slug == "" {
		return orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED, false
	}
	for number := range orkstackv1.RunOriginKind_name {
		kind := orkstackv1.RunOriginKind(number)
		if candidate, ok := OriginKindSlug(kind); ok && candidate == slug {
			return kind, true
		}
	}
	return orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED, false
}
