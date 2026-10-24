package api

import (
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

// filteredRequest is a list request that takes `repeated FieldFilter
// filters` and declares its allowlist with (orkstack.v1.filterable).
type filteredRequest interface {
	proto.Message
	GetFilters() []*orkstackv1.FieldFilter
}

// compileFilters validates a list request's filters against the allowlist
// its message declares and returns the canonical set. A refusal is
// INVALID_ARGUMENT carrying listfilter's exact message, before any read.
// docs/UI_CLI_PARITY.md "Filtering lists" is the contract.
func compileFilters(req filteredRequest) (listfilter.Set, error) {
	spec, err := listfilter.SpecFor(req)
	if err != nil {
		// A declaration bug, which TestSpec_EveryDeclarationResolves catches
		// long before a request can reach it.
		return listfilter.Set{}, connect.NewError(connect.CodeInternal, err)
	}
	set, err := spec.Compile(req.GetFilters())
	if err != nil {
		return listfilter.Set{}, connect.NewError(connect.CodeInvalidArgument, err)
	}
	return set, nil
}

// errFilterKeyMismatch is the refusal of a page token minted under another
// filter set: a cursor is a position in ONE listing.
var errFilterKeyMismatch = errors.New("page_token was issued for other filters; drop it and list from the first page")

// checkFilterKey refuses a page token whose bound filter key (Set.Key) is not
// the request's.
func checkFilterKey(tokenKey string, set listfilter.Set) error {
	if tokenKey != set.Key() {
		return connect.NewError(connect.CodeInvalidArgument, errFilterKeyMismatch)
	}
	return nil
}
