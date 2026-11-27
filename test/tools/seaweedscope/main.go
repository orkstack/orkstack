// Command seaweedscope prints the SeaweedFS identity actions of the
// documented Orker storage policy on one bucket, comma-separated, as
// `weed shell`'s s3.configure -actions takes them: what `make up` grants the
// development Orker's own key (CONTRIBUTING.md, "Task identity on the
// development stack").
//
//	seaweedscope -bucket orkstack-artifacts
//
// It is no S3 or SeaweedFS client and reads nothing: make up drives the store
// through SeaweedFS's own `weed shell` s3.configure and reads its listing
// back with jq. This only renders s3scope.OrkerPolicy, which a test holds to
// docs/DEPLOY.md ("An Orker's own storage credential") and the SeaweedFS
// scope test runs on a real store, so the stack's Orker and the proofs grant
// one policy.
package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
)

// bucketName is an S3 bucket name as SeaweedFS accepts one, so the actions
// cannot carry a comma or anything else s3.configure would split on.
var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func main() {
	bucket := flag.String("bucket", "", "the bucket the actions are scoped to")
	flag.Parse()
	out, err := actions(*bucket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "seaweedscope:", err)
		os.Exit(2)
	}
	fmt.Println(strings.Join(out, ","))
}

func actions(bucket string) ([]string, error) {
	if !bucketName.MatchString(bucket) {
		return nil, fmt.Errorf("-bucket %q is not a bucket name", bucket)
	}
	return s3scope.OrkerPolicy().SeaweedActions(bucket), nil
}
