package aws

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/orkstack/orkstack/internal/provider"
)

// service is the AWS service a secret's locator names.
type service string

const (
	serviceSSM     service = "ssm"
	serviceSecrets service = "secretsmanager"
	serviceKMS     service = "kms"
)

// maxCiphertext is the longest blob KMS decrypts.
const maxCiphertext = 6144

// reference is what Parse read of a reference; a credential's is the zero
// value, since it is the login itself.
type reference struct {
	service service
	// id is the parameter's name, the secret's id or the key's, as written.
	id string
	// field is the key of a JSON secret to read, "" for the whole value.
	field string
}

const (
	kmsID    = `[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}|mrk-[a-f0-9]{32}`
	kmsAlias = `alias/[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*`
)

var (
	// arnRegion is the region an ARN names.
	arnRegion = regexp.MustCompile(`^arn:[a-z0-9-]+:[a-z]+:([a-z0-9-]+):`)
	// ssmName is a Parameter Store name, hierarchical or not, with an
	// optional ":<version or label>", or a parameter's ARN.
	ssmName = regexp.MustCompile(`^(arn:[a-z0-9-]+:ssm:[a-z0-9-]+:[0-9]{12}:parameter/)?/?[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*(:[A-Za-z0-9_.-]+)?$`)
	// secretID is a Secrets Manager secret's name or ARN.
	secretID = regexp.MustCompile(`^(arn:[a-z0-9-]+:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:)?[A-Za-z0-9_+=.@-]+(/[A-Za-z0-9_+=.@-]+)*$`)
	// kmsKey is a KMS key's id, its alias, or the ARN of either.
	kmsKey = regexp.MustCompile(`^(` + kmsID + `|` + kmsAlias + `|arn:[a-z0-9-]+:kms:[a-z0-9-]+:[0-9]{12}:(key/(` + kmsID + `)|` + kmsAlias + `))$`)
)

// parse checks one reference (providers.md §7) to a provider of region. It
// does no I/O.
func parse(ref provider.Ref, region string) (reference, error) {
	if err := provider.ValidParams(ref.Params, nil); err != nil {
		return reference{}, err
	}
	if ref.Kind == provider.KindCredential {
		switch {
		case ref.Locator != "":
			return reference{}, fmt.Errorf("an aws credential is the role's session and takes no path")
		case len(ref.Ciphertext) > 0:
			return reference{}, fmt.Errorf("an aws credential takes no ciphertext")
		}
		return reference{}, nil
	}
	if ref.Kind != provider.KindSecret {
		return reference{}, fmt.Errorf("an aws provider has no reference of kind %s", ref.Kind)
	}
	if err := provider.ValidLocator(ref.Locator); err != nil {
		return reference{}, err
	}
	path, field, _ := strings.Cut(ref.Locator, "#")
	name, id, ok := strings.Cut(path, ":")
	out := reference{service: service(name), id: id, field: field}
	var shape *regexp.Regexp
	switch {
	case !ok:
	case out.service == serviceSSM:
		shape = ssmName
	case out.service == serviceSecrets:
		shape = secretID
	case out.service == serviceKMS:
		shape = kmsKey
	}
	switch {
	case shape == nil:
		return reference{}, fmt.Errorf("an aws secret's source is ssm:<name>, secretsmanager:<id> or kms:<key>")
	case !shape.MatchString(id):
		return reference{}, fmt.Errorf("%q is not a name %s takes", id, name)
	case hasDotSegment(id):
		return reference{}, fmt.Errorf("%q has a \".\" or \"..\" segment; a name is never normalized", id)
	case otherRegion(id, region):
		return reference{}, fmt.Errorf("%q is of another region than the provider's, %s", id, region)
	case field != "" && out.service != serviceSecrets:
		return reference{}, fmt.Errorf("only a secretsmanager source takes a \"#<key>\"")
	case out.service == serviceKMS && len(ref.Ciphertext) == 0:
		return reference{}, fmt.Errorf("a kms source decrypts the secret's ciphertext, and it has none")
	case out.service == serviceKMS && len(ref.Ciphertext) > maxCiphertext:
		return reference{}, fmt.Errorf("the ciphertext is %d bytes; KMS decrypts at most %d", len(ref.Ciphertext), maxCiphertext)
	case out.service != serviceKMS && len(ref.Ciphertext) > 0:
		return reference{}, fmt.Errorf("only a kms source takes a ciphertext")
	}
	return out, nil
}

// hasDotSegment is a "." or ".." segment of id. ValidLocator refuses one
// between slashes; the first segment of an id follows its service's ":".
func hasDotSegment(id string) bool {
	for segment := range strings.SplitSeq(id, "/") {
		for part := range strings.SplitSeq(segment, ":") {
			if part == "." || part == ".." {
				return true
			}
		}
	}
	return false
}

// otherRegion is an ARN of a region a call is not made in.
func otherRegion(id, region string) bool {
	m := arnRegion.FindStringSubmatch(id)
	return m != nil && m[1] != region
}
