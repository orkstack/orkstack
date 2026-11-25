package artifacts

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
)

// The identity records' keys (docs/TASK_IDENTITY.md). What the engine
// records as a task's or a run's result lies under AttestedPrefix, beside
// the commits it mirrors and where an Orker may read but never write; a
// resolution's detail and a resume's record lie under the run's snapshot
// prefix, which only engines read. These functions are the one place any
// of these keys is spelled.
const (
	// AttestedPrefix holds the attested twin of every commit and the
	// records the engine writes for a task or a run.
	AttestedPrefix = "metadata/attested"

	resolutionObjectName = "resolution.json"
	composedObjectName   = "composed.json"
	outputsObjectName    = "outputs.json"
	resumeLinkName       = "next.json"

	// scopeWidth is the width of a resume scope's segment, r{e:020}: a
	// uint64 in decimal.
	scopeWidth = 20
	// maxResumePlan is the highest plan number a resume's record key
	// renders: three digits, and few enough that a scope's records and its
	// link are one page of a listing.
	maxResumePlan = 998
)

// ErrInvalidRecordKey reports an identity record coordinate that cannot be
// addressed.
var ErrInvalidRecordKey = errors.New("invalid record key")

// RecordKind names which record of a task or a run a key addresses.
type RecordKind int

const (
	// RecordResolution is an awaited task's resolution.
	RecordResolution RecordKind = iota + 1
	// RecordComposed is a composed task's values.
	RecordComposed
	// RecordOutputs is a run's outputs, which belong to the run, not a
	// task: OutputsRecordKey renders its key.
	RecordOutputs
)

// AttestedRunPrefix is the prefix of every attested object of one run:
// metadata/attested/{namespace}/{encoded-run-id}/. A listing of it with
// delimiter "/" folds each task's objects into one entry.
func AttestedRunPrefix(namespace, runID string) (string, error) {
	ns, run, err := recordRun(namespace, runID)
	if err != nil {
		return "", err
	}
	return path.Join(AttestedPrefix, ns, run) + "/", nil
}

// AttestedCommitKey maps a commit key, as CommitKey or ItemCommitKey (or a
// resume scope's commit key) renders it, to its attested twin: the same
// segments under AttestedPrefix. It is the only mapping from one to the
// other, and refuses any key that is not a commit key.
func AttestedCommitKey(commitKey string) (string, error) {
	rest, ok := strings.CutPrefix(commitKey, CommitPrefix+"/")
	if !ok {
		return "", fmt.Errorf("artifacts: attested key: %q is not a commit key: %w", commitKey, ErrInvalidRecordKey)
	}
	segs := strings.Split(rest, "/")
	// {ns}/{run}/{task}/[r{e}/][{item}/]commit.json
	if len(segs) < 4 || len(segs) > 6 || segs[len(segs)-1] != CommitObjectName {
		return "", fmt.Errorf("artifacts: attested key: %q is not a commit key: %w", commitKey, ErrInvalidRecordKey)
	}
	if err := validNamespaceName(segs[0]); err != nil {
		return "", fmt.Errorf("artifacts: attested key: namespace: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	if _, ok := decodeArchiveSegment(segs[1]); !ok {
		return "", fmt.Errorf("artifacts: attested key: %q: run id segment: %w", commitKey, ErrInvalidRecordKey)
	}
	if err := validCommitTaskID(segs[2]); err != nil {
		return "", fmt.Errorf("artifacts: attested key: task id: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	mid := segs[3 : len(segs)-1]
	if len(mid) > 0 {
		if scope, ok := parseScopeSegment(mid[0]); ok {
			if scope == 0 {
				return "", fmt.Errorf("artifacts: attested key: %q names scope 0, which no key renders: %w", commitKey, ErrInvalidRecordKey)
			}
			mid = mid[1:]
		}
	}
	if len(mid) > 1 {
		return "", fmt.Errorf("artifacts: attested key: %q is not a commit key: %w", commitKey, ErrInvalidRecordKey)
	}
	if len(mid) == 1 {
		if _, ok := parseItemSegment(mid[0]); !ok {
			return "", fmt.Errorf("artifacts: attested key: %q is not a commit key: %w", commitKey, ErrInvalidRecordKey)
		}
	}
	return AttestedPrefix + "/" + rest, nil
}

// AttestedCommitURI maps a commit uri, a store root then a commit key as a
// task's input names it, to the uri of its attested twin: the same root and
// the key AttestedCommitKey maps it to. A uri that holds no commit key, or
// whose root lies under a commit or an attested prefix, so that its twin would
// be an object an Orker may write and no record, is ErrInvalidRecordKey.
func AttestedCommitURI(uri string) (string, error) {
	i := strings.LastIndex(uri, "/"+CommitPrefix+"/")
	if i < 0 {
		return "", fmt.Errorf("artifacts: attested uri: %q holds no commit key: %w", uri, ErrInvalidRecordKey)
	}
	root := uri[:i+1]
	if strings.Contains(root, "/"+CommitPrefix+"/") || strings.Contains(root, "/"+AttestedPrefix+"/") {
		return "", fmt.Errorf("artifacts: attested uri: the root of %q is no store's: %w", uri, ErrInvalidRecordKey)
	}
	key, err := AttestedCommitKey(uri[i+1:])
	if err != nil {
		return "", err
	}
	return root + key, nil
}

// RecordKey renders the key of a task's record of kind:
// metadata/attested/{namespace}/{encoded-run-id}/{task-id}/[r{scope:020}/]{resolution,composed}.json.
// scope is the root's resume scope; zero, a run never resumed or a run
// that is not a root, renders no segment.
func RecordKey(kind RecordKind, namespace, runID, taskID string, scope uint64) (string, error) {
	var leaf string
	switch kind {
	case RecordResolution:
		leaf = resolutionObjectName
	case RecordComposed:
		leaf = composedObjectName
	default:
		return "", fmt.Errorf("artifacts: record key: kind %d has no task key: %w", kind, ErrInvalidRecordKey)
	}
	prefix, err := AttestedRunPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	if err := validCommitTaskID(taskID); err != nil {
		return "", fmt.Errorf("artifacts: record key: task id: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	if scope == 0 {
		return prefix + taskID + "/" + leaf, nil
	}
	return prefix + taskID + "/" + scopeSegment(scope) + "/" + leaf, nil
}

// OutputsRecordKey renders the key of a child run's outputs:
// metadata/attested/{namespace}/{encoded-run-id}/outputs.json. A task id
// holds no ".", so it is no task's segment.
func OutputsRecordKey(namespace, runID string) (string, error) {
	prefix, err := AttestedRunPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return prefix + outputsObjectName, nil
}

// ResolutionDetailKey renders the key of a resolution's detail, named by
// the SHA-256 of its bytes (64 lowercase hex):
// metadata/snapshots/{namespace}/{run-id}/identity/resolutions/{task-id}/{sha256}.json.
func ResolutionDetailKey(namespace, runID, taskID, sha256Hex string) (string, error) {
	if !lowerHex64(sha256Hex) {
		return "", fmt.Errorf("artifacts: detail key: %q is not a SHA-256 in lowercase hex: %w", sha256Hex, ErrInvalidRecordKey)
	}
	if err := validCommitTaskID(taskID); err != nil {
		return "", fmt.Errorf("artifacts: detail key: task id: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return prefix + "resolutions/" + taskID + "/" + sha256Hex + ".json", nil
}

// ResumeRecordKey renders the key of a resume's record, scope in decimal
// and the resume's plan number n in three digits:
// metadata/snapshots/{namespace}/{run-id}/identity/resumes/{scope}/{n:03}.json.
func ResumeRecordKey(namespace, runID string, scope uint64, n int) (string, error) {
	if n < 0 || n > maxResumePlan {
		return "", fmt.Errorf("artifacts: resume record key: plan number %d is not 0 to %d: %w", n, maxResumePlan, ErrInvalidRecordKey)
	}
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%sresumes/%d/%03d.json", prefix, scope, n), nil
}

// ResumeLinkKey renders the key of the link from previousScope to the
// scope a resume opened: .../identity/resumes/{previous_scope}/next.json.
// The first resume's link leads from scope 0.
func ResumeLinkKey(namespace, runID string, previousScope uint64) (string, error) {
	prefix, err := identitySnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	return prefix + "resumes/" + strconv.FormatUint(previousScope, 10) + "/" + resumeLinkName, nil
}

// recordRun validates and encodes a record's namespace and run, as
// CommitKey does: the namespace validated, the run id encoded.
func recordRun(namespace, runID string) (string, string, error) {
	namespace = ResolveNamespace(namespace)
	if err := validNamespaceName(namespace); err != nil {
		return "", "", fmt.Errorf("artifacts: record key: namespace: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	run, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", "", fmt.Errorf("artifacts: record key: run id: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	return namespace, run, nil
}

func identitySnapshotPrefix(namespace, runID string) (string, error) {
	if err := validNamespaceName(ResolveNamespace(namespace)); err != nil {
		return "", fmt.Errorf("artifacts: record key: namespace: %w", errors.Join(ErrInvalidRecordKey, err))
	}
	prefix, err := RunSnapshotPrefix(namespace, runID)
	if err != nil {
		return "", errors.Join(ErrInvalidRecordKey, err)
	}
	return prefix + "identity/", nil
}

func scopeSegment(scope uint64) string {
	return fmt.Sprintf("r%0*d", scopeWidth, scope)
}

// parseScopeSegment reads a segment scopeSegment rendered.
func parseScopeSegment(s string) (uint64, bool) {
	digits, ok := strings.CutPrefix(s, "r")
	if !ok || len(digits) != scopeWidth {
		return 0, false
	}
	v, err := strconv.ParseUint(digits, 10, 64)
	return v, err == nil
}

func lowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
