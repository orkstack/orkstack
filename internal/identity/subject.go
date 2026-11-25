// Package identity is the engine's half of task identity
// (docs/TASK_IDENTITY.md): the issuer's signing keys and the documents it
// serves, and the subject a task's token names.
package identity

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Format is the deployment's subject format (identity.subject_format).
type Format string

const (
	// FormatOrk is the colon form, ork:namespace:<ns>:orkflow:<o>:task:<t>.
	FormatOrk Format = "ork"
	// FormatSPIFFE makes the subject a SPIFFE ID in the deployment's trust
	// domain, so a token is a JWT-SVID.
	FormatSPIFFE Format = "spiffe"
)

// ParseFormat reads identity.subject_format; empty is FormatOrk.
func ParseFormat(s string) (Format, error) {
	switch Format(s) {
	case "", FormatOrk:
		return FormatOrk, nil
	case FormatSPIFFE:
		return FormatSPIFFE, nil
	}
	return "", fmt.Errorf("subject format %q is not %q or %q", s, FormatOrk, FormatSPIFFE)
}

const (
	// MaxSubjectBytes bounds a subject in either form. An
	// Orkflow whose subject would pass it is refused when it is created.
	MaxSubjectBytes = 255
	// MaxTrustDomainBytes bounds identity.spiffe.trust_domain: what leaves room
	// for the shortest subject there is, so one that no Orkflow could use
	// refuses the boot instead of every create.
	MaxTrustDomainBytes = MaxSubjectBytes - len(spiffeScheme) - len("/ork/ns/a/orkflow/a/task/a")
	// ClaimSubject and ClaimSPIFFEID carry the form sub does not hold, on
	// every token of a deployment with a trust domain set.
	ClaimSubject  = "ork_subject"
	ClaimSPIFFEID = "ork_spiffe_id"
	maxNameBytes  = 200
	spiffeScheme  = "spiffe://"
)

// The slug rule every boundary holds a name to (AGENTS.md §7): a name holds
// no ":" and no "/", so both forms parse one way.
var (
	slugPattern        = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	trustDomainPattern = regexp.MustCompile(`^[a-z0-9._-]+$`)
)

// Subject is what a task's token is issued for. Owner is set only for a
// shared Orkflow, the namespace that owns it; Grant only for a grant's
// login.
type Subject struct {
	Namespace string
	Owner     string
	Orkflow   string
	Task      string
	Grant     string
}

func (s Subject) check() error {
	parts := []struct{ what, value string }{{"namespace", s.Namespace}, {"orkflow", s.Orkflow}, {"task", s.Task}}
	if s.Owner != "" {
		parts = append(parts, struct{ what, value string }{"owner", s.Owner})
	}
	if s.Grant != "" {
		parts = append(parts, struct{ what, value string }{"grant", s.Grant})
	}
	for _, p := range parts {
		if len(p.value) > maxNameBytes || !slugPattern.MatchString(p.value) {
			return fmt.Errorf("the subject's %s %q is not a name (^[a-z][a-z0-9-]*$, at most %d bytes)", p.what, p.value, maxNameBytes)
		}
	}
	return nil
}

// Ork is the colon form: ork:namespace:<ns>:orkflow:<orkflow>:task:<task>,
// with :grant:<grant> for a grant's login. A shared Orkflow is
// <owner>/<name>.
func (s Subject) Ork() (string, error) {
	if err := s.check(); err != nil {
		return "", err
	}
	orkflow := s.Orkflow
	if s.Owner != "" {
		orkflow = s.Owner + "/" + s.Orkflow
	}
	out := "ork:namespace:" + s.Namespace + ":orkflow:" + orkflow + ":task:" + s.Task
	if s.Grant != "" {
		out += ":grant:" + s.Grant
	}
	return out, nil
}

// SPIFFEID is the subject as a SPIFFE ID in trustDomain:
// spiffe://<td>/ork/ns/<ns>/orkflow/<orkflow>/task/<task>[/grant/<g>], or
// .../ns/<ns>/shared/<owner>/<orkflow>/... for a shared Orkflow.
func (s Subject) SPIFFEID(trustDomain string) (string, error) {
	if err := ValidTrustDomain(trustDomain); err != nil {
		return "", err
	}
	if err := s.check(); err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString(spiffeScheme + trustDomain + "/ork/ns/" + s.Namespace)
	if s.Owner != "" {
		b.WriteString("/shared/" + s.Owner + "/" + s.Orkflow)
	} else {
		b.WriteString("/orkflow/" + s.Orkflow)
	}
	b.WriteString("/task/" + s.Task)
	if s.Grant != "" {
		b.WriteString("/grant/" + s.Grant)
	}
	return b.String(), nil
}

// ParseSPIFFEID reads a SPIFFE ID SPIFFEID wrote back into its subject and
// trust domain.
func ParseSPIFFEID(id string) (Subject, string, error) {
	rest, ok := strings.CutPrefix(id, spiffeScheme)
	if !ok {
		return Subject{}, "", errors.New("not a spiffe:// ID")
	}
	domain, path, ok := strings.Cut(rest, "/")
	if !ok {
		return Subject{}, "", errors.New("a SPIFFE ID with no path")
	}
	if err := ValidTrustDomain(domain); err != nil {
		return Subject{}, "", err
	}
	seg := strings.Split(path, "/")
	for _, part := range seg {
		if part == "" {
			// An empty owner or grant would read as an ID without one.
			return Subject{}, "", fmt.Errorf("%q has an empty path segment", id)
		}
	}
	var s Subject
	switch {
	case len(seg) >= 7 && seg[0] == "ork" && seg[1] == "ns" && seg[3] == "orkflow" && seg[5] == "task":
		s, seg = Subject{Namespace: seg[2], Orkflow: seg[4], Task: seg[6]}, seg[7:]
	case len(seg) >= 8 && seg[0] == "ork" && seg[1] == "ns" && seg[3] == "shared" && seg[6] == "task":
		s, seg = Subject{Namespace: seg[2], Owner: seg[4], Orkflow: seg[5], Task: seg[7]}, seg[8:]
	default:
		return Subject{}, "", fmt.Errorf("%q is not an OrkStack task's SPIFFE ID", id)
	}
	switch {
	case len(seg) == 2 && seg[0] == "grant":
		s.Grant = seg[1]
	case len(seg) != 0:
		return Subject{}, "", fmt.Errorf("%q is not an OrkStack task's SPIFFE ID", id)
	}
	if err := s.check(); err != nil {
		return Subject{}, "", err
	}
	return s, domain, nil
}

// ValidTrustDomain holds identity.spiffe.trust_domain to its rule:
// lowercase letters, digits, ".", "-" and "_", 1 to 255 bytes, with no
// scheme, port or path.
func ValidTrustDomain(domain string) error {
	if domain == "" || len(domain) > MaxTrustDomainBytes || !trustDomainPattern.MatchString(domain) {
		return fmt.Errorf("trust domain %q is not 1 to %d bytes of lowercase letters, digits, \".\", \"-\" and \"_\"", domain, MaxTrustDomainBytes)
	}
	return nil
}

// errSubjectTooLong is a subject that does not fit, as against one that is no
// subject.
var errSubjectTooLong = errors.New("too long")

// Fits says whether the subject fits MaxSubjectBytes in the form format
// chooses and, when a trust domain is set, in the other form as well, so
// that a later change of format refuses no Orkflow that exists.
func (s Subject) Fits(format Format, trustDomain string) error {
	if format == FormatSPIFFE && trustDomain == "" {
		return errors.New("the spiffe subject format needs a trust domain")
	}
	ork, err := s.Ork()
	if err != nil {
		return err
	}
	if len(ork) > MaxSubjectBytes {
		return fmt.Errorf("%w: the subject %q is %d bytes; at most %d", errSubjectTooLong, ork, len(ork), MaxSubjectBytes)
	}
	if trustDomain == "" {
		return nil
	}
	id, err := s.SPIFFEID(trustDomain)
	if err != nil {
		return err
	}
	if len(id) > MaxSubjectBytes {
		return fmt.Errorf("%w: the SPIFFE subject %q is %d bytes; at most %d", errSubjectTooLong, id, len(id), MaxSubjectBytes)
	}
	return nil
}

// Claims answers sub in the chosen format and, with a trust domain set, the
// claim that carries the other form and its value. With none set,
// otherClaim is empty.
func (s Subject) Claims(format Format, trustDomain string) (sub, otherClaim, otherValue string, err error) {
	ork, err := s.Ork()
	if err != nil {
		return "", "", "", err
	}
	if trustDomain == "" {
		if format == FormatSPIFFE {
			return "", "", "", errors.New("the spiffe subject format needs a trust domain")
		}
		return ork, "", "", nil
	}
	id, err := s.SPIFFEID(trustDomain)
	if err != nil {
		return "", "", "", err
	}
	if format == FormatSPIFFE {
		return id, ClaimSubject, ork, nil
	}
	return ork, ClaimSPIFFEID, id, nil
}
