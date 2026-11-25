package servicehost

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sort"
)

// The reach probe (14 §7): before an Orker advertises that it hosts
// services, and every hour, a probe container on a service network tries
// what a service must not reach, and the Orker judges its answer here. The
// probe container's output is untrusted input: it is parsed strictly and
// bounded.

// The probe's four questions.
const (
	QuestionMetadata = 1 // no metadata endpoint, and no operator-listed address, answers
	QuestionLoopback = 2 // nothing bound to the host's loopback can be opened
	QuestionCluster  = 3 // the cluster serves no caller that presents no credential
	QuestionCanary   = 4 // no other container, on another service's network or the default one, answers
)

// The kinds of a probe target.
const (
	TargetService    = "service"    // a gRPC service port: closed only by the S7 list
	TargetMembership = "membership" // a cluster membership port: any connection fails
	TargetAddress    = "address"    // any other address: any connection fails
)

// The ways a call is tried (S7).
const (
	ViaTCP       = "tcp"
	ViaPlaintext = "plaintext"
	ViaTLS       = "tls"
)

// The TLS handshake outcomes a probe reports.
const (
	TLSOK                = "ok"
	TLSClientCertRequire = "client_cert_required"
	TLSFailed            = "failed"
)

// Bounds of a probe answer.
const (
	MaxProbeAnswerBytes  = 256 << 10
	maxProbeObservations = 1024
	maxProbeText         = 256
)

// Observation is one thing the probe tried and what it saw.
type Observation struct {
	Question  int    `json:"q"`
	Target    string `json:"target"`
	Kind      string `json:"kind"`
	Via       string `json:"via"`
	Connected bool   `json:"connected"`
	// Unresolved: the target is a name that resolved to no address that may
	// be asked, so nothing was asked of it.
	Unresolved bool   `json:"unresolved,omitzero"`
	TLS        string `json:"tls,omitzero"`
	// Status is the gRPC status code's name of an answered call ("OK",
	// "Unimplemented", "Unauthenticated", …), "" when no call was answered.
	Status string `json:"status,omitzero"`
}

// ParseProbeAnswer reads the probe container's answer: one JSON object per
// line, each an Observation. Anything else is an error: a probe that
// cannot be read is no verdict, and the Orker hosts no service.
func ParseProbeAnswer(raw []byte) ([]Observation, error) {
	if len(raw) > MaxProbeAnswerBytes {
		return nil, fmt.Errorf("servicehost: the probe answered %d bytes; at most %d", len(raw), MaxProbeAnswerBytes)
	}
	var out []Observation
	for i, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if len(out) == maxProbeObservations {
			return nil, fmt.Errorf("servicehost: the probe answered more than %d observations", maxProbeObservations)
		}
		var o Observation
		if err := json.Unmarshal(line, &o, json.RejectUnknownMembers(true)); err != nil {
			return nil, fmt.Errorf("servicehost: probe line %d is not an observation", i+1)
		}
		if err := o.valid(); err != nil {
			return nil, fmt.Errorf("servicehost: probe line %d: %w", i+1, err)
		}
		out = append(out, o)
	}
	return out, nil
}

func (o Observation) valid() error {
	switch {
	case o.Question < QuestionMetadata || o.Question > QuestionCanary:
		return fmt.Errorf("question %d", o.Question)
	case o.Target == "" || len(o.Target) > maxProbeText || !printable(o.Target):
		return errors.New("a target is 1 to 256 printable bytes")
	case o.Kind != TargetService && o.Kind != TargetMembership && o.Kind != TargetAddress:
		return fmt.Errorf("kind %q", o.Kind)
	case o.Via != ViaTCP && o.Via != ViaPlaintext && o.Via != ViaTLS:
		return fmt.Errorf("via %q", o.Via)
	case o.TLS != "" && o.TLS != TLSOK && o.TLS != TLSClientCertRequire && o.TLS != TLSFailed:
		return fmt.Errorf("tls %q", o.TLS)
	case len(o.Status) > 64 || !printable(o.Status):
		return errors.New("a status is at most 64 printable bytes")
	case o.Unresolved && (o.Connected || o.TLS != ""):
		return errors.New("a connection to a name that did not resolve")
	case o.Status != "" && !o.Connected:
		return errors.New("an answered call on no connection")
	}
	return nil
}

// Reached reports whether one observation is something a service could
// reach (14 §7): for a service port, any answer on a connection that was
// made, unless the server turned the caller away for having no credential;
// for anything else, any connection at all.
func (o Observation) Reached() bool {
	if !o.Connected {
		return false
	}
	if o.Kind != TargetService {
		return true
	}
	if o.TLS == TLSClientCertRequire {
		return false
	}
	switch o.Status {
	case "Unauthenticated", "PermissionDenied":
		return false
	case "":
		// Connected and nothing answered: a handshake that failed for
		// another reason, or a port that took the connection and said
		// nothing a gRPC client can read. Either way it took a connection
		// from a service's network.
		return o.TLS != TLSFailed || o.Via != ViaTLS
	}
	return true
}

// Fitness is the probe's verdict: fit when every question is clean, and
// each failure named by its question and target.
type Fitness struct {
	Fit      bool
	Failures []string
}

// JudgeProbe judges a probe's observations. A probe that asked nothing of
// a question it must ask (wantQuestions) is not clean for it: a probe
// cannot pass by saying nothing.
func JudgeProbe(obs []Observation, wantQuestions ...int) Fitness {
	asked := map[int]bool{}
	// A port whose TLS handshake demanded a client certificate is closed
	// (14 §7): the plaintext leg's failure to be answered there is the same
	// server refusing a client that came without one, not an opening.
	certDemanded := map[string]bool{}
	for _, o := range obs {
		if o.Kind == TargetService && o.Via == ViaTLS && o.Connected && o.TLS == TLSClientCertRequire {
			certDemanded[o.Target] = true
		}
	}
	var failures []string
	for _, o := range obs {
		asked[o.Question] = true
		if o.Kind == TargetService && o.Via == ViaPlaintext && certDemanded[o.Target] && unanswered(o.Status) {
			continue
		}
		if o.Reached() {
			failures = append(failures, fmt.Sprintf("question %d: %s reached %s (%s%s)", o.Question, o.Via, o.Target, o.Kind, statusText(o)))
		}
	}
	for _, q := range wantQuestions {
		if !asked[q] {
			failures = append(failures, fmt.Sprintf("question %d: not asked", q))
		}
	}
	sort.Strings(failures)
	return Fitness{Fit: len(failures) == 0, Failures: failures}
}

// unanswered is a plaintext call's status that is no answer from the server:
// none, or what a client reports when it got no gRPC reply.
func unanswered(status string) bool {
	switch status {
	case "", "Unavailable", "DeadlineExceeded", "Canceled":
		return true
	}
	return false
}

func statusText(o Observation) string {
	if o.Status == "" {
		return ""
	}
	return ", answered " + o.Status
}
