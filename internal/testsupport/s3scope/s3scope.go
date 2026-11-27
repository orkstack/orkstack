// Package s3scope is an in-process S3 proxy that holds every request to a
// storage policy, for the tests of an Orker's own storage credential
// (docs/DEPLOY.md, "An Orker's own storage credential").
//
// A test puts it in front of its own bucket and points the client under test
// at it. It forwards a request the policy allows to the upstream store, with
// the client's own signature, and answers every other one itself with the
// store's error AccessDenied. It reads the policy the way AWS S3 does in the
// one respect the proofs depend on: a GET or HEAD of a missing key under a
// prefix the policy does not let the caller list is answered 403, not 404,
// because the store will not say what it may not list.
//
// The policy is its own reading of the documented one (OrkerPolicy), never
// pkg/artifacts' table: a test that compared the scope check with the code it
// checks would prove nothing. A test can also script an answer for any
// request (Script), which the scope check's table test uses to give each
// store answer it reads.
package s3scope

import (
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
)

// Verb is one kind of request the policy allows on a prefix.
type Verb string

const (
	// Put writes an object: PutObject, a multipart upload's requests, the
	// destination of a copy.
	Put Verb = "put"
	// Get reads an object, or the source of a copy.
	Get Verb = "get"
	// Head reads an object's metadata.
	Head Verb = "head"
	// List lists keys under a prefix.
	List Verb = "list"
	// Delete removes an object.
	Delete Verb = "delete"
)

// Rule allows Verbs on every key under Prefix, which ends in its slash.
type Rule struct {
	Prefix string
	Verbs  []Verb
}

// Policy is an allow-list: a request no rule allows is refused.
type Policy []Rule

// OrkerPolicy is the documented Orker policy, prefix by prefix and verb by
// verb (docs/DEPLOY.md, "An Orker's own storage credential").
func OrkerPolicy() Policy {
	all := []Verb{Put, Get, Head, List}
	return Policy{
		{Prefix: "orkflows/", Verbs: all},
		{Prefix: "manifests/", Verbs: all},
		{Prefix: "metadata/commits/", Verbs: all},
		{Prefix: "metadata/attested/", Verbs: []Verb{Get, Head, List}},
		{Prefix: "metadata/memo/", Verbs: all},
		{Prefix: "metadata/logs/", Verbs: []Verb{Put}},
		{Prefix: "metadata/attempts/", Verbs: []Verb{Put, List}},
		{Prefix: "metadata/probes/", Verbs: []Verb{Put, Get, Head, List, Delete}},
		{Prefix: "metadata/receipts/", Verbs: []Verb{Put}},
		{Prefix: "metadata/staging/event-sources/", Verbs: []Verb{Put}},
		{Prefix: "metadata/run-context/", Verbs: []Verb{Get, Head}},
	}
}

// SeaweedActions is p in SeaweedFS's identity actions on bucket, in p's
// order: Read for get and head, Write for put and delete, List for list,
// each on the rule's prefix. SeaweedFS's Write covers put and delete
// together, so a rule that allows only one of them is granted both; no
// prefix the Orker's scope check asks lies under one of those rules.
func (p Policy) SeaweedActions(bucket string) []string {
	var out []string
	for _, rule := range p {
		for _, action := range []struct {
			name  string
			verbs []Verb
		}{
			{"Read", []Verb{Get, Head}},
			{"Write", []Verb{Put, Delete}},
			{"List", []Verb{List}},
		} {
			if slices.ContainsFunc(action.verbs, func(v Verb) bool { return slices.Contains(rule.Verbs, v) }) {
				out = append(out, fmt.Sprintf("%s:%s/%s*", action.name, bucket, rule.Prefix))
			}
		}
	}
	return out
}

// Everything allows every verb on every key: an unscoped credential.
func Everything() Policy {
	return Policy{{Prefix: "", Verbs: []Verb{Put, Get, Head, List, Delete}}}
}

// Without is p with verb taken off the rule for prefix.
func (p Policy) Without(prefix string, verb Verb) Policy {
	out := make(Policy, 0, len(p))
	for _, r := range p {
		if r.Prefix == prefix {
			r.Verbs = slices.DeleteFunc(slices.Clone(r.Verbs), func(v Verb) bool { return v == verb })
		}
		out = append(out, r)
	}
	return out
}

// With is p with one more rule.
func (p Policy) With(prefix string, verbs ...Verb) Policy {
	return append(slices.Clone(p), Rule{Prefix: prefix, Verbs: verbs})
}

// Allows reports whether the policy allows verb on key. A listing's key is
// its prefix, so a listing is allowed only of a prefix that lies wholly
// inside a rule allowing it: a listing of a rule's parent, or of the whole
// bucket, would show other prefixes' keys.
func (p Policy) Allows(verb Verb, key string) bool {
	for _, r := range p {
		if strings.HasPrefix(key, r.Prefix) && slices.Contains(r.Verbs, verb) {
			return true
		}
	}
	return false
}

// Request is one request the proxy saw, as the policy reads it.
type Request struct {
	Method string
	Verb   Verb
	// Key is the object's key, or a listing's prefix.
	Key string
	// IfMatch is the request's If-Match header.
	IfMatch string
	// Allowed is the policy's answer, before any script.
	Allowed bool
	// Status is what the store answered; set for After alone.
	Status int
}

// Answer is a scripted reply: an HTTP status and, unless the status is a
// success, the store's error code in an S3 error body. Stall holds the
// request until the client gives up.
type Answer struct {
	Status int
	Code   string
	Stall  bool
}

// Proxy is one test's policy proxy.
type Proxy struct {
	// URL is the endpoint a client under test is pointed at (path style).
	URL string

	bucket  string
	forward *httputil.ReverseProxy
	hiding  *httputil.ReverseProxy
	server  *http.Server
	done    chan struct{}

	mu       sync.Mutex
	policy   Policy
	script   func(Request) *Answer
	requests []Request
	// lose answers the next lost requests it matches 500 after forwarding
	// them, as a store whose reply is lost after the write landed.
	after    func(Request)
	open     bool
	lose     func(Request) bool
	lost     int
	lostSeen int
}

// LoseAnswer makes the next n requests that match reach the store and then
// be answered 500 InternalError, as a write whose answer was lost after it
// landed. LostSeen counts the ones answered so.
func (p *Proxy) LoseAnswer(match func(Request) bool, n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lose, p.lost = match, n
}

// LostSeen is how many answers LoseAnswer has lost.
func (p *Proxy) LostSeen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lostSeen
}

// New starts a proxy in front of upstream (an S3 endpoint URL) for bucket,
// holding requests to policy. It stops when the test ends.
func New(t testing.TB, upstream, bucket string, policy Policy) *Proxy {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatalf("s3scope: upstream %q: %v", upstream, err)
	}
	p := &Proxy{bucket: bucket, policy: policy, done: make(chan struct{})}
	p.forward = reverseProxy(target, nil)
	p.hiding = reverseProxy(target, hideMissing)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("s3scope: listen: %v", err)
	}
	p.server = &http.Server{Handler: http.HandlerFunc(p.serve)}
	go func() { _ = p.server.Serve(listener) }()
	p.URL = "http://" + listener.Addr().String()
	t.Cleanup(func() {
		close(p.done)
		_ = p.server.Close()
	})
	return p
}

// Open starts a proxy as New does that refuses nothing, bucket-level requests
// included: an engine's credential, whose requests a test scripts.
func Open(t testing.TB, upstream, bucket string) *Proxy {
	p := New(t, upstream, bucket, Everything())
	p.mu.Lock()
	defer p.mu.Unlock()
	p.open = true
	return p
}

// After sets a function called once the store has answered a request and
// before its client hears: what happens to a client lost in between. It is
// an open proxy's alone. Nil removes it.
func (p *Proxy) After(fn func(Request)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.after = fn
}

func reverseProxy(target *url.URL, modify func(*http.Response) error) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			// The client signed the Host it dialled, and the upstream
			// verifies the signature against the Host it receives.
			pr.Out.Host = pr.In.Host
		},
		ModifyResponse: modify,
	}
}

// hideMissing answers an upstream 404 as AWS answers a caller who may not
// list the key's prefix: 403 AccessDenied.
func hideMissing(resp *http.Response) error {
	if resp.StatusCode != http.StatusNotFound {
		return nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	body := errorBody("AccessDenied", "s3scope: a missing key under a prefix the caller may not list")
	resp.StatusCode = http.StatusForbidden
	resp.Status = "403 Forbidden"
	resp.Header.Set("Content-Type", "application/xml")
	if resp.Request != nil && resp.Request.Method == http.MethodHead {
		resp.Header.Del("Content-Length")
		resp.ContentLength = 0
		resp.Body = http.NoBody
		return nil
	}
	resp.Header.Set("Content-Length", fmt.Sprint(len(body)))
	resp.ContentLength = int64(len(body))
	resp.Body = io.NopCloser(strings.NewReader(body))
	return nil
}

// SetPolicy replaces the policy, as an operator widening or narrowing the
// credential would.
func (p *Proxy) SetPolicy(policy Policy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.policy = policy
}

// Script sets a function asked first for every request; a nil Answer leaves
// the request to the policy. Script(nil) removes it.
func (p *Proxy) Script(fn func(Request) *Answer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.script = fn
}

// Requests is every request the proxy saw, in order.
func (p *Proxy) Requests() []Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.requests)
}

// Reset forgets the requests seen so far.
func (p *Proxy) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = nil
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request) {
	verb, key, ok := p.classify(r)
	req := Request{Method: r.Method, Verb: verb, Key: key, IfMatch: r.Header.Get("If-Match")}
	p.mu.Lock()
	policy, script, after, open := p.policy, p.script, p.after, p.open
	req.Allowed = open || ok && policy.Allows(verb, key)
	if src, isCopy := copySource(r, p.bucket); isCopy {
		req.Allowed = req.Allowed && src != "" && policy.Allows(Get, src)
	}
	p.requests = append(p.requests, req)
	loseThis := req.Allowed && p.lose != nil && p.lost > 0 && p.lose(req)
	if loseThis {
		p.lost--
		p.lostSeen++
	}
	p.mu.Unlock()
	if loseThis {
		p.forward.ServeHTTP(httptest.NewRecorder(), r)
		writeError(w, http.StatusInternalServerError, "InternalError", "s3scope: the answer was lost after the write landed")
		return
	}

	if script != nil {
		if a := script(req); a != nil {
			switch {
			case a.Stall:
				select {
				case <-r.Context().Done():
				case <-p.done:
				}
			case a.Status < 300:
				w.WriteHeader(a.Status)
			default:
				writeError(w, a.Status, a.Code, "s3scope: scripted")
			}
			return
		}
	}
	if !req.Allowed {
		writeError(w, http.StatusForbidden, "AccessDenied", "s3scope: the policy does not allow "+string(verb)+" on "+key)
		return
	}
	if open && after != nil {
		answer := httptest.NewRecorder()
		p.forward.ServeHTTP(answer, r)
		req.Status = answer.Code
		after(req)
		maps.Copy(w.Header(), answer.Header())
		w.WriteHeader(answer.Code)
		_, _ = io.Copy(w, answer.Body)
		return
	}
	if (verb == Get || verb == Head) && !open && !policy.Allows(List, parentPrefix(key)) {
		p.hiding.ServeHTTP(w, r)
		return
	}
	p.forward.ServeHTTP(w, r)
}

// classify reads r as the policy does: its verb and the key or listing
// prefix it acts on. A request on another bucket, a batch delete (whose keys
// are in its body) and any other bucket-level request are not ones the
// policy names, so they are refused.
func (p *Proxy) classify(r *http.Request) (Verb, string, bool) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucket != p.bucket || !plainKey(key) {
		return "", "", false
	}
	if key == "" {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			prefix := r.URL.Query().Get("prefix")
			return List, prefix, prefix != ""
		}
		return "", "", false
	}
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		return Put, key, true
	case http.MethodGet:
		return Get, key, true
	case http.MethodHead:
		return Head, key, true
	case http.MethodDelete:
		// Aborting a multipart upload is part of the upload, which AWS
		// grants as s3:AbortMultipartUpload beside s3:PutObject.
		if r.URL.Query().Has("uploadId") {
			return Put, key, true
		}
		return Delete, key, true
	}
	return "", "", false
}

// copySource is the key a copy reads, and whether r is a copy; a source in
// another bucket is "".
func copySource(r *http.Request, bucket string) (string, bool) {
	src := r.Header.Get("X-Amz-Copy-Source")
	if src == "" {
		return "", false
	}
	if unescaped, err := url.PathUnescape(src); err == nil {
		src = unescaped
	}
	b, key, _ := strings.Cut(strings.TrimPrefix(src, "/"), "/")
	if b != bucket {
		return "", true
	}
	key, _, _ = strings.Cut(key, "?")
	return key, true
}

// plainKey reports a key with no "." or ".." segment and no empty segment
// but a trailing one: a store that normalised such a key could be reached
// through an allowed prefix.
func plainKey(key string) bool {
	if key == "" {
		return true
	}
	segments := strings.Split(key, "/")
	for i, seg := range segments {
		if seg == "." || seg == ".." || (seg == "" && i != len(segments)-1) {
			return false
		}
	}
	return true
}

func parentPrefix(key string) string {
	if i := strings.LastIndex(key, "/"); i >= 0 {
		return key[:i+1]
	}
	return ""
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, errorBody(code, message))
}

func errorBody(code, message string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
}
