package servicehost

import (
	"strings"
	"testing"
)

// TestReached_TheS7Table: what spike S7 found against a real cluster,
// judged as 14 §7 says.
func TestReached_TheS7Table(t *testing.T) {
	for _, tc := range []struct {
		name string
		o    Observation
		want bool
	}{
		{"open frontend, plaintext", Observation{Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "OK"}, true},
		{"TLS-only frontend, no client certificate", Observation{Kind: TargetService, Via: ViaTLS, Connected: true, TLS: TLSOK, Status: "OK"}, true},
		{"client-certificate frontend", Observation{Kind: TargetService, Via: ViaTLS, Connected: true, TLS: TLSClientCertRequire}, false},
		{"history port, unknown service", Observation{Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "Unimplemented"}, true},
		{"key required", Observation{Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "Unauthenticated"}, false},
		{"permission denied", Observation{Kind: TargetService, Via: ViaTLS, Connected: true, TLS: TLSOK, Status: "PermissionDenied"}, false},
		{"refused", Observation{Kind: TargetService, Via: ViaPlaintext}, false},
		{"a port that speaks no gRPC", Observation{Kind: TargetService, Via: ViaPlaintext, Connected: true}, true},
		{"a TLS attempt at a plaintext port", Observation{Kind: TargetService, Via: ViaTLS, Connected: true, TLS: TLSFailed}, false},
		{"membership port accepts", Observation{Kind: TargetMembership, Via: ViaTCP, Connected: true}, true},
		{"metadata answers", Observation{Kind: TargetAddress, Via: ViaTCP, Connected: true}, true},
		{"canary unreachable", Observation{Kind: TargetAddress, Via: ViaTCP}, false},
	} {
		if got := tc.o.Reached(); got != tc.want {
			t.Errorf("%s: reached %v; want %v", tc.name, got, tc.want)
		}
	}
}

func TestJudgeProbe(t *testing.T) {
	clean := []Observation{
		{Question: 1, Target: "169.254.169.254:80", Kind: TargetAddress, Via: ViaTCP},
		{Question: 2, Target: "172.17.0.1:9090", Kind: TargetAddress, Via: ViaTCP},
		{Question: 3, Target: "10.0.0.5:7233", Kind: TargetService, Via: ViaTLS, Connected: true, TLS: TLSClientCertRequire},
		{Question: 4, Target: "172.22.0.2:8080", Kind: TargetAddress, Via: ViaTCP},
	}
	if f := JudgeProbe(clean, 1, 2, 3, 4); !f.Fit {
		t.Fatalf("a clean probe: %+v", f)
	}
	if f := JudgeProbe(clean[:3], 1, 2, 3, 4); f.Fit || !strings.Contains(f.Failures[0], "question 4: not asked") {
		t.Fatalf("a probe that skipped a question: %+v", f)
	}
	open := append([]Observation{}, clean...)
	open = append(open, Observation{Question: 3, Target: "10.0.0.5:6933", Kind: TargetMembership, Via: ViaTCP, Connected: true})
	if f := JudgeProbe(open, 1, 2, 3, 4); f.Fit || len(f.Failures) != 1 || !strings.Contains(f.Failures[0], "6933") {
		t.Fatalf("a membership port that accepts: %+v", f)
	}
}

func TestParseProbeAnswer(t *testing.T) {
	raw := `{"q":3,"target":"10.0.0.5:7233","kind":"service","via":"tls","connected":true,"tls":"ok","status":"OK"}
{"q":1,"target":"169.254.169.254:80","kind":"address","via":"tcp","connected":false}
`
	obs, err := ParseProbeAnswer([]byte(raw))
	if err != nil || len(obs) != 2 || !obs[0].Reached() || obs[1].Reached() {
		t.Fatalf("%+v, %v", obs, err)
	}
	for name, line := range map[string]string{
		"unknown field":         `{"q":1,"target":"a","kind":"address","via":"tcp","connected":false,"x":1}`,
		"question 5":            `{"q":5,"target":"a","kind":"address","via":"tcp","connected":false}`,
		"bad kind":              `{"q":1,"target":"a","kind":"other","via":"tcp","connected":false}`,
		"bad via":               `{"q":1,"target":"a","kind":"address","via":"udp","connected":false}`,
		"control in text":       "{\"q\":1,\"target\":\"a\\u001b\",\"kind\":\"address\",\"via\":\"tcp\",\"connected\":false}",
		"answered, unconnected": `{"q":3,"target":"a","kind":"service","via":"plaintext","connected":false,"status":"OK"}`,
		"a name on an address":  `{"q":2,"target":"a:1","kind":"address","via":"tcp","connected":false,"name":"n"}`,
		"a long name":           `{"q":3,"target":"a:1","kind":"service","via":"tls","connected":false,"name":"` + strings.Repeat("a", 254) + `"}`,
		"connected, unresolved": `{"q":2,"target":"a:1","kind":"address","via":"tcp","connected":true,"unresolved":true}`,
		"not JSON":              `q=1`,
	} {
		if _, err := ParseProbeAnswer([]byte(line)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
	if _, err := ParseProbeAnswer(make([]byte, MaxProbeAnswerBytes+1)); err == nil {
		t.Error("an answer over its bound parsed")
	}
}

func FuzzProbeAnswer(f *testing.F) {
	f.Add([]byte(`{"q":3,"target":"10.0.0.5:7233","kind":"service","via":"tls","connected":true,"tls":"ok","status":"OK"}`))
	f.Add([]byte("{\"q\":1,\"target\":\"a\",\"kind\":\"address\",\"via\":\"tcp\",\"connected\":false}\n\n"))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		obs, err := ParseProbeAnswer(raw)
		if err != nil {
			return
		}
		verdict := JudgeProbe(obs)
		for _, o := range obs {
			if o.valid() != nil {
				t.Fatalf("an invalid observation parsed: %+v", o)
			}
			if o.Reached() && verdict.Fit {
				t.Fatalf("a probe that reached something is fit: %+v", obs)
			}
		}
	})
}

// A port is judged by both ways a client comes (14 §7): one whose TLS
// handshake demands a client certificate is closed, so the plaintext leg's
// failure to get any gRPC answer there is no opening; a plaintext leg that
// was answered is.
func TestJudgeProbe_APortIsJudgedByBothLegs(t *testing.T) {
	const port = "172.18.0.1:7233"
	mtls := Observation{Question: QuestionCluster, Target: port, Kind: TargetService, Via: ViaTLS, Connected: true, TLS: TLSClientCertRequire}
	others := []Observation{
		{Question: QuestionMetadata, Target: "169.254.169.254:80", Kind: TargetAddress, Via: ViaTCP},
		{Question: QuestionLoopback, Target: "172.18.0.1:9090", Kind: TargetAddress, Via: ViaTCP},
		{Question: QuestionCanary, Target: "172.17.0.3:8080", Kind: TargetAddress, Via: ViaTCP},
	}
	for _, tc := range []struct {
		name  string
		plain Observation
		fit   bool
	}{
		{"mutual TLS, the plaintext call unanswered", Observation{Question: QuestionCluster, Target: port, Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "Unavailable"}, true},
		{"mutual TLS, plaintext nothing at all", Observation{Question: QuestionCluster, Target: port, Kind: TargetService, Via: ViaPlaintext, Connected: true}, true},
		{"mutual TLS, plaintext timed out", Observation{Question: QuestionCluster, Target: port, Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "DeadlineExceeded"}, true},
		{"TLS demanded a certificate, but plaintext was served", Observation{Question: QuestionCluster, Target: port, Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "OK"}, false},
	} {
		obs := append([]Observation{mtls, tc.plain}, others...)
		if f := JudgeProbe(obs, QuestionMetadata, QuestionLoopback, QuestionCluster, QuestionCanary); f.Fit != tc.fit {
			t.Errorf("%s: fit %v; want %v (%q)", tc.name, f.Fit, tc.fit, f.Failures)
		}
	}
	// Without a certificate demand on the same port, an unanswered
	// plaintext leg is still a port that took a service's connection.
	lone := Observation{Question: QuestionCluster, Target: "172.18.0.1:7234", Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "Unavailable"}
	if f := JudgeProbe(append([]Observation{lone}, others...), QuestionMetadata, QuestionLoopback, QuestionCluster, QuestionCanary); f.Fit {
		t.Error("an unanswered plaintext leg on a port with no certificate demand was judged closed")
	}
}

// A port is asked under several names, and each TLS leg is judged by itself:
// one that demands a certificate under one name is open if it serves under
// another. A plaintext leg nobody answered is the same TLS port refusing
// plaintext, whichever name the demand was seen under.
func TestJudgeProbe_APortUnderSeveralNames(t *testing.T) {
	leg := func(via, name, tls, status string) Observation {
		return Observation{Question: 3, Target: "10.0.0.1:7233", Kind: TargetService, Via: via, Connected: true, TLS: tls, Status: status, Name: name}
	}
	demand := leg(ViaTLS, "temporal", TLSClientCertRequire, "")
	for _, tt := range []struct {
		name string
		obs  []Observation
		fit  bool
	}{
		{"an ingress that knows one name", []Observation{demand, leg(ViaTLS, "", TLSFailed, ""),
			leg(ViaPlaintext, "temporal", "", "Unavailable"), leg(ViaPlaintext, "", "", "Unavailable")}, true},
		{"a default that serves anyone", []Observation{demand, leg(ViaTLS, "", TLSOK, "OK")}, false},
		{"a plaintext answer under no name", []Observation{demand, leg(ViaPlaintext, "", "", "OK")}, false},
		{"no certificate demanded at all", []Observation{leg(ViaTLS, "temporal", TLSFailed, ""), leg(ViaPlaintext, "", "", "Unavailable")}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if f := JudgeProbe(tt.obs, 3); f.Fit != tt.fit {
				t.Fatalf("%+v; want fit %v", f, tt.fit)
			}
		})
	}
}
