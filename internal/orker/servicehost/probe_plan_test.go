package servicehost

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestProbePlan_Targets(t *testing.T) {
	plan := ProbePlan{
		OperatorAddresses:  []string{"10.9.9.9:10250"},
		AdminPort:          9090,
		LoopbackCanaryPort: 41000,
		HostAddresses:      []string{"172.18.0.1", "192.168.1.20"},
		Cluster:            "127.0.0.1:7233",
		Canaries:           []string{"172.19.0.2:8080", "172.17.0.3:8080"},
	}
	got := plan.Targets()
	for _, want := range []string{
		// Question 1: the closed list, the other clouds' and the names.
		"1,address,tcp,169.254.169.254:80", "1,address,tcp,[fd00:ec2::254]:80", "1,address,tcp,100.100.100.200:80",
		"1,address,tcp,192.0.0.192:80", "1,address,tcp,metadata.google.internal:80", "1,address,tcp,10.9.9.9:10250",
		// Question 2: the admin listener and the loopback canary, at every
		// address a container reaches its host by.
		"2,address,tcp,172.18.0.1:9090", "2,address,tcp,192.168.1.20:41000",
		// Question 3: a cluster on the host's loopback is asked at the
		// host's addresses, both ways, with the other services' ports and
		// the membership ports.
		"3,service,plaintext,172.18.0.1:7233", "3,service,tls,192.168.1.20:7233", "3,service,tls,172.18.0.1:7234",
		"3,service,plaintext,172.18.0.1:7235", "3,service,tls,172.18.0.1:7239", "3,membership,tcp,192.168.1.20:6933",
		"3,membership,tcp,172.18.0.1:6939",
		// Question 4: the canaries.
		"4,address,tcp,172.19.0.2:8080", "4,address,tcp,172.17.0.3:8080",
	} {
		if !slices.Contains(got, want) {
			t.Errorf("no target %q", want)
		}
	}
	seen := map[string]bool{}
	for _, g := range got {
		if strings.Contains(g, "127.0.0.1") {
			t.Errorf("a loopback address means nothing from a container: %q", g)
		}
		if seen[g] {
			t.Errorf("target %q twice", g)
		}
		seen[g] = true
	}
}

// A cluster on another host is asked at its own address only.
func TestProbePlan_RemoteCluster(t *testing.T) {
	got := ProbePlan{HostAddresses: []string{"172.18.0.1"}, Cluster: "temporal.internal:7233"}.Targets()
	for _, want := range []string{"3,service,tls,temporal.internal:7233", "3,service,plaintext,temporal.internal:7234",
		"3,membership,tcp,temporal.internal:6933"} {
		if !slices.Contains(got, want) {
			t.Errorf("no target %q", want)
		}
	}
	for _, g := range got {
		if strings.HasPrefix(g, "3,") && strings.Contains(g, "172.18.0.1") {
			t.Errorf("a remote cluster asked at the host: %q", g)
		}
	}
}

func failingQuestions(f Fitness) []int {
	var qs []int
	for q := 1; q <= 4; q++ {
		if slices.ContainsFunc(f.Failures, func(s string) bool { return strings.HasPrefix(s, fmt.Sprintf("question %d:", q)) }) {
			qs = append(qs, q)
		}
	}
	return qs
}

func TestJudgeAllowed(t *testing.T) {
	obs := []Observation{
		{Question: 1, Target: "169.254.169.254:80", Kind: TargetAddress, Via: ViaTCP, Connected: true},
		{Question: 2, Target: "172.18.0.1:9090", Kind: TargetAddress, Via: ViaTCP},
		{Question: 3, Target: "172.18.0.1:7233", Kind: TargetService, Via: ViaPlaintext, Connected: true, Status: "OK"},
		{Question: 4, Target: "172.17.0.3:8080", Kind: TargetAddress, Via: ViaTCP, Connected: true},
	}
	tests := []struct {
		name  string
		allow Allowances
		fit   bool
		left  []int
	}{
		{name: "nothing accepted", left: []int{1, 3, 4}},
		{name: "metadata accepted", allow: Allowances{MetadataReach: true}, left: []int{3, 4}},
		{name: "everything acceptable accepted", allow: Allowances{MetadataReach: true, OpenCluster: true, ContainerReach: true}, fit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := JudgeAllowed(obs, tt.allow)
			if f.Fit != tt.fit || !slices.Equal(failingQuestions(f), tt.left) {
				t.Fatalf("fit %v, questions failing %v; want %v, %v (%q)", f.Fit, failingQuestions(f), tt.fit, tt.left, f.Failures)
			}
		})
	}
	// Question 2 has no setting: a reached loopback listener is never accepted.
	open2 := []Observation{obs[0], {Question: 2, Target: "172.18.0.1:9090", Kind: TargetAddress, Via: ViaTCP, Connected: true}, obs[2], obs[3]}
	if f := JudgeAllowed(open2, Allowances{MetadataReach: true, OpenCluster: true, ContainerReach: true}); f.Fit {
		t.Fatal("a reached loopback listener was accepted")
	}
	// A question nobody asked is never clean.
	if f := JudgeAllowed(obs[:1], Allowances{MetadataReach: true}); f.Fit || !slices.Equal(failingQuestions(f), []int{2, 3, 4}) {
		t.Fatalf("unasked questions: %+v", f)
	}
}

// TestJudgeAnswered: a name the probe could not resolve is a question it did
// not ask, so the probe did not run, whatever else it found; unless the
// operator accepted that question, or the name is a metadata endpoint's own,
// whose addresses are asked by themselves.
func TestJudgeAnswered(t *testing.T) {
	closed := func(q int, kind, target string) Observation {
		return Observation{Question: q, Kind: kind, Via: ViaTCP, Target: target}
	}
	unresolved := func(q int, kind, target string) Observation {
		o := closed(q, kind, target)
		o.Unresolved = true
		return o
	}
	base := []Observation{closed(1, TargetAddress, "169.254.169.254:80"), closed(2, TargetAddress, "10.0.0.1:9"),
		closed(3, TargetMembership, "10.0.0.2:6933"), closed(4, TargetAddress, "172.18.0.2:8080")}
	tests := []struct {
		name    string
		add     Observation
		allow   Allowances
		ran     bool
		unfit   bool
		wantErr string
	}{
		{name: "every address asked", add: closed(2, TargetAddress, "10.0.0.1:10"), ran: true},
		{name: "the host's name", add: unresolved(2, TargetAddress, "host.containers.internal:9"), wantErr: "host.containers.internal:9"},
		{name: "the cluster's name", add: unresolved(3, TargetMembership, "temporal:6933"), wantErr: "temporal:6933"},
		{name: "an operator's name", add: unresolved(1, TargetAddress, "imds.example:80"), wantErr: "imds.example:80"},
		{name: "a metadata endpoint's own name", add: unresolved(1, TargetAddress, "metadata.google.internal:80"), ran: true},
		{name: "an accepted question", add: unresolved(3, TargetMembership, "temporal:6933"), allow: Allowances{OpenCluster: true}, ran: true},
		{name: "question 2 is never accepted", add: unresolved(2, TargetAddress, "host.containers.internal:9"),
			allow: Allowances{MetadataReach: true, OpenCluster: true, ContainerReach: true}, wantErr: "host.containers.internal:9"},
		{name: "something reached is still reached", add: Observation{Question: 2, Kind: TargetAddress, Via: ViaTCP, Target: "10.0.0.1:10", Connected: true},
			ran: true, unfit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := JudgeAnswered(append(slices.Clone(base), tt.add), tt.allow)
			if r.Ran != tt.ran || (tt.ran && r.Fitness.Fit == tt.unfit) || !strings.Contains(r.Err, tt.wantErr) || (r.Err == "") != tt.ran || r.ImageRefused {
				t.Fatalf("%+v; want ran %v, error holding %q", r, tt.ran, tt.wantErr)
			}
		})
	}
	// Reached and unresolved together: the opening found is what is reported.
	open := Observation{Question: 4, Kind: TargetAddress, Via: ViaTCP, Target: "172.18.0.3:8080", Connected: true}
	r := JudgeAnswered(append(slices.Clone(base), open, unresolved(2, TargetAddress, "host.containers.internal:9")), Allowances{})
	if !r.Ran || r.Fitness.Fit {
		t.Fatalf("an opening beside an unresolved name: %+v", r)
	}
}
