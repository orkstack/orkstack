package reachprobe

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
)

// FuzzParseTarget: a target carries an operator's probe address
// (orker.services.probe_addresses), so its parser never panics, and what it
// accepts is exactly the fields it was given, with a host and a port in
// range, which format back to the same string.
func FuzzParseTarget(f *testing.F) {
	for _, s := range []string{
		"3,service,tls,10.0.0.1:7233",
		"1,address,tcp,[fd00:ec2::254]:80",
		"1,address,tcp,metadata.google.internal:80",
		"3,membership,tcp,10.0.0.1:6933",
		"3,service,tls,10.0.0.1:7233,temporal.internal", "3,service,tls,10.0.0.1:7233,",
		"", "3,service,tls", "1,address,tcp,:80", "1,address,tls,1.2.3.4:80",
		"1,address,tcp,1.2.3.4:99999", "1,address,tcp,a,b:1",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		tg, err := ParseTarget(s)
		if err != nil {
			return
		}
		if got := fmt.Sprintf("%d,%s,%s,%s", tg.Question, tg.Kind, tg.Via, tg.Addr); got != s && got+","+tg.Name != s {
			t.Fatalf("ParseTarget(%q) accepted %q", s, got)
		}
		host, port, err := net.SplitHostPort(tg.Addr)
		if n, perr := strconv.Atoi(port); err != nil || host == "" || perr != nil || strconv.Itoa(n) != port ||
			len(tg.Addr) > maxAddr || strings.Contains(tg.Addr, ",") {
			t.Fatalf("ParseTarget(%q) accepted a bad address %q", s, tg.Addr)
		}
	})
}
