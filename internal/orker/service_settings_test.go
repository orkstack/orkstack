package orker

import (
	"strings"
	"testing"
	"time"
)

func TestSourceHostSettings_Table(t *testing.T) {
	env := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	s, err := SourceHostSettingsFromEnv(env(nil), "")
	if err != nil || s.HostsSources() || s.MaxRequestCPU != "500m" || s.SpoolBytes != 64<<20 || s.ConfirmWaitMax != 15*time.Minute {
		t.Fatalf("defaults %+v, %v", s, err)
	}
	s, err = SourceHostSettingsFromEnv(env(map[string]string{
		SourceHostMaxInstancesVar: "4", ServicesProbeAddressesVar: "10.0.0.1:9100, [fd00::1]:80",
		ServicesAllowOpenClusterVar: "true", ServicesProbeImageVar: "mirror.example/probe@sha256:" + strings.Repeat("a", 64),
	}), "https://ork.example:8080")
	if err != nil || !s.HostsSources() || len(s.ProbeAddresses) != 2 || !s.AllowOpenCluster || s.ProbeImage == "" {
		t.Fatalf("set %+v, %v", s, err)
	}
	for name, tc := range map[string]struct {
		kv   map[string]string
		url  string
		want string
	}{
		"source host without an engine": {map[string]string{SourceHostMaxInstancesVar: "1"}, "", EngineURLVar},
		"negative instances":            {map[string]string{SourceHostMaxInstancesVar: "-1"}, "x", SourceHostMaxInstancesVar},
		"bad cpu":                       {map[string]string{SourceHostMaxRequestCPUVar: "lots"}, "", SourceHostMaxRequestCPUVar},
		"bad memory":                    {map[string]string{SourceHostMaxRequestMemoryVar: "-3Gi"}, "", SourceHostMaxRequestMemoryVar},
		"tiny spool":                    {map[string]string{SourceHostSpoolBytesVar: "10"}, "", SourceHostSpoolBytesVar},
		"bad duration":                  {map[string]string{SourceHostConfirmWaitMaxVar: "soon"}, "", SourceHostConfirmWaitMaxVar},
		"bad bool":                      {map[string]string{ServicesAllowMetadataReachVar: "yes please"}, "", ServicesAllowMetadataReachVar},
		"bad address":                   {map[string]string{ServicesProbeAddressesVar: "10.0.0.1"}, "", ServicesProbeAddressesVar},
		"a named port":                  {map[string]string{ServicesProbeAddressesVar: "node:http"}, "", ServicesProbeAddressesVar},
		"port zero":                     {map[string]string{ServicesProbeAddressesVar: "node:0"}, "", ServicesProbeAddressesVar},
		"probe image by tag":            {map[string]string{ServicesProbeImageVar: "mirror.example/probe:1"}, "", ServicesProbeImageVar},
	} {
		if _, err := SourceHostSettingsFromEnv(env(tc.kv), tc.url); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v; want an error naming %s", name, err, tc.want)
		}
	}
}
