package aws

import (
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// declare is one aws declaration named "aws" with the given keys.
func declare(keys ...string) (*Provider, error) {
	doc := "providers:\n  aws:\n    type: aws\n"
	for _, k := range keys {
		doc += "    " + k + "\n"
	}
	set, err := provider.ParseWith([]byte(doc), map[string]provider.Type{TypeName: Type{}})
	if err != nil {
		return nil, err
	}
	p, _ := set.ByName("aws")
	return p.(*Provider), nil
}

func mustNew(tb testing.TB, keys ...string) *Provider {
	tb.Helper()
	p, err := declare(keys...)
	if err != nil {
		tb.Fatalf("declare %q: %v", keys, err)
	}
	return p
}

func TestNew_Defaults(t *testing.T) {
	p := mustNew(t, "region: eu-west-1")
	if p.Name() != "aws" || p.Type() != "aws" || p.Audience() != "sts.amazonaws.com" || p.LoginScheme() != "sts" {
		t.Errorf("%q %q %q %q", p.Name(), p.Type(), p.Audience(), p.LoginScheme())
	}
	got := p.Describe()
	want := map[string]string{"region": "eu-west-1", "session_seconds": "3600", "session_tags": "true"}
	if got.Type != "aws" || got.Audience != "sts.amazonaws.com" || !maps.Equal(got.Fields, want) {
		t.Errorf("Describe() = %+v, want the fields %v", got, want)
	}
}

func TestNew_EveryKey(t *testing.T) {
	p := mustNew(t, "region: us-gov-west-1", "audience: https://aws.example/x", "session_seconds: 900",
		"sts_endpoint: https://sts.internal", "endpoint: http://127.0.0.1:4566", "session_tags: true", "transitive_tags: [ork_task, ork_namespace]")
	want := map[string]string{"region": "us-gov-west-1", "session_seconds": "900", "session_tags": "true", "transitive_tags": "ork_namespace,ork_task",
		"sts_endpoint": "https://sts.internal", "endpoint": "http://127.0.0.1:4566"}
	if got := p.Describe().Fields; !maps.Equal(got, want) {
		t.Errorf("Describe().Fields = %v, want %v", got, want)
	}
	if p.Audience() != "https://aws.example/x" {
		t.Errorf("Audience() = %q", p.Audience())
	}
}

func TestNew_Refuses(t *testing.T) {
	const region = "region: eu-west-1"
	for name, tc := range map[string]struct {
		keys []string
		err  string
	}{
		"no region":                    {nil, "region: is required"},
		"a region that is no string":   {[]string{"region: 1"}, "region (line"},
		"a region that is a host":      {[]string{"region: evil.example/x"}, "not an AWS region"},
		"a region with an @":           {[]string{"region: eu-west-1@x"}, "not an AWS region"},
		"an audience with a space":     {[]string{region, "audience: a b"}, "must match"},
		"a session under STS's least":  {[]string{region, "session_seconds: 899"}, "from 900 to 43200"},
		"a session over STS's most":    {[]string{region, "session_seconds: 43201"}, "from 900 to 43200"},
		"a clear-text endpoint":        {[]string{region, "sts_endpoint: http://sts.internal"}, "must be https"},
		"a clear-text loopback's name": {[]string{region, "endpoint: http://localhost:1"}, "must be https"},
		"an endpoint with a user":      {[]string{region, "endpoint: https://a@b"}, "no user"},
		"an endpoint with a query":     {[]string{region, "endpoint: https://b/?x=1"}, "no user, query"},
		"an endpoint that is no URL":   {[]string{region, "endpoint: sts"}, "a URL with a host"},
		"an unknown transitive tag":    {[]string{region, "transitive_tags: [team]"}, "not a session tag OrkStack sends"},
		"a transitive tag twice":       {[]string{region, "transitive_tags: [ork_task, ork_task]"}, "listed twice"},
		"transitive tags and no tags":  {[]string{region, "session_tags: false", "transitive_tags: [ork_task]"}, "needs session_tags"},
		"a setting that is no boolean": {[]string{region, "session_tags: yes"}, "true or false"},
		"a key it does not read":       {[]string{region, "role: x"}, "not one this provider's type reads"},
		"a key that names a secret":    {[]string{region, "secret_access_key: x"}, "holds no secret"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := declare(tc.keys...); err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("declare %q = %v, want an error holding %q", tc.keys, err, tc.err)
			}
		})
	}
}

func TestSessionTags_AreTheDeclarations(t *testing.T) {
	for name, tc := range map[string]struct {
		keys       []string
		on         bool
		transitive []string
	}{
		"by default":         {nil, true, nil},
		"off":                {[]string{"session_tags: false"}, false, nil},
		"an empty list":      {[]string{"transitive_tags: []"}, true, nil},
		"transitive, sorted": {[]string{"transitive_tags: [ork_task, ork_namespace]"}, true, []string{"ork_namespace", "ork_task"}},
	} {
		t.Run(name, func(t *testing.T) {
			p := mustNew(t, append([]string{"region: eu-west-1"}, tc.keys...)...)
			on, transitive := p.SessionTags()
			if on != tc.on || !slices.Equal(transitive, tc.transitive) {
				t.Errorf("SessionTags() = %v, %v; want %v, %v", on, transitive, tc.on, tc.transitive)
			}
		})
	}
}

func TestSessionName(t *testing.T) {
	long := strings.Repeat("r", 63)
	for name, tc := range map[string]struct {
		who  provider.Attempt
		want string
	}{
		"a run and its attempt":   {provider.Attempt{RunID: "ork-01jc9", Attempt: 2}, "ork-01jc9-2"},
		"the longest":             {provider.Attempt{RunID: long[:62], Attempt: 1}, long[:62] + "-1"},
		"too long for STS":        {provider.Attempt{RunID: long, Attempt: 1}, "ork-d2d15a2c"},
		"a character STS refuses": {provider.Attempt{RunID: "ork 1", Attempt: 1}, "ork-"},
		"no run":                  {provider.Attempt{Attempt: 1}, "ork-"},
	} {
		t.Run(name, func(t *testing.T) {
			got := sessionName(tc.who)
			if !sessionNameShape.MatchString(got) {
				t.Fatalf("%q is not a name STS takes", got)
			}
			if tc.want == "ork-" || strings.HasPrefix(tc.want, "ork-d2") {
				if len(got) != 44 || !strings.HasPrefix(got, "ork-") || got != sessionName(tc.who) {
					t.Fatalf("sessionName = %q, want ork- and 40 hex digits", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("sessionName = %q, want %q", got, tc.want)
			}
		})
	}
	if sessionName(provider.Attempt{RunID: long, Attempt: 1}) == sessionName(provider.Attempt{RunID: long, Attempt: 2}) {
		t.Error("two attempts share a session name")
	}
}

func TestDuration(t *testing.T) {
	p := mustNew(t, "region: eu-west-1", "session_seconds: 7200")
	for name, tc := range map[string]struct {
		who  provider.Attempt
		want time.Duration
	}{
		"a task":                         {provider.Attempt{}, 2 * time.Hour},
		"a task is not bound by it":      {provider.Attempt{MaxLogin: time.Hour}, 2 * time.Hour},
		"an instance":                    {provider.Attempt{Instance: true, MaxLogin: time.Hour}, time.Hour},
		"an instance with a longer max":  {provider.Attempt{Instance: true, MaxLogin: 3 * time.Hour}, 2 * time.Hour},
		"never under STS's own least":    {provider.Attempt{Instance: true, MaxLogin: time.Minute}, 15 * time.Minute},
		"whole seconds":                  {provider.Attempt{Instance: true, MaxLogin: time.Hour + 500*time.Millisecond}, time.Hour},
		"an instance with none declared": {provider.Attempt{Instance: true}, 2 * time.Hour},
	} {
		if got := p.duration(tc.who); got != tc.want {
			t.Errorf("%s: duration = %s, want %s", name, got, tc.want)
		}
	}
}

func TestBundle(t *testing.T) {
	asked := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	k := keys{id: "ASIAEXAMPLE", secret: "s3cr3t/+key", token: "tok<en>", expiration: asked.Add(time.Hour), life: time.Hour, asked: asked}
	b, err := bundle("eu-west-1", "deploy", k, asked.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for file, want := range map[string]string{
		"config":       "[default]\nregion = eu-west-1\ncredential_process = cat /orkstack/credentials/deploy/process.json\n",
		"process.json": `{"Version":1,"AccessKeyId":"ASIAEXAMPLE","SecretAccessKey":"s3cr3t/+key","SessionToken":"tok<en>","Expiration":"2026-11-24T11:00:00Z"}` + "\n",
		"credentials":  "[default]\naws_access_key_id = ASIAEXAMPLE\naws_secret_access_key = s3cr3t/+key\naws_session_token = tok<en>\n",
	} {
		if got := string(b.Files[file]); got != want {
			t.Errorf("%s = %q, want %q", file, got, want)
		}
	}
	if len(b.Files) != 3 || len(b.Plain) != 1 || !b.Plain["config"] {
		t.Errorf("the bundle holds %d files, plain %v", len(b.Files), b.Plain)
	}
	var secrets []string
	for _, v := range b.Secrets {
		secrets = append(secrets, string(v))
	}
	if !slices.Equal(secrets, []string{"s3cr3t/+key", "tok<en>"}) {
		t.Errorf("Secrets = %q, want the secret key and the session token and not the key id", secrets)
	}
	if b.Lifetime != 50*time.Minute {
		t.Errorf("Lifetime = %s, want 50m", b.Lifetime)
	}
	if err := provider.ValidBundle(b); err != nil {
		t.Error(err)
	}
}

func TestFieldOf(t *testing.T) {
	const doc = `{"url":"postgres://x","port":5432,"tls":true,"none":null,"nested":{"a":"b"},"esc":"a\nb"}`
	for name, tc := range map[string]struct {
		doc, key, want, code string
		class                provider.Class
	}{
		"a string":         {doc: doc, key: "url", want: "postgres://x"},
		"escapes are read": {doc: doc, key: "esc", want: "a\nb"},
		"a number":         {doc: doc, key: "port", want: "5432"},
		"a boolean":        {doc: doc, key: "tls", want: "true"},
		"not there":        {doc: doc, key: "user", class: provider.NotFound, code: codeField},
		"null":             {doc: doc, key: "none", class: provider.Invalid, code: codeNotJSON},
		"nested":           {doc: doc, key: "nested", class: provider.Invalid, code: codeNotJSON},
		"not JSON":         {doc: "hunter2", key: "url", class: provider.Invalid, code: codeNotJSON},
		"an array":         {doc: `["url"]`, key: "url", class: provider.Invalid, code: codeNotJSON},
		"JSON null":        {doc: "null", key: "url", class: provider.Invalid, code: codeNotJSON},
		"a key twice":      {doc: `{"url":"a","url":"b"}`, key: "url", class: provider.Invalid, code: codeNotJSON},
		"two values":       {doc: `{"url":"a"} {"url":"b"}`, key: "url", class: provider.Invalid, code: codeNotJSON},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := fieldOf([]byte(tc.doc), tc.key)
			if tc.code == "" {
				if err != nil || string(got) != tc.want {
					t.Fatalf("fieldOf = %q, %v; want %q", got, err, tc.want)
				}
				return
			}
			wantError(t, err, opRead, tc.class, tc.code)
			if strings.Contains(err.Error(), "hunter2") {
				t.Error("the error holds the secret")
			}
		})
	}
}
