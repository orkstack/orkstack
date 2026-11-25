package aws

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/orkstack/orkstack/internal/provider"
)

// childEnv marks the process TestEnvironment_ProxyYesEveryAWSSettingNo runs
// itself as: Go reads the proxy settings once a process.
const childEnv = "ORK_TEST_AWS_ENVIRONMENT_CHILD"

// recorder is a server that keeps what it was asked for and gives nothing.
type recorder struct {
	*httptest.Server
	mu    sync.Mutex
	asked []string
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()
	r := &recorder{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.asked = append(r.asked, req.Method+" "+req.Host)
		r.mu.Unlock()
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.asked)
}

// The Orker's proxy settings are followed, as its other outbound calls follow
// them. Nothing else of its environment is: no AWS_* variable, no profile or
// its files, no credential_process, no instance metadata and no container
// endpoint. Every call goes to the declared region's own hosts, and with no
// endpoint declared those can be reached through the proxy alone.
func TestEnvironment_ProxyYesEveryAWSSettingNo(t *testing.T) {
	if os.Getenv(childEnv) != "" {
		callEverything(t)
		return
	}
	dir := t.TempDir()
	ran := filepath.Join(dir, "credential-process-ran")
	trap, proxy := newRecorder(t), newRecorder(t)
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	poison := map[string]string{
		"AWS_CONFIG_FILE": write("config", "[profile orker]\nregion = us-east-1\nendpoint_url = "+trap.URL+
			"\ncredential_process = touch "+ran+"\n[default]\nregion = us-east-1\nendpoint_url = "+trap.URL+"\ncredential_process = touch "+ran+"\n"),
		"AWS_SHARED_CREDENTIALS_FILE":            write("credentials", "[other]\naws_access_key_id = AKIAFILE\naws_secret_access_key = file\n"),
		"AWS_WEB_IDENTITY_TOKEN_FILE":            write("token", "orker-token"),
		"AWS_PROFILE":                            "orker",
		"AWS_DEFAULT_PROFILE":                    "orker",
		"AWS_ROLE_ARN":                           "arn:aws:iam::1:role/orker",
		"AWS_ROLE_SESSION_NAME":                  "orker",
		"AWS_REGION":                             "us-east-1",
		"AWS_DEFAULT_REGION":                     "us-east-1",
		"AWS_ENDPOINT_URL":                       trap.URL,
		"AWS_ENDPOINT_URL_STS":                   trap.URL,
		"AWS_ENDPOINT_URL_SSM":                   trap.URL,
		"AWS_ENDPOINT_URL_SECRETS_MANAGER":       trap.URL,
		"AWS_ENDPOINT_URL_KMS":                   trap.URL,
		"AWS_EC2_METADATA_SERVICE_ENDPOINT":      trap.URL,
		"AWS_CONTAINER_CREDENTIALS_FULL_URI":     trap.URL,
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/orker",
		"AWS_STS_REGIONAL_ENDPOINTS":             "legacy",
		"AWS_MAX_ATTEMPTS":                       "5",
		"AWS_RETRY_MODE":                         "adaptive",
		"AWS_SDK_UA_APP_ID":                      "orker",
	}
	for k, v := range poison {
		t.Setenv(k, v)
	}
	// The control: an SDK client that reads the environment is trapped by it.
	// The settings an SDK refuses beside an endpoint, or asks the instance
	// about, are the child's alone.
	cfg, err := config.LoadDefaultConfig(t.Context(), config.WithRetryMaxAttempts(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(t.Context(), &sts.GetCallerIdentityInput{}); err == nil || len(trap.seen()) == 0 {
		t.Fatalf("the control was not trapped (%v, %d requests); this test would prove nothing", err, len(trap.seen()))
	}
	control := len(trap.seen())
	t.Run("the control's profile", func(t *testing.T) {
		t.Setenv("AWS_ROLE_ARN", "")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
		cfg, err := config.LoadDefaultConfig(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		// The command answers nothing, so the keys are refused; it ran.
		_, _ = cfg.Credentials.Retrieve(t.Context())
		if err := os.Remove(ran); err != nil {
			t.Fatalf("the control did not run the profile's credential_process: %v", err)
		}
	})

	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")
	child.Env = []string{childEnv + "=1"}
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); !strings.HasSuffix(strings.ToUpper(name), "_PROXY") {
			child.Env = append(child.Env, kv)
		}
	}
	child.Env = append(child.Env, "AWS_ACCESS_KEY_ID=AKIAORKER", "AWS_SECRET_ACCESS_KEY=orker-secret", "AWS_SESSION_TOKEN=orker-session", "AWS_DEFAULTS_MODE=auto",
		"AWS_USE_FIPS_ENDPOINT=true", "AWS_USE_DUALSTACK_ENDPOINT=true",
		"HTTPS_PROXY="+proxy.URL, "HTTP_PROXY="+proxy.URL)
	if out, err := child.CombinedOutput(); err != nil {
		t.Fatalf("the child: %v\n%s", err, out)
	}
	// One request a call: the SDK's own retries stay off too.
	want := []string{"CONNECT sts.eu-west-1.amazonaws.com:443", "CONNECT ssm.eu-west-1.amazonaws.com:443",
		"CONNECT secretsmanager.eu-west-1.amazonaws.com:443", "CONNECT kms.eu-west-1.amazonaws.com:443"}
	if got := proxy.seen(); !slices.Equal(got, want) {
		t.Errorf("the proxy was asked for %q, want %q", got, want)
	}
	if got := trap.seen()[control:]; len(got) != 0 {
		t.Errorf("a setting of the Orker's environment was read: its endpoint was asked %q", got)
	}
	if _, err := os.Stat(ran); !os.IsNotExist(err) {
		t.Errorf("the Orker's own profile was read: its credential_process ran (%v)", err)
	}
}

// callEverything makes one call of each kind with no endpoint declared. Each
// gets no answer, since only the proxy can be reached and it gives none.
func callEverything(t *testing.T) {
	p := mustNew(t, "region: eu-west-1")
	_, err := p.Login(t.Context(), testRole, testAttempt, tokenSource("identity-token"))
	wantError(t, err, opLogin, provider.Unavailable, codeNoAnswer)
	s := &session{p: p, who: testAttempt, now: time.Now, keys: keys{id: "ASIASESSION", secret: "b", token: "c", life: time.Hour, asked: time.Now()}}
	for _, ref := range []provider.Ref{secret("ssm:a"), secret("secretsmanager:a"), kms("kms:alias/a", []byte("blob"))} {
		parsed, err := p.Parse(ref)
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Secret(t.Context(), parsed)
		wantError(t, err, opRead, provider.Unavailable, codeNoAnswer)
	}
}
