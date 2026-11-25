package orker

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/quantity"
)

// The settings of an Orker that hosts event sources and other services
// (docs/DEPLOY.md §1; docs/ORKER_SERVICE_CONTRACT.md). Every Orker of a
// pool has the same values.
const (
	SourceHostMaxInstancesVar      = "ORK_ORKER_EVENT_SOURCE_MAX_INSTANCES"
	SourceHostMaxRequestCPUVar     = "ORK_ORKER_EVENT_SOURCE_MAX_REQUEST_CPU"
	SourceHostMaxRequestMemoryVar  = "ORK_ORKER_EVENT_SOURCE_MAX_REQUEST_MEMORY"
	SourceHostSpoolBytesVar        = "ORK_ORKER_EVENT_SOURCE_SPOOL_BYTES"
	SourceHostConfirmWaitMaxVar    = "ORK_ORKER_EVENT_SOURCE_CONFIRM_WAIT_MAX"
	SourceHostParseBytesVar        = "ORK_ORKER_EVENT_SOURCE_PARSE_BYTES"
	SourceHostLogBytesPerHourVar   = "ORK_ORKER_EVENT_SOURCE_LOG_BYTES_PER_HOUR"
	ServicesAllowMetadataReachVar  = "ORK_ORKER_SERVICES_ALLOW_METADATA_REACH"
	ServicesAllowOpenClusterVar    = "ORK_ORKER_SERVICES_ALLOW_OPEN_CLUSTER"
	ServicesAllowContainerReachVar = "ORK_ORKER_SERVICES_ALLOW_CONTAINER_REACH"
	ServicesProbeAddressesVar      = "ORK_ORKER_SERVICES_PROBE_ADDRESSES"
	ServicesProbeImageVar          = "ORK_ORKER_SERVICES_PROBE_IMAGE"
)

// Their defaults.
const (
	DefaultSourceHostMaxInstances     = 0
	DefaultSourceHostMaxRequestCPU    = "500m"
	DefaultSourceHostMaxRequestMemory = "256Mi"
	DefaultSourceHostSpoolBytes       = 64 << 20
	DefaultSourceHostConfirmWaitMax   = 15 * time.Minute
	DefaultSourceHostParseBytes       = 64 << 20
	DefaultSourceHostLogBytesPerHour  = 4 << 20
)

// SourceHostSettings are an Orker's event-source and service settings.
type SourceHostSettings struct {
	MaxInstances        int
	MaxRequestCPU       string
	MaxRequestMemory    string
	SpoolBytes          int64
	ConfirmWaitMax      time.Duration
	ParseBytes          int64
	LogBytesPerHour     int64
	AllowMetadataReach  bool
	AllowOpenCluster    bool
	AllowContainerReach bool
	ProbeAddresses      []string
	ProbeImage          string
	// EngineURL is orker.engine.url, which a source host calls.
	EngineURL string
}

// HostsSources reports an Orker configured to host event-source instances.
func (s SourceHostSettings) HostsSources() bool { return s.MaxInstances > 0 }

// SourceHostSettingsFromEnv reads the settings, each refused with its own
// name when it cannot be read. engineURL is orker.engine.url: a source host
// needs an engine to call, and refuses to boot without one, naming both.
func SourceHostSettingsFromEnv(getenv func(string) string, engineURL string) (SourceHostSettings, error) {
	s := SourceHostSettings{
		MaxInstances: DefaultSourceHostMaxInstances, MaxRequestCPU: DefaultSourceHostMaxRequestCPU,
		MaxRequestMemory: DefaultSourceHostMaxRequestMemory, SpoolBytes: DefaultSourceHostSpoolBytes,
		ConfirmWaitMax: DefaultSourceHostConfirmWaitMax, ParseBytes: DefaultSourceHostParseBytes,
		LogBytesPerHour: DefaultSourceHostLogBytesPerHour, EngineURL: strings.TrimSpace(engineURL),
	}
	var errs []error
	intVar := func(name string, into *int64, min int64) {
		if v := strings.TrimSpace(getenv(name)); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < min {
				errs = append(errs, fmt.Errorf("%s=%q: want a whole number of at least %d", name, v, min))
				return
			}
			*into = n
		}
	}
	boolVar := func(name string, into *bool) {
		if v := strings.TrimSpace(getenv(name)); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s=%q: want true or false", name, v))
				return
			}
			*into = b
		}
	}
	var maxInstances int64 = DefaultSourceHostMaxInstances
	intVar(SourceHostMaxInstancesVar, &maxInstances, 0)
	s.MaxInstances = int(maxInstances)
	intVar(SourceHostSpoolBytesVar, &s.SpoolBytes, 1<<20)
	intVar(SourceHostParseBytesVar, &s.ParseBytes, 1<<20)
	intVar(SourceHostLogBytesPerHourVar, &s.LogBytesPerHour, 0)
	if v := strings.TrimSpace(getenv(SourceHostMaxRequestCPUVar)); v != "" {
		s.MaxRequestCPU = v
	}
	if v := strings.TrimSpace(getenv(SourceHostMaxRequestMemoryVar)); v != "" {
		s.MaxRequestMemory = v
	}
	if n, err := quantity.ParseCPU(s.MaxRequestCPU); err != nil || n <= 0 {
		errs = append(errs, fmt.Errorf("%s=%q: want a positive cpu quantity", SourceHostMaxRequestCPUVar, s.MaxRequestCPU))
	}
	if n, err := quantity.ParseMemory(s.MaxRequestMemory); err != nil || n <= 0 {
		errs = append(errs, fmt.Errorf("%s=%q: want a positive memory quantity", SourceHostMaxRequestMemoryVar, s.MaxRequestMemory))
	}
	if v := strings.TrimSpace(getenv(SourceHostConfirmWaitMaxVar)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("%s=%q: want a positive duration", SourceHostConfirmWaitMaxVar, v))
		} else {
			s.ConfirmWaitMax = d
		}
	}
	boolVar(ServicesAllowMetadataReachVar, &s.AllowMetadataReach)
	boolVar(ServicesAllowOpenClusterVar, &s.AllowOpenCluster)
	boolVar(ServicesAllowContainerReachVar, &s.AllowContainerReach)
	for _, a := range strings.Split(getenv(ServicesProbeAddressesVar), ",") {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		host, port, err := net.SplitHostPort(a)
		n, perr := strconv.Atoi(port)
		if err != nil || host == "" || perr != nil || strconv.Itoa(n) != port || n < 1 || n > 65535 || strings.ContainsAny(a, ", ") {
			errs = append(errs, fmt.Errorf("%s: %q is not host:port, with a port of 1 to 65535", ServicesProbeAddressesVar, a))
			continue
		}
		s.ProbeAddresses = append(s.ProbeAddresses, a)
	}
	if v := strings.TrimSpace(getenv(ServicesProbeImageVar)); v != "" {
		if !strings.Contains(v, "@sha256:") {
			errs = append(errs, fmt.Errorf("%s=%q: a mirrored probe image is named by its digest", ServicesProbeImageVar, v))
		} else {
			s.ProbeImage = v
		}
	}
	if s.HostsSources() && strings.TrimSpace(engineURL) == "" {
		errs = append(errs, fmt.Errorf("%s is %d but %s is empty: a source host calls the engine for every message",
			SourceHostMaxInstancesVar, s.MaxInstances, EngineURLVar))
	}
	return s, errors.Join(errs...)
}
