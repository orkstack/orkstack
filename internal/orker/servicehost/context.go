package servicehost

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The paths a service sees (14 §3).
const (
	PathConfig  = "/orkstack/input/config.json"
	PathSecrets = "/orkstack/input/secrets"
	PathAsk     = "/orkstack/service/ask"
	PathAnswer  = "/orkstack/service/answer"
)

// HostContracts and HostProfiles are the versions of the contract and of
// the event-source profile this Orker hosts.
var (
	HostContracts = []int32{1}
	HostProfiles  = []int32{1}
)

// Context is input/service.json (14 §3.2, 06 §3): the service's context,
// sealed before the container starts.
type Context struct {
	SchemaVersion int                     `json:"schemaVersion"`
	Contract      int32                   `json:"contract"`
	Profile       ContextProfile          `json:"profile"`
	Deployment    string                  `json:"deployment"`
	Namespace     string                  `json:"namespace,omitzero"`
	Instance      ContextInstance         `json:"instance"`
	Lanes         int32                   `json:"lanes"`
	EventSource   *ContextEventSource     `json:"eventSource,omitzero"`
	Limits        eventsources.SpecLimits `json:"limits"`
	Paths         ContextPaths            `json:"paths"`
}

// ContextProfile names the profile and its version the container runs
// under.
type ContextProfile struct {
	Name    string `json:"name"`
	Version int32  `json:"version"`
}

// ContextInstance is which instance this container is: key is the same on
// every start of the instance, run different for every start of any
// container.
type ContextInstance struct {
	Index int    `json:"index"`
	Of    int    `json:"of"`
	Key   string `json:"key"`
	Run   string `json:"run"`
}

// ContextEventSource is the event-source profile's block.
type ContextEventSource struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Key      string `json:"key"`
	Delivery string `json:"delivery"`
}

// ContextPaths are where the container finds its files.
type ContextPaths struct {
	Config  string `json:"config"`
	Secrets string `json:"secrets"`
	Ask     string `json:"ask"`
	Answer  string `json:"answer"`
}

// Highest is the highest version both sides have, false when none.
func Highest(image, host []int32) (int32, bool) {
	best, found := int32(0), false
	for _, v := range image {
		if slices.Contains(host, v) && (!found || v > best) {
			best, found = v, true
		}
	}
	return best, found
}

// BuildContext is the service.json of one start of one instance of the
// event source spec describes: index of of instances in deployment, run
// naming this start.
func BuildContext(spec eventsources.ServiceSpec, deployment string, index, of int, run string) (Context, error) {
	contract, ok := Highest(spec.Contracts, HostContracts)
	if !ok {
		return Context{}, fmt.Errorf("%s: the image implements contract %v; this Orker hosts %v", eventsources.ReasonServiceContractUnknown, spec.Contracts, HostContracts)
	}
	profile, ok := Highest(spec.Profiles, HostProfiles)
	if !ok {
		return Context{}, fmt.Errorf("%s: the image implements %s %v; this Orker hosts %v", eventsources.ReasonServiceProfileUnknown, eventsources.ProfileEventSource, spec.Profiles, HostProfiles)
	}
	switch {
	case deployment == "":
		return Context{}, errors.New("servicehost: a service's context names its deployment")
	case of < 1 || index < 0 || index >= of:
		return Context{}, fmt.Errorf("servicehost: instance %d of %d", index, of)
	case run == "":
		return Context{}, errors.New("servicehost: a start's run is required")
	case spec.Concurrency < 1:
		return Context{}, fmt.Errorf("servicehost: %d lanes", spec.Concurrency)
	}
	sourceKey := deployment + "/" + spec.Source.Namespace + "/" + spec.Source.Name
	return Context{
		SchemaVersion: SchemaVersion,
		Contract:      contract,
		Profile:       ContextProfile{Name: eventsources.ProfileEventSource, Version: profile},
		Deployment:    deployment,
		Namespace:     spec.Source.Namespace,
		Instance:      ContextInstance{Index: index, Of: of, Key: sourceKey + "/" + strconv.Itoa(index), Run: run},
		Lanes:         spec.Concurrency,
		EventSource:   &ContextEventSource{ID: spec.Source.ID, Name: spec.Source.Name, Key: sourceKey, Delivery: spec.Delivery},
		Limits:        spec.Limits,
		Paths:         ContextPaths{Config: PathConfig, Secrets: PathSecrets, Ask: PathAsk, Answer: PathAnswer},
	}, nil
}
