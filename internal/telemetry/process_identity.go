package telemetry

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Process identity (docs/DEPLOY.md, "Process identity"). Every process's
// signals carry who sent them: one service.instance.id per process, so two
// replicas never collide into one series and a restart is a new series, its
// component, its deployment and, for an Orker, its Orker id. They are merged
// AFTER the OTEL_* environment, so no environment value can make one process
// read as another.
const (
	// AttrInstanceID is the OpenTelemetry semantic-convention key.
	AttrInstanceID = "service.instance.id"
	AttrComponent  = "ork.component"

	ComponentEngine = "engine"
	ComponentOrker  = "orker"
)

// instanceID is minted once per process, at the first resource it builds.
var instanceID = sync.OnceValue(func() string {
	id, err := uuid.NewV7()
	if err != nil {
		// crypto/rand has not failed since Go 1.24 and library code does not
		// panic; this still names one process apart from another on its host.
		return fmt.Sprintf("pid-%d-%d", os.Getpid(), time.Now().UnixNano())
	}
	return id.String()
})

// InstanceID is this process's service.instance.id.
func InstanceID() string { return instanceID() }

// componentFor is the ork.component of the binary whose service.name this is
// by default, or "" for any other (a test's).
func componentFor(service string) string {
	switch service {
	case ServiceEngine:
		return ComponentEngine
	case ServiceOrker:
		return ComponentOrker
	}
	return ""
}

// identityKeys are the resource attributes the binary decides and the
// environment never overrides, in the order a conflict is reported.
var identityKeys = []attribute.Key{AttrInstanceID, AttrComponent, AttrDeployment, AttrOrkerID}

func isIdentityKey(key attribute.Key) bool {
	for _, k := range identityKeys {
		if k == key {
			return true
		}
	}
	return false
}

// identityAttrs splits extra into the process identity, with the instance id
// and component added, and every other attribute.
func identityAttrs(cfg Config, extra []attribute.KeyValue) (identity, rest []attribute.KeyValue) {
	byKey := map[attribute.Key]attribute.KeyValue{AttrInstanceID: attribute.String(AttrInstanceID, InstanceID())}
	if cfg.Component != "" {
		byKey[AttrComponent] = attribute.String(AttrComponent, cfg.Component)
	}
	for _, kv := range extra {
		switch {
		case kv.Key == AttrInstanceID:
			// The process mints its own; a caller never supplies one.
		case isIdentityKey(kv.Key):
			byKey[kv.Key] = kv
		default:
			rest = append(rest, kv)
		}
	}
	for _, key := range identityKeys {
		if kv, ok := byKey[key]; ok {
			identity = append(identity, kv)
		}
	}
	return identity, rest
}

// registerBuildInfo publishes ork.build.info = 1, labelled with the build
// identity, as an observable gauge: it is reported at every collection, so a
// delta-temporality exporter or a collector restart never loses it.
func registerBuildInfo(provider metric.MeterProvider, attrs []attribute.KeyValue) error {
	set := attribute.NewSet(attrs...)
	_, err := provider.Meter(ScopeName).Int64ObservableGauge("ork.build.info",
		metric.WithDescription("Constant 1, labelled with the build identity of this process."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(1, metric.WithAttributeSet(set))
			return nil
		}))
	if err != nil {
		return fmt.Errorf("telemetry instruments: %w", err)
	}
	return nil
}
