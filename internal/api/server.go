package api

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/webui"
)

// DefaultHTTPAddr is the listen address when ORK_HTTP_ADDR is unset.
const DefaultHTTPAddr = ":8080"

// DefaultUIEnabled is ORK_UI_ENABLED's default: the engine serves the web UI
// at / on its API listener (MuxOptions.UI, internal/webui).
const DefaultUIEnabled = true

// MuxOptions tunes the HTTP surface.
type MuxOptions struct {
	// AllowedOrigins lists Origin header patterns accepted by the WebSocket
	// bridge. Empty means same-origin requests only (coder/websocket default).
	AllowedOrigins []string

	// StatsCacheTTL is how long one stats answer is reused. The zero value
	// takes the documented default (ORK_STATS_CACHE_TTL, else
	// DefaultStatsCacheTTL); a negative value switches the cache off.
	//
	// This is the one place an unset TTL becomes a number, so nothing further
	// down can quietly re-enable an optimisation an operator turned off.
	StatsCacheTTL time.Duration

	// RequestMaxBytes bounds one Connect request message, in bytes, both as
	// sent and once decompressed (ORK_REQUEST_MAX_BYTES). Zero or less takes
	// DefaultRequestMaxBytes: there is no unbounded setting.
	RequestMaxBytes int64

	// UI, when set, answers every path no API route takes: the web UI
	// (internal/webui), mounted at / on this same listener so the browser
	// reaches the API and the sockets on the page's own origin. Nil leaves
	// the mux the API alone, where an unknown path is a 404.
	UI http.Handler
}

// NewMux mounts the Connect handlers, the plain-HTTP routes, the WebSocket
// bridge and, when opts.UI is set, the web UI at /.
//
// Every API route must lie under a path webui.Reserved claims for the API,
// so the UI can never take a path the API answers and a mistyped API path is
// a 404 rather than the app's page. A route outside them is a programming
// error that NewMux refuses the way http.ServeMux refuses a conflicting
// pattern: it panics as the route is mounted, in every test that builds a
// mux (TestNewMux_EveryRouteIsReservedFromTheUI).
func NewMux(svc *Service, opts MuxOptions) *http.ServeMux {
	serveMux := http.NewServeMux()
	mux := reservedMux{serveMux: serveMux}
	// No Connect request carries more than one message (the API has no
	// client stream), so every Connect route's body is bounded at
	// ConnectBodyFactor times the read cap (reservedMux.Handle).
	// Every handler shares one interceptor chain: the audit interceptor sees
	// every unary call, records the audited writes, and passes everything else
	// through untouched (audit.go). HandlerOptions itself is unchanged, so its
	// other callers keep exactly the codec they had.
	//
	// Every handler also shares one read cap. A message over it is refused
	// resource_exhausted before any handler or interceptor runs, and Connect
	// reads the rest of the body away first, so the client gets that answer
	// and its connection stays usable rather than being dropped mid-send.
	requestMaxBytes := opts.RequestMaxBytes
	if requestMaxBytes <= 0 {
		requestMaxBytes = DefaultRequestMaxBytes
	}
	mux.connectBodyMax = ConnectBodyFactor * requestMaxBytes
	handlerOptions := append(HandlerOptions(),
		connect.WithInterceptors(svc.auditInterceptor()),
		connect.WithReadMaxBytes(int(requestMaxBytes)),
		connect.WithCompression("gzip",
			func() connect.Decompressor { return &boundedGzip{limit: requestMaxBytes + 1} },
			func() connect.Compressor { return gzip.NewWriter(io.Discard) }))

	orkflowPath, orkflowHandler := orkstackv1connect.NewOrkflowServiceHandler(svc, handlerOptions...)
	mux.Handle(orkflowPath, orkflowHandler)

	runPath, runHandler := orkstackv1connect.NewRunServiceHandler(svc, handlerOptions...)
	mux.Handle(runPath, runHandler)

	namespacePath, namespaceHandler := orkstackv1connect.NewNamespaceServiceHandler(svc, handlerOptions...)
	mux.Handle(namespacePath, namespaceHandler)

	// ActivityService serves the catalog from engine configuration: an
	// activity is an image and which images exist is engine configuration,
	// so the catalog is served from here rather than gathered from Orkers
	// (§4.2, §6.5).
	activityPath, activityHandler := orkstackv1connect.NewActivityServiceHandler(svc, handlerOptions...)
	mux.Handle(activityPath, activityHandler)

	// System information: what this replica was built and configured as. It
	// is mounted like every other read surface — an operator behind a load
	// balancer asks whichever replica answers, and that is the point of the
	// RPC (see system.go).
	systemPath, systemHandler := orkstackv1connect.NewSystemServiceHandler(svc, handlerOptions...)
	mux.Handle(systemPath, systemHandler)

	// The stats surface is its own handler because it owns a cache. The TTL is
	// resolved HERE — the zero value means "the documented default" and only
	// this line knows that, so a handler built anywhere else takes what it is
	// given and cannot re-enable a switched-off cache.
	statsTTL := opts.StatsCacheTTL
	if statsTTL == 0 {
		statsTTL = StatsCacheTTLFromEnv()
	}
	stats := NewStatsHandler(svc, statsTTL)
	statsPath, statsHandler := orkstackv1connect.NewStatsServiceHandler(stats, handlerOptions...)
	mux.Handle(statsPath, statsHandler)

	// CacheService reads and trims the memos cached tasks reuse. It shares
	// the stats handler's window resolver and response cache — no second TTL.
	cachePath, cacheHandler := orkstackv1connect.NewCacheServiceHandler(
		NewCacheHandler(svc, stats), handlerOptions...)
	mux.Handle(cachePath, cacheHandler)

	webhookPath, webhookHandler := orkstackv1connect.NewWebhookServiceHandler(svc, handlerOptions...)
	mux.Handle(webhookPath, webhookHandler)

	// TriggerService manages inbound run starters — the mirror image of
	// WebhookService: a webhook is outbound and deployment-wide unless it
	// names a scope, a trigger is inbound and bound to one orkflow in one
	// namespace.
	triggerPath, triggerHandler := orkstackv1connect.NewTriggerServiceHandler(svc, handlerOptions...)
	mux.Handle(triggerPath, triggerHandler)

	// AutomationService plans and exports a namespace's automation document.
	// It writes nothing: an apply is the client sending the plan's requests
	// to the services above.
	automationPath, automationHandler := orkstackv1connect.NewAutomationServiceHandler(svc, handlerOptions...)
	mux.Handle(automationPath, automationHandler)

	// ScheduleService manages schedules: the third run starter, bound to one
	// orkflow in one namespace, whose every field lives in a Temporal
	// Schedule (docs/ORKFLOW_SCHEDULES.md).
	schedulePath, scheduleHandler := orkstackv1connect.NewScheduleServiceHandler(svc, handlerOptions...)
	mux.Handle(schedulePath, scheduleHandler)

	// AuditService reads the control-plane audit trail back from the shared
	// store. It is itself a read and is never recorded.
	auditPath, auditHandler := orkstackv1connect.NewAuditServiceHandler(svc, handlerOptions...)
	mux.Handle(auditPath, auditHandler)

	// The legacy webhook adapter is plain HTTP, outside Connect, so the
	// interceptor cannot see it: it records its two writes itself.
	mux.Handle(WebhookCollectionPath, NewWebhookHandler(svc.Webhooks(), svc.audit))
	mux.Handle(WebhookItemPath, NewWebhookHandler(svc.Webhooks(), svc.audit))

	// The invoke route is plain JSON so an arbitrary external caller — a CI
	// job, a cron host, one curl line — starts a run without a Connect client.
	mux.Handle(TriggerInvokePath, NewTriggerHandler(svc))

	// The callback door: the one other route family meant for callers
	// outside the operator's boundary. It resolves exactly the callback await
	// its bearer token was minted for (awaits_http.go).
	awaitHandler := NewAwaitHandler(svc)
	mux.Handle(AwaitCompletePath, awaitHandler)
	mux.Handle(AwaitRejectPath, awaitHandler)

	mux.Handle("/ws/runs/{id}", NewWebSocketBridge(svc, opts.AllowedOrigins))
	mux.Handle(TaskLogSocketPath, NewTaskLogSocket(svc, opts.AllowedOrigins))

	// The UI takes what no route above takes. "/" is less specific than every
	// pattern above, so it never conflicts with one, and the UI answers the
	// reserved paths none of them matched with a 404 itself (webui.Handler).
	if opts.UI != nil {
		serveMux.Handle("/", opts.UI)
	}
	return serveMux
}

// boundedGzip is Connect's gzip decompressor with a ceiling on what one
// message may inflate to: limit bytes, past which a Read fails. Connect
// refuses a message that inflates past the read cap either way, but on its
// own it inflates the whole message first to name its size, so a few
// kilobytes of gzip could cost seconds of work before the refusal; this
// stops at the cap, and the refusal is still resource_exhausted.
type boundedGzip struct {
	reader      gzip.Reader
	limit, read int64
}

var errInflatedPastCap = errors.New("the message inflates past the read cap")

func (d *boundedGzip) Read(p []byte) (int, error) {
	if d.read >= d.limit {
		// At the ceiling, a message that ends here ends: only a byte more
		// is past it.
		var one [1]byte
		if _, err := io.ReadFull(&d.reader, one[:]); err != nil {
			return 0, err
		}
		return 0, errInflatedPastCap
	}
	if rest := d.limit - d.read; int64(len(p)) > rest {
		p = p[:rest]
	}
	n, err := d.reader.Read(p)
	d.read += int64(n)
	return n, err
}

func (d *boundedGzip) Reset(r io.Reader) error {
	d.read = 0
	return d.reader.Reset(r)
}

func (d *boundedGzip) Close() error { return d.reader.Close() }

// reservedMux mounts API routes, each of which must be reserved from the UI.
// It offers Handle alone, so no route reaches the mux around the check.
//
// Every Connect route (a pattern under /orkstack.) reads a body no further
// than connectBodyMax, when it is set. Past it the listener stops reading
// once Connect has answered, and closes an HTTP/1.1 connection or resets an
// h2c stream: Connect reads an over-cap message's rest away so its client
// is answered, and without the bound a body that never ends would be read
// for as long as its client sends. It is applied here, not by each mount,
// so a service added later cannot be mounted without it.
type reservedMux struct {
	serveMux       *http.ServeMux
	connectBodyMax int64
}

func (m reservedMux) Handle(pattern string, handler http.Handler) {
	if !webui.Reserved(pattern) {
		panic(fmt.Sprintf("api: route %q is not reserved from the web UI (internal/webui Reserved)", pattern))
	}
	if m.connectBodyMax > 0 && strings.HasPrefix(pattern, "/orkstack.") {
		handler = http.MaxBytesHandler(handler, m.connectBodyMax)
	}
	m.serveMux.Handle(pattern, handler)
}

// The API listener's bounds (docs/DEPLOY.md, "The API listener's bounds").
// They bound what a client may make the engine wait for or hold, and never
// how long an answer takes: a run's event stream, a task's live log and the
// WebSocket bridge stay open as long as their run or task does.
const (
	// ReadHeaderTimeout bounds how long a client may take to send a
	// request's line and headers, so a connection that opens and never
	// finishes asking is closed rather than held.
	ReadHeaderTimeout = 10 * time.Second

	// MaxHeaderBytes bounds a request's line and headers together: 64 KiB,
	// above what the common proxies forward by default (nginx 32 KiB, Envoy
	// 60 KiB), so a request a gateway passes on is never refused here. One
	// over it is 431.
	MaxHeaderBytes = 64 << 10

	// DefaultIdleTimeout is ORK_HTTP_IDLE_TIMEOUT's default: how long a
	// keep-alive connection may sit with no request in flight. It is longer
	// than the backend keep-alive of the common load balancers (60 s on most,
	// 610 s on Google Cloud's), so the balancer closes an idle connection
	// first and never sends a request down one the engine is closing.
	DefaultIdleTimeout = 15 * time.Minute

	// ConnectBodyFactor bounds a Connect request's body at this many times
	// the read cap: room for the one message a request carries, as sent and
	// enveloped, with the cap's own margin to spare.
	ConnectBodyFactor = 2

	// RequestBaseBytes is the part of the read cap no task ceiling scales:
	// 1 MiB, the bound the plain-HTTP routes read under. It holds a run's
	// input at its own 256 KiB bound in either encoding, and the plan of an
	// automation document of a thousand schedules and a thousand triggers
	// (0.5 MB measured) with half again to spare.
	RequestBaseBytes = 1 << 20

	// RequestBytesPerTask is what the read cap allows a task of the task
	// ceiling: 4 KiB. The heaviest task measured adds about 920 bytes to an
	// apply in either encoding, so an Orkflow at the ceiling whose tasks
	// average four times that still fits
	// (TestRequestMaxBytesFor_IsFourTimesAnApplyAtEveryTaskCeiling).
	RequestBytesPerTask = 4 << 10

	// DefaultRequestMaxBytes is the read cap of an engine at the default
	// task ceiling: RequestMaxBytesFor(engine.DefaultOrkflowMaxTasks),
	// 17,432,576 bytes (about 16.6 MiB).
	DefaultRequestMaxBytes = RequestBaseBytes + RequestBytesPerTask*int64(engine.DefaultOrkflowMaxTasks)
)

// RequestMaxBytesFor is the read cap an engine whose task ceiling
// (engine.orkflow_max_tasks) is maxTasks needs: RequestBaseBytes plus
// RequestBytesPerTask a task. It is a transport bound that scales with the
// ceiling, never a definition limit (AGENTS.md §7): ORK_REQUEST_MAX_BYTES
// defaults to it and may only be set above it, so raising the ceiling
// raises the cap, and no Orkflow the ceiling admits is refused for the
// size of the request that applies it unless its tasks average over 4 KiB.
func RequestMaxBytesFor(maxTasks int32) int64 {
	return RequestBaseBytes + RequestBytesPerTask*int64(max(maxTasks, 0))
}

// ServerOptions tunes the API listener (NewServerWith). A zero field takes
// its default.
type ServerOptions struct {
	// IdleTimeout is ORK_HTTP_IDLE_TIMEOUT; zero takes DefaultIdleTimeout.
	IdleTimeout time.Duration
}

// NewServer is NewServerWith with every option at its default.
func NewServer(addr string, handler http.Handler) *http.Server {
	return NewServerWith(addr, handler, ServerOptions{})
}

// NewServerWith returns an HTTP server serving both HTTP/1.1 and unencrypted
// HTTP/2 (h2c), so connect-go, connect-web, and WebSocket clients share a
// port, within the listener's bounds.
//
// It sets no ReadTimeout and no WriteTimeout, deliberately. Either is a
// deadline on the whole connection while a request is answered: a write
// deadline cuts a stream once it passes, and over HTTP/1.1 a read deadline
// cancels the request's context once it passes, ending a server stream the
// same way. What a request may send is bounded by size instead (NewMux's
// read cap, and the plain-HTTP routes' own).
func NewServerWith(addr string, handler http.Handler, opts ServerOptions) *http.Server {
	if addr == "" {
		addr = DefaultHTTPAddr
	}
	idle := opts.IdleTimeout
	if idle == 0 {
		idle = DefaultIdleTimeout
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		Protocols:         protocols,
		ReadHeaderTimeout: ReadHeaderTimeout,
		IdleTimeout:       idle,
		MaxHeaderBytes:    MaxHeaderBytes,
	}
}
