package api

import (
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// NewWebSocketBridge returns the /ws/runs/{id} handler. It forwards the same
// events as RunService.StreamEvents, encoded as JSON text frames in OrkStack's
// dialect (enum values without their type prefix: "status":"RUNNING"), so
// browsers get realtime updates without a streaming RPC client.
//
// Both go through Service.StreamRunEvents, so the socket reads the same system
// of record: any replica serves any run, whether it is executing now, finished
// long ago, or was started by a different process a moment earlier.
func NewWebSocketBridge(svc *Service, allowedOrigins []string) http.Handler {
	marshaler := protojson.MarshalOptions{EmitUnpopulated: false}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("id")
		if runID == "" {
			http.Error(w, "run id is required", http.StatusBadRequest)
			return
		}

		var fromSequence int64
		if raw := r.URL.Query().Get("from_sequence"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				http.Error(w, "from_sequence must be an integer", http.StatusBadRequest)
				return
			}
			fromSequence = parsed
		}

		// Origin is checked before the run is resolved, for two reasons. A
		// foreign origin must not learn which run ids exist by telling 404 from
		// 403. And a probe with any run id can then ask one question — "will
		// this engine talk to the page I serve?" — which is what `make up` uses
		// to refuse to report a stack that looks fine and never goes live.
		if !originAllowed(r, allowedOrigins) {
			slog.Warn("websocket upgrade refused",
				"run_id", runID,
				"origin", r.Header.Get("Origin"),
				"host", r.Host,
				"allowed_origins", allowedOrigins,
				"hint", "set ORK_WS_ALLOWED_ORIGINS to the origin serving the UI")
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}

		// Resolving the run before the upgrade keeps a missing run an HTTP 404
		// rather than a socket that opens and immediately dies.
		if _, err := svc.LookupRun(r.Context(), runID); err != nil {
			status, message := http.StatusInternalServerError, "read run"
			if connect.CodeOf(err) == connect.CodeNotFound {
				status, message = http.StatusNotFound, "run not found"
			}
			http.Error(w, message, status)
			return
		}

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: allowedOrigins,
		})
		if err != nil {
			// Accept already wrote the error response, but a refused upgrade is
			// otherwise invisible: the browser shows a page that never goes
			// live and the engine says nothing at all. The common cause is an
			// origin this engine was not told to allow, so name both sides and
			// the setting that fixes it.
			slog.Warn("websocket upgrade refused",
				"run_id", runID,
				"origin", r.Header.Get("Origin"),
				"host", r.Host,
				"allowed_origins", allowedOrigins,
				"hint", "set ORK_WS_ALLOWED_ORIGINS to the origin serving the UI",
				"error", err)
			return
		}
		defer conn.CloseNow()

		ctx := conn.CloseRead(r.Context())
		terminal := false
		streamErr := svc.StreamRunEvents(ctx, runID, fromSequence,
			func(event *orkstackv1.Event) error {
				payload, err := MarshalJSONOptions(marshaler, event)
				if err != nil {
					return err
				}
				if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
					return err
				}
				if IsTerminalEvent(event) {
					terminal = true
				}
				return nil
			})
		switch {
		case streamErr != nil:
			// The stream failed, or the peer went away mid-write. Either way
			// the socket cannot carry a normal close reason for it.
			_ = conn.Close(websocket.StatusInternalError, "event stream failed")
		case terminal:
			_ = conn.Close(websocket.StatusNormalClosure, "run finished")
		default:
			_ = conn.Close(websocket.StatusNormalClosure, "event stream closed")
		}
	})
}

// originAllowed reports whether a browser at r's Origin may open this socket.
//
// It mirrors what coder/websocket enforces during Accept, and Accept still
// enforces it too — this is the same rule applied early enough to answer
// before the run is looked up. The rules:
//
//   - No Origin header: not a browser. Allowed; CLI and test clients send none.
//   - Origin whose host equals the request Host: same-origin. Always allowed,
//     which is the library's default when no patterns are configured.
//   - Otherwise: allowed only when the Origin's host matches one of patterns,
//     compared case-insensitively, with `*` wildcards (so `*.example.com` and
//     `localhost:5173` both work).
func originAllowed(r *http.Request, patterns []string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	host := strings.ToLower(parsed.Host)
	if host == strings.ToLower(r.Host) {
		return true
	}
	for _, pattern := range patterns {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		if pattern == "" {
			continue
		}
		// Mirror coder/websocket exactly (accept.go, authenticateOrigin): a
		// pattern carrying a scheme is matched against scheme://host, one
		// without it against the bare host. Both spellings appear in the wild —
		// ORK_WS_ALLOWED_ORIGINS=http://localhost:5173 and =localhost:5173 —
		// and this check must not accept a narrower set than Accept does, or it
		// rejects sockets the library would have allowed.
		target := host
		if strings.Contains(pattern, "://") {
			target = strings.ToLower(parsed.Scheme) + "://" + host
		}
		if ok, err := path.Match(pattern, target); err == nil && ok {
			return true
		}
	}
	return false
}
