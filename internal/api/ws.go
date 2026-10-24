package api

import (
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
// Both go through Service.StreamRunEvents' one streaming path — the socket
// makes the namespace check itself, before the upgrade, and then streams
// through streamCheckedRunEvents — so the socket reads the same system of
// record: any replica serves any run, whether it is executing now, finished
// long ago, or was started by a different process a moment earlier.
func NewWebSocketBridge(svc *Service, allowedOrigins []string) http.Handler {
	marshaler := protojson.MarshalOptions{EmitUnpopulated: false}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("id")
		if runID == "" {
			http.Error(w, "run id is required", http.StatusBadRequest)
			return
		}

		// The namespace is a query parameter because the path is the run's
		// address and the socket has no request body to put it in. Empty is
		// the default namespace, the same rule the RPC follows: a run of
		// another namespace is 404 here too, so a browser holding an id it
		// read somewhere cannot watch a tenant it cannot name.
		namespace := r.URL.Query().Get("namespace")

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
			svc.logger.Warn("websocket upgrade refused",
				"run_id", runID,
				"origin", r.Header.Get("Origin"),
				"host", r.Host,
				"allowed_origins", allowedOrigins,
				"hint", "set ORK_WS_ALLOWED_ORIGINS to the origin serving the UI")
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}

		// Resolving the run before the upgrade keeps a missing run an HTTP 404
		// rather than a socket that opens and immediately dies. It is resolved
		// INSIDE the namespace asked for, so the 404 also covers a run of
		// another one.
		if err := svc.runNamespaceIn(r.Context(), runID, namespace); err != nil {
			status, message := http.StatusInternalServerError, "read run"
			switch connect.CodeOf(err) {
			case connect.CodeNotFound:
				status, message = http.StatusNotFound, "run not found"
			case connect.CodeInvalidArgument, connect.CodeFailedPrecondition:
				status, message = http.StatusBadRequest, "namespace cannot be resolved"
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
			svc.logger.Warn("websocket upgrade refused",
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
		streamErr := svc.streamCheckedRunEvents(ctx, runID, fromSequence,
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

// TaskLogSocketPath is the browser's spelling of StreamTaskLogs:
//
//	/ws/runs/{id}/tasks/{task}/logs?attempt=&from_seq=&from_attempt=&follow=&namespace=&item=
//
// It exists beside the run's event socket rather than inside it because the
// two carry different things at different rates: a run emits a handful of
// events, a chatty task emits megabytes. Keeping them apart is also what lets
// a UI open a log pane per task and close it again without disturbing the run
// stream the page is built on.
//
// from_attempt is half of the resume token: a chunk's seq is dense per
// ATTEMPT, so a client reconnecting after a retry has to say which attempt its
// from_seq counted, or the engine would apply a dead attempt's cursor to a
// live one (proto StreamTaskLogsRequest.from_attempt).
const TaskLogSocketPath = "/ws/runs/{id}/tasks/{task}/logs"

// NewTaskLogSocket returns the TaskLogSocketPath handler. It forwards the same
// chunks as RunService.StreamTaskLogs, encoded as JSON text frames in
// OrkStack's dialect, so a browser follows a task without a streaming RPC
// client.
//
// Both go through Service.StreamRunTaskLogs, so the socket and the RPC read
// the same store and end on the same rule.
func NewTaskLogSocket(svc *Service, allowedOrigins []string) http.Handler {
	marshaler := protojson.MarshalOptions{EmitUnpopulated: false}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("id")
		taskID := r.PathValue("task")
		if runID == "" || taskID == "" {
			http.Error(w, "run id and task id are required", http.StatusBadRequest)
			return
		}
		query := TaskLogQuery{
			RunID:     runID,
			TaskID:    taskID,
			Namespace: r.URL.Query().Get("namespace"),
		}
		if raw := r.URL.Query().Get("attempt"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || parsed < 0 {
				http.Error(w, "attempt must be a non-negative integer", http.StatusBadRequest)
				return
			}
			query.Attempt = int32(parsed)
		}
		if raw := r.URL.Query().Get("from_seq"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || parsed < 0 {
				http.Error(w, "from_seq must be a non-negative integer", http.StatusBadRequest)
				return
			}
			query.FromSeq = parsed
		}
		if raw := r.URL.Query().Get("from_attempt"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || parsed < 0 {
				http.Error(w, "from_attempt must be a non-negative integer", http.StatusBadRequest)
				return
			}
			query.FromAttempt = int32(parsed)
		}
		if raw := r.URL.Query().Get("item"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || parsed < 0 {
				http.Error(w, "item must be a non-negative integer", http.StatusBadRequest)
				return
			}
			item := int32(parsed)
			query.Item = &item
		}
		if raw := r.URL.Query().Get("follow"); raw != "" {
			parsed, err := strconv.ParseBool(raw)
			if err != nil {
				http.Error(w, "follow must be true or false", http.StatusBadRequest)
				return
			}
			query.Follow = parsed
		}

		// Origin first, for the reasons the run socket states: a foreign
		// origin must not learn which run ids exist by telling 404 from 403.
		if !originAllowed(r, allowedOrigins) {
			svc.logger.Warn("websocket upgrade refused",
				"run_id", runID, "task_id", taskID,
				"origin", r.Header.Get("Origin"), "host", r.Host,
				"allowed_origins", allowedOrigins,
				"hint", "set ORK_WS_ALLOWED_ORIGINS to the origin serving the UI")
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		// Resolving the run before the upgrade keeps a missing run an HTTP
		// 404 rather than a socket that opens and immediately dies, and it is
		// resolved INSIDE the namespace asked for.
		if err := svc.runNamespaceIn(r.Context(), runID, query.Namespace); err != nil {
			status, message := http.StatusInternalServerError, "read run"
			switch connect.CodeOf(err) {
			case connect.CodeNotFound:
				status, message = http.StatusNotFound, "run not found"
			case connect.CodeInvalidArgument, connect.CodeFailedPrecondition:
				status, message = http.StatusBadRequest, "namespace cannot be resolved"
			}
			http.Error(w, message, status)
			return
		}

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: allowedOrigins,
		})
		if err != nil {
			svc.logger.Warn("websocket upgrade refused",
				"run_id", runID, "task_id", taskID,
				"origin", r.Header.Get("Origin"), "host", r.Host,
				"allowed_origins", allowedOrigins,
				"hint", "set ORK_WS_ALLOWED_ORIGINS to the origin serving the UI",
				"error", err)
			return
		}
		defer conn.CloseNow()

		ctx := conn.CloseRead(r.Context())
		finished := false
		streamErr := svc.StreamRunTaskLogs(ctx, query, func(chunk *orkstackv1.TaskLogChunk) error {
			payload, err := MarshalJSONOptions(marshaler, chunk)
			if err != nil {
				return err
			}
			if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
				return err
			}
			if chunk.GetLast() {
				finished = true
			}
			return nil
		})
		switch {
		case streamErr != nil:
			_ = conn.Close(websocket.StatusInternalError, "task log stream failed")
		case finished:
			_ = conn.Close(websocket.StatusNormalClosure, "task finished")
		default:
			_ = conn.Close(websocket.StatusNormalClosure, "task log stream closed")
		}
	})
}
