package api

import (
	"net/http"
	"strconv"

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
			return // Accept already wrote the error response.
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
