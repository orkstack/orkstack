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
// A run this process never executed is replayed from the archive and the socket
// closes, so a browser reattaching after an engine restart sees the run's
// history instead of a 404.
func NewWebSocketBridge(svc *Service, allowedOrigins []string) http.Handler {
	marshaler := protojson.MarshalOptions{EmitUnpopulated: false}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("id")
		if runID == "" {
			http.Error(w, "run id is required", http.StatusBadRequest)
			return
		}
		_, live := svc.Runs().Get(runID)

		var fromSequence int64
		if raw := r.URL.Query().Get("from_sequence"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				http.Error(w, "from_sequence must be an integer", http.StatusBadRequest)
				return
			}
			fromSequence = parsed
		}

		// Reading the archive before the upgrade keeps a missing run an HTTP
		// 404 rather than a socket that opens and immediately dies.
		var archived []*orkstackv1.Event
		if !live {
			events, err := svc.ArchivedEvents(r.Context(), runID, fromSequence)
			if err != nil {
				status := http.StatusInternalServerError
				message := "read run archive"
				if connect.CodeOf(err) == connect.CodeNotFound {
					status, message = http.StatusNotFound, "run not found"
				}
				http.Error(w, message, status)
				return
			}
			archived = events
		}

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: allowedOrigins,
		})
		if err != nil {
			return // Accept already wrote the error response.
		}
		defer conn.CloseNow()

		ctx := conn.CloseRead(r.Context())
		if !live {
			for _, event := range archived {
				payload, err := MarshalJSONOptions(marshaler, event)
				if err != nil {
					_ = conn.Close(websocket.StatusInternalError, "encode event")
					return
				}
				if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
					return
				}
			}
			_ = conn.Close(websocket.StatusNormalClosure, "archived run replayed")
			return
		}

		events, cancel, terminalAlreadyReached := svc.Bus().Subscribe(runID, fromSequence)
		defer cancel()
		if terminalAlreadyReached {
			_ = conn.Close(websocket.StatusNormalClosure, "terminal event already received")
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case event, open := <-events:
				if !open {
					_ = conn.Close(websocket.StatusNormalClosure, "event stream closed")
					return
				}
				payload, err := MarshalJSONOptions(marshaler, event)
				if err != nil {
					_ = conn.Close(websocket.StatusInternalError, "encode event")
					return
				}
				if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
					return
				}
				if IsTerminalEvent(event) {
					_ = conn.Close(websocket.StatusNormalClosure, "run finished")
					return
				}
			}
		}
	})
}
