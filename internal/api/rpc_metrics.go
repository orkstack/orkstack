package api

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/internal/telemetry"
)

// rpcMetricsInterceptor records every Connect call's duration and code, and
// every server stream while it is open. It decides nothing and changes no
// answer.
type rpcMetricsInterceptor struct{ m *telemetry.RPCMetrics }

func (i rpcMetricsInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		i.m.ObserveCall(ctx, req.Spec().Procedure, rpcCode(err), time.Since(start))
		return resp, err
	}
}

func (i rpcMetricsInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i rpcMetricsInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		start := time.Now()
		closed := i.m.StreamOpened(ctx, telemetry.StreamConnect)
		defer closed() // a handler that panics still closes its stream
		err := next(ctx, conn)
		i.m.ObserveCall(ctx, conn.Spec().Procedure, rpcCode(err), time.Since(start))
		return err
	}
}

// rpcCode is "ok", or the code the client receives: a handler's Connect
// code, canceled or deadline_exceeded for a bare context error (as Connect
// sends it), unknown for any other error.
func rpcCode(err error) string {
	if err == nil {
		return "ok"
	}
	var connectErr *connect.Error
	switch {
	case errors.As(err, &connectErr):
		return connectErr.Code().String()
	case errors.Is(err, context.Canceled):
		return connect.CodeCanceled.String()
	case errors.Is(err, context.DeadlineExceeded):
		return connect.CodeDeadlineExceeded.String()
	}
	return connect.CodeUnknown.String()
}
