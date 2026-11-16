package authn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"connectrpc.com/connect"
)

// MaxTokenFileBytes bounds a token file: one JWT.
const MaxTokenFileBytes = 64 << 10

// ErrTokenFile is a token file that holds no one usable token.
var ErrTokenFile = errors.New("not a file of one token")

// ReadTokenFile is the one token a file holds, as a client presents it: read
// whole, at most MaxTokenFileBytes, trimmed, and refused when empty or when
// it holds more than one token. label names the setting in every error.
// orkctl's --token-file and the Orker's orker.engine.auth.token_file are
// read by it, so both agree on what a token file is.
func ReadTokenFile(path, label string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("%s: %w: %w", label, ErrTokenFile, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, MaxTokenFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %w", label, path, ErrTokenFile, err)
	}
	token := strings.TrimSpace(string(raw))
	switch {
	case len(raw) > MaxTokenFileBytes:
		return "", fmt.Errorf("%s %s is larger than one token (%d bytes): %w", label, path, MaxTokenFileBytes, ErrTokenFile)
	case token == "":
		return "", fmt.Errorf("%s %s is empty: %w", label, path, ErrTokenFile)
	case strings.ContainsAny(token, " \t\r\n"):
		return "", fmt.Errorf("%s %s holds more than one token: %w", label, path, ErrTokenFile)
	}
	return token, nil
}

// LoopbackHost reports a host on this machine: the name localhost, or a
// loopback address. It is the one place a bearer may travel without TLS,
// for orkctl and for an Orker alike. A name under localhost is not one: not
// every resolver keeps it on this machine.
func LoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// BearerFromFile is a client interceptor presenting the token file's token,
// read again at every request so a token something else rotates is sent as
// it is now. A file that holds no token fails the request unauthenticated,
// naming label.
type BearerFromFile struct {
	Path  string
	Label string
}

func (b BearerFromFile) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		token, err := ReadTokenFile(b.Path, b.Label)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, err)
		}
		req.Header().Set("Authorization", "Bearer "+token)
		return next(ctx, req)
	}
}

func (b BearerFromFile) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		// A stream opens with no request to fail: one that cannot read its
		// token is opened without one, and the engine refuses it.
		if token, err := ReadTokenFile(b.Path, b.Label); err == nil {
			conn.RequestHeader().Set("Authorization", "Bearer "+token)
		}
		return conn
	}
}

func (BearerFromFile) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
