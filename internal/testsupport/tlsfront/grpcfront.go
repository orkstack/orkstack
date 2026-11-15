package tlsfront

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// KeyFront terminates TLS and holds every gRPC call to an API key: a call
// whose bearer is not one of the accepted keys is refused unauthenticated,
// as a cluster that checks API keys refuses it; an accepted one is forwarded
// to the upstream, without its key, message for message. It is the stand-in
// for a cluster that takes API keys, in front of the proof's own Temporal,
// which takes none.
type KeyFront struct {
	// Addr is where clients dial it.
	Addr string

	srv      *grpc.Server
	upstream *grpc.ClientConn
	mu       sync.Mutex
	accepted map[string]bool
	seen     []string // each call's key, in order
}

// maxMessage bounds a forwarded message, well above any the suites send.
const maxMessage = 64 << 20

// StartKeyFront serves on a free loopback port until the test ends,
// accepting keys.
func StartKeyFront(t testing.TB, upstream string, certificate tls.Certificate, keys ...string) *KeyFront {
	t.Helper()
	conn, err := grpc.NewClient(upstream,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessage), grpc.MaxCallSendMsgSize(maxMessage)))
	if err != nil {
		t.Fatalf("tlsfront: dial upstream %s: %v", upstream, err)
	}
	f := &KeyFront{upstream: conn}
	f.SetKeys(keys...)
	f.srv = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})),
		grpc.ForceServerCodec(rawCodec{}),
		grpc.MaxRecvMsgSize(maxMessage),
		grpc.MaxSendMsgSize(maxMessage),
		grpc.UnknownServiceHandler(f.forward),
	)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = conn.Close()
		t.Fatalf("tlsfront: listen: %v", err)
	}
	f.Addr = ln.Addr().String()
	go func() { _ = f.srv.Serve(ln) }()
	t.Cleanup(func() {
		f.srv.Stop()
		_ = f.upstream.Close()
	})
	return f
}

// SetKeys replaces the accepted keys.
func (f *KeyFront) SetKeys(keys ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accepted = map[string]bool{}
	for _, k := range keys {
		f.accepted[k] = true
	}
}

// Seen are the keys of every call so far, in order; a call with no bearer is
// an empty string.
func (f *KeyFront) Seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

func (f *KeyFront) admit(md metadata.MD) bool {
	key := ""
	if v := md.Get("authorization"); len(v) > 0 {
		key = strings.TrimPrefix(v[0], "Bearer ")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, key)
	return key != "" && f.accepted[key]
}

func (f *KeyFront) forward(_ any, ss grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(ss)
	if !ok {
		return status.Error(codes.Internal, "tlsfront: no method")
	}
	in, _ := metadata.FromIncomingContext(ss.Context())
	if !f.admit(in) {
		return status.Error(codes.Unauthenticated, "tlsfront: the API key is not accepted")
	}
	out := metadata.MD{}
	for k, v := range in {
		if k == "authorization" || k == "content-type" || k == "user-agent" || strings.HasPrefix(k, ":") {
			continue
		}
		out[k] = v
	}
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(ss.Context(), out))
	defer cancel()
	cs, err := f.upstream.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return err
	}
	go func() {
		for {
			frame := &rawFrame{}
			if err := ss.RecvMsg(frame); err != nil {
				if err == io.EOF {
					_ = cs.CloseSend()
				} else {
					cancel()
				}
				return
			}
			if err := cs.SendMsg(frame); err != nil {
				return
			}
		}
	}()
	if header, err := cs.Header(); err == nil {
		_ = ss.SendHeader(header)
	}
	for {
		frame := &rawFrame{}
		if err := cs.RecvMsg(frame); err != nil {
			ss.SetTrailer(cs.Trailer())
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := ss.SendMsg(frame); err != nil {
			return err
		}
	}
}

// rawFrame is one message's bytes, forwarded undecoded.
type rawFrame struct{ b []byte }

// rawCodec passes a message's bytes through. It answers to "proto", the
// codec name a gRPC client's content type selects.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*rawFrame)
	if !ok {
		return nil, fmt.Errorf("tlsfront: cannot marshal %T", v)
	}
	return f.b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*rawFrame)
	if !ok {
		return fmt.Errorf("tlsfront: cannot unmarshal into %T", v)
	}
	f.b = append([]byte(nil), data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }
