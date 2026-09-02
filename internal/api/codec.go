package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Connect names its JSON codec twice: bare, and with the charset parameter a
// browser may send.
const (
	codecNameJSON            = "json"
	codecNameJSONCharsetUTF8 = codecNameJSON + "; charset=utf-8"
)

// jsonCodec replaces Connect's built-in JSON codec with a decoder that accepts
// enum values in either spelling — "SUCCEEDED" and "STATUS_SUCCEEDED" both
// parse — so a hand-written client may send the short form the CLI and the
// WebSocket bridge emit.
//
// Responses keep protojson's canonical prefixed names. Generated browser
// clients (protobuf-es `fromJson`) reject an enum value they do not declare,
// so unilaterally shortening RPC responses would break every UI build that has
// not been regenerated. The surfaces OrkStack renders itself — orkctl --json
// and the WebSocket bridge — use MarshalJSON and emit the short form.
type jsonCodec struct {
	name string
}

var (
	_ connect.Codec = jsonCodec{}
)

// HandlerOptions are the Connect options every OrkStack handler is built with.
func HandlerOptions() []connect.HandlerOption {
	return []connect.HandlerOption{
		connect.WithCodec(jsonCodec{name: codecNameJSON}),
		connect.WithCodec(jsonCodec{name: codecNameJSONCharsetUTF8}),
	}
}

func (c jsonCodec) Name() string { return c.name }

func (c jsonCodec) Marshal(message any) ([]byte, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("marshal: %T does not implement proto.Message", message)
	}
	return protojson.MarshalOptions{}.Marshal(protoMessage)
}

func (c jsonCodec) Unmarshal(data []byte, message any) error {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("unmarshal: %T does not implement proto.Message", message)
	}
	if len(data) == 0 {
		return errors.New("zero-length payload is not a valid JSON object")
	}
	// DiscardUnknown matches Connect's own codec: schema drift between client
	// and server must not be fatal.
	options := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := UnmarshalJSONOptions(options, data, protoMessage); err != nil {
		return fmt.Errorf("unmarshal into %T: %w", message, err)
	}
	return nil
}

// MarshalStable gives Connect deterministic bytes for GET request encoding.
func (c jsonCodec) MarshalStable(message any) ([]byte, error) {
	encoded, err := c.Marshal(message)
	if err != nil {
		return nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return nil, fmt.Errorf("compact json: %w", err)
	}
	return compact.Bytes(), nil
}

func (c jsonCodec) IsBinary() bool { return false }
