package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const artifactChunkSize = 64 << 10

// ListRunArtifacts exposes only references recorded in the authoritative run
// snapshot. It never accepts a URI from the caller.
func (s *Service) ListRunArtifacts(ctx context.Context, req *connect.Request[orkstackv1.ListRunArtifactsRequest]) (*connect.Response[orkstackv1.ListRunArtifactsResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	}
	run, err := s.LookupRunIn(ctx, req.Msg.GetRunId(), req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	items := runArtifacts(run, req.Msg.GetTaskId())
	if req.Msg.GetTaskId() != "" && !hasTask(run, req.Msg.GetTaskId()) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("task %q not found in run %q", req.Msg.GetTaskId(), run.GetId()))
	}
	return connect.NewResponse(&orkstackv1.ListRunArtifactsResponse{Artifacts: items}), nil
}

func (s *Service) ReadArtifact(ctx context.Context, req *connect.Request[orkstackv1.ReadArtifactRequest], stream *connect.ServerStream[orkstackv1.ArtifactChunk]) error {
	if req.Msg.GetRunId() == "" || req.Msg.GetTaskId() == "" || req.Msg.GetName() == "" || req.Msg.GetKind() == orkstackv1.ArtifactKind_ARTIFACT_KIND_UNSPECIFIED {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id, task_id, kind and name are required"))
	}
	if s.artifactStore == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("artifact store is not configured"))
	}
	run, err := s.LookupRunIn(ctx, req.Msg.GetRunId(), req.Msg.GetNamespace())
	if err != nil {
		return err
	}
	var selected *orkstackv1.RunArtifact
	for _, item := range runArtifacts(run, req.Msg.GetTaskId()) {
		if item.GetKind() == req.Msg.GetKind() && item.GetArtifact().GetName() == req.Msg.GetName() {
			selected = item
			break
		}
	}
	if selected == nil {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("artifact %q of kind %s is not recorded for task %q in run %q", req.Msg.GetName(), req.Msg.GetKind(), req.Msg.GetTaskId(), run.GetId()))
	}
	reader, err := s.artifactStore.Get(ctx, selected.GetArtifact().GetUri())
	if err != nil {
		code := connect.CodeUnavailable
		if errors.Is(err, artifacts.ErrNotFound) {
			code = connect.CodeNotFound
		}
		return connect.NewError(code, fmt.Errorf("read recorded artifact %q: %w", selected.GetArtifact().GetName(), err))
	}
	defer reader.Close()
	hash := sha256.New()
	var total int64
	buf := make([]byte, artifactChunkSize)
	first := true
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			data := append([]byte(nil), buf[:n]...)
			_, _ = hash.Write(data)
			total += int64(n)
			chunk := &orkstackv1.ArtifactChunk{Data: data}
			if first {
				chunk.Artifact = selected.GetArtifact()
				first = false
			}
			if err := stream.Send(chunk); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("read recorded artifact %q: %w", selected.GetArtifact().GetName(), readErr))
		}
	}
	if total != selected.GetArtifact().GetSizeBytes() || (selected.GetArtifact().GetSha256() != "" && !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), strings.TrimPrefix(selected.GetArtifact().GetSha256(), "sha256:"))) {
		return connect.NewError(connect.CodeDataLoss, fmt.Errorf("recorded artifact %q failed integrity verification", selected.GetArtifact().GetName()))
	}
	return stream.Send(&orkstackv1.ArtifactChunk{Last: true, Artifact: func() *orkstackv1.ArtifactRef {
		if first {
			return selected.GetArtifact()
		}
		return nil
	}()})
}

func hasTask(run *orkstackv1.Run, id string) bool {
	for _, task := range run.GetTaskRuns() {
		if task.GetTaskId() == id {
			return true
		}
	}
	return false
}

func runArtifacts(run *orkstackv1.Run, taskID string) []*orkstackv1.RunArtifact {
	var result []*orkstackv1.RunArtifact
	for _, task := range run.GetTaskRuns() {
		if taskID != "" && task.GetTaskId() != taskID {
			continue
		}
		for _, ref := range task.GetArtifacts() {
			result = append(result, &orkstackv1.RunArtifact{TaskId: task.GetTaskId(), Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Artifact: ref, Attempt: task.GetAttempt()})
		}
		if ref := task.GetValuesRef(); ref != nil {
			result = append(result, &orkstackv1.RunArtifact{TaskId: task.GetTaskId(), Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES, Artifact: ref, Attempt: task.GetAttempt()})
		}
		for _, ref := range task.GetSystemArtifacts() {
			kind := orkstackv1.ArtifactKind_ARTIFACT_KIND_UNSPECIFIED
			switch ref.GetName() {
			case "stdout.log":
				kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT
			case "stderr.log":
				kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_STDERR
			}
			if kind != orkstackv1.ArtifactKind_ARTIFACT_KIND_UNSPECIFIED {
				result = append(result, &orkstackv1.RunArtifact{TaskId: task.GetTaskId(), Kind: kind, Artifact: ref, Attempt: task.GetAttempt()})
			}
		}
	}
	return result
}
