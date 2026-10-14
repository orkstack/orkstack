// Command generations reports how many executions one run's chain has — a
// run that continued as new is one workflow id with several executions, the
// closed ones ContinuedAsNew — and how many times the engine logged that the
// run held new dispatch so it could carry on.
//
//	ORK_TEMPORAL_ADDRESS=127.0.0.1:7233 ORK_TEMPORAL_NAMESPACE=ork-ui-continuation \
//		go run ./test/tools/generations -run ork-… -log engine.log
//
// It prints one JSON object: {"executions":3,"continued":2,"holds":1}.
// ui/e2e/run-continuation.spec.ts uses it to confirm what its browser cannot
// see, because a run's generations are internal (AGENTS.md §7): no
// user-facing surface names them, and the spec proves exactly that the page
// shows one run. Operator-only, like the engine log line it reads.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// holdLine is the engine's operator log line when a run starts holding new
// dispatch (internal/engine/continuation.go, updateContinuation).
const holdLine = "run pausing new tasks so it can carry on into a fresh history"

func main() {
	runID := flag.String("run", "", "the run id (its workflow id)")
	logPath := flag.String("log", "", "the engine log to count hold lines in (optional)")
	flag.Parse()
	if *runID == "" {
		log.Fatal("generations: -run is required")
	}
	address := os.Getenv("ORK_TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7233"
	}
	namespace := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if namespace == "" {
		log.Fatal("generations: ORK_TEMPORAL_NAMESPACE is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		log.Fatalf("generations: dial %s: %v", address, err)
	}
	defer c.Close()

	executions, continued := 0, 0
	var token []byte
	for {
		page, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     namespace,
			Query:         fmt.Sprintf("WorkflowId = %q", *runID),
			NextPageToken: token,
		})
		if err != nil {
			log.Fatalf("generations: list executions of %s: %v", *runID, err)
		}
		for _, info := range page.GetExecutions() {
			executions++
			if info.GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
				continued++
			}
		}
		token = page.GetNextPageToken()
		if len(token) == 0 {
			break
		}
	}

	holds := 0
	if *logPath != "" {
		f, err := os.Open(*logPath)
		if err != nil {
			log.Fatalf("generations: open %s: %v", *logPath, err)
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<24)
		for scanner.Scan() {
			if line := scanner.Text(); strings.Contains(line, holdLine) && strings.Contains(line, *runID) {
				holds++
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("generations: read %s: %v", *logPath, err)
		}
		_ = f.Close()
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]int{
		"executions": executions, "continued": continued, "holds": holds,
	}); err != nil {
		log.Fatal(err)
	}
}
