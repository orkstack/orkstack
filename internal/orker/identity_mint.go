package orker

import (
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/internal/identity"
)

// ReasonIdentitySpecSkew is a task whose digest the engine computed
// differently, on an Orker of another build than the engine that admitted
// it: the two builds disagree about the task, not the task about its
// admission. Retryable, so a retry may land on an Orker of the admitting
// release.
const ReasonIdentitySpecSkew = "IDENTITY_SPEC_SKEW"

// engineBuildHeader is the metadata a TASK_NOT_AS_ADMITTED refusal carries:
// the build of the engine that admitted the run.
const engineBuildHeader = "Ork-Engine-Build"

// mintThen is what an attempt does with a mint's answer.
type mintThen int

const (
	// mintAskAgain: ask again, within the setup budget at setup and until
	// what is on disk expires at a refresh.
	mintAskAgain mintThen = iota + 1
	// mintEnd: end the attempt now with the reason.
	mintEnd
	// mintStop: stop now. A required identity's container is stopped at
	// once with the reason; a best_effort one is no longer refreshed.
	mintStop
)

func (t mintThen) String() string {
	switch t {
	case mintAskAgain:
		return "ask again"
	case mintEnd:
		return "end the attempt"
	case mintStop:
		return "stop"
	}
	return fmt.Sprintf("mintThen(%d)", int(t))
}

// mintAnswer is one row of the Orker's table of a mint's answers.
type mintAnswer struct {
	Reason   string
	Category string
	Then     mintThen
	// Count is how ork.identity.engine_refused counts the answer:
	// EngineRefusedOrker, EngineCouldNotRead, or "" for an answer that is not the engine's refusal of the
	// Orker itself.
	Count string
	// Message says what the engine answered, for the attempt's failure.
	Message string
}

// mapMintAnswer is the Orker's mapping of a failed mint
// in one table, held by a test with one case a row. A
// ContractViolation is only a refusal that would be given again identically
// and concerns this run; a fault of the deployment is an InfrastructureError;
// anything unknown is IDENTITY_UNAVAILABLE. orkerBuild is this Orker's
// build, which tells a task the engine admitted differently from one two
// builds disagree about.
//
// IDENTITY_KEY_NOT_ACTIVE is not an answer the engine gives a mint (it
// answers IDENTITY_ENGINE_REFUSED when it has no eligible key), so it has
// no row of its own here and would read as IDENTITY_UNAVAILABLE, retried
// alike.
func mapMintAnswer(err error, orkerBuild string) mintAnswer {
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		return mintRow(identity.ReasonUnavailable, mintAskAgain, "", "the engine could not be asked: "+err.Error())
	}
	msg := cerr.Message()
	switch cerr.Code() {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied, connect.CodeUnimplemented:
		return mintRow(identity.ReasonEngineRefused, mintAskAgain, EngineRefusedOrker, "the engine refused this Orker ("+cerr.Code().String()+"): "+msg)
	case connect.CodeInvalidArgument:
		return mintRow(identity.ReasonEngineRefused, mintAskAgain, EngineCouldNotRead, "the engine could not read this Orker's request: "+msg)
	case connect.CodeFailedPrecondition:
	default:
		return mintRow(identity.ReasonUnavailable, mintAskAgain, "", "the engine could not answer ("+cerr.Code().String()+"): "+msg)
	}
	reason, _, _ := strings.Cut(msg, ":")
	switch reason {
	case identity.ReasonEngineRefused:
		return mintRow(reason, mintAskAgain, EngineRefusedOrker, msg)
	case identity.ReasonAttemptNotHeld, identity.ReasonNotAdmitted:
		return mintRow(reason, mintAskAgain, "", msg)
	case identity.ReasonTaskNotAsAdmitted:
		if engine := cerr.Meta().Get(engineBuildHeader); engine != orkerBuild {
			return mintRow(ReasonIdentitySpecSkew, mintEnd, "",
				fmt.Sprintf("%s (this Orker's build %q, the admitting engine's %q)", msg, orkerBuild, engine))
		}
		return mintRow(reason, mintEnd, "", msg)
	case identity.ReasonAttemptsExhausted, identity.ReasonAttemptExpired, identity.ReasonRequiredResultNotRecorded:
		return mintRow(reason, mintEnd, "", msg)
	case identity.ReasonRevoked:
		return mintRow(reason, mintStop, "", msg)
	}
	return mintRow(identity.ReasonUnavailable, mintAskAgain, "", "the engine answered a refusal this Orker does not know: "+msg)
}

func mintRow(reason string, then mintThen, count, message string) mintAnswer {
	return mintAnswer{Reason: reason, Category: CategoryFor(reason), Then: then, Count: count, Message: message}
}
