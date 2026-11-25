package orker

import (
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// LocalRunContext is what a local run of a recorded run's task (`orkctl task
// run --from-run`) is told of that run in place of its run-context object,
// which only an Orker's credential reaches (docs/OCI_ACTIVITY_CONTRACT.md
// §10.4).
type LocalRunContext struct {
	// Cached: the task is cached, so it reads none of the run's data.
	Cached bool
	// Lineage is the recorded run's (LineageOfRun). Unread when Cached.
	Lineage RunLineage
}

// LineageOfRun is the lineage a recorded run's own tasks read, from its
// record, or false where the record does not say the whole of it: no key is
// guessed. The run inside a shared Orkflow reads the root's origin without its
// principal and reference, as its tasks do; a record names only its own
// Orkflow's, its parent's and its root's owner, so a run whose parent is not
// the root reads it that way too, whatever lies between.
func LineageOfRun(run *orkstackv1.Run) (RunLineage, bool) {
	origin := run.GetOrigin()
	if run.GetId() == "" || origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED ||
		run.GetOrkflowName() == "" {
		return RunLineage{}, false
	}
	if origin.GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN {
		if run.GetOrkflowDefinitionDigest() == "" {
			return RunLineage{}, false
		}
		started := recordedOrigin(origin)
		return RunLineage{Origin: started, RootRunID: run.GetId(), Root: artifacts.RunContextRoot{
			Orkflow: artifacts.RunContextOrkflow{Name: run.GetOrkflowName(), Version: run.GetOrkflowVersion(),
				DefinitionDigest: run.GetOrkflowDefinitionDigest()},
			Origin: started,
		}}, true
	}
	if origin.GetId() == "" || origin.GetRootRunId() == "" || origin.GetParentTaskId() == "" ||
		origin.GetParentOrkflow() == nil || origin.GetRootOrkflow() == nil {
		return RunLineage{}, false
	}
	root := recordedOrigin(origin.GetRootOrigin())
	if run.GetOrkflowNamespace() != "" || origin.GetParentOrkflow().GetNamespace() != "" ||
		origin.GetId() != origin.GetRootRunId() {
		root = artifacts.RunContextOrigin{Kind: root.Kind, ID: root.ID, Description: root.Description}
	}
	return RunLineage{
		Origin:    artifacts.RunContextOrigin{Kind: origin.GetKind().String(), ID: origin.GetId()},
		RootRunID: origin.GetRootRunId(),
		Root:      artifacts.RunContextRoot{Orkflow: recordedOrkflow(origin.GetRootOrkflow()), Origin: root},
		Parent: &RunParent{RunID: origin.GetId(), TaskID: origin.GetParentTaskId(),
			Orkflow: recordedOrkflow(origin.GetParentOrkflow())},
	}, true
}

// recordedOrigin is what started a run as its run-context object writes it: an
// origin no one recorded is the empty one.
func recordedOrigin(o *orkstackv1.RunOrigin) artifacts.RunContextOrigin {
	if o == nil {
		return artifacts.RunContextOrigin{}
	}
	return artifacts.RunContextOrigin{Kind: o.GetKind().String(), ID: o.GetId(), Reference: o.GetReference(),
		Description: o.GetDescription(), Principal: o.GetPrincipal(), PrincipalVerified: o.GetPrincipalVerified()}
}

func recordedOrkflow(o *orkstackv1.OrkflowRef) artifacts.RunContextOrkflow {
	return artifacts.RunContextOrkflow{Name: o.GetName(), Version: o.GetVersion(), DefinitionDigest: o.GetDefinitionDigest()}
}
