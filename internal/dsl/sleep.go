package dsl

import (
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// MaxSleepSeconds bounds sleep.seconds: 30 days, the bound
// MaxAwaitTimeoutSeconds sets and for the same reason. A sleeping run is a
// live execution pinned to the release that started it (docs/DEPLOY.md §2,
// "Why RunWorkflow is Pinned"), so the sleep is also the longest it can keep
// that release's workers required. A longer wait is two runs, or a schedule.
const MaxSleepSeconds = MaxAwaitTimeoutSeconds

// sleepDocument is one sleeping task's declaration. KnownFields(true) on the
// decoder applies to it, so a misspelled key inside `sleep:` is refused.
type sleepDocument struct {
	// Seconds is required. A pointer so an absent key is refused by name
	// rather than read as 0.
	Seconds *int32 `yaml:"seconds"`
}

// sleepExecutionKeys lists, in the order they are checked, the YAML keys a
// sleeping task may not carry, each with whether the author wrote it. It is
// the YAML-layer half of rule 49: the compiled message is checked again by
// validate, which is what a hand-built request meets. `uses` and `await` are
// refused as exclusivity (rule 6) by compileSleepTask.
func sleepExecutionKeys(td taskDocument) []struct {
	key     string
	written bool
} {
	return []struct {
		key     string
		written bool
	}{
		{"uses", td.Uses != ""},
		{"await", td.Await != nil},
		{"with", present(td.With)},
		{"command", td.Command != nil},
		{"env", td.Env != nil},
		{"secrets", td.Secrets != nil},
		{"resources", td.Resources != nil},
		{"network", td.Network != ""},
		{"pool", td.Pool != ""},
		{"pin", td.Pin != ""},
		{"retry", td.Retry != nil},
		{"timeout_seconds", td.TimeoutSeconds != nil},
		{"cancellation_grace_period_seconds", td.CancellationGracePeriodSeconds != nil},
		{"run_as_orker_user", td.RunAsOrkerUser},
		{"each", td.Each != nil},
		{"cache", td.Cache != nil},
	}
}

// sleepFieldRefusal is rule 49's sentence for one key.
func sleepFieldRefusal(taskID, key string) error {
	reason := "it runs no container"
	switch key {
	case "each":
		reason = "a sleep is one wait, not N"
	case "cache":
		reason = "it computes nothing to reuse"
	case "orkflow":
		reason = "it starts no child run"
	}
	msg := fmt.Sprintf("task %q: a sleeping task takes only id, needs, condition, join and sleep; "+
		"%q does not apply (%s)", taskID, key, reason)
	if key == "timeout_seconds" {
		msg += " — how long it sleeps is sleep.seconds"
	}
	return fmt.Errorf("%s", msg)
}

// sleepUsesRefusal and sleepAwaitRefusal are rule 6's sentences for a task
// that declares sleep: beside one of the other two kinds.
func sleepUsesRefusal(taskID string) error {
	return fmt.Errorf("task %q: uses: and sleep: are exclusive — a sleeping task runs no image", taskID)
}

func sleepAwaitRefusal(taskID string) error {
	return fmt.Errorf("task %q: await: and sleep: are exclusive — an await waits for a resolution "+
		"from outside the run, a sleep for a fixed time; declare two tasks", taskID)
}

// compileSleepTask builds the Task for a document that declares `sleep:`,
// after refusing every other kind and every execution key the author wrote.
func compileSleepTask(taskIndex int, td taskDocument) (*orkstackv1.Task, error) {
	for _, field := range sleepExecutionKeys(td) {
		if !field.written {
			continue
		}
		at := fmt.Sprintf("tasks[%d].%s", taskIndex, field.key)
		switch field.key {
		case "uses":
			return nil, validationError(at, sleepUsesRefusal(td.ID))
		case "await":
			return nil, validationError(at, sleepAwaitRefusal(td.ID))
		}
		return nil, validationError(at, sleepFieldRefusal(td.ID, field.key))
	}
	if td.Sleep.Seconds == nil {
		return nil, validationError(fmt.Sprintf("tasks[%d].sleep.seconds", taskIndex),
			fmt.Errorf("task %q: sleep.seconds is required — how long the task sleeps", td.ID))
	}
	join, err := compileJoin(taskIndex, td)
	if err != nil {
		return nil, err
	}
	return &orkstackv1.Task{
		Id:        td.ID,
		Needs:     td.Needs,
		Condition: td.Condition,
		Join:      join,
		// 0, not DefaultTimeoutSeconds: a sleep's length is its own.
		TimeoutSeconds: 0,
		Sleep:          &orkstackv1.SleepSpec{Seconds: *td.Sleep.Seconds},
	}, nil
}

// validateSleepTask enforces rules 6, 49 and 50 on one compiled sleeping
// task.
func validateSleepTask(taskIndex int, task *orkstackv1.Task) error {
	id := task.GetId()
	at := func(field string) string { return fmt.Sprintf("tasks[%d].%s", taskIndex, field) }
	// Rule 6: exactly one of uses:, await: and sleep:.
	if task.GetUses() != "" {
		return validationError(at("uses"), sleepUsesRefusal(id))
	}
	if task.GetAwait() != nil {
		return validationError(at("await"), sleepAwaitRefusal(id))
	}
	// Rule 49: nothing but id, needs, condition, join and sleep.
	checks := []struct {
		field string
		set   bool
	}{
		{"with", task.GetWith() != nil},
		{"command", len(task.GetCommand()) > 0},
		{"env", len(task.GetEnv()) > 0},
		{"secrets", len(task.GetSecrets()) > 0},
		{"resources", task.GetResources() != nil},
		{"network", task.GetNetwork() != orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED},
		{"pool", task.GetPool() != ""},
		{"pin", task.GetPin() != ""},
		{"retry", task.GetRetry() != nil},
		{"timeout_seconds", task.GetTimeoutSeconds() != 0},
		{"cancellation_grace_period_seconds", task.GetCancellationGracePeriodSeconds() != 0},
		{"run_as_orker_user", task.GetRunAsOrkerUser()},
		{"image", task.GetImage() != nil},
		{"orkflow", task.GetOrkflow() != nil},
		{"each", task.GetEach() != nil},
		{"cache", task.GetCache() != nil},
	}
	for _, c := range checks {
		if c.set {
			return validationError(at(c.field), sleepFieldRefusal(id, c.field))
		}
	}
	// Rule 50: a bounded length.
	if s := task.GetSleep().GetSeconds(); s < 1 || s > MaxSleepSeconds {
		return validationError(at("sleep.seconds"), fmt.Errorf(
			"task %q: sleep.seconds %d is out of range (1 s … 30 days)", id, s))
	}
	return nil
}

// sleepReadRefusal is rule 51's sentence: a template, an output or a fan-out
// that reads a sleeping task, which produces no values and no artifacts.
func sleepReadRefusal(taskID, path, sleeper string) error {
	return fmt.Errorf("task %q: %s: %q is a sleeping task and produces no values or artifacts; "+
		"order after it with needs: alone", taskID, path, sleeper)
}
