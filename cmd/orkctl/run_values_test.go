package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// inlineValues is a task's output as a run carries it.
func inlineValues(t *testing.T, fields map[string]any) *orkstackv1.Payload {
	t.Helper()
	data, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("values: %v", err)
	}
	return &orkstackv1.Payload{Data: data}
}

func valuesRef(size int64) *orkstackv1.ArtifactRef {
	return &orkstackv1.ArtifactRef{Name: "values.json", SizeBytes: size}
}

// valuesOfExactly is an object whose compact JSON is width characters.
func valuesOfExactly(width int) map[string]any {
	return map[string]any{"v": strings.Repeat("x", width-len(`{"v":""}`))}
}

// TestRunGet_ShowsATasksValues: what a task returned reads in `run get`, on one
// line each, however long it is and whatever it holds.
func TestRunGet_ShowsATasksValues(t *testing.T) {
	succeeded := orkstackv1.Status_STATUS_SUCCEEDED
	run := &orkstackv1.Run{
		Id: "run-values", Namespace: "team-a", OrkflowName: "values", OrkflowVersion: "1.0.0",
		Status: succeeded,
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "short", Status: succeeded, Output: inlineValues(t, map[string]any{"ok": true}), ValuesRef: valuesRef(11)},
			{TaskId: "url", Status: succeeded, Output: inlineValues(t, map[string]any{"u": "a?b=1&c=<2>"}), ValuesRef: valuesRef(21)},
			{TaskId: "at-limit", Status: succeeded, Output: inlineValues(t, valuesOfExactly(64)), ValuesRef: valuesRef(64)},
			{TaskId: "past-limit", Status: succeeded, Output: inlineValues(t, valuesOfExactly(65)), ValuesRef: valuesRef(65)},
			{TaskId: "no-ref", Status: succeeded, Output: inlineValues(t, valuesOfExactly(100))},
			{TaskId: "by-ref", Status: succeeded, ValuesRef: valuesRef(8192)},
			{TaskId: "hostile", Status: succeeded, Output: inlineValues(t, map[string]any{
				"esc": "\x1b]0;owned\x07", "bi": "a\u202eb", "c1": "\u009b31m",
			}), ValuesRef: valuesRef(60)},
			// An escape is never cut in half: the cut stops before it.
			{TaskId: "cut-at-escape", Status: succeeded, Output: inlineValues(t, map[string]any{
				"v": strings.Repeat("x", 55) + "\x1b" + strings.Repeat("y", 20),
			}), ValuesRef: valuesRef(90)},
			{
				TaskId: "approval", Status: succeeded, Output: inlineValues(t, map[string]any{"approved": true}),
				ValuesRef: valuesRef(17), Await: &orkstackv1.TaskAwait{ResolvedBy: "alice",
					ResolvedVia: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API},
			},
			// None of these has a value to show, so none has a row.
			{TaskId: "empty", Status: succeeded, Output: inlineValues(t, map[string]any{}), ValuesRef: valuesRef(2)},
			{TaskId: "failed", Status: orkstackv1.Status_STATUS_FAILED},
			{TaskId: "pending", Status: orkstackv1.Status_STATUS_PENDING},
		},
	}
	var out strings.Builder
	if err := renderRun(&out, run); err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	requireGolden(t, "run-get-values.golden", out.String())
	for _, raw := range []string{"\x1b", "\x07", "\u202e", "\u009b"} {
		if strings.Contains(out.String(), raw) {
			t.Fatalf("the output holds a raw %q:\n%s", raw, out.String())
		}
	}
}

// TestRunGet_ARunWithNoValuesPrintsNoValuesTable: nothing new for a run with
// nothing to show.
func TestRunGet_ARunWithNoValuesPrintsNoValuesTable(t *testing.T) {
	var out strings.Builder
	err := renderRun(&out, &orkstackv1.Run{Id: "r", TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "a", Status: orkstackv1.Status_STATUS_SUCCEEDED},
	}})
	if err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	if strings.Contains(out.String(), "VALUES") {
		t.Fatalf("a run with no values printed a values table:\n%s", out.String())
	}
}

// TestRunGet_ManyTasksKeepTheValuesTableShort: past the rows it shows, the table
// says how many it left out and which command prints them.
func TestRunGet_ManyTasksKeepTheValuesTableShort(t *testing.T) {
	tasks := taskRunsWithValues(t, taskValuesRowsMax+3)
	var out strings.Builder
	if err := renderRun(&out, &orkstackv1.Run{Id: "r", Namespace: "team-a", TaskRuns: tasks}); err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	if got := strings.Count(out.String(), `{"n":`); got != taskValuesRowsMax {
		t.Fatalf("printed %d values, want %d:\n%s", got, taskValuesRowsMax, out.String())
	}
	if want := "3 more tasks hold values; orkctl -n team-a run get r --json prints them"; !strings.Contains(out.String(), want) {
		t.Fatalf("output lacks %q:\n%s", want, out.String())
	}

	out.Reset()
	exactly := taskRunsWithValues(t, taskValuesRowsMax)
	if err := renderRun(&out, &orkstackv1.Run{Id: "r", TaskRuns: exactly}); err != nil {
		t.Fatalf("renderRun: %v", err)
	}
	if strings.Contains(out.String(), "more task") {
		t.Fatalf("a table with nothing left out says something was:\n%s", out.String())
	}
}

func taskRunsWithValues(t *testing.T, count int) []*orkstackv1.TaskRun {
	t.Helper()
	tasks := make([]*orkstackv1.TaskRun, count)
	for i := range tasks {
		tasks[i] = &orkstackv1.TaskRun{
			TaskId: fmt.Sprintf("t%d", i), Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Output: inlineValues(t, map[string]any{"n": float64(i)}), ValuesRef: valuesRef(7),
		}
	}
	return tasks
}

// TestCutJSONLine holds the cut to whole characters and whole escapes.
func TestCutJSONLine(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		limit      int
		want       string
	}{
		{"plain", "abcdef", 3, "abc"},
		{"the whole line fits", "abc", 3, "abc"},
		{"characters, not bytes", "ééééé", 3, "ééé"},
		{"a short escape stays whole", `ab\"cd`, 3, `ab`},
		{"a short escape that fits", `ab\"cd`, 4, `ab\"`},
		{"a \\u escape stays whole", `ab\u001bcd`, 5, `ab`},
		{"a \\u escape that fits", `ab\u001bcd`, 8, `ab\u001b`},
		{"a surrogate pair stays whole", `a\udb40\udc01b`, 7, `a`},
		{"a surrogate pair that fits", `a\udb40\udc01b`, 13, `a\udb40\udc01`},
		{"an escaped backslash is two characters", `a\\u001b`, 3, `a\\`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cutJSONLine(tc.line, tc.limit); got != tc.want {
				t.Fatalf("cutJSONLine(%q, %d) = %q, want %q", tc.line, tc.limit, got, tc.want)
			}
		})
	}
}

// storedItems is a fan-out's items as a run holds them: a reference each, and
// never the values themselves.
func storedItems(sizes ...int64) *orkstackv1.TaskRun {
	task := &orkstackv1.TaskRun{TaskId: "convert", Each: &orkstackv1.TaskEach{Expanded: true, Count: int32(len(sizes))}}
	for i, size := range sizes {
		item := &orkstackv1.TaskRun{TaskId: "convert", Item: proto.Int32(int32(i)), Status: orkstackv1.Status_STATUS_SUCCEEDED}
		if size >= 0 {
			item.ValuesRef = valuesRef(size)
		}
		task.Items = append(task.Items, item)
	}
	return task
}

// TestRunGet_ExpandReadsItemValues: an item's values are not in the run, so
// --expand reads the small ones. A read that fails says so on its row and stops
// further reads, so a store that is down does not cost a timeout per item.
func TestRunGet_ExpandReadsItemValues(t *testing.T) {
	long := `{"v":"` + strings.Repeat("x", 200) + `"}`
	stored := map[int32]string{
		0: "{\"part\": \"a\"}\n",
		1: long,
		// 2 has `{}` and a line end, 3 never finished: neither has a row or a read.
		4: "unused, over the size the table reads",
		5: "{\"esc\":\"\u009b31m\\u001b]0;x\\u0007\"}",
		6: "not json",
	}
	sizes := []int64{14, int64(len(long)), 3, -1, 8192, 30, 8, 20, 20}
	task := storedItems(sizes...)
	var reads []int32
	failed := errors.New("read artifact: unavailable: the store\x1b[2J is down")
	read := func(item *orkstackv1.TaskRun) ([]byte, error) {
		reads = append(reads, item.GetItem())
		if item.GetItem() == 7 {
			return nil, failed
		}
		return []byte(stored[item.GetItem()]), nil
	}
	var out strings.Builder
	run := &orkstackv1.Run{Id: "ork-each", Namespace: "data"}
	if err := renderFanOutItemValues(&out, run, task, read); err != nil {
		t.Fatalf("renderFanOutItemValues: %v", err)
	}
	requireGolden(t, "run-get-each-item-values.golden", out.String())
	if want := []int32{0, 1, 5, 6, 7}; !slices.Equal(reads, want) {
		t.Fatalf("read items %v, want %v", reads, want)
	}
	for _, raw := range []string{"\x1b", "\u009b"} {
		if strings.Contains(out.String(), raw) {
			t.Fatalf("the output holds a raw %q:\n%s", raw, out.String())
		}
	}
}

// TestRunGet_ExpandWithNoItemValuesPrintsNoTable: the fan-out's own table is all
// there is, and nothing is read.
func TestRunGet_ExpandWithNoItemValuesPrintsNoTable(t *testing.T) {
	read := func(*orkstackv1.TaskRun) ([]byte, error) {
		t.Fatal("read an item with no values")
		return nil, nil
	}
	var out strings.Builder
	if err := renderFanOutItemValues(&out, &orkstackv1.Run{Id: "r"}, storedItems(-1, 2, 3, 4), read); err != nil {
		t.Fatalf("renderFanOutItemValues: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("printed:\n%s", out.String())
	}
}

// TestRunGet_ExpandReadsAtMostTheRowsItShows: 10,000 items cost one table's reads,
// and the line after it names the command for the first item left out.
func TestRunGet_ExpandReadsAtMostTheRowsItShows(t *testing.T) {
	sizes := make([]int64, taskValuesRowsMax+3)
	for i := range sizes {
		sizes[i] = 8
	}
	reads := 0
	read := func(*orkstackv1.TaskRun) ([]byte, error) {
		reads++
		return []byte(`{"n":1}`), nil
	}
	var out strings.Builder
	if err := renderFanOutItemValues(&out, &orkstackv1.Run{Id: "r"}, storedItems(sizes...), read); err != nil {
		t.Fatalf("renderFanOutItemValues: %v", err)
	}
	if reads != taskValuesRowsMax {
		t.Fatalf("read %d items, want %d", reads, taskValuesRowsMax)
	}
	want := fmt.Sprintf("3 more items hold values; orkctl run artifact download r --task convert --item %d --kind values "+
		"--name values.json --output convert-%d-values.json saves one", taskValuesRowsMax, taskValuesRowsMax)
	if !strings.Contains(out.String(), want) {
		t.Fatalf("output lacks %q:\n%s", want, out.String())
	}
}

// TestRunGet_ValuesOfOnlyALineEndAreEmpty: `echo '{}'` writes three bytes and
// returned nothing, so it has no row, and it is not a value held by reference.
func TestRunGet_ValuesOfOnlyALineEndAreEmpty(t *testing.T) {
	for _, size := range []int64{2, 3, 4} {
		var out strings.Builder
		err := renderRun(&out, &orkstackv1.Run{Id: "r", TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "a", Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: valuesRef(size)},
		}})
		if err != nil {
			t.Fatalf("renderRun: %v", err)
		}
		if strings.Contains(out.String(), "VALUES") {
			t.Fatalf("%d bytes of nothing printed a values table:\n%s", size, out.String())
		}
	}
}

// TestRunGet_TreeShowsEachChildsValues: under each child's table, indented with
// it, and a command names the child that holds the value.
func TestRunGet_TreeShowsEachChildsValues(t *testing.T) {
	parent, child, grand := composedRuns("team-a")
	child.TaskRuns[0].Output = inlineValues(t, map[string]any{"signed": true})
	child.TaskRuns[0].ValuesRef = valuesRef(15)
	child.TaskRuns[1].ValuesRef = valuesRef(8192)
	grand.TaskRuns[0].Output = inlineValues(t, map[string]any{"stamp": "ok"})
	grand.TaskRuns[0].ValuesRef = valuesRef(14)
	f := newComposedCLIFixture(t, "team-a", parent, child, grand)
	got := f.run("-n", "team-a", "run", "get", composedParentID, "--tree").requireExit(t, exitOK)
	requireGolden(t, "run-get-composed-tree-values.golden", got.stdout)
}
