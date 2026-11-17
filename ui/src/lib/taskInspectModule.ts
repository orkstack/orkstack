// The run page's Inspect body, a lazily loaded component, loaded once.
//
// A fresh `import()` on every open resolves a task later even when the module
// is already loaded, so the body of a warm open was drawn only after whatever
// the page had queued meanwhile: on a live run of thousands of tasks, the
// event applies of its starts, each tens of milliseconds, one per frame. One
// promise, settled after the first load, is answered in a microtask, so a
// warm open draws its body in the press's own task. A load that failed is
// forgotten, so the next open tries again.
type TaskInspectModule = typeof import('./components/TaskInspect.svelte');

let loading: Promise<TaskInspectModule> | undefined;

export function loadTaskInspect(
	load: () => Promise<TaskInspectModule> = () => import('./components/TaskInspect.svelte')
): Promise<TaskInspectModule> {
	if (!loading) {
		const attempt = load();
		loading = attempt;
		attempt.catch(() => {
			if (loading === attempt) loading = undefined;
		});
	}
	return loading;
}
