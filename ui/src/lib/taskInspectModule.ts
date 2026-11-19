// The run page's Inspect body, a lazily loaded component, loaded once.
//
// A fresh `import()` on every open resolves a task later even when the module
// is already loaded, so the body of a warm open was drawn only after whatever
// the page had queued meanwhile: on a live run of thousands of tasks, the
// event applies of its starts, each tens of milliseconds, one per frame. One
// promise, settled after the first load, is answered in a microtask, so a
// warm open draws its body in the press's own task. A load that failed is
// forgotten, so the next open tries again. The run page starts the load when
// it mounts (preloadTaskInspect), so a first open finds it settled too.
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

// preloadTaskInspect starts the Inspect body's load, for the run page to call
// when it mounts. A first press after a page load would otherwise start it,
// and the import answers a task later: on a live run of thousands of tasks,
// behind event applies of a few hundred milliseconds each on a slow runner,
// so the body of that first open was drawn a second after its press. Started
// at mount, the load has normally settled by the time a card's Inspect can
// be pressed (its cards take longer to draw than the chunk to load), and the
// press is answered in a microtask; a press before then still waits for it. It never rejects: a failed load
// is forgotten (loadTaskInspect), so the press loads again and shows its own
// failure.
export function preloadTaskInspect(load?: () => Promise<TaskInspectModule>): Promise<void> {
	return loadTaskInspect(load).then(
		() => undefined,
		() => undefined
	);
}
