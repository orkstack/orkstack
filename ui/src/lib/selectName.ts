// How a Select names itself to assistive technology.
//
// A combobox's trigger shows its VALUE, so without a name a screen reader
// announces "default, combobox" and never says what is being chosen. There
// are two ways to name it: a sibling <label for=id> (the caller passes `id`),
// or a `label` the control carries itself as aria-label, for the places whose
// visible caption is not a <label> (the sidebar's namespace picker, a filter
// row's "Value" span, the purge dialog's radio-nested select). A <label>
// wins when both are given: it is the visible text, and an aria-label would
// silently override it.
export interface SelectAria {
	trigger: { 'aria-label'?: string };
	listbox: { 'aria-labelledby'?: string; 'aria-label'?: string };
}

export function selectAria(id: string | undefined, label: string | undefined): SelectAria {
	if (id) return { trigger: {}, listbox: { 'aria-labelledby': id } };
	if (label) return { trigger: { 'aria-label': label }, listbox: { 'aria-label': label } };
	return { trigger: {}, listbox: {} };
}
