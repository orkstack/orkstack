import { expect, type Locator, type Page } from '@playwright/test';

// The authored-document fields (ui/src/lib/yaml/YamlField.svelte), driven
// from one place. A field is a textarea until the YAML editor's code arrives,
// then CodeMirror, whose editable content takes the field's test id, so a
// spec waits for the editor before it types: filling the textarea a moment
// before the editor replaces it would fill an element that is going away.
//
// The editor draws only the lines in view, so its text is not read from the
// DOM: the field keeps the whole document in a hidden input,
// `{testid}-value`, which is what these helpers and the specs assert on.
//
// Playwright collects `*.spec.ts`, so this file is imported and never run.

/** Waits until the field's editor has loaded and holds the field's test id. */
export async function editorReady(page: Page, testid: string): Promise<void> {
	await expect(page.getByTestId(`${testid}-editor`)).toBeVisible();
	await expect(page.getByTestId(testid)).toHaveAttribute('contenteditable', 'true');
}

/** Replaces the field's document with `text`, the way a paste does, and waits until the field holds exactly it. */
export async function fillYaml(page: Page, testid: string, text: string): Promise<void> {
	await editorReady(page, testid);
	await page.getByTestId(testid).fill(text);
	await expect(yamlValue(page, testid)).toHaveValue(text);
}

/** The field's whole document, for toHaveValue. */
export function yamlValue(page: Page, testid: string): Locator {
	return page.getByTestId(`${testid}-value`);
}
