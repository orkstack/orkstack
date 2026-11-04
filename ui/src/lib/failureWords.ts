// A failure category in words, as a reader says it ("activity failed",
// "timed out"), never as its enum spelling.
import { FailureCategory } from './gen/orkstack/v1/common_pb';

export function fmtCategoryWords(category: FailureCategory): string {
	if (category === FailureCategory.UNSPECIFIED) return 'no category';
	const name = FailureCategory[category];
	return name ? name.toLowerCase().replaceAll('_', ' ') : String(category);
}
