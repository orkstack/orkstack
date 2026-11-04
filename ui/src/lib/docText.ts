// An image's descriptions are plain text: one paragraph, where a word in
// backticks is drawn in the code face. Nothing else is interpreted.

export interface DocPart {
	text: string;
	code: boolean;
}

/** The text cut at its backticks. A backtick with no partner is a plain character. */
export function docParts(text: string): DocPart[] {
	const pieces = text.split('`');
	const parts: DocPart[] = [];
	pieces.forEach((piece, i) => {
		const closed = i % 2 === 1 && i < pieces.length - 1;
		if (i % 2 === 1 && !closed) {
			parts.push({ text: `\`${piece}`, code: false });
		} else if (piece !== '') {
			parts.push({ text: piece, code: closed });
		}
	});
	return parts;
}

/** Whether a description is past its display limit, counted in code points. */
export function docLong(text: string, limit: number): boolean {
	return [...text].length > limit;
}
