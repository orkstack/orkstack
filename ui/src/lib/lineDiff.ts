// A unified diff of two texts, line by line (longest common subsequence),
// for DiffView: small documents only, such as two access revisions.
export function unifiedDiff(a: string, b: string, from = 'before', to = 'after'): string {
	const x = a.split('\n');
	const y = b.split('\n');
	const n = x.length;
	const m = y.length;
	const lcs: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
	for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) lcs[i][j] = x[i] === y[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
	const out = [`--- ${from}`, `+++ ${to}`, `@@ -1,${n} +1,${m} @@`];
	let i = 0;
	let j = 0;
	while (i < n && j < m) {
		if (x[i] === y[j]) {
			out.push(` ${x[i]}`);
			i++;
			j++;
		} else if (lcs[i + 1][j] >= lcs[i][j + 1]) out.push(`-${x[i++]}`);
		else out.push(`+${y[j++]}`);
	}
	while (i < n) out.push(`-${x[i++]}`);
	while (j < m) out.push(`+${y[j++]}`);
	return out.join('\n');
}
