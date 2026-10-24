// Moves the terminal recordings into site/public/media with their poster
// frames, and records them in the media manifest beside the screenshots.
// Called by media/run.sh with the VHS output directory and image.
import { readFileSync, writeFileSync, copyFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import sharp from 'sharp';
import { siteRoot } from '../scripts/tokens.mjs';

const [out, vhsImage] = process.argv.slice(2);
const pub = path.join(siteRoot, 'public', 'media');
const manifestPath = path.join(siteRoot, 'src', 'assets', 'media', 'manifest.json');
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
const words = {
	'cli-quickstart': {
		alt: 'A terminal recording: orkctl applies each-fan-out, plans it, submits it and watches it to SUCCEEDED, then prints the merge task’s log.',
		caption: 'orkctl apply, run plan, run submit --watch and run logs, recorded against the local stack: the run finishes with status SUCCEEDED, and the merge task’s log lists the four parts, a to d.',
	},
};
for (const file of readdirSync(out).sort()) {
	const m = /^(cli-[a-z-]+?)-(light|dark)\.(webm|mp4|png)$/.exec(file);
	if (!m) continue;
	const [, name, theme, ext] = m;
	if (!words[name]) throw new Error(`media/finish.mjs has no alt text and caption for ${name}`);
	const item = (manifest.items[name] ??= { ...words[name] });
	Object.assign(item, words[name]);
	const suffix = theme === 'dark' ? 'Dark' : '';
	if (ext === 'png') {
		const poster = `${name}-${theme}.webp`;
		await sharp(path.join(out, file)).webp({ quality: 82 }).toFile(path.join(pub, poster));
		item[`poster${suffix}`] = poster;
	} else {
		copyFileSync(path.join(out, file), path.join(pub, file));
		item[`${ext}${suffix}`] = file;
	}
}
manifest.vhsImage = vhsImage;
writeFileSync(manifestPath, JSON.stringify(manifest, null, '\t') + '\n');
