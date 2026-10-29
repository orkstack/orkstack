// Moves the terminal recordings into site/public/media with their poster
// frames, and records them in the media manifest beside the screenshots.
// Called by media/run.sh with the VHS output directory and image.
import { readFileSync, writeFileSync, copyFileSync, readdirSync, rmSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import sharp from 'sharp';
import { repoRoot, siteRoot } from '../scripts/tokens.mjs';

const [out, vhsImage] = process.argv.slice(2);
const pub = path.join(siteRoot, 'public', 'media');
// The README shows the quickstart recording as a GIF, which GitHub plays
// inline, 1760 pixels wide: it draws it 880 CSS pixels wide, so that is two
// device pixels per CSS pixel. The site keeps the videos.
const readmeImages = path.join(repoRoot, 'docs', 'images');
const manifestPath = path.join(siteRoot, 'src', 'assets', 'media', 'manifest.json');
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
const words = {
	'cli-quickstart': {
		alt: 'A terminal recording: orkctl applies each-fan-out, plans it, submits it and watches it to SUCCEEDED, then prints the merge task’s log.',
		caption: 'orkctl apply, run plan, run submit --watch and run logs, recorded against the local stack: the run finishes with status SUCCEEDED, and the merge task’s log lists the four parts, a to d.',
	},
	'cli-activity-test': {
		alt: 'A terminal recording: orkctl activity test runs the first-party shell image through the six contract checks, and all six pass.',
		caption: 'orkctl activity test against the shell image in the local registry: six checks, all PASS. The checks take about fifty seconds; the recording cuts the wait.',
	},
	'cli-schedules-triggers': {
		alt: 'A terminal recording: orkctl creates a nightly schedule for each-fan-out and fires it once, creates a trigger, curl posts to the trigger’s URL with its secret, and orkctl run list shows the two runs, one started by the schedule and one by the trigger.',
		caption: 'A schedule fired by hand and a trigger called with curl: run list shows each run’s origin.',
	},
};
for (const file of readdirSync(out).sort()) {
	const m = /^(cli-[a-z-]+?)-(light|dark)\.(webm|mp4|png|gif)$/.exec(file);
	if (!m) continue;
	const [, name, theme, ext] = m;
	if (!words[name]) throw new Error(`media/finish.mjs has no alt text and caption for ${name}`);
	const item = (manifest.items[name] ??= { ...words[name] });
	Object.assign(item, words[name]);
	// The recordings are MP4 only now: a WebM an earlier run made goes.
	delete item[`webm${theme === 'dark' ? 'Dark' : ''}`];
	rmSync(path.join(pub, `${name}-${theme}.webm`), { force: true });
	const suffix = theme === 'dark' ? 'Dark' : '';
	if (ext === 'gif') {
		execFileSync('ffmpeg', [
			'-y', '-loglevel', 'error', '-i', path.join(out, file),
			'-vf', 'scale=1760:-1:flags=lanczos,split[a][b];[a]palettegen=stats_mode=full[p];[b][p]paletteuse=dither=none',
			path.join(readmeImages, file),
		]);
		continue;
	}
	if (ext === 'png') {
		const poster = `${name}-${theme}.webp`;
		await sharp(path.join(out, file)).webp({ lossless: true, effort: 6 }).toFile(path.join(pub, poster));
		item[`poster${suffix}`] = poster;
	} else {
		copyFileSync(path.join(out, file), path.join(pub, file));
		item[`${ext}${suffix}`] = file;
		// Its size in CSS pixels: the tape draws two device pixels per CSS pixel.
		const [w, h] = execFileSync('ffprobe', ['-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height', '-of', 'csv=p=0', path.join(out, file)], { encoding: 'utf8' })
			.trim()
			.split(',')
			.map(Number);
		item.width = w / 2;
		item.height = h / 2;
	}
}
manifest.vhsImage = vhsImage;
writeFileSync(manifestPath, JSON.stringify(manifest, null, '\t') + '\n');
