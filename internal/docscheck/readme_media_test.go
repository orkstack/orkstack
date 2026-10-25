package docscheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The README shows its diagrams and recordings as <picture> elements, so
// GitHub serves the light or the dark file by the reader's colour scheme.

var (
	pictureRE   = regexp.MustCompile(`(?s)<picture>(.*?)</picture>`)
	darkSrcRE   = regexp.MustCompile(`<source media="\(prefers-color-scheme: dark\)" srcset="([^"]+)">`)
	lightImgRE  = regexp.MustCompile(`<img alt="([^"]+)" src="([^"]+)"`)
	readmeMedia = []string{".svg", ".gif", ".webp"}
)

// readmePictures returns each <picture>'s light and dark files and its alt text.
func readmePictures(t *testing.T, readme string) [][3]string {
	t.Helper()
	var out [][3]string
	for _, m := range pictureRE.FindAllStringSubmatch(readme, -1) {
		dark := darkSrcRE.FindStringSubmatch(m[1])
		light := lightImgRE.FindStringSubmatch(m[1])
		if dark == nil || light == nil {
			t.Errorf("a <picture> lacks a dark <source> or a light <img alt src>:\n%s", m[0])
			continue
		}
		out = append(out, [3]string{light[2], dark[1], light[1]})
	}
	return out
}

// TestReadme_EveryPictureHasADarkSourceAndItsFilesExist: every picture in the
// README names a light file and its -dark twin, both in the repository.
func TestReadme_EveryPictureHasADarkSourceAndItsFilesExist(t *testing.T) {
	root := repoRoot(t)
	pics := readmePictures(t, readFile(t, filepath.Join(root, "README.md")))
	if len(pics) == 0 {
		t.Fatal("README.md has no <picture>")
	}
	for _, p := range pics {
		light, dark, alt := p[0], p[1], p[2]
		if want := strings.Replace(light, "-light.", "-dark.", 1); dark != want || dark == light {
			t.Errorf("the dark source of %s is %s, want %s", light, dark, want)
		}
		if len(alt) < 20 {
			t.Errorf("%s has alt text %q: describe what it shows", light, alt)
		}
		for _, f := range []string{light, dark} {
			if _, err := os.Stat(filepath.Join(root, f)); err != nil {
				t.Errorf("README.md shows %s, which does not exist", f)
			}
		}
	}
}

// TestReadme_MediaFitTheBudget: what the README makes every reader download
// stays small. A diagram is at most 64 KiB, an animation 3 MiB, all of them
// together 10 MiB.
func TestReadme_MediaFitTheBudget(t *testing.T) {
	root := repoRoot(t)
	const (
		svgMax       = 64 << 10
		animationMax = 3 << 20
		totalMax     = 10 << 20
	)
	var total int64
	for _, p := range readmePictures(t, readFile(t, filepath.Join(root, "README.md"))) {
		for _, f := range []string{p[0], p[1]} {
			info, err := os.Stat(filepath.Join(root, f))
			if err != nil {
				continue // the picture test names it
			}
			size := info.Size()
			total += size
			limit := int64(animationMax)
			if strings.HasSuffix(f, ".svg") {
				limit = svgMax
			}
			if size > limit {
				t.Errorf("%s is %d bytes, over its %d", f, size, limit)
			}
			ok := false
			for _, ext := range readmeMedia {
				ok = ok || strings.HasSuffix(f, ext)
			}
			if !ok {
				t.Errorf("%s is not one of %v", f, readmeMedia)
			}
		}
	}
	if total > totalMax {
		t.Errorf("the README's media are %d bytes in all, over %d", total, totalMax)
	}
}
