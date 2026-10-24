# Third-party code in the site

## Landpack

The landing page's structure follows [Landpack](https://github.com/lauroguedes/landpack)
(commit `ccb566704abf336ba3620a17e56a2466a4975c9a`), an Astro landing-page
template built on Lumos for Astro. These files are adapted from it, restyled
entirely in the OrkStack UI's tokens (`ui/src/tokens.css`), and say so in
their header comments:

| Site file | Adapted from |
|---|---|
| `src/layouts/Landing.astro` | `src/layouts/BaseLayout.astro`, `src/components/Utility/BaseHead.astro` |
| `src/components/shared/TopBar.astro` | `src/components/Global/Nav.astro` |
| `src/components/shared/ThemeShot.astro` | `src/components/Media/ThemeImage.astro` |
| `src/components/landing/Quickstart.astro` | `src/components/Content/ContentCommand.astro` |

Landpack's agent files, deployment files, demo content and fonts are not
included. Its license:

```text
MIT License

Copyright (c) 2026 Timothy Ricks

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
