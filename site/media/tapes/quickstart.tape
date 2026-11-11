# The quickstart's orkctl commands, run for real against the local stack by
# make site-media (site/media/run.sh), which wraps this tape once per theme
# with the output paths and a terminal theme made from ui/src/tokens.css.
# The setup is hidden: a neutral prompt in /work, where the container sees
# only examples/ and a static orkctl on its PATH. The commands are the
# README's and docs/GETTING_STARTED.md's, in the namespace they use
# (`default`). run.sh refuses to record when each-fan-out is already stored there
# and the images may have changed since.

Set Shell "bash"
# Twice the size the site shows it at (sync-docs.mjs), so a screen with two
# device pixels per CSS pixel shows the text one to one.
Set FontSize 30
Set Width 2400
# Tall enough that the finished watch's last lines sit above a paused
# video's controls band.
Set Height 1720
Set Padding 48
# A frame this size takes VHS longer to capture than its default 50 a second
# allows, and a frame it misses shortens the recording: at 10 a second it plays
# in real time.
Set Framerate 10
Set TypingSpeed 30ms
Set CursorBlink false

Hide
Type "export PS1='$ ' PATH=/work/bin:$PATH && cd /work && clear"
Enter
Sleep 500ms
Show

Type "orkctl orkflow create examples/each-fan-out.ork.yaml"
Sleep 500ms
Enter
Sleep 2.5s

Type "orkctl run plan each-fan-out"
Sleep 500ms
Enter
Sleep 3s

# The run takes about ten seconds on the local stack. (Wait+Screen stalled
# the recording in VHS 0.12.1, so this is a fixed pause; the hidden step
# below waits for the run to end before its logs are read.)
Type "orkctl run submit each-fan-out -w"
Sleep 500ms
Enter
Sleep 18s
# The poster: the watch finished, as the terminal drew it, lossless.
# media/run.sh renames it for the theme. A screenshot is taken at the next
# frame, so the tape stays visible until there has been one, and media/finish.mjs
# refuses a poster with nothing on it.
Screenshot out/poster.png
Sleep 2s

Hide
Type "RUN=$(orkctl run list --limit 1 -j | grep -o 'ork-[0-9]*-[0-9]*' | head -1) && orkctl run watch $RUN >/dev/null 2>&1; clear"
Enter
Sleep 2s
Show

Type "orkctl run logs $RUN -t merge"
Sleep 500ms
Enter
Sleep 4s
