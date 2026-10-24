# The quickstart's orkctl commands, run for real against the local stack by
# make site-media (site/media/run.sh), which wraps this tape once per theme
# with the output paths and a terminal theme made from ui/src/tokens.css.
# The setup is hidden: a neutral prompt in /work, where the container sees
# only examples/ and a static orkctl. The commands are the README's and
# docs/GETTING_STARTED.md's, in the namespace they use (`default`), and the
# apply is a first apply: run.sh refuses to record when each-fan-out is
# already stored there.

Set Shell "bash"
Set FontSize 15
Set Width 1200
# Tall enough that the finished watch's last lines sit above a paused
# video's controls band.
Set Height 860
Set Padding 24
Set TypingSpeed 30ms
Set CursorBlink false

Hide
Type "export PS1='$ ' && cd /work && clear"
Enter
Sleep 500ms
Show

Type "./bin/orkctl apply examples/each-fan-out.ork.yaml"
Sleep 500ms
Enter
Sleep 2.5s

Type "./bin/orkctl run plan each-fan-out"
Sleep 500ms
Enter
Sleep 3s

# The run takes about ten seconds on the local stack. (Wait+Screen stalled
# the recording in VHS 0.12.1, so this is a fixed pause; the hidden step
# below waits for the run to end before its logs are read.)
Type "./bin/orkctl run submit each-fan-out --watch"
Sleep 500ms
Enter
Sleep 18s

Hide
Type "RUN=$(./bin/orkctl run list --limit 1 --json | grep -o 'ork-[0-9]*-[0-9]*' | head -1) && ./bin/orkctl run watch $RUN >/dev/null 2>&1; clear"
Enter
Sleep 2s
Show

Type "./bin/orkctl run logs $RUN --task merge"
Sleep 500ms
Enter
Sleep 4s
