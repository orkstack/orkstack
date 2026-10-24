# The quickstart's orkctl commands, run for real against the local stack by
# make site-media (site/media/run.sh), which wraps this tape once per theme
# with the output paths and a terminal theme made from ui/src/tokens.css.
# The setup is hidden: a neutral prompt in /work, where the container sees
# only examples/ and a static orkctl. The runs land in the `examples`
# namespace (ORK_NAMESPACE), the one the docs' examples use.

Set Shell "bash"
Set FontSize 15
Set Width 1200
Set Height 720
Set Padding 24
Set TypingSpeed 30ms
Set CursorBlink false

Hide
Type "export PS1='$ ' ORK_NAMESPACE=examples && cd /work && clear"
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

Type "./bin/orkctl run submit each-fan-out --watch"
Sleep 500ms
Enter
Wait+Screen@180s /SUCCEEDED/
Sleep 2s

Hide
Type "RUN=$(./bin/orkctl run list --limit 1 --json | grep -o 'ork-[0-9]*-[0-9]*' | head -1) && clear"
Enter
Sleep 1s
Show

Type "./bin/orkctl run logs $RUN --task merge"
Sleep 500ms
Enter
Sleep 4s
