# orkctl activity test, run for real against the first-party shell image in
# the local registry by make site-media (site/media/run.sh), which wraps this
# tape once per theme with the output paths and a terminal theme made from
# ui/src/tokens.css. The container runs the host's own docker CLI against
# the host's daemon, as an Orker does, with its workspaces under a directory
# the host and the container both see at one path, named in host/tmpdir
# (run.sh) and given to orkctl as ORK_ORKER_WORKDIR, the root it takes first.
#
# The six checks take about fifty seconds, most of it the SIGTERM check's
# grace period. The recording does not show that wait: it is cut, and the
# caption says so (media/finish.mjs).

Set Shell "bash"
# Twice the size the site shows it at; the report's widest line is 165
# columns, and this fits it.
Set FontSize 21
Set Width 2400
Set Height 700
Set Padding 48
Set TypingSpeed 30ms
Set CursorBlink false

Hide
Type "export PS1='$ ' PATH=/work/bin:$PATH ORK_ORKER_WORKDIR=$(cat /work/host/tmpdir) && cd /work && clear"
Enter
Sleep 500ms
Show

Type "orkctl activity test localhost:5001/orkstack/activity-shell:dev"
Sleep 500ms
Enter
Sleep 1.5s
Hide
Sleep 80s
Show
Sleep 6s
# The poster: the finished report, as the terminal drew it, lossless.
Screenshot out/poster.png
Sleep 1s
