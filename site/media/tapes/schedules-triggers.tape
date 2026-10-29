# A schedule and a trigger starting runs of each-fan-out, run for real
# against the local stack by make site-media (site/media/run.sh), which wraps
# this tape once per theme with the output paths and a terminal theme made
# from ui/src/tokens.css. curl is the host's own (run.sh).
#
# The setup is hidden: a neutral prompt in /work, each-fan-out applied (a
# no-op when the quickstart tape stored it already), and the schedule and
# trigger of an earlier take deleted. orkctl runs through a shell function
# that also writes its output to a log and, after a trigger create, reads
# the trigger's URL and secret from what it printed into URL and SECRET: a
# secret is shown once, and the curl that follows uses the one shown.

Set Shell "bash"
# Twice the size the site shows it at; run list's rows are about 160
# columns, and this fits them.
Set FontSize 22
Set Width 2400
Set Height 1500
Set Padding 48
Set TypingSpeed 30ms
Set CursorBlink false

Hide
Type "export PS1='$ ' PATH=/work/bin:$PATH && cd /work"
Enter
Type "orkctl apply examples/each-fan-out.ork.yaml >/dev/null; orkctl schedule delete nightly >/dev/null 2>&1; for t in $(orkctl trigger list --json | grep -o 'trg-[0-9a-f]*' | sort -u); do orkctl trigger delete $t >/dev/null; done"
Enter
Type `orkctl() { command orkctl "$@" | tee /tmp/term.log; if [ "$1 $2" = "trigger create" ]; then URL=$(grep -o "http://[^']*/v1/triggers/trg-[0-9a-f]*" /tmp/term.log | tail -1); SECRET=$(grep -o 'Bearer [0-9a-f]*' /tmp/term.log | tail -1 | cut -d' ' -f2); fi; }; clear`
Enter
Sleep 3s
Show

Type "orkctl schedule create nightly --orkflow each-fan-out --cron '0 2 * * *' --description 'nightly batch'"
Sleep 500ms
Enter
Sleep 3.5s

Type "orkctl schedule fire nightly"
Sleep 500ms
Enter
Sleep 3s

Type "clear"
Enter
Type "orkctl trigger create --orkflow each-fan-out --name ci-kickoff --description 'CI kickoff'"
Sleep 500ms
Enter
Sleep 4s

Type `curl -sS -X POST "$URL" -H "Authorization: Bearer $SECRET" -d '{}'; echo`
Sleep 500ms
Enter
Sleep 3s

Type "orkctl run list --limit 2"
Sleep 500ms
Enter
Sleep 4s
# The poster: both runs listed with their origins, lossless.
Screenshot out/poster.png
Sleep 1s
