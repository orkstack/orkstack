#!/usr/bin/env bash
# check-alerts.sh — checks the observability kit's Prometheus rules with
# promtool, at the Prometheus the kit pins (deploy/docker-compose.yml's
# prometheus image, read from that file so there is one pin): `promtool check
# rules` on every rule file, `promtool check config` on the kit's
# prometheus.yml with them mounted, and `promtool test rules` on every test
# file under deploy/observability/prometheus/rules. promtool is Prometheus's own checker;
# nothing here parses PromQL itself. Needs Docker and the image: by hand it is
# pulled by digest if absent; a proof fetches it first and checks it is there
# (`--image` prints the reference), so a gate never pulls. Exit 0 pass, 1 a
# check failed, 77 Docker missing.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
image=$(awk '/^  prometheus:$/{p=1} p && $1=="image:"{print $2; exit}' deploy/docker-compose.yml)
[[ $image == prom/prometheus:*@sha256:* ]] || { echo "check-alerts: no pinned prometheus image in deploy/docker-compose.yml (got '$image')" >&2; exit 1; }
if [[ ${1:-} == --image ]]; then
	echo "$image"
	exit 0
fi
command -v docker >/dev/null || { echo "check-alerts: docker is not installed" >&2; exit 77; }
docker info >/dev/null 2>&1 || { echo "check-alerts: the Docker daemon does not answer" >&2; exit 77; }
dir=deploy/observability/prometheus/rules
shopt -s nullglob
rules=("$dir"/*.yml)
tests=("$dir"/*_test.yaml)
(( ${#rules[@]} )) || { echo "check-alerts: no rule files in $dir" >&2; exit 1; }
(( ${#tests[@]} )) || { echo "check-alerts: no test files in $dir" >&2; exit 1; }
run() { docker run --rm --network none -v "$PWD/$dir:/rules:ro" -w /rules --entrypoint promtool "$image" "$@"; }
run check rules "${rules[@]#"$dir"/}"
# The kit's own Prometheus config, with the rules mounted where compose puts
# them: its rule_files glob must load them.
docker run --rm --network none -v "$PWD/deploy/observability:/etc/kit:ro" -v "$PWD/$dir:/etc/prometheus/rules:ro" \
  --entrypoint promtool "$image" check config /etc/kit/prometheus.yml
# promtool evaluates every rule of a test's files at every step of every
# case, so the test files (one per rule group) run side by side.
export image dir
printf '%s\n' "${tests[@]#"$dir"/}" | xargs -P "${CHECK_ALERTS_JOBS:-4}" -I{} \
  sh -c 'o=$(docker run --rm --network none -v "$PWD/$dir:/rules:ro" -w /rules --entrypoint promtool "$image" test rules "$1" 2>&1) || { printf "%s\n%s\n" "$1" "$o"; exit 1; }' _ {} \
  || { echo "check-alerts: a promtool test failed (above)" >&2; exit 1; }
echo "check-alerts: ${#rules[@]} rule files and ${#tests[@]} test files pass promtool ($image)"
