#!/usr/bin/env bash
# gpu-host-setup.sh: the one-time setup of a GPU proof host (docs/PROOFS.md,
# "The GPU-host tier"). It never runs sudo: it prints the root steps this host
# still needs, for a person to read and run.
#
#   scripts/gpu-host-setup.sh              print the root steps still needed
#   scripts/gpu-host-setup.sh --print      print this tree's test CDI spec
#   scripts/gpu-host-setup.sh --print-dir  write it to a new 0700 directory,
#                                          print the directory, and its sha256
#                                          on stderr
#   scripts/gpu-host-setup.sh --name       print the spec's file name
#   scripts/gpu-host-setup.sh --script     print one shell script of the root
#                                          steps still needed, for a person to
#                                          read and then run once:
#                                          sudo sh <file>
#
# The spec is this tree's template over this host's GPUs: unit `amd` is the
# amdgpu render node and /dev/kfd, unit `intel` the i915 render node, each with
# the render group (additionalGids, so cdiVersion 0.7.0). A host node is its
# own /dev/dri/renderD<N>, never a /dev/dri/by-path link: the CDI library
# Lstat()s hostPath and refuses a symlink at the container's start ("not a
# device node"). A unit's container path is fixed (renderD128 for amd,
# renderD129 for intel). A boot that numbers the render nodes another way
# changes the spec, so its name too: the tests then fail naming this script,
# and it is run again. Its file name and CDI kind carry
# the first 8 hex digits of the template's SHA-256, so specs of different trees
# coexist in /etc/cdi.
#
# What the spike measured on Docker 29.9.2 (design Q1, Q3, Q14, Q18): a spec
# written to an /etc/cdi that existed when dockerd started is read with no
# reload; only an /etc/cdi this setup creates needs one reload. dockerd is
# never restarted. An existing tmpfiles entry for the lock directory is never
# removed or rewritten.
set -euo pipefail

LOCK_GROUP=orkstack-gpu
LOCK_DIR=/run/orkstack/gpu-orker
TMPFILES=/etc/tmpfiles.d/orkstack-gpu.conf
CDI_DIR=/etc/cdi

die() { echo "gpu-host-setup: $*" >&2; exit 77; }

# render_node DRIVER: this host's render node bound to DRIVER.
render_node() {
	local d
	for d in /sys/class/drm/renderD*; do
		[ -e "$d/device/uevent" ] || continue
		if grep -qx "DRIVER=$1" "$d/device/uevent"; then
			basename "$d"
			return 0
		fi
	done
	return 1
}

read -r AMD_NODE < <(render_node amdgpu) || die "no amdgpu render node on this host"
read -r INTEL_NODE < <(render_node i915) || die "no i915 render node on this host"
[ -c /dev/kfd ] || die "no /dev/kfd on this host"
RENDER_GID=$(stat -c %g "/dev/dri/$AMD_NODE")
[ "$(stat -c %g "/dev/dri/$INTEL_NODE")" = "$RENDER_GID" ] || die "the two render nodes have different groups"

# template KIND: the spec, block style, with the kind KIND.
template() {
	cat <<EOF
cdiVersion: 0.7.0
kind: $1
devices:
  - name: amd
    containerEdits:
      deviceNodes:
        - path: /dev/dri/renderD128
          hostPath: /dev/dri/$AMD_NODE
          type: c
          permissions: rw
        - path: /dev/kfd
          hostPath: /dev/kfd
          type: c
          permissions: rw
      additionalGids:
        - $RENDER_GID
  - name: intel
    containerEdits:
      deviceNodes:
        - path: /dev/dri/renderD129
          hostPath: /dev/dri/$INTEL_NODE
          type: c
          permissions: rw
      additionalGids:
        - $RENDER_GID
EOF
}

H8=$(template test.orkstack.dev/gpu | sha256sum | cut -c1-8)
NAME=orkstack-test-gpu-$H8.yaml
spec() { template "test.orkstack.dev/gpu-$H8"; }

case "${1:-}" in
--print)
	spec
	exit 0
	;;
--name)
	echo "$NAME"
	exit 0
	;;
--script) ;;
--print-dir)
	dir=$(mktemp -d)
	chmod 0700 "$dir"
	spec >"$dir/$NAME"
	echo "$dir"
	sha256sum "$dir/$NAME" >&2
	exit 0
	;;
"") ;;
*)
	echo "usage: $0 [--print | --print-dir | --name | --script]" >&2
	exit 2
	;;
esac

# The root steps this host still needs, and nothing it already has: printed
# one per line by default, or with --script as one shell script whose every
# step checks again before it acts.
script=()
steps=()
created_cdi_dir=0
if [ ! -d "$CDI_DIR" ]; then
	steps+=("sudo install -d -o root -g root -m 0755 $CDI_DIR")
	script+=("[ -d $CDI_DIR ] || { install -d -o root -g root -m 0755 $CDI_DIR; reload_docker=1; }")
	created_cdi_dir=1
fi
if [ ! -f "$CDI_DIR/$NAME" ] || ! cmp -s <(spec) "$CDI_DIR/$NAME"; then
	steps+=("dir=\$($0 --print-dir)   # prints the spec's sha256: compare it with the next line's")
	steps+=("sha256sum \"\$dir/$NAME\"")
	steps+=("sudo install -o root -g root -m 0644 \"\$dir/$NAME\" $CDI_DIR/")
	script+=("spec=\$(mktemp)
cat >\"\$spec\" <<'ORKSTACK_SPEC'
$(spec)
ORKSTACK_SPEC
install -o root -g root -m 0644 \"\$spec\" $CDI_DIR/$NAME
rm -f \"\$spec\"")
fi
if ! getent group "$LOCK_GROUP" >/dev/null; then
	steps+=("sudo groupadd --system $LOCK_GROUP")
	script+=("getent group $LOCK_GROUP >/dev/null || groupadd --system $LOCK_GROUP")
fi
if ! id -nG "$USER" | tr ' ' '\n' | grep -qx "$LOCK_GROUP"; then
	steps+=("sudo usermod -aG $LOCK_GROUP $USER   # takes effect at your next login, or now under newgrp $LOCK_GROUP")
	script+=("usermod -aG $LOCK_GROUP $USER")
fi
if [ ! -e "$TMPFILES" ]; then
	steps+=("echo 'd $LOCK_DIR 0750 root $LOCK_GROUP -' | sudo tee $TMPFILES")
	script+=("[ -e $TMPFILES ] || echo 'd $LOCK_DIR 0750 root $LOCK_GROUP -' >$TMPFILES")
fi
if [ ! -d "$LOCK_DIR" ]; then
	steps+=("sudo systemd-tmpfiles --create $TMPFILES")
	script+=("systemd-tmpfiles --create $TMPFILES")
fi
if [ "$created_cdi_dir" = 1 ]; then
	steps+=("sudo systemctl reload docker   # once, because $CDI_DIR is new; never a restart")
	script+=("[ \"\$reload_docker\" = 1 ] && systemctl reload docker   # once, because $CDI_DIR is new; never a restart")
fi

if [ "${#steps[@]}" = 0 ]; then
	echo "gpu-host-setup: this host is set up: $CDI_DIR/$NAME, group $LOCK_GROUP, $LOCK_DIR" >&2
	[ "${1:-}" = --script ] && echo "# this host is set up: nothing to run"
	exit 0
fi
if [ "${1:-}" = --script ]; then
	echo "#!/bin/sh"
	echo "# gpu-host-setup.sh --script for $USER on $(hostname): read it, then run it once with sudo sh <file>."
	echo "# It writes $CDI_DIR/$NAME, the group $LOCK_GROUP and $LOCK_DIR; it never restarts dockerd."
	echo "set -eu"
	echo '[ "$(id -u)" = 0 ] || { echo "run it as root: sudo sh $0" >&2; exit 1; }'
	echo "reload_docker=0"
	printf '%s\n' "${script[@]}"
	echo "echo 'gpu-host-setup: done; log in again, or use newgrp $LOCK_GROUP, for the group to take effect'"
	exit 0
fi
echo "# Run these as yourself; each sudo line needs root. Nothing has been run."
echo "# Or as one script: $0 --script >/tmp/gpu-setup.sh, read it, then sudo sh /tmp/gpu-setup.sh"
printf '%s\n' "${steps[@]}"
