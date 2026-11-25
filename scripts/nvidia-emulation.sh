#!/usr/bin/env bash
# nvidia-emulation.sh: makes the prerequisites of TestIntegration_NVIDIAEmulation
# ready, the same way for a local proof, a CI part and a by-hand run
# (scripts/integration.sh): the images its dind runs (cdi-dind-images.sh), which
# the test never pulls, and every nvidia-cdi-hook build the compiled table lists
# (nvidia-hooks-cache.sh), each hashed against the table. It prints the directory
# to export as ORK_TEST_NVIDIA_HOOKS on stdout; everything else goes to stderr,
# the cause of a failure last.
#
# Exit status: 0 ready; 77 a registry, the container engine or NVIDIA's repository
# could not be reached, a missing prerequisite; 1 an image or a build is not the
# pinned one, which fails the gate.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
scripts/cdi-dind-images.sh --pull >&2 || exit $?
exec scripts/nvidia-hooks-cache.sh
