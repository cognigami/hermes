#!/bin/sh
# OPERATOR ONLY: explicit sudo invocation after inspector/S4 gates pass.
set -eu
assets=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test "$#" = 1 || { echo 'usage: sudo sh run.sh EXISTING_NONROOT_ACCOUNT' >&2; exit 1; }
root=$("$assets/trial-helper" create "$assets" "$1")
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if ! "$assets/trial-helper" cleanup "$root"; then
        echo "Trial cleanup incomplete; inspect retained installation/state at $root" >&2
        status=1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
# Keep the exact owned helper child so signals cancel/reap it before cleanup.
"$assets/trial-helper" exercise "$root" &
child=$!
interrupt() {
    trap - HUP INT TERM
    kill -TERM "$child" 2>/dev/null || :
    wait "$child" 2>/dev/null || :
    exit 1
}
trap interrupt HUP INT TERM
wait "$child"
trap 'exit 1' HUP INT TERM
printf 'Trial passed within leaf-only scope. Retained state: %s\n' "$root"
